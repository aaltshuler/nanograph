use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::catalog::schema_ir::{PropDef, SchemaIR};
use crate::embedding::EmbeddingClient;
use crate::error::{NanoError, Result};
use crate::store::manifest::hash_string;
use crate::types::ScalarType;

const EMBEDDING_CACHE_FILENAME: &str = "_embedding_cache.jsonl";
const DEFAULT_EMBED_BATCH_SIZE: usize = 64;
const DEFAULT_EMBED_CHUNK_CHARS: usize = 0;
const DEFAULT_EMBED_CHUNK_OVERLAP_CHARS: usize = 128;
const DEFAULT_EMBED_CACHE_MAX_ENTRIES: usize = 50_000;
const DEFAULT_EMBED_CACHE_LOCK_STALE_SECS: usize = 60;
const EMBEDDING_CACHE_LOCK_RETRIES: usize = 200;
const EMBEDDING_CACHE_LOCK_RETRY_DELAY_MS: u64 = 10;

#[derive(Debug, Clone)]
struct EmbedSpec {
    target_prop: String,
    source_prop: String,
    dim: usize,
}

#[derive(Debug, Clone)]
struct PendingAssignment {
    line_index: usize,
    target_prop: String,
    source_text: String,
    dim: usize,
    content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    model: String,
    dim: usize,
    content_hash: String,
    chunk_chars: usize,
    chunk_overlap_chars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheRecord {
    model: String,
    dim: usize,
    content_hash: String,
    vector: Vec<f32>,
    #[serde(default)]
    chunk_chars: usize,
    #[serde(default)]
    chunk_overlap_chars: usize,
}

enum ParsedLine {
    Raw(String),
    Json(serde_json::Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmbedChunkingConfig {
    chunk_chars: usize,
    chunk_overlap_chars: usize,
}

impl EmbedChunkingConfig {
    fn from_env() -> Self {
        let chunk_chars = parse_env_usize("NANOGRAPH_EMBED_CHUNK_CHARS", DEFAULT_EMBED_CHUNK_CHARS);
        let overlap = parse_env_usize(
            "NANOGRAPH_EMBED_CHUNK_OVERLAP_CHARS",
            DEFAULT_EMBED_CHUNK_OVERLAP_CHARS,
        );
        Self::new(chunk_chars, overlap)
    }

    fn new(chunk_chars: usize, chunk_overlap_chars: usize) -> Self {
        let chunk_overlap_chars = if chunk_chars == 0 {
            0
        } else {
            chunk_overlap_chars.min(chunk_chars.saturating_sub(1))
        };
        Self {
            chunk_chars,
            chunk_overlap_chars,
        }
    }

    fn is_enabled(self) -> bool {
        self.chunk_chars > 0
    }
}

pub(crate) async fn materialize_embeddings_for_load(
    db_path: &Path,
    schema_ir: &SchemaIR,
    data_source: &str,
) -> Result<String> {
    materialize_embeddings_for_load_inner(db_path, schema_ir, data_source, None).await
}

async fn materialize_embeddings_for_load_inner(
    db_path: &Path,
    schema_ir: &SchemaIR,
    data_source: &str,
    client_override: Option<&EmbeddingClient>,
) -> Result<String> {
    materialize_embeddings_for_load_inner_with_chunking(
        db_path,
        schema_ir,
        data_source,
        client_override,
        EmbedChunkingConfig::from_env(),
    )
    .await
}

pub(crate) fn has_embedding_specs(schema_ir: &SchemaIR) -> bool {
    schema_ir.node_types().any(|node| {
        node.properties
            .iter()
            .any(|prop| prop.embed_source.is_some())
    })
}

async fn materialize_embeddings_for_load_inner_with_chunking(
    db_path: &Path,
    schema_ir: &SchemaIR,
    data_source: &str,
    client_override: Option<&EmbeddingClient>,
    chunking: EmbedChunkingConfig,
) -> Result<String> {
    let embed_specs = collect_embed_specs(schema_ir)?;
    if embed_specs.is_empty() {
        return Ok(data_source.to_string());
    }

    let mut lines = Vec::new();
    let mut pending = Vec::new();
    parse_input_lines(data_source, &embed_specs, &mut lines, &mut pending)?;
    if pending.is_empty() {
        return Ok(data_source.to_string());
    }

    let cache_path = db_path.join(EMBEDDING_CACHE_FILENAME);
    let mut cache = load_embedding_cache(&cache_path)?;

    let owned_client;
    let client = if let Some(client) = client_override {
        client
    } else {
        owned_client = EmbeddingClient::from_env().map_err(|err| {
            NanoError::Storage(format!("embedding initialization failed: {}", err))
        })?;
        &owned_client
    };
    let model = client.model().to_string();

    let mut missing_by_dim: BTreeMap<usize, Vec<(CacheKey, String)>> = BTreeMap::new();
    for assignment in &pending {
        let key = CacheKey {
            model: model.clone(),
            dim: assignment.dim,
            content_hash: assignment.content_hash.clone(),
            chunk_chars: chunking.chunk_chars,
            chunk_overlap_chars: chunking.chunk_overlap_chars,
        };
        if cache.contains_key(&key) {
            continue;
        }
        let entries = missing_by_dim.entry(assignment.dim).or_default();
        if !entries.iter().any(|(existing, _)| existing == &key) {
            entries.push((key, assignment.source_text.clone()));
        }
    }

    let batch_size = parse_env_usize("NANOGRAPH_EMBED_BATCH_SIZE", DEFAULT_EMBED_BATCH_SIZE);
    let mut new_cache_records = Vec::new();
    for (dim, entries) in missing_by_dim {
        if chunking.is_enabled() {
            for (key, text) in entries {
                let vector =
                    embed_text_with_chunking(client, &text, dim, batch_size, chunking).await?;
                if vector.len() != dim {
                    return Err(NanoError::Storage(format!(
                        "embedding dimension mismatch for {}: expected {}, got {}",
                        key.content_hash,
                        dim,
                        vector.len()
                    )));
                }
                cache.insert(key.clone(), vector.clone());
                new_cache_records.push(CacheRecord {
                    model: key.model.clone(),
                    dim: key.dim,
                    content_hash: key.content_hash.clone(),
                    vector,
                    chunk_chars: key.chunk_chars,
                    chunk_overlap_chars: key.chunk_overlap_chars,
                });
            }
            continue;
        }

        for chunk in entries.chunks(batch_size) {
            let texts: Vec<String> = chunk.iter().map(|(_, text)| text.clone()).collect();
            let vectors = client
                .embed_texts(&texts, dim)
                .await
                .map_err(|err| NanoError::Storage(format!("embedding request failed: {}", err)))?;
            if vectors.len() != chunk.len() {
                return Err(NanoError::Storage(format!(
                    "embedding response size mismatch: expected {}, got {}",
                    chunk.len(),
                    vectors.len()
                )));
            }
            for ((key, _), vector) in chunk.iter().zip(vectors.into_iter()) {
                if vector.len() != dim {
                    return Err(NanoError::Storage(format!(
                        "embedding dimension mismatch for {}: expected {}, got {}",
                        key.content_hash,
                        dim,
                        vector.len()
                    )));
                }
                cache.insert(key.clone(), vector.clone());
                new_cache_records.push(CacheRecord {
                    model: key.model.clone(),
                    dim: key.dim,
                    content_hash: key.content_hash.clone(),
                    vector,
                    chunk_chars: key.chunk_chars,
                    chunk_overlap_chars: key.chunk_overlap_chars,
                });
            }
        }
    }
    append_embedding_cache(&cache_path, &new_cache_records)?;

    apply_embeddings_to_lines(&mut lines, &pending, &cache, &model, chunking)?;
    render_output_lines(data_source, lines)
}

fn parse_input_lines(
    data_source: &str,
    embed_specs: &HashMap<String, Vec<EmbedSpec>>,
    lines: &mut Vec<ParsedLine>,
    pending: &mut Vec<PendingAssignment>,
) -> Result<()> {
    for (line_no, raw_line) in data_source.lines().enumerate() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            lines.push(ParsedLine::Raw(raw_line.to_string()));
            continue;
        }

        let mut obj: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            NanoError::Storage(format!("JSON parse error on line {}: {}", line_no + 1, e))
        })?;

        if let Some(type_name) = obj
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        {
            if let Some(specs) = embed_specs.get(type_name.as_str()) {
                let data_obj = obj
                    .get_mut("data")
                    .and_then(|v| v.as_object_mut())
                    .ok_or_else(|| {
                        NanoError::Storage(format!(
                            "node {} line {} is missing object field `data`",
                            type_name,
                            line_no + 1
                        ))
                    })?;
                let line_index = lines.len();

                for spec in specs {
                    let needs_embedding = match data_obj.get(&spec.target_prop) {
                        Some(value) => value.is_null(),
                        None => true,
                    };
                    if !needs_embedding {
                        continue;
                    }

                    let source_value = data_obj.get(&spec.source_prop).ok_or_else(|| {
                        NanoError::Storage(format!(
                            "node {} line {} missing @embed source property `{}` for `{}`",
                            type_name,
                            line_no + 1,
                            spec.source_prop,
                            spec.target_prop
                        ))
                    })?;
                    let source_text = source_value.as_str().ok_or_else(|| {
                        NanoError::Storage(format!(
                            "node {} line {} @embed source property `{}` must be String",
                            type_name,
                            line_no + 1,
                            spec.source_prop
                        ))
                    })?;

                    pending.push(PendingAssignment {
                        line_index,
                        target_prop: spec.target_prop.clone(),
                        source_text: source_text.to_string(),
                        dim: spec.dim,
                        content_hash: hash_string(source_text),
                    });
                }
            }
        }

        lines.push(ParsedLine::Json(obj));
    }
    Ok(())
}

fn apply_embeddings_to_lines(
    lines: &mut [ParsedLine],
    pending: &[PendingAssignment],
    cache: &HashMap<CacheKey, Vec<f32>>,
    model: &str,
    chunking: EmbedChunkingConfig,
) -> Result<()> {
    for assignment in pending {
        let key = CacheKey {
            model: model.to_string(),
            dim: assignment.dim,
            content_hash: assignment.content_hash.clone(),
            chunk_chars: chunking.chunk_chars,
            chunk_overlap_chars: chunking.chunk_overlap_chars,
        };
        let vector = cache.get(&key).ok_or_else(|| {
            NanoError::Storage(format!(
                "embedding cache miss for content hash {}",
                assignment.content_hash
            ))
        })?;
        let line = lines.get_mut(assignment.line_index).ok_or_else(|| {
            NanoError::Storage(format!(
                "embedding assignment line out of range: {}",
                assignment.line_index
            ))
        })?;
        let ParsedLine::Json(obj) = line else {
            return Err(NanoError::Storage(format!(
                "embedding assignment line {} is not JSON",
                assignment.line_index
            )));
        };
        let data_obj = obj
            .get_mut("data")
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| {
                NanoError::Storage("node row is missing object field `data`".to_string())
            })?;
        data_obj.insert(
            assignment.target_prop.clone(),
            serde_json::to_value(vector).map_err(|e| {
                NanoError::Storage(format!("serialize embedding vector failed: {}", e))
            })?,
        );
    }
    Ok(())
}

fn render_output_lines(original: &str, lines: Vec<ParsedLine>) -> Result<String> {
    let mut out = String::new();
    for (idx, line) in lines.into_iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        match line {
            ParsedLine::Raw(raw) => out.push_str(&raw),
            ParsedLine::Json(obj) => {
                out.push_str(&serde_json::to_string(&obj).map_err(|e| {
                    NanoError::Storage(format!("serialize JSONL row failed: {}", e))
                })?)
            }
        }
    }
    if original.ends_with('\n') {
        out.push('\n');
    }
    Ok(out)
}

async fn embed_text_with_chunking(
    client: &EmbeddingClient,
    source_text: &str,
    dim: usize,
    batch_size: usize,
    chunking: EmbedChunkingConfig,
) -> Result<Vec<f32>> {
    let chunks = split_text_into_chunks(
        source_text,
        chunking.chunk_chars,
        chunking.chunk_overlap_chars,
    );
    if chunks.len() == 1 {
        return client
            .embed_text(&chunks[0], dim)
            .await
            .map_err(|err| NanoError::Storage(format!("embedding request failed: {}", err)));
    }

    let batch_size = batch_size.max(1);
    let mut vectors = Vec::with_capacity(chunks.len());
    for chunk_batch in chunks.chunks(batch_size) {
        let texts: Vec<String> = chunk_batch.to_vec();
        let mut embedded = client
            .embed_texts(&texts, dim)
            .await
            .map_err(|err| NanoError::Storage(format!("embedding request failed: {}", err)))?;
        if embedded.len() != texts.len() {
            return Err(NanoError::Storage(format!(
                "embedding response size mismatch: expected {}, got {}",
                texts.len(),
                embedded.len()
            )));
        }
        vectors.append(&mut embedded);
    }

    average_pool_embeddings(&vectors, dim)
}

fn split_text_into_chunks(text: &str, chunk_chars: usize, overlap_chars: usize) -> Vec<String> {
    if chunk_chars == 0 {
        return vec![text.to_string()];
    }

    let total_chars = text.chars().count();
    if total_chars <= chunk_chars {
        return vec![text.to_string()];
    }

    let mut char_boundaries = Vec::with_capacity(total_chars + 1);
    char_boundaries.push(0);
    for (idx, _) in text.char_indices().skip(1) {
        char_boundaries.push(idx);
    }
    char_boundaries.push(text.len());

    let step = chunk_chars.saturating_sub(overlap_chars).max(1);
    let mut out = Vec::new();
    let mut start_char = 0usize;
    while start_char < total_chars {
        let end_char = (start_char + chunk_chars).min(total_chars);
        let start_byte = char_boundaries[start_char];
        let end_byte = char_boundaries[end_char];
        out.push(text[start_byte..end_byte].to_string());
        if end_char == total_chars {
            break;
        }
        start_char = start_char.saturating_add(step);
    }

    if out.is_empty() {
        vec![text.to_string()]
    } else {
        out
    }
}

fn average_pool_embeddings(vectors: &[Vec<f32>], dim: usize) -> Result<Vec<f32>> {
    if vectors.is_empty() {
        return Err(NanoError::Storage(
            "embedding aggregation received no chunk vectors".to_string(),
        ));
    }

    let mut accum = vec![0.0f64; dim];
    for vector in vectors {
        if vector.len() != dim {
            return Err(NanoError::Storage(format!(
                "embedding dimension mismatch during chunk aggregation: expected {}, got {}",
                dim,
                vector.len()
            )));
        }
        for (idx, value) in vector.iter().enumerate() {
            accum[idx] += *value as f64;
        }
    }

    let inv_len = 1.0f64 / vectors.len() as f64;
    let mut pooled: Vec<f32> = accum
        .into_iter()
        .map(|sum| (sum * inv_len) as f32)
        .collect();
    let norm = pooled
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm > f32::EPSILON {
        for value in &mut pooled {
            *value /= norm;
        }
    }
    Ok(pooled)
}

fn collect_embed_specs(schema_ir: &SchemaIR) -> Result<HashMap<String, Vec<EmbedSpec>>> {
    let mut specs_by_type: HashMap<String, Vec<EmbedSpec>> = HashMap::new();
    for node in schema_ir.node_types() {
        let mut prop_by_name: HashMap<&str, &PropDef> = HashMap::new();
        for prop in &node.properties {
            prop_by_name.insert(prop.name.as_str(), prop);
        }

        let mut node_specs = Vec::new();
        for prop in &node.properties {
            let Some(source_prop) = prop.embed_source.as_ref() else {
                continue;
            };

            if prop.list {
                return Err(NanoError::Storage(format!(
                    "@embed target {}.{} cannot be a list type",
                    node.name, prop.name
                )));
            }
            let dim = match ScalarType::from_str_name(&prop.scalar_type) {
                Some(ScalarType::Vector(dim)) if dim > 0 => dim as usize,
                _ => {
                    return Err(NanoError::Storage(format!(
                        "@embed target {}.{} must be Vector(dim)",
                        node.name, prop.name
                    )));
                }
            };

            let source_def = prop_by_name.get(source_prop.as_str()).ok_or_else(|| {
                NanoError::Storage(format!(
                    "@embed on {}.{} references unknown source property {}",
                    node.name, prop.name, source_prop
                ))
            })?;
            if source_def.list || source_def.scalar_type != "String" {
                return Err(NanoError::Storage(format!(
                    "@embed source {}.{} must be String",
                    node.name, source_prop
                )));
            }

            node_specs.push(EmbedSpec {
                target_prop: prop.name.clone(),
                source_prop: source_prop.clone(),
                dim,
            });
        }

        if !node_specs.is_empty() {
            specs_by_type.insert(node.name.clone(), node_specs);
        }
    }
    Ok(specs_by_type)
}

fn load_embedding_cache(path: &Path) -> Result<HashMap<CacheKey, Vec<f32>>> {
    let records = load_embedding_cache_records(path)?;
    let mut cache = HashMap::new();
    for record in records {
        let key = cache_key_from_record(&record);
        cache.insert(key, record.vector);
    }
    Ok(cache)
}

fn append_embedding_cache(path: &Path, records: &[CacheRecord]) -> Result<()> {
    let max_entries = parse_env_usize(
        "NANOGRAPH_EMBED_CACHE_MAX_ENTRIES",
        DEFAULT_EMBED_CACHE_MAX_ENTRIES,
    );
    append_embedding_cache_with_limit(path, records, max_entries)
}

fn append_embedding_cache_with_limit(
    path: &Path,
    records: &[CacheRecord],
    max_entries: usize,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let _lock = acquire_embedding_cache_lock(path)?;
    let mut merged = load_embedding_cache_records(path)?;
    merged.extend(records.iter().cloned());
    let compacted = compact_embedding_cache_records(merged, max_entries);
    write_embedding_cache_records(path, &compacted)?;
    Ok(())
}

fn load_embedding_cache_records(path: &Path) -> Result<Vec<CacheRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(path)?;
    parse_embedding_cache_records(path, &data)
}

fn parse_embedding_cache_records(path: &Path, data: &str) -> Result<Vec<CacheRecord>> {
    let mut records = Vec::new();
    for (line_no, line) in data.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record: CacheRecord = serde_json::from_str(trimmed).map_err(|e| {
            NanoError::Storage(format!(
                "invalid embedding cache at {} line {}: {}",
                path.display(),
                line_no + 1,
                e
            ))
        })?;
        if record.vector.len() != record.dim {
            return Err(NanoError::Storage(format!(
                "invalid embedding cache at {} line {}: vector dim {} does not match {}",
                path.display(),
                line_no + 1,
                record.vector.len(),
                record.dim
            )));
        }
        records.push(record);
    }
    Ok(records)
}

fn write_embedding_cache_records(path: &Path, records: &[CacheRecord]) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    for record in records {
        let mut line = serde_json::to_vec(record).map_err(|e| {
            NanoError::Storage(format!(
                "failed to write embedding cache {}: {}",
                path.display(),
                e
            ))
        })?;
        line.push(b'\n');
        file.write_all(&line)?;
    }
    file.flush()?;
    Ok(())
}

fn compact_embedding_cache_records(
    records: Vec<CacheRecord>,
    max_entries: usize,
) -> Vec<CacheRecord> {
    let max_entries = max_entries.max(1);
    let mut seen = HashSet::new();
    let mut compacted_rev = Vec::with_capacity(records.len().min(max_entries));
    for record in records.into_iter().rev() {
        if seen.insert(cache_key_from_record(&record)) {
            compacted_rev.push(record);
            if compacted_rev.len() == max_entries {
                break;
            }
        }
    }
    compacted_rev.reverse();
    compacted_rev
}

fn cache_key_from_record(record: &CacheRecord) -> CacheKey {
    CacheKey {
        model: record.model.clone(),
        dim: record.dim,
        content_hash: record.content_hash.clone(),
        chunk_chars: record.chunk_chars,
        chunk_overlap_chars: record.chunk_overlap_chars,
    }
}

struct EmbeddingCacheLock {
    path: PathBuf,
    _file: std::fs::File,
}

impl Drop for EmbeddingCacheLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn embedding_cache_lock_path(path: &Path) -> PathBuf {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".lock");
    PathBuf::from(lock_path)
}

fn acquire_embedding_cache_lock(path: &Path) -> Result<EmbeddingCacheLock> {
    let stale_after_secs = parse_env_usize(
        "NANOGRAPH_EMBED_CACHE_LOCK_STALE_SECS",
        DEFAULT_EMBED_CACHE_LOCK_STALE_SECS,
    );
    let stale_after = Duration::from_secs(stale_after_secs as u64);
    acquire_embedding_cache_lock_with_stale_after(path, stale_after)
}

fn acquire_embedding_cache_lock_with_stale_after(
    path: &Path,
    stale_after: Duration,
) -> Result<EmbeddingCacheLock> {
    let lock_path = embedding_cache_lock_path(path);
    for attempt in 0..EMBEDDING_CACHE_LOCK_RETRIES {
        match std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(file) => {
                return Ok(EmbeddingCacheLock {
                    path: lock_path,
                    _file: file,
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if lock_file_is_stale(&lock_path, stale_after) {
                    match std::fs::remove_file(&lock_path) {
                        Ok(()) => continue,
                        Err(remove_err) if remove_err.kind() == std::io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(remove_err) => {
                            return Err(NanoError::Storage(format!(
                                "failed to remove stale embedding cache lock {}: {}",
                                lock_path.display(),
                                remove_err
                            )));
                        }
                    }
                }
                if attempt + 1 == EMBEDDING_CACHE_LOCK_RETRIES {
                    return Err(NanoError::Storage(format!(
                        "embedding cache lock timed out for {} (lock file: {})",
                        path.display(),
                        lock_path.display()
                    )));
                }
                std::thread::sleep(Duration::from_millis(EMBEDDING_CACHE_LOCK_RETRY_DELAY_MS));
            }
            Err(err) => {
                return Err(NanoError::Storage(format!(
                    "failed to acquire embedding cache lock {}: {}",
                    lock_path.display(),
                    err
                )));
            }
        }
    }

    Err(NanoError::Storage(format!(
        "embedding cache lock acquisition failed for {}",
        path.display()
    )))
}

fn lock_file_is_stale(lock_path: &Path, stale_after: Duration) -> bool {
    let metadata = match std::fs::metadata(lock_path) {
        Ok(meta) => meta,
        Err(_) => return false,
    };
    let timestamp = metadata.modified().ok().or_else(|| metadata.created().ok());
    let Some(timestamp) = timestamp else {
        return false;
    };
    match timestamp.elapsed() {
        Ok(age) => age >= stale_after,
        Err(_) => false,
    }
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    use crate::catalog::schema_ir::build_schema_ir;
    use crate::schema::parser::parse_schema;

    use super::*;

    #[tokio::test]
    async fn materialize_embeddings_populates_missing_vector() {
        let schema = parse_schema(
            r#"
node Doc {
    slug: String @key
    title: String
    embedding: Vector(6) @embed(title)
}
"#,
        )
        .unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let data = r#"{"type":"Doc","data":{"slug":"a","title":"alpha"}}
{"type":"Doc","data":{"slug":"b","title":"beta"}}
"#;
        let temp = TempDir::new().unwrap();
        let client = EmbeddingClient::mock_for_tests();
        let out = materialize_embeddings_for_load_inner(temp.path(), &ir, data, Some(&client))
            .await
            .unwrap();
        assert!(out.contains("\"embedding\""));
        assert!(temp.path().join(EMBEDDING_CACHE_FILENAME).exists());
    }

    #[tokio::test]
    async fn materialize_embeddings_is_noop_when_vectors_present() {
        let schema = parse_schema(
            r#"
node Doc {
    slug: String @key
    title: String
    embedding: Vector(3) @embed(title)
}
"#,
        )
        .unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let data =
            r#"{"type":"Doc","data":{"slug":"a","title":"alpha","embedding":[1.0,0.0,0.0]}}"#;
        let temp = TempDir::new().unwrap();
        let out = materialize_embeddings_for_load_inner(
            temp.path(),
            &ir,
            data,
            Some(&EmbeddingClient::mock_for_tests()),
        )
        .await
        .unwrap();
        assert_eq!(out, data);
        assert!(!temp.path().join(EMBEDDING_CACHE_FILENAME).exists());
    }

    #[test]
    fn split_text_into_chunks_respects_overlap() {
        let chunks = split_text_into_chunks("abcdefghij", 4, 1);
        assert_eq!(chunks, vec!["abcd", "defg", "ghij"]);
    }

    #[test]
    fn append_embedding_cache_handles_concurrent_writers() {
        let temp = TempDir::new().unwrap();
        let cache_path = temp.path().join(EMBEDDING_CACHE_FILENAME);
        let writer_count = 8usize;
        let barrier = Arc::new(Barrier::new(writer_count));
        let mut threads = Vec::new();

        for idx in 0..writer_count {
            let path = cache_path.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                let record = CacheRecord {
                    model: "test-model".to_string(),
                    dim: 3,
                    content_hash: format!("hash-{}", idx),
                    vector: vec![idx as f32, 1.0, 2.0],
                    chunk_chars: 0,
                    chunk_overlap_chars: 0,
                };
                barrier.wait();
                append_embedding_cache(&path, &[record]).unwrap();
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        let file = std::fs::read_to_string(&cache_path).unwrap();
        let lines: Vec<&str> = file
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        assert_eq!(lines.len(), writer_count);

        let mut seen = HashSet::new();
        for line in lines {
            let record: CacheRecord = serde_json::from_str(line).unwrap();
            assert!(seen.insert(record.content_hash));
        }
    }

    #[test]
    fn append_embedding_cache_with_limit_compacts_and_deduplicates() {
        let temp = TempDir::new().unwrap();
        let cache_path = temp.path().join(EMBEDDING_CACHE_FILENAME);

        let record = |hash: &str, marker: f32| CacheRecord {
            model: "test-model".to_string(),
            dim: 3,
            content_hash: hash.to_string(),
            vector: vec![marker, 1.0, 2.0],
            chunk_chars: 0,
            chunk_overlap_chars: 0,
        };

        append_embedding_cache_with_limit(
            &cache_path,
            &[record("a", 1.0), record("b", 2.0), record("c", 3.0)],
            3,
        )
        .unwrap();
        append_embedding_cache_with_limit(&cache_path, &[record("d", 4.0), record("b", 20.0)], 3)
            .unwrap();

        let cache = load_embedding_cache(&cache_path).unwrap();
        assert_eq!(cache.len(), 3);

        let key_b = CacheKey {
            model: "test-model".to_string(),
            dim: 3,
            content_hash: "b".to_string(),
            chunk_chars: 0,
            chunk_overlap_chars: 0,
        };
        let key_c = CacheKey {
            content_hash: "c".to_string(),
            ..key_b.clone()
        };
        let key_d = CacheKey {
            content_hash: "d".to_string(),
            ..key_b.clone()
        };

        assert_eq!(cache.get(&key_b).unwrap()[0], 20.0);
        assert!(cache.contains_key(&key_c));
        assert!(cache.contains_key(&key_d));
    }

    #[test]
    fn acquire_embedding_cache_lock_reclaims_stale_lock_file() {
        let temp = TempDir::new().unwrap();
        let cache_path = temp.path().join(EMBEDDING_CACHE_FILENAME);
        let lock_path = embedding_cache_lock_path(&cache_path);

        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));

        let lock =
            acquire_embedding_cache_lock_with_stale_after(&cache_path, Duration::from_secs(1))
                .unwrap();
        drop(lock);

        assert!(!lock_path.exists());
    }

    #[tokio::test]
    async fn materialize_embeddings_chunking_pools_chunk_vectors() {
        let schema = parse_schema(
            r#"
node Doc {
    slug: String @key
    body: String
    embedding: Vector(6) @embed(body)
}
"#,
        )
        .unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let data = r#"{"type":"Doc","data":{"slug":"doc-1","body":"alpha beta gamma delta epsilon zeta"}}"#;
        let temp = TempDir::new().unwrap();
        let client = EmbeddingClient::mock_for_tests();
        let chunking = EmbedChunkingConfig::new(12, 3);
        let out = materialize_embeddings_for_load_inner_with_chunking(
            temp.path(),
            &ir,
            data,
            Some(&client),
            chunking,
        )
        .await
        .unwrap();

        let embedded: serde_json::Value = serde_json::from_str(&out).unwrap();
        let values = embedded["data"]["embedding"].as_array().unwrap();
        let actual: Vec<f32> = values.iter().map(|v| v.as_f64().unwrap() as f32).collect();

        let chunk_texts = split_text_into_chunks(
            "alpha beta gamma delta epsilon zeta",
            chunking.chunk_chars,
            chunking.chunk_overlap_chars,
        );
        let chunk_vectors = client.embed_texts(&chunk_texts, 6).await.unwrap();
        let expected = average_pool_embeddings(&chunk_vectors, 6).unwrap();

        assert_eq!(actual.len(), expected.len());
        for (got, want) in actual.iter().zip(expected.iter()) {
            assert!((got - want).abs() < 1e-6, "got={}, want={}", got, want);
        }
    }

    #[test]
    fn cache_key_differs_by_chunking_config() {
        let key_a = CacheKey {
            model: "text-embedding-3-small".to_string(),
            dim: 8,
            content_hash: "abc".to_string(),
            chunk_chars: 0,
            chunk_overlap_chars: 0,
        };
        let key_b = CacheKey {
            chunk_chars: 256,
            chunk_overlap_chars: 64,
            ..key_a.clone()
        };
        assert_ne!(key_a, key_b);
    }
}
