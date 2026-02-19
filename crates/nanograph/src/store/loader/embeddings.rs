use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;

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

pub(crate) fn materialize_embeddings_for_load<'a>(
    db_path: &Path,
    schema_ir: &SchemaIR,
    data_source: &'a str,
) -> Result<Cow<'a, str>> {
    materialize_embeddings_for_load_inner(db_path, schema_ir, data_source, None)
}

fn materialize_embeddings_for_load_inner<'a>(
    db_path: &Path,
    schema_ir: &SchemaIR,
    data_source: &'a str,
    client_override: Option<&EmbeddingClient>,
) -> Result<Cow<'a, str>> {
    materialize_embeddings_for_load_inner_with_chunking(
        db_path,
        schema_ir,
        data_source,
        client_override,
        EmbedChunkingConfig::from_env(),
    )
}

fn materialize_embeddings_for_load_inner_with_chunking<'a>(
    db_path: &Path,
    schema_ir: &SchemaIR,
    data_source: &'a str,
    client_override: Option<&EmbeddingClient>,
    chunking: EmbedChunkingConfig,
) -> Result<Cow<'a, str>> {
    let embed_specs = collect_embed_specs(schema_ir)?;
    if embed_specs.is_empty() {
        return Ok(Cow::Borrowed(data_source));
    }

    let mut lines = Vec::new();
    let mut pending = Vec::new();
    parse_input_lines(data_source, &embed_specs, &mut lines, &mut pending)?;
    if pending.is_empty() {
        return Ok(Cow::Borrowed(data_source));
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
                let vector = embed_text_with_chunking(client, &text, dim, batch_size, chunking)?;
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

fn render_output_lines<'a>(original: &'a str, lines: Vec<ParsedLine>) -> Result<Cow<'a, str>> {
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
    Ok(Cow::Owned(out))
}

fn embed_text_with_chunking(
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
            .map_err(|err| NanoError::Storage(format!("embedding request failed: {}", err)));
    }

    let batch_size = batch_size.max(1);
    let mut vectors = Vec::with_capacity(chunks.len());
    for chunk_batch in chunks.chunks(batch_size) {
        let texts: Vec<String> = chunk_batch.to_vec();
        let mut embedded = client
            .embed_texts(&texts, dim)
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
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let mut cache = HashMap::new();
    let data = std::fs::read_to_string(path)?;
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
        let key = CacheKey {
            model: record.model,
            dim: record.dim,
            content_hash: record.content_hash,
            chunk_chars: record.chunk_chars,
            chunk_overlap_chars: record.chunk_overlap_chars,
        };
        cache.insert(key, record.vector);
    }
    Ok(cache)
}

fn append_embedding_cache(path: &Path, records: &[CacheRecord]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    for record in records {
        serde_json::to_writer(&mut file, record).map_err(|e| {
            NanoError::Storage(format!(
                "failed to append embedding cache {}: {}",
                path.display(),
                e
            ))
        })?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    Ok(())
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
    use tempfile::TempDir;

    use crate::catalog::schema_ir::build_schema_ir;
    use crate::schema::parser::parse_schema;

    use super::*;

    #[test]
    fn materialize_embeddings_populates_missing_vector() {
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
        let out =
            materialize_embeddings_for_load_inner(temp.path(), &ir, data, Some(&client)).unwrap();
        assert!(out.contains("\"embedding\""));
        assert!(temp.path().join(EMBEDDING_CACHE_FILENAME).exists());
    }

    #[test]
    fn materialize_embeddings_is_noop_when_vectors_present() {
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
        .unwrap();
        assert!(matches!(out, Cow::Borrowed(_)));
        assert!(!temp.path().join(EMBEDDING_CACHE_FILENAME).exists());
    }

    #[test]
    fn split_text_into_chunks_respects_overlap() {
        let chunks = split_text_into_chunks("abcdefghij", 4, 1);
        assert_eq!(chunks, vec!["abcd", "defg", "ghij"]);
    }

    #[test]
    fn materialize_embeddings_chunking_pools_chunk_vectors() {
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
        .unwrap();

        let embedded: serde_json::Value = serde_json::from_str(out.as_ref()).unwrap();
        let values = embedded["data"]["embedding"].as_array().unwrap();
        let actual: Vec<f32> = values.iter().map(|v| v.as_f64().unwrap() as f32).collect();

        let chunk_texts = split_text_into_chunks(
            "alpha beta gamma delta epsilon zeta",
            chunking.chunk_chars,
            chunking.chunk_overlap_chars,
        );
        let chunk_vectors = client.embed_texts(&chunk_texts, 6).unwrap();
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
