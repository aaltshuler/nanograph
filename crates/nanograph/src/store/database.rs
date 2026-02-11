use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
};
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use tracing::{debug, info, instrument};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SchemaIR, build_catalog_from_ir, build_schema_ir};
use crate::error::{NanoError, Result};
use crate::schema::parser::parse_schema;
use crate::store::graph::GraphStorage;
use crate::store::indexing::rebuild_node_scalar_indexes;
use crate::store::loader::build_next_storage_for_load;
use crate::store::manifest::{DatasetEntry, GraphManifest, hash_string};
use crate::store::migration::reconcile_migration_sidecars;

const SCHEMA_PG_FILENAME: &str = "schema.pg";
const SCHEMA_IR_FILENAME: &str = "schema.ir.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    Overwrite,
    Append,
    Merge,
}

#[derive(Debug, Clone)]
pub struct DeletePredicate {
    pub property: String,
    pub op: DeleteOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeleteResult {
    pub deleted_nodes: usize,
    pub deleted_edges: usize,
}

pub struct Database {
    path: PathBuf,
    pub schema_ir: SchemaIR,
    pub catalog: Catalog,
    pub storage: GraphStorage,
}

impl Database {
    /// Create a new database directory from schema source text.
    #[instrument(skip(schema_source), fields(db_path = %db_path.display()))]
    pub async fn init(db_path: &Path, schema_source: &str) -> Result<Self> {
        info!("initializing database");
        // Parse and validate schema
        let schema_file = parse_schema(schema_source)?;
        let schema_ir = build_schema_ir(&schema_file)?;
        let catalog = build_catalog_from_ir(&schema_ir)?;

        // Create directory structure
        std::fs::create_dir_all(db_path)?;
        std::fs::create_dir_all(db_path.join("nodes"))?;
        std::fs::create_dir_all(db_path.join("edges"))?;

        // Write schema.pg (human-authored source)
        std::fs::write(db_path.join(SCHEMA_PG_FILENAME), schema_source)?;

        // Write schema.ir.json
        let ir_json = serde_json::to_string_pretty(&schema_ir)
            .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
        std::fs::write(db_path.join(SCHEMA_IR_FILENAME), &ir_json)?;

        // Write empty manifest
        let ir_hash = hash_string(&ir_json);
        let mut manifest = GraphManifest::new(ir_hash);
        let (next_type_id, next_prop_id) = next_schema_identity_counters(&schema_ir);
        manifest.next_type_id = next_type_id;
        manifest.next_prop_id = next_prop_id;
        manifest.write_atomic(db_path)?;

        let storage = GraphStorage::new(catalog.clone());
        info!("database initialized");

        Ok(Database {
            path: db_path.to_path_buf(),
            schema_ir,
            catalog,
            storage,
        })
    }

    /// Open an existing database.
    #[instrument(fields(db_path = %db_path.display()))]
    pub async fn open(db_path: &Path) -> Result<Self> {
        info!("opening database");
        reconcile_migration_sidecars(db_path)?;
        if !db_path.exists() {
            return Err(NanoError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("database not found: {}", db_path.display()),
            )));
        }

        // Read schema IR
        let ir_json = std::fs::read_to_string(db_path.join(SCHEMA_IR_FILENAME))?;
        let schema_ir: SchemaIR = serde_json::from_str(&ir_json)
            .map_err(|e| NanoError::Manifest(format!("parse IR error: {}", e)))?;

        // Build catalog from IR
        let catalog = build_catalog_from_ir(&schema_ir)?;

        // Read manifest
        let manifest = GraphManifest::read(db_path)?;

        // Verify schema hash matches manifest
        let computed_hash = hash_string(&ir_json);
        if computed_hash != manifest.schema_ir_hash {
            return Err(NanoError::Manifest(format!(
                "schema mismatch: schema.ir.json has been modified since last load \
                 (expected hash {}, got {}). Re-run 'nanograph load' to update.",
                &manifest.schema_ir_hash[..8.min(manifest.schema_ir_hash.len())],
                &computed_hash[..8.min(computed_hash.len())]
            )));
        }

        // Create storage and set ID counters
        let mut storage = GraphStorage::new(catalog.clone());
        storage.set_next_node_id(manifest.next_node_id);
        storage.set_next_edge_id(manifest.next_edge_id);

        // Load only datasets listed in the manifest (authoritative source)
        for entry in &manifest.datasets {
            let dir_name = SchemaIR::dir_name(entry.type_id);
            debug!(
                kind = %entry.kind,
                type_name = %entry.type_name,
                row_count = entry.row_count,
                "restoring dataset from manifest"
            );
            match entry.kind.as_str() {
                "node" => {
                    let dataset_path = db_path.join("nodes").join(&dir_name);
                    if dataset_path.exists() {
                        let batches = read_lance_batches(&dataset_path).await?;
                        for batch in batches {
                            storage.load_node_batch(&entry.type_name, batch)?;
                        }
                        storage.set_node_dataset_path(&entry.type_name, dataset_path);
                    }
                }
                "edge" => {
                    let dataset_path = db_path.join("edges").join(&dir_name);
                    if dataset_path.exists() {
                        let batches = read_lance_batches(&dataset_path).await?;
                        for batch in batches {
                            storage.load_edge_batch(&entry.type_name, batch)?;
                        }
                    }
                }
                _ => {}
            }
        }

        // Build CSR/CSC indices
        storage.build_indices()?;
        info!(
            node_types = storage.node_segments.len(),
            edge_types = storage.edge_segments.len(),
            "database open complete"
        );

        Ok(Database {
            path: db_path.to_path_buf(),
            schema_ir,
            catalog,
            storage,
        })
    }

    /// Load JSONL data using compatibility defaults:
    /// - any `@key` in schema => `LoadMode::Merge`
    /// - no `@key` in schema => `LoadMode::Overwrite`
    #[instrument(skip(self, data_source))]
    pub async fn load(&mut self, data_source: &str) -> Result<()> {
        let mode = if self
            .schema_ir
            .node_types()
            .any(|node| node.properties.iter().any(|prop| prop.key))
        {
            LoadMode::Merge
        } else {
            LoadMode::Overwrite
        };
        self.load_with_mode(data_source, mode).await
    }

    /// Load JSONL data using explicit semantics.
    #[instrument(skip(self, data_source), fields(mode = ?mode))]
    pub async fn load_with_mode(&mut self, data_source: &str, mode: LoadMode) -> Result<()> {
        info!("starting database load");
        self.storage = build_next_storage_for_load(
            &self.path,
            &self.storage,
            &self.schema_ir,
            data_source,
            mode,
        )
        .await?;
        self.persist_storage().await?;
        info!(
            mode = ?mode,
            node_types = self.storage.node_segments.len(),
            edge_types = self.storage.edge_segments.len(),
            "database load complete"
        );

        Ok(())
    }

    /// Delete nodes of a given type matching a predicate, cascading incident edges.
    #[instrument(skip(self), fields(type_name = type_name, property = %predicate.property))]
    pub async fn delete_nodes(
        &mut self,
        type_name: &str,
        predicate: &DeletePredicate,
    ) -> Result<DeleteResult> {
        let target_batch = match self.storage.get_all_nodes(type_name)? {
            Some(batch) => batch,
            None => return Ok(DeleteResult::default()),
        };

        let delete_mask = build_delete_mask_for_mutation(&target_batch, predicate)?;
        let deleted_node_ids = collect_deleted_node_ids(&target_batch, &delete_mask)?;
        if deleted_node_ids.is_empty() {
            return Ok(DeleteResult::default());
        }
        let deleted_node_set: HashSet<u64> = deleted_node_ids.into_iter().collect();

        let mut keep_builder = BooleanBuilder::with_capacity(target_batch.num_rows());
        for row in 0..target_batch.num_rows() {
            let delete = !delete_mask.is_null(row) && delete_mask.value(row);
            keep_builder.append_value(!delete);
        }
        let keep_mask = keep_builder.finish();
        let filtered_target = arrow::compute::filter_record_batch(&target_batch, &keep_mask)
            .map_err(|e| NanoError::Storage(format!("node delete filter error: {}", e)))?;

        let old_next_node_id = self.storage.next_node_id();
        let old_next_edge_id = self.storage.next_edge_id();
        let mut new_storage = GraphStorage::new(self.catalog.clone());

        for node_def in self.schema_ir.node_types() {
            if node_def.name == type_name {
                if filtered_target.num_rows() > 0 {
                    new_storage.load_node_batch(type_name, filtered_target.clone())?;
                }
                continue;
            }

            if let Some(batch) = self.storage.get_all_nodes(&node_def.name)? {
                new_storage.load_node_batch(&node_def.name, batch)?;
            }
        }

        let mut deleted_edges = 0usize;
        for edge_def in self.schema_ir.edge_types() {
            if let Some(edge_batch) = self.storage.edge_batch_for_save(&edge_def.name)? {
                let filtered = filter_edge_batch_by_deleted_nodes(&edge_batch, &deleted_node_set)?;
                deleted_edges += edge_batch.num_rows().saturating_sub(filtered.num_rows());
                if filtered.num_rows() > 0 {
                    new_storage.load_edge_batch(&edge_def.name, filtered)?;
                }
            }
        }

        if new_storage.next_node_id() < old_next_node_id {
            new_storage.set_next_node_id(old_next_node_id);
        }
        if new_storage.next_edge_id() < old_next_edge_id {
            new_storage.set_next_edge_id(old_next_edge_id);
        }
        new_storage.build_indices()?;
        self.storage = new_storage;
        self.persist_storage().await?;

        Ok(DeleteResult {
            deleted_nodes: deleted_node_set.len(),
            deleted_edges,
        })
    }

    async fn persist_storage(&mut self) -> Result<()> {
        self.storage.clear_node_dataset_paths();
        // Write each node type to Lance
        for node_def in self.schema_ir.node_types() {
            if let Some(batch) = self.storage.get_all_nodes(&node_def.name)? {
                debug!(
                    node_type = %node_def.name,
                    rows = batch.num_rows(),
                    "writing node dataset"
                );
                let dir_name = SchemaIR::dir_name(node_def.type_id);
                let dataset_path = self.path.join("nodes").join(&dir_name);
                write_lance_batch(&dataset_path, batch).await?;
                rebuild_node_scalar_indexes(&dataset_path, node_def).await?;
                self.storage
                    .set_node_dataset_path(&node_def.name, dataset_path);
            }
        }

        // Write each edge type to Lance
        for edge_def in self.schema_ir.edge_types() {
            if let Some(batch) = self.storage.edge_batch_for_save(&edge_def.name)? {
                debug!(
                    edge_type = %edge_def.name,
                    rows = batch.num_rows(),
                    "writing edge dataset"
                );
                let dir_name = SchemaIR::dir_name(edge_def.type_id);
                let dataset_path = self.path.join("edges").join(&dir_name);
                write_lance_batch(&dataset_path, batch).await?;
            }
        }

        // Update manifest
        let ir_json = serde_json::to_string_pretty(&self.schema_ir)
            .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
        let ir_hash = hash_string(&ir_json);

        let mut manifest = GraphManifest::new(ir_hash);
        manifest.next_node_id = self.storage.next_node_id();
        manifest.next_edge_id = self.storage.next_edge_id();
        let (next_type_id, next_prop_id) = next_schema_identity_counters(&self.schema_ir);
        manifest.next_type_id = next_type_id;
        manifest.next_prop_id = next_prop_id;

        for node_def in self.schema_ir.node_types() {
            if let Some(batch) = self.storage.get_all_nodes(&node_def.name)? {
                manifest.datasets.push(DatasetEntry {
                    type_id: node_def.type_id,
                    type_name: node_def.name.clone(),
                    kind: "node".to_string(),
                    row_count: batch.num_rows() as u64,
                });
            }
        }

        for edge_def in self.schema_ir.edge_types() {
            let seg = self.storage.edge_segments.get(&edge_def.name);
            if let Some(seg) = seg {
                if !seg.edge_ids.is_empty() {
                    manifest.datasets.push(DatasetEntry {
                        type_id: edge_def.type_id,
                        type_name: edge_def.name.clone(),
                        kind: "edge".to_string(),
                        row_count: seg.edge_ids.len() as u64,
                    });
                }
            }
        }

        manifest.write_atomic(&self.path)?;

        // Clean up stale Lance dirs not in the new manifest
        cleanup_stale_dirs(&self.path, &manifest)?;
        Ok(())
    }

    /// Get catalog reference for typechecking.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Clone storage into Arc for query execution.
    pub fn snapshot(&self) -> Arc<GraphStorage> {
        Arc::new(self.storage.clone())
    }
}

fn parse_predicate_array(value: &str, dt: &DataType, num_rows: usize) -> Result<ArrayRef> {
    let trim_quotes = trim_surrounding_quotes(value);
    let arr: ArrayRef = match dt {
        DataType::Utf8 => Arc::new(StringArray::from(vec![trim_quotes; num_rows])),
        DataType::Boolean => {
            let parsed = trim_quotes.parse::<bool>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid boolean literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(BooleanArray::from(vec![parsed; num_rows]))
        }
        DataType::Int32 => {
            let parsed = trim_quotes.parse::<i32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid i32 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Int32Array::from(vec![parsed; num_rows]))
        }
        DataType::Int64 => {
            let parsed = trim_quotes.parse::<i64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid i64 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Int64Array::from(vec![parsed; num_rows]))
        }
        DataType::UInt32 => {
            let parsed = trim_quotes.parse::<u32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid u32 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(UInt32Array::from(vec![parsed; num_rows]))
        }
        DataType::UInt64 => {
            let parsed = trim_quotes.parse::<u64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid u64 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(UInt64Array::from(vec![parsed; num_rows]))
        }
        DataType::Float32 => {
            let parsed = trim_quotes.parse::<f32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid f32 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Float32Array::from(vec![parsed; num_rows]))
        }
        DataType::Float64 => {
            let parsed = trim_quotes.parse::<f64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid f64 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Float64Array::from(vec![parsed; num_rows]))
        }
        DataType::Date32 => {
            let parsed = trim_quotes.parse::<i32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid Date32 literal '{}' for delete predicate (expected days since epoch)",
                    value
                ))
            })?;
            let base: ArrayRef = Arc::new(Int32Array::from(vec![parsed; num_rows]));
            arrow::compute::cast(&base, &DataType::Date32).map_err(|e| {
                NanoError::Storage(format!("date32 cast error for delete predicate: {}", e))
            })?
        }
        DataType::Date64 => {
            let parsed = trim_quotes.parse::<i64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid Date64 literal '{}' for delete predicate (expected ms since epoch)",
                    value
                ))
            })?;
            let base: ArrayRef = Arc::new(Int64Array::from(vec![parsed; num_rows]));
            arrow::compute::cast(&base, &DataType::Date64).map_err(|e| {
                NanoError::Storage(format!("date64 cast error for delete predicate: {}", e))
            })?
        }
        _ => {
            return Err(NanoError::Storage(format!(
                "delete predicate on unsupported data type {:?}",
                dt
            )));
        }
    };

    Ok(arr)
}

fn trim_surrounding_quotes(s: &str) -> &str {
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        &s[1..s.len().saturating_sub(1)]
    } else {
        s
    }
}

fn compare_for_delete(left: &ArrayRef, right: &ArrayRef, op: DeleteOp) -> Result<BooleanArray> {
    use arrow::compute::kernels::cmp;

    match op {
        DeleteOp::Eq => cmp::eq(left, right),
        DeleteOp::Ne => cmp::neq(left, right),
        DeleteOp::Gt => cmp::gt(left, right),
        DeleteOp::Ge => cmp::gt_eq(left, right),
        DeleteOp::Lt => cmp::lt(left, right),
        DeleteOp::Le => cmp::lt_eq(left, right),
    }
    .map_err(|e| NanoError::Storage(format!("delete predicate compare error: {}", e)))
}

pub(crate) fn build_delete_mask_for_mutation(
    batch: &RecordBatch,
    predicate: &DeletePredicate,
) -> Result<BooleanArray> {
    let left = batch
        .column_by_name(&predicate.property)
        .ok_or_else(|| {
            NanoError::Storage(format!(
                "property '{}' not found for delete predicate",
                predicate.property
            ))
        })?
        .clone();
    let right = parse_predicate_array(&predicate.value, left.data_type(), batch.num_rows())?;
    compare_for_delete(&left, &right, predicate.op)
}

fn collect_deleted_node_ids(batch: &RecordBatch, delete_mask: &BooleanArray) -> Result<Vec<u64>> {
    let id_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("node id column is not UInt64".to_string()))?;
    let mut ids = Vec::new();
    for row in 0..batch.num_rows() {
        if !delete_mask.is_null(row) && delete_mask.value(row) {
            ids.push(id_arr.value(row));
        }
    }
    Ok(ids)
}

fn filter_edge_batch_by_deleted_nodes(
    batch: &RecordBatch,
    deleted_node_ids: &HashSet<u64>,
) -> Result<RecordBatch> {
    if deleted_node_ids.is_empty() {
        return Ok(batch.clone());
    }

    let src_arr = batch
        .column_by_name("src")
        .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge src column is not UInt64".to_string()))?;
    let dst_arr = batch
        .column_by_name("dst")
        .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge dst column is not UInt64".to_string()))?;

    let mut keep_builder = BooleanBuilder::with_capacity(batch.num_rows());
    let mut kept_rows = 0usize;
    for row in 0..batch.num_rows() {
        let keep = !deleted_node_ids.contains(&src_arr.value(row))
            && !deleted_node_ids.contains(&dst_arr.value(row));
        keep_builder.append_value(keep);
        if keep {
            kept_rows += 1;
        }
    }
    if kept_rows == batch.num_rows() {
        return Ok(batch.clone());
    }
    if kept_rows == 0 {
        return Ok(RecordBatch::new_empty(batch.schema()));
    }

    let keep_mask = keep_builder.finish();
    arrow::compute::filter_record_batch(batch, &keep_mask)
        .map_err(|e| NanoError::Storage(format!("edge delete filter error: {}", e)))
}

/// Remove Lance dirs under nodes/ and edges/ that are not in the manifest.
fn cleanup_stale_dirs(db_path: &Path, manifest: &GraphManifest) -> Result<()> {
    let valid_node_dirs: HashSet<String> = manifest
        .datasets
        .iter()
        .filter(|d| d.kind == "node")
        .map(|d| SchemaIR::dir_name(d.type_id))
        .collect();
    let valid_edge_dirs: HashSet<String> = manifest
        .datasets
        .iter()
        .filter(|d| d.kind == "edge")
        .map(|d| SchemaIR::dir_name(d.type_id))
        .collect();

    for (subdir, valid) in [("nodes", &valid_node_dirs), ("edges", &valid_edge_dirs)] {
        let dir = db_path.join(subdir);
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if let Some(name) = entry.file_name().to_str() {
                    if !valid.contains(name) {
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                }
            }
        }
    }

    Ok(())
}

// ── Lance helpers ───────────────────────────────────────────────────────────

async fn write_lance_batch(path: &Path, batch: RecordBatch) -> Result<()> {
    info!(
        dataset_path = %path.display(),
        rows = batch.num_rows(),
        "writing Lance dataset"
    );
    let schema = batch.schema();
    let uri = path.to_string_lossy().to_string();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };

    Dataset::write(reader, &uri, Some(write_params))
        .await
        .map_err(|e| NanoError::Lance(format!("write error: {}", e)))?;

    Ok(())
}

async fn read_lance_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    info!(
        dataset_path = %path.display(),
        "reading Lance dataset"
    );
    let uri = path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;

    let scanner = dataset.scan();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .map_err(|e| NanoError::Lance(format!("scan error: {}", e)))?
        .map(|b| b.map_err(|e| NanoError::Lance(format!("stream error: {}", e))))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(batches)
}

fn next_schema_identity_counters(ir: &SchemaIR) -> (u32, u32) {
    use crate::catalog::schema_ir::TypeDef;

    let mut max_type_id = 0u32;
    let mut max_prop_id = 0u32;
    for ty in &ir.types {
        match ty {
            TypeDef::Node(n) => {
                max_type_id = max_type_id.max(n.type_id);
                for p in &n.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
            TypeDef::Edge(e) => {
                max_type_id = max_type_id.max(e.type_id);
                for p in &e.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
        }
    }
    (
        max_type_id.saturating_add(1).max(1),
        max_prop_id.saturating_add(1).max(1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use lance_index::DatasetIndexExt;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn test_schema_src() -> &'static str {
        r#"node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#
    }

    fn test_data_src() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"type": "Person", "data": {"name": "Charlie", "age": 35}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "Knows", "from": "Alice", "to": "Charlie"}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
"#
    }

    fn duplicate_edge_data_src() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
"#
    }

    fn keyed_schema_src() -> &'static str {
        r#"node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person
"#
    }

    fn keyed_data_initial() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
"#
    }

    fn keyed_data_upsert() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "age": 31}}
{"type": "Person", "data": {"name": "Charlie", "age": 40}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "Knows", "from": "Alice", "to": "Charlie"}
{"edge": "Knows", "from": "Alice", "to": "Charlie"}
"#
    }

    fn keyed_data_append_duplicate() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "age": 99}}
"#
    }

    fn append_data_new_person_with_edge_to_existing() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Diana", "age": 28}}
{"edge": "Knows", "from": "Diana", "to": "Alice"}
"#
    }

    fn unique_schema_src() -> &'static str {
        r#"node Person {
    name: String @key
    email: String @unique
}
"#
    }

    fn unique_data_initial() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "email": "alice@example.com"}}
{"type": "Person", "data": {"name": "Bob", "email": "bob@example.com"}}
"#
    }

    fn unique_data_existing_incoming_conflict() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Charlie", "email": "bob@example.com"}}
"#
    }

    fn unique_data_incoming_incoming_conflict() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Charlie", "email": "charlie@example.com"}}
{"type": "Person", "data": {"name": "Diana", "email": "charlie@example.com"}}
"#
    }

    fn nullable_unique_schema_src() -> &'static str {
        r#"node Person {
    name: String
    nick: String? @unique
}
"#
    }

    fn indexed_schema_src() -> &'static str {
        r#"node Person {
    name: String @key
    handle: String @index
    age: I32?
}
"#
    }

    fn indexed_data_src() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "handle": "a", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "handle": "b", "age": 25}}
"#
    }

    fn nullable_unique_ok_data() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "nick": null}}
{"type": "Person", "data": {"name": "Bob", "nick": null}}
"#
    }

    fn nullable_unique_duplicate_data() -> &'static str {
        r#"{"type": "Person", "data": {"name": "Alice", "nick": "ally"}}
{"type": "Person", "data": {"name": "Bob", "nick": "ally"}}
"#
    }

    fn person_id_by_name(batch: &RecordBatch, name: &str) -> u64 {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .find(|&i| name_col.value(i) == name)
            .map(|i| id_col.value(i))
            .unwrap()
    }

    fn person_age_by_name(batch: &RecordBatch, name: &str) -> Option<i32> {
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let age_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        (0..batch.num_rows()).find_map(|i| {
            if name_col.value(i) == name {
                Some(if age_col.is_null(i) {
                    None
                } else {
                    Some(age_col.value(i))
                })
            } else {
                None
            }
        })?
    }

    fn person_email_by_name(batch: &RecordBatch, name: &str) -> String {
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let email_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .find(|&i| name_col.value(i) == name)
            .map(|i| email_col.value(i).to_string())
            .unwrap()
    }

    fn test_dir(name: &str) -> TempDir {
        tempfile::Builder::new()
            .prefix(&format!("nanograph_{}_", name))
            .tempdir()
            .unwrap()
    }

    #[tokio::test]
    async fn test_init_creates_directory_structure() {
        let dir = test_dir("init");
        let path = dir.path();

        let db = Database::init(path, test_schema_src()).await.unwrap();

        assert!(path.join("schema.pg").exists());
        assert!(path.join("schema.ir.json").exists());
        assert!(path.join("graph.manifest.json").exists());
        assert!(path.join("nodes").exists());
        assert!(path.join("edges").exists());

        assert_eq!(db.catalog.node_types.len(), 2);
        assert_eq!(db.catalog.edge_types.len(), 2);
    }

    #[tokio::test]
    async fn test_open_fresh_db() {
        let dir = test_dir("open_fresh");
        let path = dir.path();

        Database::init(path, test_schema_src()).await.unwrap();
        let db = Database::open(path).await.unwrap();

        assert_eq!(db.catalog.node_types.len(), 2);
        assert_eq!(db.catalog.edge_types.len(), 2);
    }

    #[tokio::test]
    async fn test_load_and_reopen_preserves_data() {
        let dir = test_dir("load_reopen");
        let path = dir.path();

        // Init + load
        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        // Verify in-memory
        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 3);

        // Reopen
        let db2 = Database::open(path).await.unwrap();
        let persons2 = db2.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons2.num_rows(), 3);

        let companies = db2.storage.get_all_nodes("Company").unwrap().unwrap();
        assert_eq!(companies.num_rows(), 1);

        // Verify edges survived
        let knows_seg = &db2.storage.edge_segments["Knows"];
        assert_eq!(knows_seg.edge_ids.len(), 2);
        assert!(knows_seg.csr.is_some());
    }

    #[tokio::test]
    async fn test_load_deduplicates_edges() {
        let dir = test_dir("dedup_edges");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(duplicate_edge_data_src()).await.unwrap();

        let knows_seg = &db.storage.edge_segments["Knows"];
        assert_eq!(knows_seg.edge_ids.len(), 1);
    }

    #[tokio::test]
    async fn test_load_mode_overwrite_replaces_existing_data() {
        let dir = test_dir("mode_overwrite");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load_with_mode(test_data_src(), LoadMode::Overwrite)
            .await
            .unwrap();
        db.load_with_mode(
            r#"{"type": "Person", "data": {"name": "OnlyOne", "age": 77}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 1);
        let names = persons
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "OnlyOne");
    }

    #[tokio::test]
    async fn test_load_mode_append_adds_rows_and_can_reference_existing_nodes() {
        let dir = test_dir("mode_append");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load_with_mode(test_data_src(), LoadMode::Overwrite)
            .await
            .unwrap();
        db.load_with_mode(
            append_data_new_person_with_edge_to_existing(),
            LoadMode::Append,
        )
        .await
        .unwrap();

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 4);
        let alice_id = person_id_by_name(&persons, "Alice");
        let diana_id = person_id_by_name(&persons, "Diana");
        let knows = db.storage.edge_batch_for_save("Knows").unwrap().unwrap();
        let src = knows
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let dst = knows
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(
            (0..knows.num_rows())
                .any(|row| src.value(row) == diana_id && dst.value(row) == alice_id)
        );
    }

    #[tokio::test]
    async fn test_load_mode_merge_requires_keyed_schema() {
        let dir = test_dir("mode_merge_requires_key");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        let err = db
            .load_with_mode(test_data_src(), LoadMode::Merge)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires at least one node @key"));
    }

    #[tokio::test]
    async fn test_load_mode_merge_upserts_keyed_rows() {
        let dir = test_dir("mode_merge_keyed");
        let path = dir.path();

        let mut db = Database::init(path, keyed_schema_src()).await.unwrap();
        db.load_with_mode(keyed_data_initial(), LoadMode::Overwrite)
            .await
            .unwrap();
        db.load_with_mode(keyed_data_upsert(), LoadMode::Merge)
            .await
            .unwrap();

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 3);
        assert_eq!(person_age_by_name(&persons, "Alice"), Some(31));
    }

    #[tokio::test]
    async fn test_load_mode_append_rejects_duplicate_key_values() {
        let dir = test_dir("mode_append_key_duplicate");
        let path = dir.path();

        let mut db = Database::init(path, keyed_schema_src()).await.unwrap();
        db.load_with_mode(keyed_data_initial(), LoadMode::Overwrite)
            .await
            .unwrap();
        let err = db
            .load_with_mode(keyed_data_append_duplicate(), LoadMode::Append)
            .await
            .unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                ..
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "name");
                assert_eq!(value, "Alice");
            }
            other => panic!("expected UniqueConstraint, got {}", other),
        }
    }

    #[tokio::test]
    async fn test_load_modes_table_driven() {
        struct Case {
            name: &'static str,
            schema: &'static str,
            initial: &'static str,
            next_data: &'static str,
            mode: LoadMode,
            expected_person_rows: Option<usize>,
            expected_error_contains: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "overwrite",
                schema: test_schema_src(),
                initial: test_data_src(),
                next_data: r#"{"type": "Person", "data": {"name": "OnlyOne", "age": 1}}"#,
                mode: LoadMode::Overwrite,
                expected_person_rows: Some(1),
                expected_error_contains: None,
            },
            Case {
                name: "append",
                schema: test_schema_src(),
                initial: test_data_src(),
                next_data: r#"{"type": "Person", "data": {"name": "Eve", "age": 44}}"#,
                mode: LoadMode::Append,
                expected_person_rows: Some(4),
                expected_error_contains: None,
            },
            Case {
                name: "merge_requires_key",
                schema: test_schema_src(),
                initial: test_data_src(),
                next_data: r#"{"type": "Person", "data": {"name": "Eve", "age": 44}}"#,
                mode: LoadMode::Merge,
                expected_person_rows: None,
                expected_error_contains: Some("requires at least one node @key"),
            },
        ];

        for case in cases {
            let dir = test_dir(&format!("mode_table_{}", case.name));
            let path = dir.path();
            let mut db = Database::init(path, case.schema).await.unwrap();
            db.load_with_mode(case.initial, LoadMode::Overwrite)
                .await
                .unwrap();
            let result = db.load_with_mode(case.next_data, case.mode).await;

            if let Some(msg) = case.expected_error_contains {
                let err = result.unwrap_err();
                assert!(
                    err.to_string().contains(msg),
                    "case {} expected error containing '{}', got '{}'",
                    case.name,
                    msg,
                    err
                );
                continue;
            }

            result.unwrap();
            let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
            assert_eq!(
                persons.num_rows(),
                case.expected_person_rows.unwrap(),
                "case {} person row count",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn test_load_builds_scalar_indexes_for_indexed_properties() {
        let dir = test_dir("indexed_props");
        let path = dir.path();

        let mut db = Database::init(path, indexed_schema_src()).await.unwrap();
        db.load(indexed_data_src()).await.unwrap();

        let user = db
            .schema_ir
            .node_types()
            .find(|n| n.name == "Person")
            .expect("person node type");
        let expected_index_names: Vec<String> = user
            .properties
            .iter()
            .filter(|p| p.index)
            .map(|p| crate::store::indexing::scalar_index_name(user.type_id, &p.name))
            .collect();
        let dataset_path = path.join("nodes").join(SchemaIR::dir_name(user.type_id));

        let uri = dataset_path.to_string_lossy().to_string();
        let dataset = Dataset::open(&uri).await.unwrap();
        let index_names: HashSet<String> = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .map(|idx| idx.name.clone())
            .collect();
        for expected in &expected_index_names {
            assert!(
                index_names.contains(expected),
                "expected scalar index {} to exist",
                expected
            );
        }

        drop(db);
        Database::open(path).await.unwrap();
        let reopened = Dataset::open(&uri).await.unwrap();
        let reopened_names: HashSet<String> = reopened
            .load_indices()
            .await
            .unwrap()
            .iter()
            .map(|idx| idx.name.clone())
            .collect();
        for expected in &expected_index_names {
            assert!(
                reopened_names.contains(expected),
                "expected scalar index {} after reopen",
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_delete_nodes_cascades_edges() {
        let dir = test_dir("delete_cascade");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        let result = db
            .delete_nodes(
                "Person",
                &DeletePredicate {
                    property: "name".to_string(),
                    op: DeleteOp::Eq,
                    value: "Alice".to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(result.deleted_nodes, 1);
        assert_eq!(result.deleted_edges, 3);

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 2);
        let name_col = persons
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let mut names: Vec<String> = (0..persons.num_rows())
            .map(|i| name_col.value(i).to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Bob".to_string(), "Charlie".to_string()]);

        assert_eq!(db.storage.edge_segments["Knows"].edge_ids.len(), 0);
        assert_eq!(db.storage.edge_segments["WorksAt"].edge_ids.len(), 0);

        drop(db);
        let reopened = Database::open(path).await.unwrap();
        let persons2 = reopened.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons2.num_rows(), 2);
        assert_eq!(reopened.storage.edge_segments["Knows"].edge_ids.len(), 0);
        assert_eq!(reopened.storage.edge_segments["WorksAt"].edge_ids.len(), 0);
    }

    #[tokio::test]
    async fn test_load_reopen_query() {
        let dir = test_dir("load_query");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();
        drop(db);

        // Reopen and verify CSR is functional
        let db2 = Database::open(path).await.unwrap();
        let snapshot = db2.snapshot();

        // Find Alice's id from the Person node segment
        let persons = snapshot.get_all_nodes("Person").unwrap().unwrap();
        let id_col = persons
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let name_col = persons
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let alice_id = (0..persons.num_rows())
            .find(|&i| name_col.value(i) == "Alice")
            .map(|i| id_col.value(i))
            .expect("Alice not found");

        let knows_seg = &snapshot.edge_segments["Knows"];
        let csr = knows_seg.csr.as_ref().unwrap();
        let alice_friends = csr.neighbors(alice_id);
        assert_eq!(alice_friends.len(), 2);
    }

    #[tokio::test]
    async fn test_keyed_load_upsert_preserves_ids_and_remaps_edges() {
        let dir = test_dir("keyed_upsert");
        let path = dir.path();

        let mut db = Database::init(path, keyed_schema_src()).await.unwrap();
        db.load(keyed_data_initial()).await.unwrap();

        let persons_before = db.storage.get_all_nodes("Person").unwrap().unwrap();
        let alice_id_before = person_id_by_name(&persons_before, "Alice");
        let bob_id_before = person_id_by_name(&persons_before, "Bob");

        db.load(keyed_data_upsert()).await.unwrap();

        let persons_after = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons_after.num_rows(), 3);
        let alice_id_after = person_id_by_name(&persons_after, "Alice");
        let bob_id_after = person_id_by_name(&persons_after, "Bob");
        let charlie_id_after = person_id_by_name(&persons_after, "Charlie");

        assert_eq!(alice_id_after, alice_id_before);
        assert_eq!(bob_id_after, bob_id_before);
        assert_eq!(person_age_by_name(&persons_after, "Alice"), Some(31));

        let knows_seg = &db.storage.edge_segments["Knows"];
        assert_eq!(knows_seg.edge_ids.len(), 2);
        assert!(
            knows_seg
                .src_ids
                .iter()
                .zip(knows_seg.dst_ids.iter())
                .any(|(&src, &dst)| src == alice_id_after && dst == bob_id_after)
        );
        assert!(
            knows_seg
                .src_ids
                .iter()
                .zip(knows_seg.dst_ids.iter())
                .any(|(&src, &dst)| src == alice_id_after && dst == charlie_id_after)
        );

        let companies_after = db.storage.get_all_nodes("Company").unwrap().unwrap();
        assert_eq!(companies_after.num_rows(), 1);
        let company_name_col = companies_after
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(company_name_col.value(0), "Acme");
    }

    #[tokio::test]
    async fn test_unique_rejects_existing_existing_conflict() {
        let dir = test_dir("unique_existing_existing");
        let path = dir.path();

        let mut db = Database::init(path, unique_schema_src()).await.unwrap();

        let person_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(StringArray::from(vec![
                    "dupe@example.com",
                    "dupe@example.com",
                ])),
            ],
        )
        .unwrap();
        db.storage.insert_nodes("Person", batch).unwrap();

        let err = db.load("").await.unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                first_row,
                second_row,
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "email");
                assert_eq!(value, "dupe@example.com");
                assert_eq!(first_row, 0);
                assert_eq!(second_row, 1);
            }
            other => panic!("expected UniqueConstraint, got {}", other),
        }
    }

    #[tokio::test]
    async fn test_unique_rejects_existing_incoming_conflict() {
        let dir = test_dir("unique_existing_incoming");
        let path = dir.path();

        let mut db = Database::init(path, unique_schema_src()).await.unwrap();
        db.load(unique_data_initial()).await.unwrap();

        let err = db
            .load(unique_data_existing_incoming_conflict())
            .await
            .unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                ..
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "email");
                assert_eq!(value, "bob@example.com");
            }
            other => panic!("expected UniqueConstraint, got {}", other),
        }

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 2);
        assert_eq!(person_email_by_name(&persons, "Bob"), "bob@example.com");
    }

    #[tokio::test]
    async fn test_unique_rejects_incoming_incoming_conflict() {
        let dir = test_dir("unique_incoming_incoming");
        let path = dir.path();

        let mut db = Database::init(path, unique_schema_src()).await.unwrap();
        let err = db
            .load(unique_data_incoming_incoming_conflict())
            .await
            .unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                ..
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "email");
                assert_eq!(value, "charlie@example.com");
            }
            other => panic!("expected UniqueConstraint, got {}", other),
        }

        assert!(db.storage.get_all_nodes("Person").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_nullable_unique_allows_nulls_and_rejects_duplicate_non_null() {
        let dir = test_dir("nullable_unique");
        let path = dir.path();

        let mut db = Database::init(path, nullable_unique_schema_src())
            .await
            .unwrap();
        db.load(nullable_unique_ok_data()).await.unwrap();

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 2);
        let nick_col = persons
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(nick_col.is_null(0));
        assert!(nick_col.is_null(1));

        let err = db.load(nullable_unique_duplicate_data()).await.unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                ..
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "nick");
                assert_eq!(value, "ally");
            }
            other => panic!("expected UniqueConstraint, got {}", other),
        }

        let persons_after_err = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons_after_err.num_rows(), 2);
    }
}
