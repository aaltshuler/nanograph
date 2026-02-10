use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, UInt32Array, UInt64Array, UInt64Builder,
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
use crate::schema::ast::SchemaDecl;
use crate::schema::parser::parse_schema;
use crate::store::graph::GraphStorage;
use crate::store::loader::{json_values_to_array, load_jsonl_data, load_jsonl_data_with_name_seed};
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

    /// Load JSONL data.
    ///
    /// Default behavior is full replacement. If the schema contains `@key` on a node property,
    /// keyed node types are merged as upserts and existing IDs are preserved for matched keys.
    #[instrument(skip(self, data_source))]
    pub async fn load(&mut self, data_source: &str) -> Result<()> {
        info!("starting database load");
        let constraints = load_node_constraint_annotations(&self.path)?;
        let key_props = constraints.key_props;
        let mut incoming_storage = GraphStorage::new(self.catalog.clone());
        if key_props.is_empty() {
            load_jsonl_data(&mut incoming_storage, data_source)?;
        } else {
            let incoming_node_types = collect_incoming_node_types(data_source)?;
            let name_seed = build_name_seed_for_keyed_load(
                &self.storage,
                &self.schema_ir,
                &key_props,
                &incoming_node_types,
            )?;
            load_jsonl_data_with_name_seed(&mut incoming_storage, data_source, Some(&name_seed))?;
        }

        let mut next_storage = if key_props.is_empty() {
            incoming_storage
        } else {
            merge_storage_with_node_keys(
                &self.storage,
                &incoming_storage,
                &self.schema_ir,
                &key_props,
            )?
        };
        enforce_node_unique_constraints(&next_storage, &constraints.unique_props)?;
        next_storage.build_indices()?;
        self.storage = next_storage;
        self.persist_storage().await?;
        info!(
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

        let delete_mask = build_delete_mask(&target_batch, predicate)?;
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

fn build_delete_mask(batch: &RecordBatch, predicate: &DeletePredicate) -> Result<BooleanArray> {
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

#[derive(Debug, Default)]
struct NodeConstraintAnnotations {
    key_props: HashMap<String, String>,
    unique_props: HashMap<String, Vec<String>>,
}

fn load_node_constraint_annotations(db_path: &Path) -> Result<NodeConstraintAnnotations> {
    let schema_source = std::fs::read_to_string(db_path.join(SCHEMA_PG_FILENAME))?;
    let schema_file = parse_schema(&schema_source)?;
    let mut constraints = NodeConstraintAnnotations::default();

    for decl in schema_file.declarations {
        if let SchemaDecl::Node(node) = decl {
            let mut node_key_prop: Option<String> = None;
            let mut node_unique_props: Vec<String> = Vec::new();

            for prop in node.properties {
                let key_annotations: Vec<_> = prop
                    .annotations
                    .iter()
                    .filter(|a| a.name == "key")
                    .collect();
                if key_annotations.len() > 1 {
                    return Err(NanoError::Storage(format!(
                        "property {}.{} declares @key multiple times",
                        node.name, prop.name
                    )));
                }
                if let Some(annotation) = key_annotations.first() {
                    if annotation.value.is_some() {
                        return Err(NanoError::Storage(format!(
                            "@key on {}.{} does not accept a value",
                            node.name, prop.name
                        )));
                    }
                    if node_key_prop.replace(prop.name.clone()).is_some() {
                        return Err(NanoError::Storage(format!(
                            "node type {} has multiple @key properties; only one is currently supported",
                            node.name
                        )));
                    }
                }

                let unique_annotations: Vec<_> = prop
                    .annotations
                    .iter()
                    .filter(|a| a.name == "unique")
                    .collect();
                if unique_annotations.len() > 1 {
                    return Err(NanoError::Storage(format!(
                        "property {}.{} declares @unique multiple times",
                        node.name, prop.name
                    )));
                }
                if let Some(annotation) = unique_annotations.first() {
                    if annotation.value.is_some() {
                        return Err(NanoError::Storage(format!(
                            "@unique on {}.{} does not accept a value",
                            node.name, prop.name
                        )));
                    }
                    node_unique_props.push(prop.name.clone());
                }
            }

            if let Some(prop_name) = node_key_prop {
                if !node_unique_props.contains(&prop_name) {
                    node_unique_props.push(prop_name.clone());
                }
                constraints.key_props.insert(node.name.clone(), prop_name);
            }
            if !node_unique_props.is_empty() {
                node_unique_props.sort();
                node_unique_props.dedup();
                constraints
                    .unique_props
                    .insert(node.name.clone(), node_unique_props);
            }
        }
    }

    Ok(constraints)
}

fn enforce_node_unique_constraints(
    storage: &GraphStorage,
    unique_props: &HashMap<String, Vec<String>>,
) -> Result<()> {
    for (type_name, properties) in unique_props {
        let Some(batch) = storage.get_all_nodes(type_name)? else {
            continue;
        };

        for property in properties {
            let prop_idx = batch.schema().index_of(property).map_err(|e| {
                NanoError::Storage(format!(
                    "node type {} missing @unique property {}: {}",
                    type_name, property, e
                ))
            })?;
            let arr = batch.column(prop_idx);
            let mut seen: HashMap<String, usize> = HashMap::new();
            for row in 0..batch.num_rows() {
                let Some(value) = unique_value_string(arr, row, type_name, property)? else {
                    continue;
                };
                if let Some(prev_row) = seen.insert(value.clone(), row) {
                    return Err(NanoError::Storage(format!(
                        "@unique constraint violation on {}.{}: duplicate value '{}' at rows {} and {}",
                        type_name, property, value, prev_row, row
                    )));
                }
            }
        }
    }
    Ok(())
}

fn collect_incoming_node_types(data_source: &str) -> Result<HashSet<String>> {
    let mut node_types = HashSet::new();
    for line in data_source.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        let obj: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| NanoError::Storage(format!("JSON parse error: {}", e)))?;
        if let Some(type_name) = obj.get("type").and_then(|v| v.as_str()) {
            node_types.insert(type_name.to_string());
        }
    }
    Ok(node_types)
}

fn build_name_seed_for_keyed_load(
    storage: &GraphStorage,
    schema_ir: &SchemaIR,
    key_props: &HashMap<String, String>,
    incoming_node_types: &HashSet<String>,
) -> Result<HashMap<(String, String), u64>> {
    let mut seed = HashMap::new();

    for node_def in schema_ir.node_types() {
        let preserve_existing =
            key_props.contains_key(&node_def.name) || !incoming_node_types.contains(&node_def.name);
        if !preserve_existing {
            continue;
        }

        let Some(batch) = storage.get_all_nodes(&node_def.name)? else {
            continue;
        };
        let Some(name_col) = batch.column_by_name("name") else {
            continue;
        };
        let Some(name_arr) = name_col.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        let id_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "node type {} has non-UInt64 id column",
                    node_def.name
                ))
            })?;

        for row in 0..batch.num_rows() {
            if name_arr.is_null(row) {
                continue;
            }
            seed.insert(
                (node_def.name.clone(), name_arr.value(row).to_string()),
                id_arr.value(row),
            );
        }
    }

    Ok(seed)
}

fn merge_storage_with_node_keys(
    existing: &GraphStorage,
    incoming: &GraphStorage,
    schema_ir: &SchemaIR,
    key_props: &HashMap<String, String>,
) -> Result<GraphStorage> {
    let mut merged = GraphStorage::new(existing.catalog.clone());
    let mut next_node_id = existing.next_node_id();
    let mut next_edge_id = existing.next_edge_id();
    let mut id_remap_by_type: HashMap<String, HashMap<u64, u64>> = HashMap::new();
    let mut replaced_unkeyed_types: HashSet<String> = HashSet::new();

    for node_def in schema_ir.node_types() {
        let existing_batch = existing.get_all_nodes(&node_def.name)?;
        let incoming_batch = incoming.get_all_nodes(&node_def.name)?;

        if let Some(key_prop) = key_props.get(&node_def.name) {
            let (merged_batch, remap) = merge_keyed_node_batches(
                existing_batch.as_ref(),
                incoming_batch.as_ref(),
                key_prop,
                &mut next_node_id,
            )?;
            id_remap_by_type.insert(node_def.name.clone(), remap);
            if let Some(batch) = merged_batch {
                merged.load_node_batch(&node_def.name, batch)?;
            }
        } else {
            match (existing_batch.as_ref(), incoming_batch.as_ref()) {
                (_, Some(incoming_batch)) => {
                    let (reassigned, remap) = reassign_node_ids(incoming_batch, &mut next_node_id)?;
                    replaced_unkeyed_types.insert(node_def.name.clone());
                    id_remap_by_type.insert(node_def.name.clone(), remap);
                    merged.load_node_batch(&node_def.name, reassigned)?;
                }
                (Some(existing_batch), None) => {
                    id_remap_by_type.insert(node_def.name.clone(), HashMap::new());
                    merged.load_node_batch(&node_def.name, existing_batch.clone())?;
                }
                (None, None) => {
                    id_remap_by_type.insert(node_def.name.clone(), HashMap::new());
                }
            }
        }
    }

    for edge_def in schema_ir.edge_types() {
        let src_remap = id_remap_by_type
            .get(&edge_def.src_type_name)
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "missing source ID remap for node type {}",
                    edge_def.src_type_name
                ))
            })?;
        let dst_remap = id_remap_by_type
            .get(&edge_def.dst_type_name)
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "missing destination ID remap for node type {}",
                    edge_def.dst_type_name
                ))
            })?;
        let existing_batch = existing.edge_batch_for_save(&edge_def.name)?;
        let incoming_batch = incoming.edge_batch_for_save(&edge_def.name)?;
        let preserve_existing = !replaced_unkeyed_types.contains(&edge_def.src_type_name)
            && !replaced_unkeyed_types.contains(&edge_def.dst_type_name);

        let merged_edge_batch = merge_edge_batches(
            existing_batch.as_ref(),
            incoming_batch.as_ref(),
            src_remap,
            dst_remap,
            &edge_def.name,
            preserve_existing,
            &mut next_edge_id,
        )?;
        if let Some(batch) = merged_edge_batch {
            merged.load_edge_batch(&edge_def.name, batch)?;
        }
    }

    Ok(merged)
}

fn merge_keyed_node_batches(
    existing: Option<&RecordBatch>,
    incoming: Option<&RecordBatch>,
    key_prop: &str,
    next_node_id: &mut u64,
) -> Result<(Option<RecordBatch>, HashMap<u64, u64>)> {
    match (existing, incoming) {
        (None, None) => Ok((None, HashMap::new())),
        (Some(existing), None) => Ok((Some(existing.clone()), HashMap::new())),
        (None, Some(incoming)) => {
            let (reassigned, remap) = reassign_node_ids(incoming, next_node_id)?;
            Ok((Some(reassigned), remap))
        }
        (Some(existing), Some(incoming)) => {
            if existing.num_columns() != incoming.num_columns() {
                return Err(NanoError::Storage(format!(
                    "schema mismatch while merging keyed nodes on {}",
                    key_prop
                )));
            }

            let existing_key_idx = existing.schema().index_of(key_prop).map_err(|e| {
                NanoError::Storage(format!("missing key property {}: {}", key_prop, e))
            })?;
            let incoming_key_idx = incoming.schema().index_of(key_prop).map_err(|e| {
                NanoError::Storage(format!("missing key property {}: {}", key_prop, e))
            })?;

            let existing_id_arr = existing
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    NanoError::Storage("existing node batch id column is not UInt64".to_string())
                })?;
            let incoming_id_arr = incoming
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    NanoError::Storage("incoming node batch id column is not UInt64".to_string())
                })?;

            let mut existing_key_to_row: HashMap<String, usize> = HashMap::new();
            for row in 0..existing.num_rows() {
                let key = key_value_string(existing.column(existing_key_idx), row, key_prop)?;
                if existing_key_to_row.insert(key.clone(), row).is_some() {
                    return Err(NanoError::Storage(format!(
                        "existing data contains duplicate @key value '{}' for {}",
                        key, key_prop
                    )));
                }
            }

            let mut incoming_seen_keys: HashSet<String> = HashSet::new();
            let mut existing_updates: Vec<Option<usize>> = vec![None; existing.num_rows()];
            let mut new_rows: Vec<(usize, u64)> = Vec::new();
            let mut remap: HashMap<u64, u64> = HashMap::new();

            for incoming_row in 0..incoming.num_rows() {
                let key =
                    key_value_string(incoming.column(incoming_key_idx), incoming_row, key_prop)?;
                if !incoming_seen_keys.insert(key.clone()) {
                    return Err(NanoError::Storage(format!(
                        "incoming load contains duplicate @key value '{}' for {}",
                        key, key_prop
                    )));
                }

                let incoming_id = incoming_id_arr.value(incoming_row);
                if let Some(&existing_row) = existing_key_to_row.get(&key) {
                    existing_updates[existing_row] = Some(incoming_row);
                    remap.insert(incoming_id, existing_id_arr.value(existing_row));
                } else {
                    let assigned_id = *next_node_id;
                    *next_node_id = next_node_id.saturating_add(1);
                    new_rows.push((incoming_row, assigned_id));
                    remap.insert(incoming_id, assigned_id);
                }
            }

            let existing_schema = existing.schema();
            let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(existing.num_columns());
            for col_idx in 0..existing.num_columns() {
                if col_idx == 0 {
                    let mut id_builder =
                        UInt64Builder::with_capacity(existing.num_rows() + new_rows.len());
                    for row in 0..existing.num_rows() {
                        id_builder.append_value(existing_id_arr.value(row));
                    }
                    for (_, new_id) in &new_rows {
                        id_builder.append_value(*new_id);
                    }
                    out_columns.push(Arc::new(id_builder.finish()) as ArrayRef);
                    continue;
                }

                let mut values: Vec<serde_json::Value> =
                    Vec::with_capacity(existing.num_rows() + new_rows.len());
                for existing_row in 0..existing.num_rows() {
                    let (source, source_row) =
                        if let Some(incoming_row) = existing_updates[existing_row] {
                            (incoming.column(col_idx), incoming_row)
                        } else {
                            (existing.column(col_idx), existing_row)
                        };
                    values.push(array_value_to_json(source, source_row));
                }
                for (incoming_row, _) in &new_rows {
                    values.push(array_value_to_json(incoming.column(col_idx), *incoming_row));
                }

                let field = existing_schema.field(col_idx);
                let arr = json_values_to_array(&values, field.data_type(), field.is_nullable())?;
                out_columns.push(arr);
            }

            let out_batch = RecordBatch::try_new(existing_schema, out_columns)
                .map_err(|e| NanoError::Storage(format!("merge keyed batch error: {}", e)))?;
            Ok((Some(out_batch), remap))
        }
    }
}

fn reassign_node_ids(
    batch: &RecordBatch,
    next_node_id: &mut u64,
) -> Result<(RecordBatch, HashMap<u64, u64>)> {
    let id_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("node batch id column is not UInt64".to_string()))?;

    let mut remap = HashMap::new();
    let mut id_builder = UInt64Builder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let old_id = id_arr.value(row);
        let new_id = *next_node_id;
        *next_node_id = next_node_id.saturating_add(1);
        remap.insert(old_id, new_id);
        id_builder.append_value(new_id);
    }

    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    out_columns.push(Arc::new(id_builder.finish()) as ArrayRef);
    for col in batch.columns().iter().skip(1) {
        out_columns.push(col.clone());
    }

    let out_batch = RecordBatch::try_new(batch.schema(), out_columns)
        .map_err(|e| NanoError::Storage(format!("reassign node id batch error: {}", e)))?;
    Ok((out_batch, remap))
}

fn merge_edge_batches(
    existing: Option<&RecordBatch>,
    incoming: Option<&RecordBatch>,
    src_remap: &HashMap<u64, u64>,
    dst_remap: &HashMap<u64, u64>,
    edge_name: &str,
    preserve_existing: bool,
    next_edge_id: &mut u64,
) -> Result<Option<RecordBatch>> {
    let remapped_existing = if preserve_existing {
        existing.cloned()
    } else {
        None
    };
    let remapped_incoming = incoming
        .map(|batch| remap_edge_batch_endpoints(batch, src_remap, dst_remap, edge_name))
        .transpose()?;

    let schema = remapped_incoming
        .as_ref()
        .map(|b| b.schema())
        .or_else(|| remapped_existing.as_ref().map(|b| b.schema()));
    let Some(schema) = schema else {
        return Ok(None);
    };

    // No multigraph support: keep one row per (src, dst) edge.
    // Existing rows are loaded first and incoming rows overwrite duplicates.
    let mut row_order: Vec<(u64, u64)> = Vec::new();
    let mut row_props: HashMap<(u64, u64), Vec<serde_json::Value>> = HashMap::new();
    let prop_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if field.name() == "id" || field.name() == "src" || field.name() == "dst" {
                None
            } else {
                Some(idx)
            }
        })
        .collect();

    let mut ingest = |batch: &RecordBatch, overwrite: bool| -> Result<()> {
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

        for row in 0..batch.num_rows() {
            let key = (src_arr.value(row), dst_arr.value(row));
            let props = prop_indices
                .iter()
                .map(|&idx| array_value_to_json(batch.column(idx), row))
                .collect::<Vec<_>>();
            if row_props.contains_key(&key) {
                if overwrite {
                    row_props.insert(key, props);
                }
            } else {
                row_order.push(key);
                row_props.insert(key, props);
            }
        }

        Ok(())
    };

    if let Some(batch) = remapped_existing.as_ref() {
        ingest(batch, false)?;
    }
    if let Some(batch) = remapped_incoming.as_ref() {
        ingest(batch, true)?;
    }
    if row_order.is_empty() {
        return Ok(None);
    }

    let mut id_builder = UInt64Builder::with_capacity(row_order.len());
    let mut src_builder = UInt64Builder::with_capacity(row_order.len());
    let mut dst_builder = UInt64Builder::with_capacity(row_order.len());
    let mut prop_values: Vec<Vec<serde_json::Value>> = (0..prop_indices.len())
        .map(|_| Vec::with_capacity(row_order.len()))
        .collect();

    for (src, dst) in &row_order {
        let edge_id = *next_edge_id;
        *next_edge_id = next_edge_id.saturating_add(1);
        id_builder.append_value(edge_id);
        src_builder.append_value(*src);
        dst_builder.append_value(*dst);

        let props = row_props.get(&(*src, *dst)).ok_or_else(|| {
            NanoError::Storage(format!(
                "internal edge dedup error for {} at ({}, {})",
                edge_name, src, dst
            ))
        })?;
        for (idx, prop) in props.iter().enumerate() {
            prop_values[idx].push(prop.clone());
        }
    }

    let mut built_props: HashMap<String, ArrayRef> = HashMap::new();
    for (prop_pos, &col_idx) in prop_indices.iter().enumerate() {
        let field = schema.field(col_idx);
        let arr = json_values_to_array(
            &prop_values[prop_pos],
            field.data_type(),
            field.is_nullable(),
        )?;
        built_props.insert(field.name().clone(), arr);
    }

    let id_arr: ArrayRef = Arc::new(id_builder.finish());
    let src_arr: ArrayRef = Arc::new(src_builder.finish());
    let dst_arr: ArrayRef = Arc::new(dst_builder.finish());
    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        match field.name().as_str() {
            "id" => out_columns.push(id_arr.clone()),
            "src" => out_columns.push(src_arr.clone()),
            "dst" => out_columns.push(dst_arr.clone()),
            name => {
                let arr = built_props.get(name).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "missing merged edge property column {} for {}",
                        name, edge_name
                    ))
                })?;
                out_columns.push(arr.clone());
            }
        }
    }

    let batch = RecordBatch::try_new(schema, out_columns)
        .map_err(|e| NanoError::Storage(format!("edge merge batch error: {}", e)))?;
    Ok(Some(batch))
}

fn remap_edge_batch_endpoints(
    batch: &RecordBatch,
    src_remap: &HashMap<u64, u64>,
    dst_remap: &HashMap<u64, u64>,
    _edge_name: &str,
) -> Result<RecordBatch> {
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

    let mut src_builder = UInt64Builder::with_capacity(batch.num_rows());
    let mut dst_builder = UInt64Builder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let src = src_arr.value(row);
        let dst = dst_arr.value(row);
        let mapped_src = src_remap.get(&src).copied().unwrap_or(src);
        let mapped_dst = dst_remap.get(&dst).copied().unwrap_or(dst);
        src_builder.append_value(mapped_src);
        dst_builder.append_value(mapped_dst);
    }
    let src_arr: ArrayRef = Arc::new(src_builder.finish());
    let dst_arr: ArrayRef = Arc::new(dst_builder.finish());

    let mut out_columns = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        match field.name().as_str() {
            "src" => out_columns.push(src_arr.clone()),
            "dst" => out_columns.push(dst_arr.clone()),
            _ => out_columns.push(batch.column(idx).clone()),
        }
    }

    RecordBatch::try_new(batch.schema(), out_columns)
        .map_err(|e| NanoError::Storage(format!("edge remap batch error: {}", e)))
}

fn key_value_string(array: &ArrayRef, row: usize, prop_name: &str) -> Result<String> {
    let value = scalar_value_string(array, row, "key", None, prop_name)?;
    if let Some(value) = value {
        return Ok(value);
    }
    Err(NanoError::Storage(format!(
        "@key property {} cannot be null",
        prop_name
    )))
}

fn unique_value_string(
    array: &ArrayRef,
    row: usize,
    type_name: &str,
    prop_name: &str,
) -> Result<Option<String>> {
    scalar_value_string(array, row, "unique", Some(type_name), prop_name)
}

fn scalar_value_string(
    array: &ArrayRef,
    row: usize,
    annotation: &str,
    type_name: Option<&str>,
    prop_name: &str,
) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }

    use arrow::array::*;
    let value = match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string()),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string()),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| a.value(row).to_string()),
        _ => None,
    };

    let value = value.ok_or_else(|| {
        let target = match type_name {
            Some(name) => format!("{}.{}", name, prop_name),
            None => prop_name.to_string(),
        };
        NanoError::Storage(format!(
            "unsupported @{} data type {:?} for {}",
            annotation,
            array.data_type(),
            target
        ))
    })?;

    Ok(Some(value))
}

fn array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    use arrow::array::*;
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as u64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| {
                serde_json::Number::from_f64(a.value(row) as f64).map(serde_json::Value::Number)
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row)).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
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
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
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
        let msg = err.to_string();
        assert!(msg.contains("@unique constraint violation"));
        assert!(msg.contains("Person.email"));
        assert!(msg.contains("dupe@example.com"));
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
        let msg = err.to_string();
        assert!(msg.contains("@unique constraint violation"));
        assert!(msg.contains("Person.email"));
        assert!(msg.contains("bob@example.com"));

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
        let msg = err.to_string();
        assert!(msg.contains("@unique constraint violation"));
        assert!(msg.contains("Person.email"));
        assert!(msg.contains("charlie@example.com"));

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
        let msg = err.to_string();
        assert!(msg.contains("@unique constraint violation"));
        assert!(msg.contains("Person.nick"));
        assert!(msg.contains("ally"));

        let persons_after_err = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons_after_err.num_rows(), 2);
    }
}
