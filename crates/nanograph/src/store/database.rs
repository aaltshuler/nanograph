use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date64Array, Float32Array,
        Float64Array, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
};
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::cleanup::CleanupPolicyBuilder;
use lance::dataset::optimize::{CompactionOptions as LanceCompactionOptions, compact_files};
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SchemaIR, build_catalog_from_ir, build_schema_ir};
use crate::error::{NanoError, Result};
use crate::schema::parser::parse_schema;
use crate::store::graph::GraphStorage;
use crate::store::indexing::rebuild_node_scalar_indexes;
use crate::store::loader::{build_next_storage_for_load, json_values_to_array};
use crate::store::manifest::{DatasetEntry, GraphManifest, hash_string};
use crate::store::migration::reconcile_migration_sidecars;
use crate::store::txlog::{
    CdcLogEntry, commit_manifest_and_logs, prune_logs_for_replay_window, read_cdc_log_entries,
    read_tx_catalog_entries, read_visible_cdc_entries, reconcile_logs_to_manifest,
};

const SCHEMA_PG_FILENAME: &str = "schema.pg";
const SCHEMA_IR_FILENAME: &str = "schema.ir.json";
const CDC_ANALYTICS_DATASET_DIR: &str = "__cdc_analytics";
const CDC_ANALYTICS_STATE_FILE: &str = "__cdc_analytics.state.json";

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

#[derive(Debug, Clone, Copy)]
pub struct CompactOptions {
    pub target_rows_per_fragment: usize,
    pub materialize_deletions: bool,
    pub materialize_deletions_threshold: f32,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            target_rows_per_fragment: 1_048_576,
            materialize_deletions: true,
            materialize_deletions_threshold: 0.1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CompactResult {
    pub datasets_considered: usize,
    pub datasets_compacted: usize,
    pub fragments_removed: usize,
    pub fragments_added: usize,
    pub files_removed: usize,
    pub files_added: usize,
    pub manifest_committed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanupOptions {
    pub retain_tx_versions: u64,
    pub retain_dataset_versions: usize,
}

impl Default for CleanupOptions {
    fn default() -> Self {
        Self {
            retain_tx_versions: 128,
            retain_dataset_versions: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CleanupResult {
    pub tx_rows_removed: usize,
    pub tx_rows_kept: usize,
    pub cdc_rows_removed: usize,
    pub cdc_rows_kept: usize,
    pub datasets_cleaned: usize,
    pub dataset_old_versions_removed: u64,
    pub dataset_bytes_removed: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CdcAnalyticsMaterializeOptions {
    pub min_new_rows: usize,
    pub force: bool,
}

impl Default for CdcAnalyticsMaterializeOptions {
    fn default() -> Self {
        Self {
            min_new_rows: 0,
            force: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CdcAnalyticsMaterializeResult {
    pub source_rows: usize,
    pub previously_materialized_rows: usize,
    pub new_rows_since_last_run: usize,
    pub materialized_rows: usize,
    pub dataset_written: bool,
    pub skipped_by_threshold: bool,
    pub dataset_version: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct DoctorReport {
    pub healthy: bool,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
    pub manifest_db_version: u64,
    pub datasets_checked: usize,
    pub tx_rows: usize,
    pub cdc_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CdcAnalyticsState {
    rows_materialized: usize,
    manifest_db_version: u64,
    dataset_version: Option<u64>,
    updated_at_unix: String,
}

#[derive(Debug, Clone)]
enum MutationSource {
    Load { mode: LoadMode, data_source: String },
    PreparedStorage(GraphStorage),
}

#[derive(Debug, Clone)]
pub struct MutationPlan {
    source: MutationSource,
    pub op_summary: String,
    pub cdc_events: Vec<CdcLogEntry>,
}

impl MutationPlan {
    pub fn for_load(data_source: &str, mode: LoadMode) -> Self {
        Self {
            source: MutationSource::Load {
                mode,
                data_source: data_source.to_string(),
            },
            op_summary: load_mode_op_summary(mode).to_string(),
            cdc_events: Vec::new(),
        }
    }

    pub fn append_mutation(data_source: &str, op_summary: &str) -> Self {
        Self {
            source: MutationSource::Load {
                mode: LoadMode::Append,
                data_source: data_source.to_string(),
            },
            op_summary: op_summary.to_string(),
            cdc_events: Vec::new(),
        }
    }

    pub fn merge_mutation(data_source: &str, op_summary: &str) -> Self {
        Self {
            source: MutationSource::Load {
                mode: LoadMode::Merge,
                data_source: data_source.to_string(),
            },
            op_summary: op_summary.to_string(),
            cdc_events: Vec::new(),
        }
    }

    pub fn prepared_storage(storage: GraphStorage, op_summary: &str) -> Self {
        Self {
            source: MutationSource::PreparedStorage(storage),
            op_summary: op_summary.to_string(),
            cdc_events: Vec::new(),
        }
    }
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
        manifest.committed_at = now_unix_seconds_string();
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
        reconcile_logs_to_manifest(db_path, manifest.db_version)?;

        // Create storage and set ID counters
        let mut storage = GraphStorage::new(catalog.clone());
        storage.set_next_node_id(manifest.next_node_id);
        storage.set_next_edge_id(manifest.next_edge_id);

        // Load only datasets listed in the manifest (authoritative source)
        for entry in &manifest.datasets {
            let dataset_path = db_path.join(&entry.dataset_path);
            debug!(
                kind = %entry.kind,
                type_name = %entry.type_name,
                dataset_path = %dataset_path.display(),
                dataset_version = entry.dataset_version,
                row_count = entry.row_count,
                "restoring dataset from manifest"
            );
            match entry.kind.as_str() {
                "node" => {
                    let batches = read_lance_batches(&dataset_path, entry.dataset_version).await?;
                    for batch in batches {
                        storage.load_node_batch(&entry.type_name, batch)?;
                    }
                    storage.set_node_dataset_path(&entry.type_name, dataset_path);
                }
                "edge" => {
                    let batches = read_lance_batches(&dataset_path, entry.dataset_version).await?;
                    for batch in batches {
                        storage.load_edge_batch(&entry.type_name, batch)?;
                    }
                }
                other => {
                    return Err(NanoError::Manifest(format!(
                        "unknown dataset kind `{}` for type `{}`",
                        other, entry.type_name
                    )));
                }
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
        self.apply_mutation_plan(MutationPlan::for_load(data_source, mode))
            .await?;
        info!(
            mode = ?mode,
            node_types = self.storage.node_segments.len(),
            edge_types = self.storage.edge_segments.len(),
            "database load complete"
        );

        Ok(())
    }

    /// Apply one append-only mutation payload through the unified mutation path.
    pub async fn apply_append_mutation(
        &mut self,
        data_source: &str,
        op_summary: &str,
    ) -> Result<()> {
        self.apply_mutation_plan(MutationPlan::append_mutation(data_source, op_summary))
            .await
    }

    /// Apply one keyed-merge mutation payload through the unified mutation path.
    pub async fn apply_merge_mutation(
        &mut self,
        data_source: &str,
        op_summary: &str,
    ) -> Result<()> {
        self.apply_mutation_plan(MutationPlan::merge_mutation(data_source, op_summary))
            .await
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
        self.apply_mutation_plan(MutationPlan::prepared_storage(
            new_storage,
            "mutation:delete_nodes",
        ))
        .await?;

        Ok(DeleteResult {
            deleted_nodes: deleted_node_set.len(),
            deleted_edges,
        })
    }

    /// Delete edges of a given type matching a predicate.
    #[instrument(skip(self), fields(type_name = type_name, property = %predicate.property))]
    pub async fn delete_edges(
        &mut self,
        type_name: &str,
        predicate: &DeletePredicate,
    ) -> Result<DeleteResult> {
        if !self.catalog.edge_types.contains_key(type_name) {
            return Err(NanoError::Storage(format!(
                "unknown edge type `{}`",
                type_name
            )));
        }

        let target_batch = match self.storage.edge_batch_for_save(type_name)? {
            Some(batch) => batch,
            None => return Ok(DeleteResult::default()),
        };

        let delete_mask = build_delete_mask_for_mutation(&target_batch, predicate)?;
        let mut keep_builder = BooleanBuilder::with_capacity(target_batch.num_rows());
        for row in 0..target_batch.num_rows() {
            let delete = !delete_mask.is_null(row) && delete_mask.value(row);
            keep_builder.append_value(!delete);
        }
        let keep_mask = keep_builder.finish();
        let filtered_target = arrow::compute::filter_record_batch(&target_batch, &keep_mask)
            .map_err(|e| NanoError::Storage(format!("edge delete filter error: {}", e)))?;
        let deleted_edges = target_batch
            .num_rows()
            .saturating_sub(filtered_target.num_rows());
        if deleted_edges == 0 {
            return Ok(DeleteResult::default());
        }

        let old_next_node_id = self.storage.next_node_id();
        let old_next_edge_id = self.storage.next_edge_id();
        let mut new_storage = GraphStorage::new(self.catalog.clone());

        for node_def in self.schema_ir.node_types() {
            if let Some(batch) = self.storage.get_all_nodes(&node_def.name)? {
                new_storage.load_node_batch(&node_def.name, batch)?;
            }
        }

        for edge_def in self.schema_ir.edge_types() {
            if edge_def.name == type_name {
                if filtered_target.num_rows() > 0 {
                    new_storage.load_edge_batch(type_name, filtered_target.clone())?;
                }
                continue;
            }

            if let Some(batch) = self.storage.edge_batch_for_save(&edge_def.name)? {
                new_storage.load_edge_batch(&edge_def.name, batch)?;
            }
        }

        if new_storage.next_node_id() < old_next_node_id {
            new_storage.set_next_node_id(old_next_node_id);
        }
        if new_storage.next_edge_id() < old_next_edge_id {
            new_storage.set_next_edge_id(old_next_edge_id);
        }
        new_storage.build_indices()?;
        self.apply_mutation_plan(MutationPlan::prepared_storage(
            new_storage,
            "mutation:delete_edges",
        ))
        .await?;

        Ok(DeleteResult {
            deleted_nodes: 0,
            deleted_edges,
        })
    }

    /// Compact all manifest-tracked Lance datasets and commit updated dataset versions.
    pub async fn compact(&mut self, options: CompactOptions) -> Result<CompactResult> {
        let previous_manifest = GraphManifest::read(&self.path)?;
        reconcile_logs_to_manifest(&self.path, previous_manifest.db_version)?;
        let mut next_manifest = previous_manifest.clone();
        let mut result = CompactResult {
            datasets_considered: next_manifest.datasets.len(),
            ..Default::default()
        };

        for entry in &mut next_manifest.datasets {
            let dataset_path = self.path.join(&entry.dataset_path);
            let uri = dataset_path.to_string_lossy().to_string();
            let dataset = Dataset::open(&uri)
                .await
                .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
            let mut dataset = dataset
                .checkout_version(entry.dataset_version)
                .await
                .map_err(|e| {
                    NanoError::Lance(format!(
                        "checkout version {} error: {}",
                        entry.dataset_version, e
                    ))
                })?;
            let before_version = dataset.version().version;

            let mut compact_opts = LanceCompactionOptions::default();
            compact_opts.target_rows_per_fragment = options.target_rows_per_fragment;
            compact_opts.materialize_deletions = options.materialize_deletions;
            compact_opts.materialize_deletions_threshold = options.materialize_deletions_threshold;

            let metrics = compact_files(&mut dataset, compact_opts, None)
                .await
                .map_err(|e| NanoError::Lance(format!("compact error: {}", e)))?;
            result.fragments_removed += metrics.fragments_removed;
            result.fragments_added += metrics.fragments_added;
            result.files_removed += metrics.files_removed;
            result.files_added += metrics.files_added;

            let after_version = dataset.version().version;
            if after_version != before_version {
                entry.dataset_version = after_version;
                result.datasets_compacted += 1;
            }
        }

        if result.datasets_compacted > 0 {
            next_manifest.db_version = previous_manifest.db_version.saturating_add(1);
            next_manifest.last_tx_id = format!("manifest-{}", next_manifest.db_version);
            next_manifest.committed_at = now_unix_seconds_string();
            commit_manifest_and_logs(&self.path, &next_manifest, &[], "maintenance:compact")?;
            result.manifest_committed = true;
        }

        Ok(result)
    }

    /// Prune tx/CDC logs and old Lance dataset versions while preserving manifest-visible state.
    pub async fn cleanup(&mut self, options: CleanupOptions) -> Result<CleanupResult> {
        if options.retain_tx_versions == 0 {
            return Err(NanoError::Storage(
                "retain_tx_versions must be >= 1".to_string(),
            ));
        }
        if options.retain_dataset_versions == 0 {
            return Err(NanoError::Storage(
                "retain_dataset_versions must be >= 1".to_string(),
            ));
        }

        let manifest = GraphManifest::read(&self.path)?;
        reconcile_logs_to_manifest(&self.path, manifest.db_version)?;
        let log_prune = prune_logs_for_replay_window(&self.path, options.retain_tx_versions)?;
        let mut result = CleanupResult {
            tx_rows_removed: log_prune.tx_rows_removed,
            tx_rows_kept: log_prune.tx_rows_kept,
            cdc_rows_removed: log_prune.cdc_rows_removed,
            cdc_rows_kept: log_prune.cdc_rows_kept,
            ..Default::default()
        };

        for entry in &manifest.datasets {
            let dataset_path = self.path.join(&entry.dataset_path);
            let uri = dataset_path.to_string_lossy().to_string();
            let dataset = Dataset::open(&uri)
                .await
                .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
            // Ensure manifest-pinned version remains readable before cleanup.
            dataset
                .checkout_version(entry.dataset_version)
                .await
                .map_err(|e| {
                    NanoError::Lance(format!(
                        "checkout version {} error: {}",
                        entry.dataset_version, e
                    ))
                })?;

            let versions = dataset
                .versions()
                .await
                .map_err(|e| NanoError::Lance(format!("list versions error: {}", e)))?;
            let effective_retain_n = versions
                .iter()
                .position(|v| v.version == entry.dataset_version)
                .map(|idx| {
                    let needed_for_manifest = versions.len().saturating_sub(idx);
                    options.retain_dataset_versions.max(needed_for_manifest)
                })
                .unwrap_or(options.retain_dataset_versions);
            let policy = CleanupPolicyBuilder::default()
                .retain_n_versions(&dataset, effective_retain_n)
                .await
                .map_err(|e| NanoError::Lance(format!("cleanup policy error: {}", e)))?
                .build();
            let stats = dataset
                .cleanup_with_policy(policy)
                .await
                .map_err(|e| NanoError::Lance(format!("cleanup error: {}", e)))?;
            if stats.old_versions > 0 {
                result.datasets_cleaned += 1;
            }
            result.dataset_old_versions_removed += stats.old_versions;
            result.dataset_bytes_removed += stats.bytes_removed;
        }

        Ok(result)
    }

    /// Materialize visible CDC rows into a derived Lance dataset for analytics workloads.
    ///
    /// JSONL remains the authoritative CDC source; this dataset is best-effort acceleration.
    pub async fn materialize_cdc_analytics(
        &self,
        options: CdcAnalyticsMaterializeOptions,
    ) -> Result<CdcAnalyticsMaterializeResult> {
        let manifest = GraphManifest::read(&self.path)?;
        reconcile_logs_to_manifest(&self.path, manifest.db_version)?;

        let rows = read_visible_cdc_entries(&self.path, 0, Some(manifest.db_version))?;
        let source_rows = rows.len();
        let state = read_cdc_analytics_state(&self.path)?;
        let previous_rows = state.rows_materialized;
        let shrank = source_rows < previous_rows || manifest.db_version < state.manifest_db_version;
        let new_rows_since_last_run = if shrank {
            source_rows
        } else {
            source_rows.saturating_sub(previous_rows)
        };

        if !options.force
            && !shrank
            && options.min_new_rows > 0
            && new_rows_since_last_run < options.min_new_rows
        {
            return Ok(CdcAnalyticsMaterializeResult {
                source_rows,
                previously_materialized_rows: previous_rows,
                new_rows_since_last_run,
                materialized_rows: previous_rows.min(source_rows),
                dataset_written: false,
                skipped_by_threshold: true,
                dataset_version: state.dataset_version,
            });
        }

        let dataset_path = cdc_analytics_dataset_path(&self.path);
        let mut dataset_written = false;
        let dataset_version = if rows.is_empty() {
            if dataset_path.exists() {
                std::fs::remove_dir_all(&dataset_path)?;
                dataset_written = true;
            }
            None
        } else {
            let batch = cdc_rows_to_analytics_batch(&rows)?;
            let version = write_lance_batch(&dataset_path, batch).await?;
            dataset_written = true;
            Some(version)
        };

        write_cdc_analytics_state(
            &self.path,
            &CdcAnalyticsState {
                rows_materialized: source_rows,
                manifest_db_version: manifest.db_version,
                dataset_version,
                updated_at_unix: now_unix_seconds_string(),
            },
        )?;

        Ok(CdcAnalyticsMaterializeResult {
            source_rows,
            previously_materialized_rows: previous_rows,
            new_rows_since_last_run,
            materialized_rows: source_rows,
            dataset_written,
            skipped_by_threshold: false,
            dataset_version,
        })
    }

    /// Validate manifest/log/dataset consistency and in-memory graph integrity.
    pub async fn doctor(&self) -> Result<DoctorReport> {
        let manifest = GraphManifest::read(&self.path)?;
        reconcile_logs_to_manifest(&self.path, manifest.db_version)?;
        let mut issues = Vec::new();
        let mut warnings = Vec::new();

        let tx_rows = read_tx_catalog_entries(&self.path)?;
        for (idx, window) in tx_rows.windows(2).enumerate() {
            if window[0].db_version >= window[1].db_version {
                issues.push(format!(
                    "non-monotonic tx db_version at rows {} and {}",
                    idx + 1,
                    idx + 2
                ));
            }
        }
        if let Some(last) = tx_rows.last() {
            if last.db_version > manifest.db_version {
                issues.push(format!(
                    "tx catalog db_version {} exceeds manifest db_version {}",
                    last.db_version, manifest.db_version
                ));
            } else if last.db_version < manifest.db_version {
                warnings.push(format!(
                    "tx catalog trimmed to db_version {} while manifest is {}",
                    last.db_version, manifest.db_version
                ));
            }
        }

        let mut datasets_checked = 0usize;
        for entry in &manifest.datasets {
            let dataset_path = self.path.join(&entry.dataset_path);
            if !dataset_path.exists() {
                issues.push(format!("dataset path missing: {}", dataset_path.display()));
                continue;
            }
            let uri = dataset_path.to_string_lossy().to_string();
            match Dataset::open(&uri).await {
                Ok(dataset) => {
                    if let Err(e) = dataset.checkout_version(entry.dataset_version).await {
                        issues.push(format!(
                            "dataset {} missing pinned version {}: {}",
                            entry.dataset_path, entry.dataset_version, e
                        ));
                    } else {
                        datasets_checked += 1;
                    }
                }
                Err(e) => {
                    issues.push(format!(
                        "failed to open dataset {}: {}",
                        entry.dataset_path, e
                    ));
                }
            }
        }

        for edge_def in self.schema_ir.edge_types() {
            let src_nodes =
                collect_existing_ids(self.storage.get_all_nodes(&edge_def.src_type_name)?)?;
            let dst_nodes =
                collect_existing_ids(self.storage.get_all_nodes(&edge_def.dst_type_name)?)?;
            if let Some(edge_batch) = self.storage.edge_batch_for_save(&edge_def.name)? {
                let src_arr = edge_batch
                    .column_by_name("src")
                    .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        NanoError::Storage("edge src column is not UInt64".to_string())
                    })?;
                let dst_arr = edge_batch
                    .column_by_name("dst")
                    .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        NanoError::Storage("edge dst column is not UInt64".to_string())
                    })?;
                let mut orphan_count = 0usize;
                for row in 0..edge_batch.num_rows() {
                    if !src_nodes.contains(&src_arr.value(row))
                        || !dst_nodes.contains(&dst_arr.value(row))
                    {
                        orphan_count += 1;
                    }
                }
                if orphan_count > 0 {
                    issues.push(format!(
                        "edge type {} has {} orphan endpoint row(s)",
                        edge_def.name, orphan_count
                    ));
                }
            }
        }

        let cdc_rows = read_cdc_log_entries(&self.path)?;
        let healthy = issues.is_empty();
        Ok(DoctorReport {
            healthy,
            issues,
            warnings,
            manifest_db_version: manifest.db_version,
            datasets_checked,
            tx_rows: tx_rows.len(),
            cdc_rows: cdc_rows.len(),
        })
    }

    async fn apply_mutation_plan(&mut self, plan: MutationPlan) -> Result<()> {
        let MutationPlan {
            source,
            op_summary,
            cdc_events,
        } = plan;
        let previous_storage = self.storage.clone();
        let next_storage = match source {
            MutationSource::Load { mode, data_source } => {
                build_next_storage_for_load(
                    &self.path,
                    &self.storage,
                    &self.schema_ir,
                    &data_source,
                    mode,
                )
                .await?
            }
            MutationSource::PreparedStorage(storage) => storage,
        };
        let effective_cdc_events = if cdc_events.is_empty() {
            build_cdc_events_for_storage_transition(
                &previous_storage,
                &next_storage,
                &self.schema_ir,
            )?
        } else {
            cdc_events
        };
        self.storage = next_storage;
        self.persist_storage_with_cdc(&op_summary, &effective_cdc_events)
            .await
    }

    async fn persist_storage_with_cdc(
        &mut self,
        op_summary: &str,
        cdc_events: &[CdcLogEntry],
    ) -> Result<()> {
        let previous_manifest = GraphManifest::read(&self.path)?;
        self.storage.clear_node_dataset_paths();
        let mut dataset_entries = Vec::new();
        let mut previous_entries_by_key: HashMap<String, DatasetEntry> = HashMap::new();
        for entry in &previous_manifest.datasets {
            previous_entries_by_key.insert(
                dataset_entity_key(&entry.kind, &entry.type_name),
                entry.clone(),
            );
        }

        let mut changed_entities: HashSet<String> = HashSet::new();
        let mut non_insert_entities: HashSet<String> = HashSet::new();
        let mut non_upsert_entities: HashSet<String> = HashSet::new();
        let mut non_delete_entities: HashSet<String> = HashSet::new();
        let mut insert_events_by_entity: HashMap<String, Vec<&CdcLogEntry>> = HashMap::new();
        let mut upsert_events_by_entity: HashMap<String, Vec<&CdcLogEntry>> = HashMap::new();
        let mut delete_events_by_entity: HashMap<String, Vec<&CdcLogEntry>> = HashMap::new();
        for event in cdc_events {
            let key = dataset_entity_key(&event.entity_kind, &event.type_name);
            changed_entities.insert(key.clone());
            if event.op == "insert" {
                insert_events_by_entity
                    .entry(key.clone())
                    .or_default()
                    .push(event);
                upsert_events_by_entity.entry(key).or_default().push(event);
                non_delete_entities
                    .insert(dataset_entity_key(&event.entity_kind, &event.type_name));
            } else if event.op == "update" {
                non_insert_entities.insert(key.clone());
                upsert_events_by_entity.entry(key).or_default().push(event);
                non_delete_entities
                    .insert(dataset_entity_key(&event.entity_kind, &event.type_name));
            } else if event.op == "delete" {
                non_insert_entities.insert(key.clone());
                non_upsert_entities.insert(key.clone());
                delete_events_by_entity.entry(key).or_default().push(event);
            } else {
                non_insert_entities.insert(key.clone());
                non_upsert_entities.insert(key);
                non_delete_entities
                    .insert(dataset_entity_key(&event.entity_kind, &event.type_name));
            }
        }
        let append_only_commit = op_summary == "load:append";
        let merge_commit = op_summary == "load:merge" || op_summary == "mutation:update_node";

        // Write each node type to Lance
        for node_def in self.schema_ir.node_types() {
            if let Some(batch) = self.storage.get_all_nodes(&node_def.name)? {
                let entity_key = dataset_entity_key("node", &node_def.name);
                let previous_entry = previous_entries_by_key.get(&entity_key).cloned();

                if !changed_entities.contains(&entity_key) {
                    if let Some(prev) = previous_entry {
                        self.storage.set_node_dataset_path(
                            &node_def.name,
                            self.path.join(&prev.dataset_path),
                        );
                        dataset_entries.push(prev);
                        continue;
                    }
                }

                let row_count = batch.num_rows() as u64;
                let dataset_rel_path = previous_entry
                    .as_ref()
                    .map(|entry| entry.dataset_path.clone())
                    .unwrap_or_else(|| format!("nodes/{}", SchemaIR::dir_name(node_def.type_id)));
                let dataset_path = self.path.join(&dataset_rel_path);
                let key_prop = node_def
                    .properties
                    .iter()
                    .find(|prop| prop.key)
                    .map(|prop| prop.name.as_str());
                let can_merge_upsert = merge_commit
                    && previous_entry.is_some()
                    && key_prop.is_some()
                    && changed_entities.contains(&entity_key)
                    && !non_upsert_entities.contains(&entity_key);
                let can_append = append_only_commit
                    && previous_entry.is_some()
                    && changed_entities.contains(&entity_key)
                    && !non_insert_entities.contains(&entity_key);
                let can_native_delete = previous_entry.is_some()
                    && changed_entities.contains(&entity_key)
                    && !non_delete_entities.contains(&entity_key);
                let dataset_version = if can_merge_upsert {
                    let upsert_events = upsert_events_by_entity
                        .get(&entity_key)
                        .map(|rows| rows.as_slice())
                        .unwrap_or(&[]);
                    match build_upsert_batch_from_cdc(batch.schema(), upsert_events)? {
                        Some(source_batch) if source_batch.num_rows() > 0 => {
                            let key_prop = key_prop.unwrap_or_default();
                            let pinned_version = previous_entry
                                .as_ref()
                                .map(|entry| entry.dataset_version)
                                .ok_or_else(|| {
                                    NanoError::Storage(format!(
                                        "missing previous dataset version for {}",
                                        node_def.name
                                    ))
                                })?;
                            debug!(
                                node_type = %node_def.name,
                                rows = source_batch.num_rows(),
                                key_prop = key_prop,
                                "merging node rows into existing Lance dataset"
                            );
                            run_lance_merge_insert_with_key(
                                &dataset_path,
                                pinned_version,
                                source_batch,
                                key_prop,
                            )
                            .await?
                        }
                        _ => previous_entry
                            .as_ref()
                            .map(|entry| entry.dataset_version)
                            .unwrap_or(0),
                    }
                } else if can_append {
                    let insert_events = insert_events_by_entity
                        .get(&entity_key)
                        .map(|rows| rows.as_slice())
                        .unwrap_or(&[]);
                    match build_append_batch_from_cdc(batch.schema(), insert_events)? {
                        Some(delta_batch) if delta_batch.num_rows() > 0 => {
                            debug!(
                                node_type = %node_def.name,
                                rows = delta_batch.num_rows(),
                                "appending node rows to existing Lance dataset"
                            );
                            write_lance_batch_with_mode(
                                &dataset_path,
                                delta_batch,
                                WriteMode::Append,
                            )
                            .await?
                        }
                        _ => previous_entry
                            .as_ref()
                            .map(|entry| entry.dataset_version)
                            .unwrap_or(0),
                    }
                } else if can_native_delete {
                    let delete_events = delete_events_by_entity
                        .get(&entity_key)
                        .map(|rows| rows.as_slice())
                        .unwrap_or(&[]);
                    let delete_ids = deleted_ids_from_cdc_events(delete_events)?;
                    let pinned_version = previous_entry
                        .as_ref()
                        .map(|entry| entry.dataset_version)
                        .ok_or_else(|| {
                            NanoError::Storage(format!(
                                "missing previous dataset version for {}",
                                node_def.name
                            ))
                        })?;
                    debug!(
                        node_type = %node_def.name,
                        rows = delete_ids.len(),
                        "deleting node rows from existing Lance dataset"
                    );
                    run_lance_delete_by_ids(&dataset_path, pinned_version, &delete_ids).await?
                } else {
                    debug!(
                        node_type = %node_def.name,
                        rows = row_count,
                        "writing node dataset"
                    );
                    write_lance_batch(&dataset_path, batch).await?
                };
                rebuild_node_scalar_indexes(&dataset_path, node_def).await?;
                self.storage
                    .set_node_dataset_path(&node_def.name, dataset_path.clone());
                dataset_entries.push(DatasetEntry {
                    type_id: node_def.type_id,
                    type_name: node_def.name.clone(),
                    kind: "node".to_string(),
                    dataset_path: dataset_rel_path,
                    dataset_version,
                    row_count,
                });
            }
        }

        // Write each edge type to Lance
        for edge_def in self.schema_ir.edge_types() {
            if let Some(batch) = self.storage.edge_batch_for_save(&edge_def.name)? {
                let entity_key = dataset_entity_key("edge", &edge_def.name);
                let previous_entry = previous_entries_by_key.get(&entity_key).cloned();

                if !changed_entities.contains(&entity_key) {
                    if let Some(prev) = previous_entry {
                        dataset_entries.push(prev);
                        continue;
                    }
                }

                let row_count = batch.num_rows() as u64;
                let dataset_rel_path = previous_entry
                    .as_ref()
                    .map(|entry| entry.dataset_path.clone())
                    .unwrap_or_else(|| format!("edges/{}", SchemaIR::dir_name(edge_def.type_id)));
                let dataset_path = self.path.join(&dataset_rel_path);
                let can_append = append_only_commit
                    && previous_entry.is_some()
                    && changed_entities.contains(&entity_key)
                    && !non_insert_entities.contains(&entity_key);
                let can_native_delete = previous_entry.is_some()
                    && changed_entities.contains(&entity_key)
                    && !non_delete_entities.contains(&entity_key);
                let dataset_version = if can_append {
                    let insert_events = insert_events_by_entity
                        .get(&entity_key)
                        .map(|rows| rows.as_slice())
                        .unwrap_or(&[]);
                    match build_append_batch_from_cdc(batch.schema(), insert_events)? {
                        Some(delta_batch) if delta_batch.num_rows() > 0 => {
                            debug!(
                                edge_type = %edge_def.name,
                                rows = delta_batch.num_rows(),
                                "appending edge rows to existing Lance dataset"
                            );
                            write_lance_batch_with_mode(
                                &dataset_path,
                                delta_batch,
                                WriteMode::Append,
                            )
                            .await?
                        }
                        _ => previous_entry
                            .as_ref()
                            .map(|entry| entry.dataset_version)
                            .unwrap_or(0),
                    }
                } else if can_native_delete {
                    let delete_events = delete_events_by_entity
                        .get(&entity_key)
                        .map(|rows| rows.as_slice())
                        .unwrap_or(&[]);
                    let delete_ids = deleted_ids_from_cdc_events(delete_events)?;
                    let pinned_version = previous_entry
                        .as_ref()
                        .map(|entry| entry.dataset_version)
                        .ok_or_else(|| {
                            NanoError::Storage(format!(
                                "missing previous dataset version for {}",
                                edge_def.name
                            ))
                        })?;
                    debug!(
                        edge_type = %edge_def.name,
                        rows = delete_ids.len(),
                        "deleting edge rows from existing Lance dataset"
                    );
                    run_lance_delete_by_ids(&dataset_path, pinned_version, &delete_ids).await?
                } else {
                    debug!(
                        edge_type = %edge_def.name,
                        rows = row_count,
                        "writing edge dataset"
                    );
                    write_lance_batch(&dataset_path, batch).await?
                };
                dataset_entries.push(DatasetEntry {
                    type_id: edge_def.type_id,
                    type_name: edge_def.name.clone(),
                    kind: "edge".to_string(),
                    dataset_path: dataset_rel_path,
                    dataset_version,
                    row_count,
                });
            }
        }

        // Update manifest
        let ir_json = serde_json::to_string_pretty(&self.schema_ir)
            .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
        let ir_hash = hash_string(&ir_json);

        let mut manifest = GraphManifest::new(ir_hash);
        manifest.db_version = previous_manifest.db_version.saturating_add(1);
        manifest.last_tx_id = format!("manifest-{}", manifest.db_version);
        manifest.committed_at = now_unix_seconds_string();
        manifest.next_node_id = self.storage.next_node_id();
        manifest.next_edge_id = self.storage.next_edge_id();
        let (next_type_id, next_prop_id) = next_schema_identity_counters(&self.schema_ir);
        manifest.next_type_id = next_type_id;
        manifest.next_prop_id = next_prop_id;
        manifest.schema_identity_version = previous_manifest.schema_identity_version.max(1);
        manifest.datasets = dataset_entries;

        let committed_cdc_events = finalize_cdc_entries_for_manifest(cdc_events, &manifest);
        commit_manifest_and_logs(&self.path, &manifest, &committed_cdc_events, op_summary)?;

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

fn build_cdc_events_for_storage_transition(
    previous: &GraphStorage,
    next: &GraphStorage,
    schema_ir: &SchemaIR,
) -> Result<Vec<CdcLogEntry>> {
    let mut events = Vec::new();

    for node_def in schema_ir.node_types() {
        let before_rows = collect_rows_by_id(previous.get_all_nodes(&node_def.name)?)?;
        let after_rows = collect_rows_by_id(next.get_all_nodes(&node_def.name)?)?;
        append_entity_diff_events(
            &mut events,
            "node",
            &node_def.name,
            &before_rows,
            &after_rows,
        );
    }

    for edge_def in schema_ir.edge_types() {
        let before_rows = collect_rows_by_id(previous.edge_batch_for_save(&edge_def.name)?)?;
        let after_rows = collect_rows_by_id(next.edge_batch_for_save(&edge_def.name)?)?;
        append_entity_diff_events(
            &mut events,
            "edge",
            &edge_def.name,
            &before_rows,
            &after_rows,
        );
    }

    Ok(events)
}

fn collect_rows_by_id(
    batch: Option<RecordBatch>,
) -> Result<BTreeMap<u64, serde_json::Map<String, serde_json::Value>>> {
    let mut rows = BTreeMap::new();
    let Some(batch) = batch else {
        return Ok(rows);
    };

    let id_arr = batch
        .column_by_name("id")
        .ok_or_else(|| NanoError::Storage("batch missing id column for CDC".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("CDC id column is not UInt64".to_string()))?;

    for row in 0..batch.num_rows() {
        rows.insert(
            id_arr.value(row),
            record_batch_row_to_json_map(&batch, row)?,
        );
    }

    Ok(rows)
}

fn record_batch_row_to_json_map(
    batch: &RecordBatch,
    row: usize,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut map = serde_json::Map::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let value = cdc_array_value_to_json(batch.column(col_idx), row);
        map.insert(field.name().clone(), value);
    }
    Ok(map)
}

fn cdc_array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }

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
        _ => serde_json::Value::String(
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default(),
        ),
    }
}

fn append_entity_diff_events(
    out: &mut Vec<CdcLogEntry>,
    entity_kind: &str,
    type_name: &str,
    before_rows: &BTreeMap<u64, serde_json::Map<String, serde_json::Value>>,
    after_rows: &BTreeMap<u64, serde_json::Map<String, serde_json::Value>>,
) {
    for (id, before) in before_rows {
        if !after_rows.contains_key(id) {
            out.push(make_pending_cdc_event(
                "delete",
                entity_kind,
                type_name,
                *id,
                before,
                serde_json::Value::Object(before.clone()),
            ));
        }
    }

    for (id, after) in after_rows {
        if let Some(before) = before_rows.get(id) {
            if before != after {
                out.push(make_pending_cdc_event(
                    "update",
                    entity_kind,
                    type_name,
                    *id,
                    after,
                    serde_json::json!({
                        "before": before,
                        "after": after,
                    }),
                ));
            }
        }
    }

    for (id, after) in after_rows {
        if !before_rows.contains_key(id) {
            out.push(make_pending_cdc_event(
                "insert",
                entity_kind,
                type_name,
                *id,
                after,
                serde_json::Value::Object(after.clone()),
            ));
        }
    }
}

fn make_pending_cdc_event(
    op: &str,
    entity_kind: &str,
    type_name: &str,
    id: u64,
    row: &serde_json::Map<String, serde_json::Value>,
    payload: serde_json::Value,
) -> CdcLogEntry {
    CdcLogEntry {
        tx_id: String::new(),
        db_version: 0,
        seq_in_tx: 0,
        op: op.to_string(),
        entity_kind: entity_kind.to_string(),
        type_name: type_name.to_string(),
        entity_key: cdc_entity_key(entity_kind, id, row),
        payload,
        committed_at: String::new(),
    }
}

fn cdc_entity_key(
    entity_kind: &str,
    id: u64,
    row: &serde_json::Map<String, serde_json::Value>,
) -> String {
    if entity_kind == "edge" {
        let src = row.get("src").and_then(|v| v.as_u64());
        let dst = row.get("dst").and_then(|v| v.as_u64());
        if let (Some(src), Some(dst)) = (src, dst) {
            return format!("id={},src={},dst={}", id, src, dst);
        }
    }
    format!("id={}", id)
}

fn dataset_entity_key(kind: &str, type_name: &str) -> String {
    format!("{}:{}", kind, type_name)
}

fn build_append_batch_from_cdc(
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    insert_events: &[&CdcLogEntry],
) -> Result<Option<RecordBatch>> {
    if insert_events.is_empty() {
        return Ok(None);
    }

    let mut values_by_column: Vec<Vec<serde_json::Value>> = schema
        .fields()
        .iter()
        .map(|_| Vec::with_capacity(insert_events.len()))
        .collect();

    for event in insert_events {
        let payload = event.payload.as_object().ok_or_else(|| {
            NanoError::Storage(format!(
                "CDC insert payload must be object for {} {}",
                event.entity_kind, event.type_name
            ))
        })?;
        for (idx, field) in schema.fields().iter().enumerate() {
            values_by_column[idx].push(
                payload
                    .get(field.name())
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            );
        }
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let arr = json_values_to_array(
            &values_by_column[idx],
            field.data_type(),
            field.is_nullable(),
        )?;
        columns.push(arr);
    }

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| NanoError::Storage(format!("append CDC batch build error: {}", e)))?;
    Ok(Some(batch))
}

fn build_upsert_batch_from_cdc(
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    upsert_events: &[&CdcLogEntry],
) -> Result<Option<RecordBatch>> {
    if upsert_events.is_empty() {
        return Ok(None);
    }

    let mut values_by_column: Vec<Vec<serde_json::Value>> = schema
        .fields()
        .iter()
        .map(|_| Vec::with_capacity(upsert_events.len()))
        .collect();

    for event in upsert_events {
        let row = match event.op.as_str() {
            "insert" => event.payload.as_object(),
            "update" => event
                .payload
                .get("after")
                .and_then(|value| value.as_object()),
            op => {
                return Err(NanoError::Storage(format!(
                    "unsupported CDC op '{}' for upsert source",
                    op
                )));
            }
        }
        .ok_or_else(|| {
            NanoError::Storage(format!(
                "CDC {} payload missing object row for {} {}",
                event.op, event.entity_kind, event.type_name
            ))
        })?;

        for (idx, field) in schema.fields().iter().enumerate() {
            values_by_column[idx].push(
                row.get(field.name())
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            );
        }
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let arr = json_values_to_array(
            &values_by_column[idx],
            field.data_type(),
            field.is_nullable(),
        )?;
        columns.push(arr);
    }

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| NanoError::Storage(format!("upsert CDC batch build error: {}", e)))?;
    Ok(Some(batch))
}

fn deleted_ids_from_cdc_events(events: &[&CdcLogEntry]) -> Result<Vec<u64>> {
    let mut ids = Vec::with_capacity(events.len());
    for event in events {
        let id = cdc_deleted_entity_id(event).ok_or_else(|| {
            NanoError::Storage(format!(
                "CDC delete payload missing id for {} {}",
                event.entity_kind, event.type_name
            ))
        })?;
        ids.push(id);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn cdc_deleted_entity_id(event: &CdcLogEntry) -> Option<u64> {
    if let Some(id) = event
        .payload
        .as_object()
        .and_then(|row| row.get("id"))
        .and_then(|value| value.as_u64())
    {
        return Some(id);
    }

    event
        .entity_key
        .strip_prefix("id=")
        .and_then(|raw| raw.split(',').next())
        .and_then(|value| value.parse::<u64>().ok())
}

fn finalize_cdc_entries_for_manifest(
    cdc_events: &[CdcLogEntry],
    manifest: &GraphManifest,
) -> Vec<CdcLogEntry> {
    cdc_events
        .iter()
        .enumerate()
        .map(|(seq, entry)| CdcLogEntry {
            tx_id: manifest.last_tx_id.clone(),
            db_version: manifest.db_version,
            seq_in_tx: seq.min(u32::MAX as usize) as u32,
            op: entry.op.clone(),
            entity_kind: entry.entity_kind.clone(),
            type_name: entry.type_name.clone(),
            entity_key: entry.entity_key.clone(),
            payload: entry.payload.clone(),
            committed_at: manifest.committed_at.clone(),
        })
        .collect()
}

fn collect_existing_ids(batch: Option<RecordBatch>) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    let Some(batch) = batch else {
        return Ok(ids);
    };
    let id_arr = batch
        .column_by_name("id")
        .ok_or_else(|| NanoError::Storage("batch missing id column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("id column is not UInt64".to_string()))?;
    for row in 0..batch.num_rows() {
        ids.insert(id_arr.value(row));
    }
    Ok(ids)
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

fn cdc_analytics_dataset_path(db_path: &Path) -> PathBuf {
    db_path.join(CDC_ANALYTICS_DATASET_DIR)
}

fn cdc_analytics_state_path(db_path: &Path) -> PathBuf {
    db_path.join(CDC_ANALYTICS_STATE_FILE)
}

fn read_cdc_analytics_state(db_path: &Path) -> Result<CdcAnalyticsState> {
    let path = cdc_analytics_state_path(db_path);
    if !path.exists() {
        return Ok(CdcAnalyticsState::default());
    }

    let raw = std::fs::read_to_string(&path)?;
    let state: CdcAnalyticsState = serde_json::from_str(&raw).map_err(|e| {
        NanoError::Manifest(format!(
            "parse CDC analytics state {}: {}",
            path.display(),
            e
        ))
    })?;
    Ok(state)
}

fn write_cdc_analytics_state(db_path: &Path, state: &CdcAnalyticsState) -> Result<()> {
    let path = cdc_analytics_state_path(db_path);
    let json = serde_json::to_string_pretty(state)
        .map_err(|e| NanoError::Manifest(format!("serialize CDC analytics state: {}", e)))?;
    std::fs::write(path, json)?;
    Ok(())
}

fn cdc_rows_to_analytics_batch(rows: &[CdcLogEntry]) -> Result<RecordBatch> {
    use arrow::datatypes::{Field, Schema};

    let payload_json: Vec<String> = rows
        .iter()
        .map(|row| {
            serde_json::to_string(&row.payload)
                .map_err(|e| NanoError::Manifest(format!("serialize CDC payload: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("tx_id", DataType::Utf8, false),
        Field::new("db_version", DataType::UInt64, false),
        Field::new("seq_in_tx", DataType::UInt32, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("entity_kind", DataType::Utf8, false),
        Field::new("type_name", DataType::Utf8, false),
        Field::new("entity_key", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
        Field::new("committed_at", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.tx_id.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.db_version).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.seq_in_tx).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.op.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.entity_kind.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.type_name.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.entity_key.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(payload_json)),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.committed_at.clone())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| NanoError::Storage(format!("build CDC analytics batch: {}", e)))
}

// ── Lance helpers ───────────────────────────────────────────────────────────

async fn write_lance_batch(path: &Path, batch: RecordBatch) -> Result<u64> {
    write_lance_batch_with_mode(path, batch, WriteMode::Overwrite).await
}

async fn write_lance_batch_with_mode(
    path: &Path,
    batch: RecordBatch,
    mode: WriteMode,
) -> Result<u64> {
    info!(
        dataset_path = %path.display(),
        rows = batch.num_rows(),
        mode = ?mode,
        "writing Lance dataset"
    );
    let schema = batch.schema();
    let uri = path.to_string_lossy().to_string();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

    let write_params = WriteParams {
        mode,
        ..Default::default()
    };

    let dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .map_err(|e| NanoError::Lance(format!("write error: {}", e)))?;

    Ok(dataset.version().version)
}

async fn run_lance_merge_insert_with_key(
    dataset_path: &Path,
    pinned_version: u64,
    source_batch: RecordBatch,
    key_prop: &str,
) -> Result<u64> {
    let uri = dataset_path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("merge open error: {}", e)))?;
    let dataset = dataset
        .checkout_version(pinned_version)
        .await
        .map_err(|e| {
            NanoError::Lance(format!(
                "merge checkout version {} error: {}",
                pinned_version, e
            ))
        })?;

    let mut builder = MergeInsertBuilder::try_new(Arc::new(dataset), vec![key_prop.to_string()])
        .map_err(|e| NanoError::Lance(format!("merge builder error: {}", e)))?;
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0);

    let source_schema = source_batch.schema();
    let source = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch)].into_iter(),
        source_schema,
    ));
    let job = builder
        .try_build()
        .map_err(|e| NanoError::Lance(format!("merge build error: {}", e)))?;
    let (merged_dataset, _) = job
        .execute_reader(source)
        .await
        .map_err(|e| NanoError::Lance(format!("merge execute error: {}", e)))?;

    Ok(merged_dataset.version().version)
}

async fn run_lance_delete_by_ids(
    dataset_path: &Path,
    pinned_version: u64,
    ids: &[u64],
) -> Result<u64> {
    if ids.is_empty() {
        return Ok(pinned_version);
    }

    let uri = dataset_path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("delete open error: {}", e)))?;
    let mut dataset = dataset
        .checkout_version(pinned_version)
        .await
        .map_err(|e| {
            NanoError::Lance(format!(
                "delete checkout version {} error: {}",
                pinned_version, e
            ))
        })?;

    let predicate = ids
        .iter()
        .map(|id| format!("id = {}", id))
        .collect::<Vec<_>>()
        .join(" OR ");
    dataset
        .delete(&predicate)
        .await
        .map_err(|e| NanoError::Lance(format!("delete execute error: {}", e)))?;

    Ok(dataset.version().version)
}

async fn read_lance_batches(path: &Path, version: u64) -> Result<Vec<RecordBatch>> {
    info!(
        dataset_path = %path.display(),
        dataset_version = version,
        "reading Lance dataset"
    );
    let uri = path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
    let dataset = dataset
        .checkout_version(version)
        .await
        .map_err(|e| NanoError::Lance(format!("checkout version {} error: {}", version, e)))?;

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

fn now_unix_seconds_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn load_mode_op_summary(mode: LoadMode) -> &'static str {
    match mode {
        LoadMode::Overwrite => "load:overwrite",
        LoadMode::Append => "load:append",
        LoadMode::Merge => "load:merge",
    }
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
    use crate::store::txlog::{
        append_tx_catalog_entry, read_tx_catalog_entries, read_visible_cdc_entries,
    };
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

    fn dataset_version_for(manifest: &GraphManifest, kind: &str, type_name: &str) -> u64 {
        manifest
            .datasets
            .iter()
            .find(|entry| entry.kind == kind && entry.type_name == type_name)
            .map(|entry| entry.dataset_version)
            .unwrap()
    }

    fn dataset_rel_path_for(manifest: &GraphManifest, kind: &str, type_name: &str) -> String {
        manifest
            .datasets
            .iter()
            .find(|entry| entry.kind == kind && entry.type_name == type_name)
            .map(|entry| entry.dataset_path.clone())
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
    async fn test_load_appends_tx_catalog_row() {
        let dir = test_dir("tx_catalog_row");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        let rows = read_tx_catalog_entries(path).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].db_version, 1);
        assert_eq!(rows[0].tx_id, "manifest-1");
        assert_eq!(rows[0].op_summary, "load:overwrite");
        assert!(
            rows[0]
                .dataset_versions
                .keys()
                .any(|key| key.starts_with("nodes/"))
        );
    }

    #[tokio::test]
    async fn test_load_overwrite_emits_insert_cdc_events() {
        let dir = test_dir("cdc_load_insert");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load_with_mode(test_data_src(), LoadMode::Overwrite)
            .await
            .unwrap();

        let cdc = read_visible_cdc_entries(path, 0, None).unwrap();
        assert_eq!(cdc.len(), 7);
        assert!(cdc.iter().all(|e| e.db_version == 1));
        assert!(cdc.iter().all(|e| e.op == "insert"));
        assert!(
            cdc.iter()
                .any(|e| e.entity_kind == "node" && e.type_name == "Person")
        );
        assert!(
            cdc.iter()
                .any(|e| e.entity_kind == "edge" && e.type_name == "Knows")
        );
    }

    #[tokio::test]
    async fn test_load_merge_emits_update_and_insert_cdc_events() {
        let dir = test_dir("cdc_load_merge");
        let path = dir.path();

        let mut db = Database::init(path, keyed_schema_src()).await.unwrap();
        db.load_with_mode(keyed_data_initial(), LoadMode::Overwrite)
            .await
            .unwrap();
        db.load_with_mode(keyed_data_upsert(), LoadMode::Merge)
            .await
            .unwrap();

        let cdc = read_visible_cdc_entries(path, 1, Some(2)).unwrap();
        assert!(
            cdc.iter().any(|e| {
                e.op == "update" && e.entity_kind == "node" && e.type_name == "Person"
            })
        );
        assert!(
            cdc.iter().any(|e| {
                e.op == "insert" && e.entity_kind == "node" && e.type_name == "Person"
            })
        );

        let person_update = cdc
            .iter()
            .find(|e| e.op == "update" && e.entity_kind == "node" && e.type_name == "Person")
            .unwrap();
        assert!(person_update.payload.get("before").is_some());
        assert!(person_update.payload.get("after").is_some());
    }

    #[tokio::test]
    async fn test_open_repairs_trailing_partial_tx_catalog_row() {
        let dir = test_dir("tx_catalog_repair");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();
        drop(db);

        let tx_path = path.join("_tx_catalog.jsonl");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&tx_path)
            .unwrap();
        use std::io::Write;
        file.write_all(br#"{"tx_id":"partial""#).unwrap();
        file.sync_all().unwrap();

        let reopened = Database::open(path).await.unwrap();
        let persons = reopened.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 3);

        let rows = read_tx_catalog_entries(path).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].db_version, 1);
    }

    #[tokio::test]
    async fn test_open_truncates_tx_catalog_rows_beyond_manifest_version() {
        let dir = test_dir("tx_catalog_manifest_gate");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();
        drop(db);

        let mut rows = read_tx_catalog_entries(path).unwrap();
        assert_eq!(rows.len(), 1);

        let mut future = rows.pop().unwrap();
        future.tx_id = "future-tx".to_string();
        future.db_version = 2;
        append_tx_catalog_entry(path, &future).unwrap();

        let rows_before = read_tx_catalog_entries(path).unwrap();
        assert_eq!(rows_before.len(), 2);

        let reopened = Database::open(path).await.unwrap();
        let persons = reopened.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 3);

        let rows_after = read_tx_catalog_entries(path).unwrap();
        assert_eq!(rows_after.len(), 1);
        assert_eq!(rows_after[0].db_version, 1);
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
    async fn test_load_mode_append_keeps_untouched_dataset_versions() {
        let dir = test_dir("mode_append_versions");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load_with_mode(test_data_src(), LoadMode::Overwrite)
            .await
            .unwrap();
        let manifest_before = GraphManifest::read(path).unwrap();

        db.load_with_mode(
            append_data_new_person_with_edge_to_existing(),
            LoadMode::Append,
        )
        .await
        .unwrap();
        let manifest_after = GraphManifest::read(path).unwrap();

        assert!(
            dataset_version_for(&manifest_after, "node", "Person")
                > dataset_version_for(&manifest_before, "node", "Person")
        );
        assert!(
            dataset_version_for(&manifest_after, "edge", "Knows")
                > dataset_version_for(&manifest_before, "edge", "Knows")
        );
        assert_eq!(
            dataset_version_for(&manifest_after, "node", "Company"),
            dataset_version_for(&manifest_before, "node", "Company")
        );
        assert_eq!(
            dataset_version_for(&manifest_after, "edge", "WorksAt"),
            dataset_version_for(&manifest_before, "edge", "WorksAt")
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

        let tx_rows = read_tx_catalog_entries(path).unwrap();
        assert_eq!(tx_rows.len(), 2);
        assert_eq!(tx_rows[1].db_version, 2);
        assert_eq!(tx_rows[1].op_summary, "mutation:delete_nodes");

        let cdc_rows = read_visible_cdc_entries(path, 1, Some(2)).unwrap();
        assert!(
            cdc_rows.iter().any(|e| {
                e.op == "delete" && e.entity_kind == "node" && e.type_name == "Person"
            })
        );
        assert!(
            cdc_rows
                .iter()
                .any(|e| { e.op == "delete" && e.entity_kind == "edge" })
        );

        drop(db);
        let reopened = Database::open(path).await.unwrap();
        let persons2 = reopened.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons2.num_rows(), 2);
        assert_eq!(reopened.storage.edge_segments["Knows"].edge_ids.len(), 0);
        assert_eq!(reopened.storage.edge_segments["WorksAt"].edge_ids.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_edges_commits_through_mutation_pipeline() {
        let dir = test_dir("delete_edges_pipeline");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        let alice_id = person_id_by_name(&persons, "Alice");
        let result = db
            .delete_edges(
                "Knows",
                &DeletePredicate {
                    property: "src".to_string(),
                    op: DeleteOp::Eq,
                    value: alice_id.to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(result.deleted_nodes, 0);
        assert_eq!(result.deleted_edges, 2);
        assert_eq!(db.storage.edge_segments["Knows"].edge_ids.len(), 0);
        assert_eq!(db.storage.edge_segments["WorksAt"].edge_ids.len(), 1);

        let tx_rows = read_tx_catalog_entries(path).unwrap();
        assert_eq!(tx_rows.len(), 2);
        assert_eq!(tx_rows[1].db_version, 2);
        assert_eq!(tx_rows[1].op_summary, "mutation:delete_edges");

        let cdc_rows = read_visible_cdc_entries(path, 1, Some(2)).unwrap();
        assert!(
            cdc_rows
                .iter()
                .any(|e| { e.op == "delete" && e.entity_kind == "edge" && e.type_name == "Knows" })
        );

        drop(db);
        let reopened = Database::open(path).await.unwrap();
        assert_eq!(reopened.storage.edge_segments["Knows"].edge_ids.len(), 0);
        assert_eq!(reopened.storage.edge_segments["WorksAt"].edge_ids.len(), 1);
    }

    #[tokio::test]
    async fn test_delete_edges_uses_lance_native_delete_path() {
        let dir = test_dir("delete_edges_native");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();
        let manifest_before = GraphManifest::read(path).unwrap();
        let knows_path_before = dataset_rel_path_for(&manifest_before, "edge", "Knows");
        let works_at_version_before = dataset_version_for(&manifest_before, "edge", "WorksAt");

        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        let bob_id = person_id_by_name(&persons, "Bob");
        let result = db
            .delete_edges(
                "Knows",
                &DeletePredicate {
                    property: "dst".to_string(),
                    op: DeleteOp::Eq,
                    value: bob_id.to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(result.deleted_edges, 1);

        let manifest_after = GraphManifest::read(path).unwrap();
        let knows_path_after = dataset_rel_path_for(&manifest_after, "edge", "Knows");
        assert_eq!(knows_path_before, knows_path_after);
        assert_eq!(
            dataset_version_for(&manifest_after, "edge", "WorksAt"),
            works_at_version_before
        );

        let uri = path.join(&knows_path_after).to_string_lossy().to_string();
        let dataset = Dataset::open(&uri).await.unwrap();
        assert!(
            dataset.count_deleted_rows().await.unwrap() > 0,
            "expected native delete tombstones in Knows dataset"
        );
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

    #[tokio::test]
    async fn test_cdc_analytics_materialization_writes_dataset_and_preserves_changes() {
        let dir = test_dir("cdc_analytics_materialize");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        let visible_before = read_visible_cdc_entries(path, 0, None).unwrap();
        let manifest_before = GraphManifest::read(path).unwrap();

        let result = db
            .materialize_cdc_analytics(CdcAnalyticsMaterializeOptions {
                min_new_rows: 0,
                force: true,
            })
            .await
            .unwrap();

        assert!(result.dataset_written);
        assert_eq!(result.source_rows, visible_before.len());
        assert_eq!(result.materialized_rows, visible_before.len());
        assert!(path.join(CDC_ANALYTICS_DATASET_DIR).exists());

        let state = read_cdc_analytics_state(path).unwrap();
        assert_eq!(state.rows_materialized, visible_before.len());
        assert_eq!(state.manifest_db_version, manifest_before.db_version);
        assert!(state.dataset_version.is_some());

        let visible_after = read_visible_cdc_entries(path, 0, None).unwrap();
        assert_eq!(visible_before, visible_after);
        let manifest_after = GraphManifest::read(path).unwrap();
        assert_eq!(manifest_after.db_version, manifest_before.db_version);
    }

    #[tokio::test]
    async fn test_cdc_analytics_materialization_threshold_skip() {
        let dir = test_dir("cdc_analytics_threshold");
        let path = dir.path();

        let mut db = Database::init(path, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        db.materialize_cdc_analytics(CdcAnalyticsMaterializeOptions {
            min_new_rows: 0,
            force: true,
        })
        .await
        .unwrap();

        let skipped = db
            .materialize_cdc_analytics(CdcAnalyticsMaterializeOptions {
                min_new_rows: 10_000,
                force: false,
            })
            .await
            .unwrap();

        assert!(skipped.skipped_by_threshold);
        assert!(!skipped.dataset_written);
        assert_eq!(skipped.new_rows_since_last_run, 0);
    }
}
