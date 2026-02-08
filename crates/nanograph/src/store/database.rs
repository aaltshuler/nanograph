use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;

use crate::catalog::schema_ir::{build_catalog_from_ir, build_schema_ir, SchemaIR};
use crate::catalog::Catalog;
use crate::error::{NanoError, Result};
use crate::schema::parser::parse_schema;
use crate::store::graph::GraphStorage;
use crate::store::loader::load_jsonl_data;
use crate::store::manifest::{DatasetEntry, GraphManifest};

const SCHEMA_PG_FILENAME: &str = "schema.pg";
const SCHEMA_IR_FILENAME: &str = "schema.ir.json";

pub struct Database {
    path: PathBuf,
    pub schema_ir: SchemaIR,
    pub catalog: Catalog,
    pub storage: GraphStorage,
}

impl Database {
    /// Create a new database directory from schema source text.
    pub async fn init(db_path: &Path, schema_source: &str) -> Result<Self> {
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
        let manifest = GraphManifest::new(ir_hash);
        manifest.write_atomic(db_path)?;

        let storage = GraphStorage::new(catalog.clone());

        Ok(Database {
            path: db_path.to_path_buf(),
            schema_ir,
            catalog,
            storage,
        })
    }

    /// Open an existing database.
    pub async fn open(db_path: &Path) -> Result<Self> {
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
            match entry.kind.as_str() {
                "node" => {
                    let dataset_path = db_path.join("nodes").join(&dir_name);
                    if dataset_path.exists() {
                        let batches = read_lance_batches(&dataset_path).await?;
                        for batch in batches {
                            storage.load_node_batch(&entry.type_name, batch)?;
                        }
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

        Ok(Database {
            path: db_path.to_path_buf(),
            schema_ir,
            catalog,
            storage,
        })
    }

    /// Load JSONL data. For v1, full replacement (WriteMode::Overwrite).
    pub async fn load(&mut self, data_source: &str) -> Result<()> {
        // Reset storage for full replacement
        self.storage = GraphStorage::new(self.catalog.clone());

        // Parse JSONL into in-memory storage
        load_jsonl_data(&mut self.storage, data_source)?;
        self.storage.build_indices()?;

        // Write each node type to Lance
        for node_def in self.schema_ir.node_types() {
            if let Some(batch) = self.storage.get_all_nodes(&node_def.name)? {
                let dir_name = SchemaIR::dir_name(node_def.type_id);
                let dataset_path = self.path.join("nodes").join(&dir_name);
                write_lance_batch(&dataset_path, batch).await?;
            }
        }

        // Write each edge type to Lance
        for edge_def in self.schema_ir.edge_types() {
            if let Some(batch) = self.storage.edge_batch_for_save(&edge_def.name)? {
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

/// Simple FNV-1a hash of a string → hex.
fn hash_string(s: &str) -> String {
    let mut hash: u64 = 14695981039346656037;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    format!("{:016x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "nanograph_{}_{}", name, std::process::id()
        ))
    }

    #[tokio::test]
    async fn test_init_creates_directory_structure() {
        let dir = unique_test_dir("init");
        let _ = std::fs::remove_dir_all(&dir);

        let db = Database::init(&dir, test_schema_src()).await.unwrap();

        assert!(dir.join("schema.pg").exists());
        assert!(dir.join("schema.ir.json").exists());
        assert!(dir.join("graph.manifest.json").exists());
        assert!(dir.join("nodes").exists());
        assert!(dir.join("edges").exists());

        assert_eq!(db.catalog.node_types.len(), 2);
        assert_eq!(db.catalog.edge_types.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_open_fresh_db() {
        let dir = unique_test_dir("open_fresh");
        let _ = std::fs::remove_dir_all(&dir);

        Database::init(&dir, test_schema_src()).await.unwrap();
        let db = Database::open(&dir).await.unwrap();

        assert_eq!(db.catalog.node_types.len(), 2);
        assert_eq!(db.catalog.edge_types.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_load_and_reopen_preserves_data() {
        let dir = unique_test_dir("load_reopen");
        let _ = std::fs::remove_dir_all(&dir);

        // Init + load
        let mut db = Database::init(&dir, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();

        // Verify in-memory
        let persons = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons.num_rows(), 3);

        // Reopen
        let db2 = Database::open(&dir).await.unwrap();
        let persons2 = db2.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(persons2.num_rows(), 3);

        let companies = db2.storage.get_all_nodes("Company").unwrap().unwrap();
        assert_eq!(companies.num_rows(), 1);

        // Verify edges survived
        let knows_seg = &db2.storage.edge_segments["Knows"];
        assert_eq!(knows_seg.edge_ids.len(), 2);
        assert!(knows_seg.csr.is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_load_reopen_query() {
        let dir = unique_test_dir("load_query");
        let _ = std::fs::remove_dir_all(&dir);

        let mut db = Database::init(&dir, test_schema_src()).await.unwrap();
        db.load(test_data_src()).await.unwrap();
        drop(db);

        // Reopen and verify CSR is functional
        let db2 = Database::open(&dir).await.unwrap();
        let snapshot = db2.snapshot();

        // Find Alice's id from the Person node segment
        let persons = snapshot.get_all_nodes("Person").unwrap().unwrap();
        let id_col = persons.column(0).as_any()
            .downcast_ref::<arrow::array::UInt64Array>().unwrap();
        let name_col = persons.column(1).as_any()
            .downcast_ref::<arrow::array::StringArray>().unwrap();
        let alice_id = (0..persons.num_rows())
            .find(|&i| name_col.value(i) == "Alice")
            .map(|i| id_col.value(i))
            .expect("Alice not found");

        let knows_seg = &snapshot.edge_segments["Knows"];
        let csr = knows_seg.csr.as_ref().unwrap();
        let alice_friends = csr.neighbors(alice_id);
        assert_eq!(alice_friends.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
