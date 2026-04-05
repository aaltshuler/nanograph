use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{NanoError, Result};

const MANIFEST_FILENAME: &str = "graph.manifest.json";
const MANIFEST_FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphManifest {
    pub format_version: u32,
    pub db_version: u64,
    pub last_tx_id: String,
    pub committed_at: String,
    pub schema_ir_hash: String,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub next_type_id: u32,
    pub next_prop_id: u32,
    pub schema_identity_version: u32,
    pub datasets: Vec<DatasetEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetEntry {
    pub type_id: u32,
    pub type_name: String,
    pub kind: String,
    #[serde(default)]
    pub table_id: Option<String>,
    pub dataset_path: String,
    pub dataset_version: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct GraphManifestCompat {
    pub format_version: u32,
    pub db_version: u64,
    pub last_tx_id: String,
    pub committed_at: String,
    pub schema_ir_hash: String,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub next_type_id: u32,
    pub next_prop_id: u32,
    #[serde(default = "default_schema_identity_version")]
    pub schema_identity_version: u32,
    pub datasets: Vec<DatasetEntryCompat>,
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetEntryCompat {
    pub type_id: u32,
    pub type_name: String,
    pub kind: String,
    #[serde(default)]
    pub table_id: Option<String>,
    #[serde(default)]
    pub dataset_path: String,
    pub dataset_version: u64,
    pub row_count: u64,
}

impl GraphManifest {
    pub fn new(schema_ir_hash: String) -> Self {
        GraphManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            db_version: 0,
            last_tx_id: "init".to_string(),
            committed_at: "0".to_string(),
            schema_ir_hash,
            next_node_id: 0,
            next_edge_id: 0,
            next_type_id: 0,
            next_prop_id: 0,
            schema_identity_version: 1,
            datasets: Vec::new(),
        }
    }

    /// Write atomically: write .tmp → fsync → rename.
    pub fn write_atomic(&self, db_dir: &Path) -> Result<()> {
        let path = db_dir.join(MANIFEST_FILENAME);
        let tmp_path = db_dir.join(format!("{}.tmp", MANIFEST_FILENAME));

        let json = serde_json::to_string_pretty(self)
            .map_err(|e| NanoError::Manifest(format!("serialize error: {}", e)))?;

        // Write + fsync on the same handle. File::open() is read-only which
        // causes sync_all() to fail on Windows (FlushFileBuffers needs write access).
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
        }

        std::fs::rename(&tmp_path, &path)?;
        Ok(())
    }

    pub fn read(db_dir: &Path) -> Result<Self> {
        let path = db_dir.join(MANIFEST_FILENAME);
        let data = std::fs::read_to_string(&path)?;
        let manifest: GraphManifestCompat = serde_json::from_str(&data)
            .map_err(|e| NanoError::Manifest(format!("parse error: {}", e)))?;
        if manifest.format_version != 2 && manifest.format_version != MANIFEST_FORMAT_VERSION {
            return Err(NanoError::Manifest(format!(
                "unsupported manifest format_version {} (expected {})",
                manifest.format_version, MANIFEST_FORMAT_VERSION
            )));
        }
        Ok(GraphManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            db_version: manifest.db_version,
            last_tx_id: manifest.last_tx_id,
            committed_at: manifest.committed_at,
            schema_ir_hash: manifest.schema_ir_hash,
            next_node_id: manifest.next_node_id,
            next_edge_id: manifest.next_edge_id,
            next_type_id: manifest.next_type_id,
            next_prop_id: manifest.next_prop_id,
            schema_identity_version: manifest.schema_identity_version.max(1),
            datasets: manifest
                .datasets
                .into_iter()
                .map(|entry| DatasetEntry {
                    type_id: entry.type_id,
                    type_name: entry.type_name,
                    kind: entry.kind,
                    table_id: entry.table_id.or_else(|| Some(entry.dataset_path.clone())),
                    dataset_path: entry.dataset_path,
                    dataset_version: entry.dataset_version,
                    row_count: entry.row_count,
                })
                .collect(),
        })
    }
}

impl DatasetEntry {
    pub fn new(
        type_id: u32,
        type_name: impl Into<String>,
        kind: impl Into<String>,
        table_id: impl Into<String>,
        dataset_path: impl Into<String>,
        dataset_version: u64,
        row_count: u64,
    ) -> Self {
        Self {
            type_id,
            type_name: type_name.into(),
            kind: kind.into(),
            table_id: Some(table_id.into()),
            dataset_path: dataset_path.into(),
            dataset_version,
            row_count,
        }
    }

    pub fn internal(
        table_id: impl Into<String>,
        dataset_path: impl Into<String>,
        dataset_version: u64,
        row_count: u64,
    ) -> Self {
        let table_id = table_id.into();
        Self {
            type_id: 0,
            type_name: table_id.clone(),
            kind: "internal".to_string(),
            table_id: Some(table_id),
            dataset_path: dataset_path.into(),
            dataset_version,
            row_count,
        }
    }

    pub fn effective_table_id(&self) -> &str {
        self.table_id.as_deref().unwrap_or(&self.dataset_path)
    }
}

fn default_schema_identity_version() -> u32 {
    1
}

/// Simple FNV-1a hash of a string -> hex.
pub(crate) fn hash_string(s: &str) -> String {
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
    use tempfile::TempDir;

    #[test]
    fn test_manifest_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let mut manifest = GraphManifest::new("abc123".to_string());
        manifest.next_node_id = 42;
        manifest.next_edge_id = 10;
        manifest.datasets.push(DatasetEntry::new(
            100,
            "Person",
            "node",
            "nodes/00000064",
            "nodes/00000064",
            7,
            5,
        ));

        manifest.write_atomic(path).unwrap();
        let loaded = GraphManifest::read(path).unwrap();

        assert_eq!(loaded.format_version, 3);
        assert_eq!(loaded.db_version, 0);
        assert_eq!(loaded.schema_ir_hash, "abc123");
        assert_eq!(loaded.next_node_id, 42);
        assert_eq!(loaded.next_edge_id, 10);
        assert_eq!(loaded.datasets.len(), 1);
        assert_eq!(loaded.datasets[0].type_name, "Person");
        assert_eq!(loaded.datasets[0].dataset_path, "nodes/00000064");
        assert_eq!(loaded.datasets[0].effective_table_id(), "nodes/00000064");
        assert_eq!(loaded.datasets[0].dataset_version, 7);
    }

    #[test]
    fn test_atomic_write_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let manifest = GraphManifest::new("def456".to_string());
        manifest.write_atomic(path).unwrap();

        assert!(path.join("graph.manifest.json").exists());
        // tmp file should be gone (renamed)
        assert!(!path.join("graph.manifest.json.tmp").exists());
    }
}
