use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{NanoError, Result};

const MANIFEST_FILENAME: &str = "graph.manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphManifest {
    pub format_version: u32,
    pub schema_ir_hash: String,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub datasets: Vec<DatasetEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetEntry {
    pub type_id: u32,
    pub type_name: String,
    pub kind: String,
    pub row_count: u64,
}

impl GraphManifest {
    pub fn new(schema_ir_hash: String) -> Self {
        GraphManifest {
            format_version: 1,
            schema_ir_hash,
            next_node_id: 0,
            next_edge_id: 0,
            datasets: Vec::new(),
        }
    }

    /// Write atomically: write .tmp → fsync → rename.
    pub fn write_atomic(&self, db_dir: &Path) -> Result<()> {
        let path = db_dir.join(MANIFEST_FILENAME);
        let tmp_path = db_dir.join(format!("{}.tmp", MANIFEST_FILENAME));

        let json = serde_json::to_string_pretty(self)
            .map_err(|e| NanoError::Manifest(format!("serialize error: {}", e)))?;

        std::fs::write(&tmp_path, json.as_bytes())?;

        // fsync the file
        let file = std::fs::File::open(&tmp_path)?;
        file.sync_all()?;

        std::fs::rename(&tmp_path, &path)?;
        Ok(())
    }

    pub fn read(db_dir: &Path) -> Result<Self> {
        let path = db_dir.join(MANIFEST_FILENAME);
        let data = std::fs::read_to_string(&path)?;
        let manifest: GraphManifest = serde_json::from_str(&data)
            .map_err(|e| NanoError::Manifest(format!("parse error: {}", e)))?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_manifest_roundtrip() {
        let dir = std::env::temp_dir().join("nanograph_manifest_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut manifest = GraphManifest::new("abc123".to_string());
        manifest.next_node_id = 42;
        manifest.next_edge_id = 10;
        manifest.datasets.push(DatasetEntry {
            type_id: 100,
            type_name: "Person".to_string(),
            kind: "node".to_string(),
            row_count: 5,
        });

        manifest.write_atomic(&dir).unwrap();
        let loaded = GraphManifest::read(&dir).unwrap();

        assert_eq!(loaded.format_version, 1);
        assert_eq!(loaded.schema_ir_hash, "abc123");
        assert_eq!(loaded.next_node_id, 42);
        assert_eq!(loaded.next_edge_id, 10);
        assert_eq!(loaded.datasets.len(), 1);
        assert_eq!(loaded.datasets[0].type_name, "Person");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_atomic_write_creates_file() {
        let dir = std::env::temp_dir().join("nanograph_manifest_test_atomic");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let manifest = GraphManifest::new("def456".to_string());
        manifest.write_atomic(&dir).unwrap();

        assert!(dir.join("graph.manifest.json").exists());
        // tmp file should be gone (renamed)
        assert!(!dir.join("graph.manifest.json.tmp").exists());

        let _ = fs::remove_dir_all(&dir);
    }
}
