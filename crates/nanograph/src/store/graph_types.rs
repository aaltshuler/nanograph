use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct GraphVersion(pub(crate) u64);

impl GraphVersion {
    pub(crate) fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for GraphVersion {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct GraphTxId(pub(crate) String);

impl GraphTxId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for GraphTxId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for GraphTxId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct GraphTableId(pub(crate) String);

impl GraphTableId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for GraphTableId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for GraphTableId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphTableVersion {
    pub(crate) table_id: GraphTableId,
    pub(crate) version: u64,
}

impl GraphTableVersion {
    pub(crate) fn new(table_id: impl Into<GraphTableId>, version: u64) -> Self {
        Self {
            table_id: table_id.into(),
            version,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphSnapshot {
    pub(crate) graph_version: GraphVersion,
    pub(crate) table_versions: Vec<GraphTableVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphCommitRecord {
    pub(crate) tx_id: GraphTxId,
    pub(crate) graph_version: GraphVersion,
    pub(crate) table_versions: Vec<GraphTableVersion>,
    pub(crate) committed_at: String,
    pub(crate) op_summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct GraphChangeRecord {
    pub(crate) tx_id: GraphTxId,
    pub(crate) graph_version: GraphVersion,
    pub(crate) seq_in_tx: u32,
    pub(crate) op: String,
    pub(crate) entity_kind: String,
    pub(crate) type_name: String,
    pub(crate) entity_key: String,
    pub(crate) payload: serde_json::Value,
    #[serde(default)]
    pub(crate) rowid_if_known: Option<u64>,
    pub(crate) committed_at: String,
}
