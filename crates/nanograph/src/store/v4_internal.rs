use std::path::Path;

use crate::error::Result;
use crate::store::blob_store::{blob_store_manifest_entry, ensure_blob_store_table};
use crate::store::manifest::DatasetEntry;
use crate::store::storage_generation::{StorageGeneration, detect_storage_generation};
use crate::store::v4_graph_log::{ensure_graph_changes_table, ensure_graph_tx_table};

pub(crate) async fn ensure_v4_internal_dataset_entries(
    db_path: &Path,
) -> Result<Vec<DatasetEntry>> {
    if !matches!(
        detect_storage_generation(db_path)?,
        Some(StorageGeneration::V4Namespace)
    ) {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    entries.push(ensure_graph_tx_table(db_path).await?);
    entries.push(ensure_graph_changes_table(db_path).await?);
    entries.push(
        blob_store_manifest_entry(db_path)
            .await?
            .unwrap_or(ensure_blob_store_table(db_path).await?),
    );
    Ok(entries)
}

pub(crate) async fn merge_v4_internal_dataset_entries(
    db_path: &Path,
    dataset_entries: &mut Vec<DatasetEntry>,
) -> Result<()> {
    let internal_entries = ensure_v4_internal_dataset_entries(db_path).await?;
    if internal_entries.is_empty() {
        return Ok(());
    }

    let internal_ids = internal_entries
        .iter()
        .map(|entry| entry.effective_table_id().to_string())
        .collect::<std::collections::HashSet<_>>();
    dataset_entries.retain(|entry| !internal_ids.contains(entry.effective_table_id()));
    dataset_entries.extend(internal_entries);
    Ok(())
}
