use std::path::Path;

use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};

use super::database::LoadMode;
use super::graph::GraphStorage;

mod constraints;
mod jsonl;
mod merge;

pub use jsonl::{json_values_to_array, load_jsonl_data, load_jsonl_data_with_name_seed};

/// Build the next storage snapshot for a `Database::load` operation.
///
/// Behavior:
/// - `overwrite`: full replacement with incoming data.
/// - `append`: append incoming nodes/edges to existing data.
/// - `merge`: keyed node merge with stable IDs and edge endpoint remap.
/// - Always enforces node-level `@unique`/`@key` uniqueness before returning.
pub async fn build_next_storage_for_load(
    db_path: &Path,
    existing: &GraphStorage,
    schema_ir: &SchemaIR,
    data_source: &str,
    mode: LoadMode,
) -> Result<GraphStorage> {
    let annotations = constraints::load_node_constraint_annotations(schema_ir)?;
    let key_props = annotations.key_props;
    if mode == LoadMode::Merge && key_props.is_empty() {
        return Err(NanoError::Storage(
            "load mode 'merge' requires at least one node @key property in schema".to_string(),
        ));
    }

    let mut incoming_storage = GraphStorage::new(existing.catalog.clone());
    match mode {
        LoadMode::Overwrite => {
            load_jsonl_data(&mut incoming_storage, data_source)?;
        }
        LoadMode::Merge => {
            let incoming_node_types = constraints::collect_incoming_node_types(data_source)?;
            let name_seed = constraints::build_name_seed_for_keyed_load(
                existing,
                schema_ir,
                &key_props,
                &incoming_node_types,
            )?;
            load_jsonl_data_with_name_seed(&mut incoming_storage, data_source, Some(&name_seed))?;
        }
        LoadMode::Append => {
            let name_seed = constraints::build_name_seed_for_append(existing, schema_ir)?;
            load_jsonl_data_with_name_seed(&mut incoming_storage, data_source, Some(&name_seed))?;
        }
    }

    let mut next_storage = match mode {
        LoadMode::Overwrite => incoming_storage,
        LoadMode::Merge => {
            merge::merge_storage_with_node_keys(
                db_path,
                existing,
                &incoming_storage,
                schema_ir,
                &key_props,
            )
            .await?
        }
        LoadMode::Append => merge::append_storage(existing, &incoming_storage, schema_ir)?,
    };

    constraints::enforce_node_unique_constraints(&next_storage, &annotations.unique_props)?;
    next_storage.build_indices()?;
    Ok(next_storage)
}
