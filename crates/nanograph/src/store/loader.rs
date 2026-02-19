use std::path::Path;

use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};

use super::database::LoadMode;
use super::graph::GraphStorage;

mod constraints;
mod embeddings;
mod jsonl;
mod merge;

pub(crate) use jsonl::{
    json_values_to_array, load_jsonl_data, load_jsonl_data_with_name_seed, parse_date32_literal,
    parse_date64_literal,
};

/// Build the next storage snapshot for a `Database::load` operation.
///
/// Behavior:
/// - `overwrite`: full replacement with incoming data.
/// - `append`: append incoming nodes/edges to existing data.
/// - `merge`: keyed node merge with stable IDs and edge endpoint remap.
/// - Always enforces node-level `@unique`/`@key` uniqueness before returning.
pub(crate) async fn build_next_storage_for_load(
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
    let data_source = embeddings::materialize_embeddings_for_load(db_path, schema_ir, data_source)?;

    let mut incoming_storage = GraphStorage::new(existing.catalog.clone());
    match mode {
        LoadMode::Overwrite => {
            load_jsonl_data(&mut incoming_storage, data_source.as_ref(), &key_props)?;
        }
        LoadMode::Merge => {
            let key_seed = constraints::build_name_seed_for_keyed_load(existing, &key_props)?;
            load_jsonl_data_with_name_seed(
                &mut incoming_storage,
                data_source.as_ref(),
                &key_props,
                Some(&key_seed),
            )?;
        }
        LoadMode::Append => {
            let key_seed = constraints::build_name_seed_for_append(existing, &key_props)?;
            load_jsonl_data_with_name_seed(
                &mut incoming_storage,
                data_source.as_ref(),
                &key_props,
                Some(&key_seed),
            )?;
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
