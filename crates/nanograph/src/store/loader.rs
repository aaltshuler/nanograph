use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{NanoError, Result};

use super::graph::GraphStorage;

/// Load JSONL-formatted data into a GraphStorage.
/// Each line is either a node `{"type": "...", "data": {...}}` or edge `{"edge": "...", "from": "...", "to": "..."}`.
pub fn load_jsonl_data(storage: &mut GraphStorage, data: &str) -> Result<()> {
    load_jsonl_data_with_name_seed(storage, data, None)
}

/// Load JSONL-formatted data into a GraphStorage with an optional pre-populated
/// name-to-id mapping for resolving edges that reference existing nodes.
pub fn load_jsonl_data_with_name_seed(
    storage: &mut GraphStorage,
    data: &str,
    name_seed: Option<&HashMap<(String, String), u64>>,
) -> Result<()> {
    let mut node_data: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
    let mut edge_data: Vec<serde_json::Value> = Vec::new();

    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        let obj: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| NanoError::Storage(format!("JSON parse error: {}", e)))?;

        if let Some(type_name) = obj.get("type").and_then(|v| v.as_str()) {
            node_data
                .entry(type_name.to_string())
                .or_default()
                .push(obj);
        } else if obj.get("edge").is_some() {
            edge_data.push(obj);
        }
    }

    // Insert nodes
    let mut name_to_id: HashMap<(String, String), u64> = name_seed.cloned().unwrap_or_default();

    for (type_name, nodes) in &node_data {
        let node_type = storage.catalog.node_types.get(type_name).ok_or_else(|| {
            NanoError::Storage(format!("unknown node type in data: {}", type_name))
        })?;

        let props: Vec<String> = node_type
            .arrow_schema
            .fields()
            .iter()
            .skip(1) // skip id
            .map(|f| f.name().clone())
            .collect();

        let mut builders: Vec<Vec<serde_json::Value>> = vec![Vec::new(); props.len()];

        for (row_idx, node) in nodes.iter().enumerate() {
            if let Some(data_obj) = node.get("data").and_then(|d| d.as_object()) {
                for (i, prop) in props.iter().enumerate() {
                    let field = &node_type.arrow_schema.fields()[i + 1]; // skip id
                    let val = data_obj
                        .get(prop)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    if val.is_null() && !field.is_nullable() {
                        return Err(NanoError::Storage(format!(
                            "node {}: required field '{}' missing on row {}",
                            type_name, prop, row_idx
                        )));
                    }
                    builders[i].push(val);
                }
            }
        }

        let mut columns: Vec<Arc<dyn Array>> = Vec::new();
        for (i, prop) in props.iter().enumerate() {
            let field = node_type.arrow_schema.field_with_name(prop).unwrap();
            let col = json_values_to_array(&builders[i], field.data_type(), field.is_nullable())?;
            columns.push(col);
        }

        let prop_fields: Vec<Field> = node_type
            .arrow_schema
            .fields()
            .iter()
            .skip(1)
            .map(|f| f.as_ref().clone())
            .collect();
        let prop_schema = Arc::new(Schema::new(prop_fields));
        let batch = RecordBatch::try_new(prop_schema, columns)
            .map_err(|e| NanoError::Storage(format!("batch error: {}", e)))?;

        let ids = storage.insert_nodes(type_name, batch)?;

        // Build name -> id mapping for edge resolution
        for (idx, node) in nodes.iter().enumerate() {
            if let Some(data_obj) = node.get("data").and_then(|d| d.as_object()) {
                if let Some(name) = data_obj.get("name").and_then(|n| n.as_str()) {
                    name_to_id.insert((type_name.clone(), name.to_string()), ids[idx]);
                }
            }
        }
    }

    // Resolve edges and group by type
    struct ResolvedEdge {
        from_id: u64,
        to_id: u64,
        data: Option<serde_json::Map<String, serde_json::Value>>,
    }
    // No multigraph support: deduplicate by (src, dst) per edge type.
    // Last occurrence wins so later lines can override edge properties.
    let mut edges_by_type: BTreeMap<String, BTreeMap<(u64, u64), ResolvedEdge>> = BTreeMap::new();

    for edge_obj in &edge_data {
        let edge_type = edge_obj
            .get("edge")
            .and_then(|e| e.as_str())
            .ok_or_else(|| NanoError::Storage("edge missing type".to_string()))?;

        let from_name = edge_obj
            .get("from")
            .and_then(|f| f.as_str())
            .ok_or_else(|| NanoError::Storage("edge missing from".to_string()))?;
        let to_name = edge_obj
            .get("to")
            .and_then(|t| t.as_str())
            .ok_or_else(|| NanoError::Storage("edge missing to".to_string()))?;

        let et = storage
            .catalog
            .edge_types
            .get(edge_type)
            .or_else(|| {
                storage
                    .catalog
                    .edge_name_index
                    .get(edge_type)
                    .and_then(|key| storage.catalog.edge_types.get(key))
            })
            .ok_or_else(|| NanoError::Storage(format!("unknown edge type: {}", edge_type)))?;

        let from_type = et.from_type.clone();
        let to_type = et.to_type.clone();
        let edge_name = et.name.clone();

        let from_id = *name_to_id
            .get(&(from_type.clone(), from_name.to_string()))
            .ok_or_else(|| {
                NanoError::Storage(format!("node not found: {}:{}", from_type, from_name))
            })?;
        let to_id = *name_to_id
            .get(&(to_type.clone(), to_name.to_string()))
            .ok_or_else(|| {
                NanoError::Storage(format!("node not found: {}:{}", to_type, to_name))
            })?;

        let data = edge_obj.get("data").and_then(|d| d.as_object()).cloned();

        edges_by_type.entry(edge_name).or_default().insert(
            (from_id, to_id),
            ResolvedEdge {
                from_id,
                to_id,
                data,
            },
        );
    }

    // Insert edges batched by type
    for (edge_name, edges_map) in &edges_by_type {
        let edges: Vec<&ResolvedEdge> = edges_map.values().collect();
        let src_ids: Vec<u64> = edges.iter().map(|e| e.from_id).collect();
        let dst_ids: Vec<u64> = edges.iter().map(|e| e.to_id).collect();

        let et = storage
            .catalog
            .edge_types
            .get(edge_name)
            .ok_or_else(|| NanoError::Storage(format!("unknown edge type: {}", edge_name)))?;

        let prop_batch = if !et.properties.is_empty() {
            // Build property columns from edge data
            let edge_seg = storage
                .edge_segments
                .get(edge_name)
                .ok_or_else(|| NanoError::Storage(format!("no edge segment: {}", edge_name)))?;
            // Edge segment schema: id, src, dst, ...props — skip first 3
            let prop_fields: Vec<Field> = edge_seg
                .schema
                .fields()
                .iter()
                .skip(3)
                .map(|f| f.as_ref().clone())
                .collect();

            let mut columns: Vec<Arc<dyn Array>> = Vec::new();
            for field in &prop_fields {
                let values: Vec<serde_json::Value> = edges
                    .iter()
                    .map(|edge| {
                        let e = *edge;
                        e.data
                            .as_ref()
                            .and_then(|d| d.get(field.name()))
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                columns.push(json_values_to_array(
                    &values,
                    field.data_type(),
                    field.is_nullable(),
                )?);
            }

            if prop_fields.is_empty() {
                None
            } else {
                let schema = Arc::new(Schema::new(prop_fields));
                Some(
                    RecordBatch::try_new(schema, columns)
                        .map_err(|e| NanoError::Storage(format!("edge prop batch error: {}", e)))?,
                )
            }
        } else {
            None
        };

        storage.insert_edges(edge_name, &src_ids, &dst_ids, prop_batch)?;
    }

    Ok(())
}

/// Convert JSON values to an Arrow array based on the target DataType.
pub fn json_values_to_array(
    values: &[serde_json::Value],
    dt: &DataType,
    nullable: bool,
) -> Result<Arc<dyn Array>> {
    use arrow::array::*;
    let arr: Arc<dyn Array> = match dt {
        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            Arc::new(arr)
        }
        DataType::Int32 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| v.as_i64().map(|n| n as i32))
                .collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = values.iter().map(|v| v.as_i64()).collect();
            Arc::new(arr)
        }
        DataType::UInt64 => {
            let arr: UInt64Array = values.iter().map(|v| v.as_u64()).collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = values.iter().map(|v| v.as_f64()).collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = values.iter().map(|v| v.as_bool()).collect();
            Arc::new(arr)
        }
        DataType::Float32 => {
            let arr: Float32Array = values
                .iter()
                .map(|v| v.as_f64().map(|n| n as f32))
                .collect();
            Arc::new(arr)
        }
        DataType::UInt32 => {
            let arr: UInt32Array = values
                .iter()
                .map(|v| v.as_u64().map(|n| n as u32))
                .collect();
            Arc::new(arr)
        }
        DataType::Date32 => {
            // Date32 stores days since epoch; nulls for JSON nulls
            let arr: Int32Array = values
                .iter()
                .map(|v| v.as_i64().map(|n| n as i32))
                .collect();
            Arc::new(arrow::compute::cast(&arr, &DataType::Date32).unwrap_or(Arc::new(arr)))
        }
        DataType::Date64 => {
            // Date64 stores milliseconds since epoch
            let arr: Int64Array = values.iter().map(|v| v.as_i64()).collect();
            Arc::new(arrow::compute::cast(&arr, &DataType::Date64).unwrap_or(Arc::new(arr)))
        }
        _ => {
            // Fallback to string
            let arr: StringArray = values.iter().map(|v| Some(v.to_string())).collect();
            Arc::new(arr)
        }
    };
    if !nullable && arr.null_count() > 0 {
        return Err(NanoError::Storage(format!(
            "field has {} null value(s) from type mismatch (expected {:?})",
            arr.null_count(),
            dt
        )));
    }
    Ok(arr)
}
