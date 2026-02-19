use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Date64Builder, FixedSizeListBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    UInt32Builder, UInt64Builder, make_builder,
};
use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};

use crate::error::{NanoError, Result};

use super::super::graph::GraphStorage;
use super::constraints::key_value_string;

/// Load JSONL-formatted data into a GraphStorage.
/// Each line is either a node `{"type": "...", "data": {...}}` or edge `{"edge": "...", "from": "...", "to": "..."}`.
pub(crate) fn load_jsonl_data(
    storage: &mut GraphStorage,
    data: &str,
    key_props: &HashMap<String, String>,
) -> Result<()> {
    load_jsonl_data_with_name_seed(storage, data, key_props, None)
}

/// Load JSONL-formatted data into a GraphStorage with an optional pre-populated
/// @key-value-to-id mapping for resolving edges that reference existing nodes.
pub(crate) fn load_jsonl_data_with_name_seed(
    storage: &mut GraphStorage,
    data: &str,
    key_props: &HashMap<String, String>,
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
    let mut key_to_id: HashMap<(String, String), u64> = name_seed.cloned().unwrap_or_default();

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

        storage.insert_nodes(type_name, batch)?;
    }

    // Build @key -> id mapping for incoming rows so edges can resolve to nodes created
    // in this same JSONL payload.
    for (type_name, key_prop) in key_props {
        let Some(batch) = storage.get_all_nodes(type_name)? else {
            continue;
        };

        let key_idx = batch.schema().index_of(key_prop).map_err(|e| {
            NanoError::Storage(format!(
                "node type {} missing @key property {}: {}",
                type_name, key_prop, e
            ))
        })?;
        let key_arr = batch.column(key_idx).clone();
        let id_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                NanoError::Storage(format!("node type {} has non-UInt64 id column", type_name))
            })?;

        for row in 0..batch.num_rows() {
            let key = key_value_string(&key_arr, row, key_prop)?;
            key_to_id.insert((type_name.clone(), key), id_arr.value(row));
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

        let from_token = edge_obj
            .get("from")
            .and_then(|f| f.as_str())
            .ok_or_else(|| NanoError::Storage("edge missing from".to_string()))?;
        let to_token = edge_obj
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

        let (src_key_prop, dst_key_prop) =
            match (key_props.get(&from_type), key_props.get(&to_type)) {
                (Some(src), Some(dst)) => (src, dst),
                _ => {
                    return Err(NanoError::Storage(format!(
                        "edge '{}' requires @key on source type '{}' and destination type '{}'",
                        edge_name, from_type, to_type
                    )));
                }
            };

        let from_key_type = storage
            .catalog
            .node_types
            .get(&from_type)
            .and_then(|nt| nt.arrow_schema.field_with_name(src_key_prop).ok())
            .map(|f| f.data_type().clone())
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "missing @key field {} on source type {}",
                    src_key_prop, from_type
                ))
            })?;
        let to_key_type = storage
            .catalog
            .node_types
            .get(&to_type)
            .and_then(|nt| nt.arrow_schema.field_with_name(dst_key_prop).ok())
            .map(|f| f.data_type().clone())
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "missing @key field {} on destination type {}",
                    dst_key_prop, to_type
                ))
            })?;

        let from_key = parse_edge_endpoint_key_token(from_token, &from_key_type).map_err(|e| {
            NanoError::Storage(format!(
                "invalid edge endpoint key for {}.{} from='{}': {}",
                from_type, src_key_prop, from_token, e
            ))
        })?;
        let to_key = parse_edge_endpoint_key_token(to_token, &to_key_type).map_err(|e| {
            NanoError::Storage(format!(
                "invalid edge endpoint key for {}.{} to='{}': {}",
                to_type, dst_key_prop, to_token, e
            ))
        })?;

        let from_id = *key_to_id
            .get(&(from_type.clone(), from_key.clone()))
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "node not found by @key: {}.{}={}",
                    from_type, src_key_prop, from_key
                ))
            })?;
        let to_id = *key_to_id
            .get(&(to_type.clone(), to_key.clone()))
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "node not found by @key: {}.{}={}",
                    to_type, dst_key_prop, to_key
                ))
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
            // Edge segment schema: id, src, dst, ...props - skip first 3
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
pub(crate) fn json_values_to_array(
    values: &[serde_json::Value],
    dt: &DataType,
    nullable: bool,
) -> Result<Arc<dyn Array>> {
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
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(parse_date32_json_value(value)?);
            }
            Arc::new(Date32Array::from(out))
        }
        DataType::Date64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(parse_date64_json_value(value)?);
            }
            Arc::new(Date64Array::from(out))
        }
        DataType::List(field) => {
            let mut builder = ListBuilder::with_capacity(
                make_builder(field.data_type(), values.len()),
                values.len(),
            )
            .with_field(field.clone());
            for value in values {
                if value.is_null() {
                    builder.append(false);
                    continue;
                }
                let Some(items) = value.as_array() else {
                    builder.append(false);
                    continue;
                };
                for item in items {
                    append_json_to_builder(builder.values(), field.data_type(), item)?;
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        DataType::FixedSizeList(field, dim) => {
            if *dim <= 0 {
                return Err(NanoError::Storage(format!(
                    "invalid FixedSizeList dimension: {}",
                    dim
                )));
            }
            if field.data_type() != &DataType::Float32 {
                return Err(NanoError::Storage(format!(
                    "unsupported FixedSizeList element type {:?}; expected Float32",
                    field.data_type()
                )));
            }

            let list_len = *dim as usize;
            let mut builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(values.len() * list_len),
                *dim,
                values.len(),
            )
            .with_field(field.clone());

            for value in values {
                if value.is_null() {
                    for _ in 0..list_len {
                        builder.values().append_null();
                    }
                    builder.append(false);
                    continue;
                }
                let items = value.as_array().ok_or_else(|| {
                    NanoError::Storage(format!(
                        "expected JSON array for FixedSizeList<Float32, {}>, got {}",
                        dim, value
                    ))
                })?;
                if items.len() != list_len {
                    return Err(NanoError::Storage(format!(
                        "FixedSizeList<Float32, {}> length mismatch: got {}",
                        dim,
                        items.len()
                    )));
                }

                for item in items {
                    let num = item.as_f64().ok_or_else(|| {
                        NanoError::Storage(format!(
                            "expected numeric vector element in FixedSizeList<Float32, {}>, got {}",
                            dim, item
                        ))
                    })?;
                    builder.values().append_value(num as f32);
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
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

fn append_json_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    dt: &DataType,
    value: &serde_json::Value,
) -> Result<()> {
    match dt {
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Utf8 builder downcast failed".to_string())
                })?;
            if let Some(s) = value.as_str() {
                b.append_value(s);
            } else {
                b.append_null();
            }
        }
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Boolean builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_bool() {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DataType::Int32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Int32 builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_i64() {
                if let Ok(n) = i32::try_from(v) {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            } else {
                b.append_null();
            }
        }
        DataType::Int64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Int64 builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_i64() {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DataType::UInt32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list UInt32 builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_u64() {
                if let Ok(n) = u32::try_from(v) {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            } else {
                b.append_null();
            }
        }
        DataType::UInt64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list UInt64 builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_u64() {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DataType::Float32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Float32 builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_f64() {
                b.append_value(v as f32);
            } else {
                b.append_null();
            }
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Float64 builder downcast failed".to_string())
                })?;
            if let Some(v) = value.as_f64() {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DataType::Date32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Date32 builder downcast failed".to_string())
                })?;
            if let Some(v) = parse_date32_json_value(value)? {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DataType::Date64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date64Builder>()
                .ok_or_else(|| {
                    NanoError::Storage("list Date64 builder downcast failed".to_string())
                })?;
            if let Some(v) = parse_date64_json_value(value)? {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DataType::List(field) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| {
                    NanoError::Storage("nested list builder downcast failed".to_string())
                })?;
            if value.is_null() {
                b.append(false);
            } else if let Some(items) = value.as_array() {
                for item in items {
                    append_json_to_builder(b.values(), field.data_type(), item)?;
                }
                b.append(true);
            } else {
                b.append(false);
            }
        }
        other => {
            return Err(NanoError::Storage(format!(
                "unsupported list element data type {:?}",
                other
            )));
        }
    }

    Ok(())
}

fn parse_date32_json_value(value: &serde_json::Value) -> Result<Option<i32>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(days) = value.as_i64() {
        return i32::try_from(days)
            .map(Some)
            .map_err(|_| NanoError::Storage(format!("Date32 value out of range: {}", days)));
    }
    if let Some(days) = value.as_u64() {
        return i32::try_from(days)
            .map(Some)
            .map_err(|_| NanoError::Storage(format!("Date32 value out of range: {}", days)));
    }
    if let Some(s) = value.as_str() {
        return Ok(Some(parse_date32_literal(s)?));
    }
    Ok(None)
}

fn parse_date64_json_value(value: &serde_json::Value) -> Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(ms) = value.as_i64() {
        return Ok(Some(ms));
    }
    if let Some(ms) = value.as_u64() {
        return i64::try_from(ms)
            .map(Some)
            .map_err(|_| NanoError::Storage(format!("Date64 value out of range: {}", ms)));
    }
    if let Some(s) = value.as_str() {
        return Ok(Some(parse_date64_literal(s)?));
    }
    Ok(None)
}

fn parse_edge_endpoint_key_token(token: &str, dt: &DataType) -> Result<String> {
    match dt {
        DataType::Utf8 => Ok(token.to_string()),
        DataType::Boolean => token
            .parse::<bool>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected bool token: {}", e))),
        DataType::Int32 => token
            .parse::<i32>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected Int32 token: {}", e))),
        DataType::Int64 => token
            .parse::<i64>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected Int64 token: {}", e))),
        DataType::UInt32 => token
            .parse::<u32>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected UInt32 token: {}", e))),
        DataType::UInt64 => token
            .parse::<u64>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected UInt64 token: {}", e))),
        DataType::Float32 => token
            .parse::<f32>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected Float32 token: {}", e))),
        DataType::Float64 => token
            .parse::<f64>()
            .map(|v| v.to_string())
            .map_err(|e| NanoError::Storage(format!("expected Float64 token: {}", e))),
        DataType::Date32 => parse_date32_literal(token).map(|v| v.to_string()),
        DataType::Date64 => parse_date64_literal(token).map(|v| v.to_string()),
        other => Err(NanoError::Storage(format!(
            "unsupported @key type for edge endpoint resolution: {:?}",
            other
        ))),
    }
}

pub(crate) fn parse_date32_literal(s: &str) -> Result<i32> {
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(s)]));
    let casted = arrow_cast::cast(raw.as_ref(), &DataType::Date32)
        .map_err(|e| NanoError::Storage(format!("invalid Date literal '{}': {}", s, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| NanoError::Storage("Date32 cast produced unexpected array".to_string()))?;
    if out.is_null(0) {
        return Err(NanoError::Storage(format!("invalid Date literal '{}'", s)));
    }
    Ok(out.value(0))
}

pub(crate) fn parse_date64_literal(s: &str) -> Result<i64> {
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(s)]));
    let casted = arrow_cast::cast(raw.as_ref(), &DataType::Date64)
        .map_err(|e| NanoError::Storage(format!("invalid DateTime literal '{}': {}", s, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| NanoError::Storage("Date64 cast produced unexpected array".to_string()))?;
    if out.is_null(0) {
        return Err(NanoError::Storage(format!(
            "invalid DateTime literal '{}'",
            s
        )));
    }
    Ok(out.value(0))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::catalog::schema_ir::{build_catalog_from_ir, build_schema_ir};
    use crate::schema::parser::parse_schema;

    use super::*;

    fn test_schema() -> &'static str {
        r#"node Person {
    name: String @key
}
edge Knows: Person -> Person
"#
    }

    fn build_storage(schema_src: &str) -> GraphStorage {
        let schema = parse_schema(schema_src).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let catalog = build_catalog_from_ir(&ir).unwrap();
        GraphStorage::new(catalog)
    }

    fn person_key_props() -> HashMap<String, String> {
        HashMap::from([("Person".to_string(), "name".to_string())])
    }

    fn person_id_by_name(storage: &GraphStorage, name: &str) -> u64 {
        let batch = storage.get_all_nodes("Person").unwrap().unwrap();
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .find(|&i| name_col.value(i) == name)
            .map(|i| id_col.value(i))
            .unwrap()
    }

    #[test]
    fn json_values_to_array_rejects_non_nullable_mismatch() {
        let values = vec![json!("abc"), json!(42)];
        let err = json_values_to_array(&values, &DataType::Int32, false).unwrap_err();
        assert!(
            err.to_string().contains("null value"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn json_values_to_array_accepts_iso_date_strings() {
        let values = vec![json!("2026-02-14"), json!(null)];
        let arr = json_values_to_array(&values, &DataType::Date32, true).unwrap();
        let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
    }

    #[test]
    fn json_values_to_array_accepts_iso_datetime_strings() {
        let values = vec![json!("2026-02-14T10:00:00Z"), json!(null)];
        let arr = json_values_to_array(&values, &DataType::Date64, true).unwrap();
        let arr = arr.as_any().downcast_ref::<Date64Array>().unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
    }

    #[test]
    fn json_values_to_array_builds_list_values() {
        let values = vec![json!([1, 2]), json!(null), json!([3])];
        let dt = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let arr = json_values_to_array(&values, &dt, true).unwrap();
        let list = arr
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert_eq!(list.len(), 3);
        assert!(!list.is_null(0));
        assert!(list.is_null(1));
        assert!(!list.is_null(2));

        let first = list.value(0);
        let first = first.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first.value(0), 1);
        assert_eq!(first.value(1), 2);
    }

    #[test]
    fn json_values_to_array_builds_fixed_size_list_vectors() {
        let values = vec![json!([0.1, 0.2, 0.3]), json!(null), json!([1, 2, 3])];
        let dt = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3);
        let arr = json_values_to_array(&values, &dt, true).unwrap();
        let vecs = arr
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .unwrap();
        assert_eq!(vecs.len(), 3);
        assert!(!vecs.is_null(0));
        assert!(vecs.is_null(1));
        assert!(!vecs.is_null(2));
    }

    #[test]
    fn load_jsonl_with_name_seed_resolves_edges_to_existing_nodes() {
        let mut existing = build_storage(test_schema());
        load_jsonl_data(
            &mut existing,
            r#"{"type":"Person","data":{"name":"Alice"}}"#,
            &person_key_props(),
        )
        .unwrap();
        let alice_id = person_id_by_name(&existing, "Alice");

        let data = r#"{"type":"Person","data":{"name":"Bob"}}
{"edge":"Knows","from":"Alice","to":"Bob"}"#;

        let mut no_seed = build_storage(test_schema());
        let err = load_jsonl_data(&mut no_seed, data, &person_key_props()).unwrap_err();
        assert!(
            err.to_string().contains("node not found by @key"),
            "unexpected error: {err}"
        );

        let mut seeded = build_storage(test_schema());
        let mut seed = HashMap::new();
        seed.insert(("Person".to_string(), "Alice".to_string()), alice_id);
        load_jsonl_data_with_name_seed(&mut seeded, data, &person_key_props(), Some(&seed))
            .unwrap();

        let bob_id = person_id_by_name(&seeded, "Bob");
        let knows = &seeded.edge_segments["Knows"];
        assert_eq!(knows.edge_ids.len(), 1);
        assert_eq!(knows.src_ids[0], alice_id);
        assert_eq!(knows.dst_ids[0], bob_id);
    }

    #[test]
    fn load_jsonl_deduplicates_duplicate_edges() {
        let mut storage = build_storage(test_schema());
        let data = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}
{"edge":"Knows","from":"Alice","to":"Bob"}
{"edge":"Knows","from":"Alice","to":"Bob"}"#;

        load_jsonl_data(&mut storage, data, &person_key_props()).unwrap();
        let knows = &storage.edge_segments["Knows"];
        assert_eq!(knows.edge_ids.len(), 1);
    }

    #[test]
    fn load_jsonl_edges_require_endpoint_key_annotations() {
        let schema = r#"node Event {
    title: String
    at: Date
}
edge Precedes: Event -> Event
"#;
        let mut storage = build_storage(schema);
        let data = r#"{"type":"Event","data":{"title":"Kickoff","at":"2026-02-14"}}
{"type":"Event","data":{"title":"Wrap","at":"2026-02-15"}}
{"edge":"Precedes","from":"Kickoff","to":"Wrap"}"#;

        let err = load_jsonl_data(&mut storage, data, &HashMap::new()).unwrap_err();
        assert!(
            err.to_string()
                .contains("requires @key on source type 'Event' and destination type 'Event'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn load_jsonl_edges_resolve_by_non_name_key() {
        let schema = r#"node User {
    uid: String @key
    display_name: String
}
edge Follows: User -> User
"#;
        let mut storage = build_storage(schema);
        let key_props = HashMap::from([("User".to_string(), "uid".to_string())]);
        let data = r#"{"type":"User","data":{"uid":"usr_01","display_name":"Alice"}}
{"type":"User","data":{"uid":"usr_02","display_name":"Bob"}}
{"edge":"Follows","from":"usr_01","to":"usr_02"}"#;

        load_jsonl_data(&mut storage, data, &key_props).unwrap();
        let follows = &storage.edge_segments["Follows"];
        assert_eq!(follows.edge_ids.len(), 1);
    }

    #[test]
    fn load_jsonl_edges_parse_non_string_key_tokens() {
        let schema = r#"node User {
    uid: U64 @key
    display_name: String
}
edge Follows: User -> User
"#;
        let mut storage = build_storage(schema);
        let key_props = HashMap::from([("User".to_string(), "uid".to_string())]);
        let data = r#"{"type":"User","data":{"uid":1,"display_name":"Alice"}}
{"type":"User","data":{"uid":2,"display_name":"Bob"}}
{"edge":"Follows","from":"1","to":"2"}"#;

        load_jsonl_data(&mut storage, data, &key_props).unwrap();
        let follows = &storage.edge_segments["Follows"];
        assert_eq!(follows.edge_ids.len(), 1);
    }
}
