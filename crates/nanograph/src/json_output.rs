use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::DataType;

pub const JS_MAX_SAFE_INTEGER_I64: i64 = 9_007_199_254_740_991;
pub const JS_MAX_SAFE_INTEGER_U64: u64 = 9_007_199_254_740_991;

pub fn is_js_safe_integer_i64(value: i64) -> bool {
    (-JS_MAX_SAFE_INTEGER_I64..=JS_MAX_SAFE_INTEGER_I64).contains(&value)
}

/// Convert Arrow RecordBatches into a Vec of JSON objects (one per row).
pub fn record_batches_to_json_rows(results: &[RecordBatch]) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for batch in results {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col_arr = batch.column(col_idx);
                map.insert(field.name().clone(), array_value_to_json(col_arr, row));
            }
            out.push(serde_json::Value::Object(map));
        }
    }
    out
}

/// Convert a single cell from an Arrow array to a serde_json::Value.
pub fn array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
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
            .map(|a| {
                let value = a.value(row);
                if is_js_safe_integer_i64(value) {
                    serde_json::Value::Number(value.into())
                } else {
                    serde_json::Value::String(value.to_string())
                }
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as u64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| {
                let value = a.value(row);
                if value <= JS_MAX_SAFE_INTEGER_U64 {
                    serde_json::Value::Number(value.into())
                } else {
                    serde_json::Value::String(value.to_string())
                }
            })
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
            .map(|a| {
                let days = a.value(row);
                arrow_array::temporal_conversions::date32_to_datetime(days)
                    .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d").to_string()))
                    .unwrap_or_else(|| serde_json::Value::Number((days as i64).into()))
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| {
                let ms = a.value(row);
                arrow_array::temporal_conversions::date64_to_datetime(ms)
                    .map(|dt| {
                        serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
                    })
                    .unwrap_or_else(|| serde_json::Value::Number(ms.into()))
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|a| {
                let values = a.value(row);
                serde_json::Value::Array(
                    (0..values.len())
                        .map(|idx| array_value_to_json(&values, idx))
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|a| {
                let values = a.value(row);
                serde_json::Value::Array(
                    (0..values.len())
                        .map(|idx| array_value_to_json(&values, idx))
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        _ => {
            let display =
                arrow_cast::display::array_value_to_string(array, row).unwrap_or_default();
            serde_json::Value::String(display)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::array_value_to_json;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, UInt64Array};

    #[test]
    fn int64_outside_js_safe_range_is_stringified() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(9_007_199_254_740_992)]));
        assert_eq!(
            array_value_to_json(&values, 0),
            serde_json::Value::String("9007199254740992".to_string())
        );
    }

    #[test]
    fn uint64_outside_js_safe_range_is_stringified() {
        let values: ArrayRef = Arc::new(UInt64Array::from(vec![Some(9_007_199_254_740_992)]));
        assert_eq!(
            array_value_to_json(&values, 0),
            serde_json::Value::String("9007199254740992".to_string())
        );
    }

    #[test]
    fn uint64_within_js_safe_range_stays_numeric() {
        let values: ArrayRef = Arc::new(UInt64Array::from(vec![Some(9_007_199_254_740_991)]));
        assert_eq!(
            array_value_to_json(&values, 0),
            serde_json::json!(9_007_199_254_740_991u64)
        );
    }
}
