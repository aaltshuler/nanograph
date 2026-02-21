use nanograph::ParamMap;
use nanograph::query::ast::{Literal, Param};
use nanograph::store::database::{CleanupOptions, CompactOptions, LoadMode};

const JS_MAX_SAFE_INTEGER_U64: u64 = 9_007_199_254_740_991;
const JS_MAX_SAFE_INTEGER_I64: i64 = JS_MAX_SAFE_INTEGER_U64 as i64;

fn is_js_safe_integer_i64(value: i64) -> bool {
    (-JS_MAX_SAFE_INTEGER_I64..=JS_MAX_SAFE_INTEGER_I64).contains(&value)
}

fn parse_i64_param(key: &str, value: &serde_json::Value) -> napi::Result<i64> {
    match value {
        serde_json::Value::Number(n) => {
            let parsed = if let Some(parsed) = n.as_i64() {
                parsed
            } else if let Some(parsed) = n.as_f64() {
                if !parsed.is_finite() || parsed.fract() != 0.0 {
                    return Err(napi::Error::from_reason(format!(
                        "param '{}': expected integer, got number",
                        key
                    )));
                }
                if parsed < i64::MIN as f64 || parsed > i64::MAX as f64 {
                    return Err(napi::Error::from_reason(format!(
                        "param '{}': integer {} is outside i64 range",
                        key, parsed
                    )));
                }
                parsed as i64
            } else {
                return Err(napi::Error::from_reason(format!(
                    "param '{}': expected integer, got number",
                    key
                )));
            };
            if !is_js_safe_integer_i64(parsed) {
                return Err(napi::Error::from_reason(format!(
                    "param '{}': integer {} exceeds JS safe integer range; pass a decimal string for exact values",
                    key, parsed
                )));
            }
            Ok(parsed)
        }
        serde_json::Value::String(s) => s.parse::<i64>().map_err(|_| {
            napi::Error::from_reason(format!(
                "param '{}': expected integer string, got '{}'",
                key, s
            ))
        }),
        _ => Err(napi::Error::from_reason(format!(
            "param '{}': expected integer, got {}",
            key,
            json_type_name(value)
        ))),
    }
}

fn parse_u64_param(key: &str, value: &serde_json::Value) -> napi::Result<u64> {
    match value {
        serde_json::Value::Number(n) => {
            let parsed = if let Some(parsed) = n.as_u64() {
                parsed
            } else if let Some(parsed) = n.as_f64() {
                if !parsed.is_finite() || parsed.fract() != 0.0 || parsed < 0.0 {
                    return Err(napi::Error::from_reason(format!(
                        "param '{}': expected unsigned integer, got number",
                        key
                    )));
                }
                if parsed > u64::MAX as f64 {
                    return Err(napi::Error::from_reason(format!(
                        "param '{}': integer {} is outside u64 range",
                        key, parsed
                    )));
                }
                parsed as u64
            } else {
                return Err(napi::Error::from_reason(format!(
                    "param '{}': expected unsigned integer, got number",
                    key
                )));
            };
            if parsed > JS_MAX_SAFE_INTEGER_U64 {
                return Err(napi::Error::from_reason(format!(
                    "param '{}': integer {} exceeds JS safe integer range; pass a decimal string for exact values",
                    key, parsed
                )));
            }
            Ok(parsed)
        }
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|_| {
            napi::Error::from_reason(format!(
                "param '{}': expected unsigned integer string, got '{}'",
                key, s
            ))
        }),
        _ => Err(napi::Error::from_reason(format!(
            "param '{}': expected unsigned integer, got {}",
            key,
            json_type_name(value)
        ))),
    }
}

/// Convert a JS params object to a ParamMap, using query param type declarations
/// for type-guided conversion.
pub fn js_object_to_param_map(
    params: Option<&serde_json::Value>,
    query_params: &[Param],
) -> napi::Result<ParamMap> {
    let mut map = ParamMap::new();
    let obj = match params {
        Some(serde_json::Value::Object(obj)) => obj,
        Some(serde_json::Value::Null) | None => return Ok(map),
        Some(other) => {
            return Err(napi::Error::from_reason(format!(
                "params must be an object, got {}",
                json_type_name(other)
            )));
        }
    };

    for (key, value) in obj {
        let decl = query_params.iter().find(|p| p.name == *key);
        let lit = if let Some(decl) = decl {
            convert_with_type_hint(key, value, &decl.type_name)?
        } else {
            convert_inferred(key, value)?
        };
        map.insert(key.clone(), lit);
    }
    Ok(map)
}

/// Convert a JS value to a Literal using the declared query param type.
fn convert_with_type_hint(
    key: &str,
    value: &serde_json::Value,
    type_name: &str,
) -> napi::Result<Literal> {
    match type_name {
        "String" => match value {
            serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
            other => Ok(Literal::String(other.to_string())),
        },
        "I32" | "I64" => {
            let n = parse_i64_param(key, value)?;
            Ok(Literal::Integer(n))
        }
        "U32" => {
            let n = parse_u64_param(key, value)?;
            if n > u32::MAX as u64 {
                return Err(napi::Error::from_reason(format!(
                    "param '{}': value {} exceeds U32 range",
                    key, n
                )));
            }
            Ok(Literal::Integer(n as i64))
        }
        "U64" => {
            let n = parse_u64_param(key, value)?;
            let n = i64::try_from(n).map_err(|_| {
                napi::Error::from_reason(format!(
                    "param '{}': value {} exceeds current engine range for U64 parameters (max {})",
                    key,
                    n,
                    i64::MAX
                ))
            })?;
            Ok(Literal::Integer(n))
        }
        "F32" | "F64" => {
            let f = value.as_f64().ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "param '{}': expected float, got {}",
                    key,
                    json_type_name(value)
                ))
            })?;
            Ok(Literal::Float(f))
        }
        "Bool" => {
            let b = value.as_bool().ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "param '{}': expected boolean, got {}",
                    key,
                    json_type_name(value)
                ))
            })?;
            Ok(Literal::Bool(b))
        }
        "Date" => match value {
            serde_json::Value::String(s) => Ok(Literal::Date(s.clone())),
            other => Err(napi::Error::from_reason(format!(
                "param '{}': expected date string, got {}",
                key,
                json_type_name(other)
            ))),
        },
        "DateTime" => match value {
            serde_json::Value::String(s) => Ok(Literal::DateTime(s.clone())),
            other => Err(napi::Error::from_reason(format!(
                "param '{}': expected datetime string, got {}",
                key,
                json_type_name(other)
            ))),
        },
        other if other.starts_with("Vector(") => {
            let expected_dim = parse_vector_dim(other).ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "param '{}': invalid vector type '{}' (expected Vector(N))",
                    key, other
                ))
            })?;
            let items = value.as_array().ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "param '{}': expected array for {}, got {}",
                    key,
                    other,
                    json_type_name(value)
                ))
            })?;
            if items.len() != expected_dim {
                return Err(napi::Error::from_reason(format!(
                    "param '{}': expected {} values for {}, got {}",
                    key,
                    expected_dim,
                    other,
                    items.len()
                )));
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let num = item.as_f64().ok_or_else(|| {
                    napi::Error::from_reason(format!(
                        "param '{}': vector element '{}' is not numeric",
                        key, item
                    ))
                })?;
                out.push(Literal::Float(num));
            }
            Ok(Literal::List(out))
        }
        _ => {
            // Enum or unknown type — treat as string
            match value {
                serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
                other => Ok(Literal::String(other.to_string())),
            }
        }
    }
}

/// Infer Literal type from JS value when no type declaration is available.
fn convert_inferred(key: &str, value: &serde_json::Value) -> napi::Result<Literal> {
    match value {
        serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
        serde_json::Value::Bool(b) => Ok(Literal::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if !is_js_safe_integer_i64(i) {
                    return Err(napi::Error::from_reason(format!(
                        "param '{}': integer {} exceeds JS safe integer range; use a decimal string and a typed query parameter for exact values",
                        key, i
                    )));
                }
                Ok(Literal::Integer(i))
            } else if let Some(u) = n.as_u64() {
                if u > JS_MAX_SAFE_INTEGER_U64 {
                    return Err(napi::Error::from_reason(format!(
                        "param '{}': integer {} exceeds JS safe integer range; use a decimal string and a typed query parameter for exact values",
                        key, u
                    )));
                }
                let i = i64::try_from(u).map_err(|_| {
                    napi::Error::from_reason(format!(
                        "param '{}': integer {} exceeds supported range (max {})",
                        key,
                        u,
                        i64::MAX
                    ))
                })?;
                Ok(Literal::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Literal::Float(f))
            } else {
                Err(napi::Error::from_reason(format!(
                    "param '{}': unsupported number value",
                    key
                )))
            }
        }
        serde_json::Value::Array(arr) => {
            let mut items = Vec::with_capacity(arr.len());
            for item in arr {
                items.push(convert_inferred(key, item)?);
            }
            Ok(Literal::List(items))
        }
        serde_json::Value::Null => Err(napi::Error::from_reason(format!(
            "param '{}': null values are not supported as query parameters",
            key
        ))),
        serde_json::Value::Object(_) => Err(napi::Error::from_reason(format!(
            "param '{}': object values are not supported as query parameters",
            key
        ))),
    }
}

fn parse_vector_dim(type_name: &str) -> Option<usize> {
    let dim = type_name
        .strip_prefix("Vector(")?
        .strip_suffix(')')?
        .parse::<usize>()
        .ok()?;
    if dim == 0 { None } else { Some(dim) }
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

pub fn parse_load_mode(mode: &str) -> napi::Result<LoadMode> {
    match mode {
        "overwrite" => Ok(LoadMode::Overwrite),
        "append" => Ok(LoadMode::Append),
        "merge" => Ok(LoadMode::Merge),
        _ => Err(napi::Error::from_reason(format!(
            "invalid load mode '{}': expected 'overwrite', 'append', or 'merge'",
            mode
        ))),
    }
}

pub fn parse_compact_options(opts: Option<&serde_json::Value>) -> napi::Result<CompactOptions> {
    let mut result = CompactOptions::default();
    let obj = match opts {
        Some(serde_json::Value::Object(obj)) => obj,
        Some(serde_json::Value::Null) | None => return Ok(result),
        Some(_) => {
            return Err(napi::Error::from_reason(
                "compact options must be an object",
            ));
        }
    };
    if let Some(v) = obj.get("targetRowsPerFragment") {
        let parsed = v.as_u64().ok_or_else(|| {
            napi::Error::from_reason("targetRowsPerFragment must be a positive integer")
        })?;
        if parsed == 0 {
            return Err(napi::Error::from_reason(
                "targetRowsPerFragment must be a positive integer",
            ));
        }
        result.target_rows_per_fragment = usize::try_from(parsed).map_err(|_| {
            napi::Error::from_reason("targetRowsPerFragment is too large for this platform")
        })?;
    }
    if let Some(v) = obj.get("materializeDeletions") {
        result.materialize_deletions = v
            .as_bool()
            .ok_or_else(|| napi::Error::from_reason("materializeDeletions must be a boolean"))?;
    }
    if let Some(v) = obj.get("materializeDeletionsThreshold") {
        let threshold = v.as_f64().ok_or_else(|| {
            napi::Error::from_reason("materializeDeletionsThreshold must be a number")
        })?;
        if !(0.0..=1.0).contains(&threshold) {
            return Err(napi::Error::from_reason(
                "materializeDeletionsThreshold must be between 0.0 and 1.0",
            ));
        }
        result.materialize_deletions_threshold = threshold as f32;
    }
    Ok(result)
}

pub fn parse_cleanup_options(opts: Option<&serde_json::Value>) -> napi::Result<CleanupOptions> {
    let mut result = CleanupOptions::default();
    let obj = match opts {
        Some(serde_json::Value::Object(obj)) => obj,
        Some(serde_json::Value::Null) | None => return Ok(result),
        Some(_) => {
            return Err(napi::Error::from_reason(
                "cleanup options must be an object",
            ));
        }
    };
    if let Some(v) = obj.get("retainTxVersions") {
        let parsed = v.as_u64().ok_or_else(|| {
            napi::Error::from_reason("retainTxVersions must be a positive integer")
        })?;
        if parsed == 0 {
            return Err(napi::Error::from_reason(
                "retainTxVersions must be a positive integer",
            ));
        }
        result.retain_tx_versions = parsed;
    }
    if let Some(v) = obj.get("retainDatasetVersions") {
        let parsed = v.as_u64().ok_or_else(|| {
            napi::Error::from_reason("retainDatasetVersions must be a positive integer")
        })?;
        if parsed == 0 {
            return Err(napi::Error::from_reason(
                "retainDatasetVersions must be a positive integer",
            ));
        }
        result.retain_dataset_versions = usize::try_from(parsed).map_err(|_| {
            napi::Error::from_reason("retainDatasetVersions is too large for this platform")
        })?;
    }
    Ok(result)
}
