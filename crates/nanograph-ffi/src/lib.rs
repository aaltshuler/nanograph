use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::{Mutex, OnceLock};

use tokio::runtime::Runtime;

use nanograph::error::NanoError;
use nanograph::json_output::record_batches_to_json_rows;
use nanograph::query::ast::{Literal, Param};
use nanograph::query::parser::parse_query;
use nanograph::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use nanograph::store::database::{CleanupOptions, CompactOptions, Database, LoadMode};
use nanograph::{ParamMap, execute_mutation, execute_query, lower_mutation_query, lower_query};

type FfiResult<T> = std::result::Result<T, String>;

const STATUS_OK: c_int = 0;
const STATUS_ERR: c_int = -1;

thread_local! {
    static LAST_ERROR_CSTR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

static LAST_ERROR: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn last_error_slot() -> &'static Mutex<Option<String>> {
    LAST_ERROR.get_or_init(|| Mutex::new(None))
}

fn set_last_error(message: impl Into<String>) {
    let next = message.into().replace('\0', "\\0");
    if let Ok(mut slot) = last_error_slot().lock() {
        *slot = Some(next);
    }
}

fn clear_last_error() {
    if let Ok(mut slot) = last_error_slot().lock() {
        *slot = None;
    }
}

fn to_status(result: FfiResult<()>) -> c_int {
    match result {
        Ok(()) => {
            clear_last_error();
            STATUS_OK
        }
        Err(err) => {
            set_last_error(err);
            STATUS_ERR
        }
    }
}

fn json_result_to_ptr(result: FfiResult<serde_json::Value>) -> *mut c_char {
    match result {
        Ok(value) => match serde_json::to_string(&value) {
            Ok(serialized) => match CString::new(serialized) {
                Ok(s) => {
                    clear_last_error();
                    s.into_raw()
                }
                Err(_) => {
                    set_last_error("failed to build CString for JSON response");
                    ptr::null_mut()
                }
            },
            Err(e) => {
                set_last_error(format!("failed to serialize JSON response: {}", e));
                ptr::null_mut()
            }
        },
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

fn parse_required_str(arg_name: &str, value: *const c_char) -> FfiResult<String> {
    if value.is_null() {
        return Err(format!("{} must not be null", arg_name));
    }
    // SAFETY: `value` is verified non-null and points to a caller-provided C string.
    let c_str = unsafe { CStr::from_ptr(value) };
    c_str
        .to_str()
        .map(|s| s.to_string())
        .map_err(|e| format!("{} must be valid UTF-8: {}", arg_name, e))
}

fn parse_optional_json(value: *const c_char) -> FfiResult<Option<serde_json::Value>> {
    if value.is_null() {
        return Ok(None);
    }
    let s = parse_required_str("json", value)?;
    if s.trim().is_empty() {
        return Ok(None);
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&s).map_err(|e| format!("invalid JSON payload: {}", e))?;
    Ok(Some(parsed))
}

fn to_ffi_err(err: NanoError) -> String {
    err.to_string()
}

fn parse_load_mode(mode: &str) -> FfiResult<LoadMode> {
    match mode {
        "overwrite" => Ok(LoadMode::Overwrite),
        "append" => Ok(LoadMode::Append),
        "merge" => Ok(LoadMode::Merge),
        _ => Err(format!(
            "invalid load mode '{}': expected 'overwrite', 'append', or 'merge'",
            mode
        )),
    }
}

fn parse_compact_options(opts: Option<&serde_json::Value>) -> FfiResult<CompactOptions> {
    let mut result = CompactOptions::default();
    let obj = match opts {
        Some(serde_json::Value::Object(obj)) => obj,
        Some(serde_json::Value::Null) | None => return Ok(result),
        Some(_) => return Err("compact options must be a JSON object".to_string()),
    };
    for key in obj.keys() {
        match key.as_str() {
            "targetRowsPerFragment" | "materializeDeletions" | "materializeDeletionsThreshold" => {}
            _ => return Err(format!("unknown compact option '{}'", key)),
        }
    }
    if let Some(v) = obj.get("targetRowsPerFragment") {
        let parsed = v
            .as_u64()
            .ok_or_else(|| "targetRowsPerFragment must be a positive integer".to_string())?;
        if parsed == 0 {
            return Err("targetRowsPerFragment must be a positive integer".to_string());
        }
        result.target_rows_per_fragment = usize::try_from(parsed)
            .map_err(|_| "targetRowsPerFragment is too large for this platform".to_string())?;
    }
    if let Some(v) = obj.get("materializeDeletions") {
        result.materialize_deletions = v
            .as_bool()
            .ok_or_else(|| "materializeDeletions must be a boolean".to_string())?;
    }
    if let Some(v) = obj.get("materializeDeletionsThreshold") {
        let threshold = v
            .as_f64()
            .ok_or_else(|| "materializeDeletionsThreshold must be a number".to_string())?;
        if !(0.0..=1.0).contains(&threshold) {
            return Err("materializeDeletionsThreshold must be between 0.0 and 1.0".to_string());
        }
        result.materialize_deletions_threshold = threshold as f32;
    }
    Ok(result)
}

fn parse_cleanup_options(opts: Option<&serde_json::Value>) -> FfiResult<CleanupOptions> {
    let mut result = CleanupOptions::default();
    let obj = match opts {
        Some(serde_json::Value::Object(obj)) => obj,
        Some(serde_json::Value::Null) | None => return Ok(result),
        Some(_) => return Err("cleanup options must be a JSON object".to_string()),
    };
    for key in obj.keys() {
        match key.as_str() {
            "retainTxVersions" | "retainDatasetVersions" => {}
            _ => return Err(format!("unknown cleanup option '{}'", key)),
        }
    }
    if let Some(v) = obj.get("retainTxVersions") {
        let parsed = v
            .as_u64()
            .ok_or_else(|| "retainTxVersions must be a positive integer".to_string())?;
        if parsed == 0 {
            return Err("retainTxVersions must be a positive integer".to_string());
        }
        result.retain_tx_versions = parsed;
    }
    if let Some(v) = obj.get("retainDatasetVersions") {
        let parsed = v
            .as_u64()
            .ok_or_else(|| "retainDatasetVersions must be a positive integer".to_string())?;
        if parsed == 0 {
            return Err("retainDatasetVersions must be a positive integer".to_string());
        }
        result.retain_dataset_versions = usize::try_from(parsed)
            .map_err(|_| "retainDatasetVersions is too large for this platform".to_string())?;
    }
    Ok(result)
}

fn parse_i64_param(key: &str, value: &serde_json::Value) -> FfiResult<i64> {
    match value {
        serde_json::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| format!("param '{}': expected integer number", key)),
        serde_json::Value::String(s) => s
            .parse::<i64>()
            .map_err(|_| format!("param '{}': expected integer string, got '{}'", key, s)),
        _ => Err(format!("param '{}': expected integer", key)),
    }
}

fn parse_u64_param(key: &str, value: &serde_json::Value) -> FfiResult<u64> {
    match value {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| format!("param '{}': expected unsigned integer number", key)),
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|_| {
            format!(
                "param '{}': expected unsigned integer string, got '{}'",
                key, s
            )
        }),
        _ => Err(format!("param '{}': expected unsigned integer", key)),
    }
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn json_value_to_literal_inferred(key: &str, value: &serde_json::Value) -> FfiResult<Literal> {
    match value {
        serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
        serde_json::Value::Bool(v) => Ok(Literal::Bool(*v)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Literal::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Literal::Float(f))
            } else {
                Err(format!("param '{}': unsupported numeric value", key))
            }
        }
        serde_json::Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(json_value_to_literal_inferred(key, value)?);
            }
            Ok(Literal::List(out))
        }
        serde_json::Value::Null => Err(format!("param '{}': null is not supported", key)),
        serde_json::Value::Object(_) => Err(format!("param '{}': object is not supported", key)),
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

fn json_value_to_literal_typed(
    key: &str,
    value: &serde_json::Value,
    type_name: &str,
) -> FfiResult<Literal> {
    match type_name {
        "String" => match value {
            serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
            other => Err(format!(
                "param '{}': expected string, got {}",
                key,
                json_type_name(other)
            )),
        },
        "I32" | "I64" => Ok(Literal::Integer(parse_i64_param(key, value)?)),
        "U32" => {
            let v = parse_u64_param(key, value)?;
            let v = u32::try_from(v)
                .map_err(|_| format!("param '{}': value {} exceeds U32", key, v))?;
            Ok(Literal::Integer(i64::from(v)))
        }
        "U64" => {
            let v = parse_u64_param(key, value)?;
            let v = i64::try_from(v).map_err(|_| {
                format!(
                    "param '{}': value {} exceeds current engine range for U64 (max {})",
                    key,
                    v,
                    i64::MAX
                )
            })?;
            Ok(Literal::Integer(v))
        }
        "F32" | "F64" => value
            .as_f64()
            .map(Literal::Float)
            .ok_or_else(|| format!("param '{}': expected float", key)),
        "Bool" => value
            .as_bool()
            .map(Literal::Bool)
            .ok_or_else(|| format!("param '{}': expected boolean", key)),
        "Date" => match value {
            serde_json::Value::String(s) => Ok(Literal::Date(s.clone())),
            _ => Err(format!("param '{}': expected date string", key)),
        },
        "DateTime" => match value {
            serde_json::Value::String(s) => Ok(Literal::DateTime(s.clone())),
            _ => Err(format!("param '{}': expected datetime string", key)),
        },
        t if t.starts_with("Vector(") => {
            let expected_dim = parse_vector_dim(t)
                .ok_or_else(|| format!("param '{}': invalid vector type '{}'", key, t))?;
            let values = value
                .as_array()
                .ok_or_else(|| format!("param '{}': expected array for {}", key, t))?;
            if values.len() != expected_dim {
                return Err(format!(
                    "param '{}': expected {} values for {}, got {}",
                    key,
                    expected_dim,
                    t,
                    values.len()
                ));
            }
            let mut out = Vec::with_capacity(values.len());
            for item in values {
                let num = item
                    .as_f64()
                    .ok_or_else(|| format!("param '{}': vector element is not numeric", key))?;
                out.push(Literal::Float(num));
            }
            Ok(Literal::List(out))
        }
        _ => match value {
            serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
            other => Err(format!(
                "param '{}': expected string for type '{}', got {}",
                key,
                type_name,
                json_type_name(other)
            )),
        },
    }
}

fn json_params_to_param_map(
    params: Option<&serde_json::Value>,
    query_params: &[Param],
) -> FfiResult<ParamMap> {
    let mut map = ParamMap::new();
    let object = match params {
        Some(serde_json::Value::Object(obj)) => obj,
        Some(serde_json::Value::Null) | None => return Ok(map),
        Some(_) => return Err("params must be a JSON object".to_string()),
    };

    for (key, value) in object {
        let decl = query_params.iter().find(|p| p.name == *key);
        let literal = if let Some(decl) = decl {
            json_value_to_literal_typed(key, value, &decl.type_name)?
        } else {
            json_value_to_literal_inferred(key, value)?
        };
        map.insert(key.clone(), literal);
    }
    Ok(map)
}

fn prop_def_to_json(prop: &nanograph::schema_ir::PropDef) -> serde_json::Value {
    let mut obj = serde_json::json!({
        "name": prop.name,
        "type": prop.scalar_type,
        "nullable": prop.nullable,
    });
    if prop.list {
        obj["list"] = serde_json::Value::Bool(true);
    }
    if prop.key {
        obj["key"] = serde_json::Value::Bool(true);
    }
    if prop.unique {
        obj["unique"] = serde_json::Value::Bool(true);
    }
    if prop.index {
        obj["index"] = serde_json::Value::Bool(true);
    }
    if !prop.enum_values.is_empty() {
        obj["enumValues"] = serde_json::json!(prop.enum_values);
    }
    if let Some(ref src) = prop.embed_source {
        obj["embedSource"] = serde_json::Value::String(src.clone());
    }
    obj
}

pub struct NanoGraphHandle {
    runtime: Runtime,
    db: Mutex<Option<Database>>,
}

impl NanoGraphHandle {
    fn with_runtime(runtime: Runtime, db: Database) -> Self {
        Self {
            runtime,
            db: Mutex::new(Some(db)),
        }
    }
}

fn with_handle<T>(
    handle: *mut NanoGraphHandle,
    f: impl FnOnce(&NanoGraphHandle) -> FfiResult<T>,
) -> FfiResult<T> {
    if handle.is_null() {
        return Err("database handle is null".to_string());
    }
    // SAFETY: pointer is checked for null above and expected to come from this library.
    let handle = unsafe { &*handle };
    f(handle)
}

fn run_query_json(
    handle: &NanoGraphHandle,
    query_source: &str,
    query_name: &str,
    params: Option<serde_json::Value>,
) -> FfiResult<serde_json::Value> {
    let queries = parse_query(query_source).map_err(to_ffi_err)?;
    let query = queries
        .queries
        .iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("query '{}' not found", query_name))?
        .clone();
    let param_map = json_params_to_param_map(params.as_ref(), &query.params)?;

    if query.mutation.is_some() {
        let mut guard = handle
            .db
            .lock()
            .map_err(|_| "database mutex is poisoned".to_string())?;
        let db = guard
            .as_mut()
            .ok_or_else(|| "database is closed".to_string())?;
        let checked = typecheck_query_decl(db.catalog(), &query).map_err(to_ffi_err)?;
        if !matches!(checked, CheckedQuery::Mutation(_)) {
            return Err("expected mutation query".to_string());
        }
        let mutation_ir = lower_mutation_query(&query).map_err(to_ffi_err)?;
        let result = handle
            .runtime
            .block_on(execute_mutation(&mutation_ir, db, &param_map))
            .map_err(to_ffi_err)?;
        Ok(serde_json::json!({
            "affectedNodes": result.affected_nodes,
            "affectedEdges": result.affected_edges,
        }))
    } else {
        let (catalog, storage) = {
            let guard = handle
                .db
                .lock()
                .map_err(|_| "database mutex is poisoned".to_string())?;
            let db = guard
                .as_ref()
                .ok_or_else(|| "database is closed".to_string())?;
            (db.catalog().clone(), db.snapshot())
        };
        let type_ctx = typecheck_query(&catalog, &query).map_err(to_ffi_err)?;
        let ir = lower_query(&catalog, &query, &type_ctx).map_err(to_ffi_err)?;
        let results = handle
            .runtime
            .block_on(execute_query(&ir, storage, &param_map))
            .map_err(to_ffi_err)?;
        Ok(serde_json::Value::Array(record_batches_to_json_rows(
            &results,
        )))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_last_error_message() -> *const c_char {
    let message = match last_error_slot().lock() {
        Ok(guard) => guard.clone(),
        Err(_) => None,
    };
    let Some(message) = message else {
        return ptr::null();
    };

    LAST_ERROR_CSTR.with(|slot| {
        let fallback = CString::new("nanograph-ffi: error contained interior null byte")
            .expect("static string must be valid CString");
        let next = CString::new(message).unwrap_or(fallback);
        let mut slot = slot.borrow_mut();
        *slot = Some(next);
        slot.as_ref().map_or(ptr::null(), |s| s.as_ptr())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_string_free(value: *mut c_char) {
    if value.is_null() {
        return;
    }
    // SAFETY: pointer must originate from CString::into_raw in this library.
    unsafe {
        let _ = CString::from_raw(value);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_init(
    db_path: *const c_char,
    schema_source: *const c_char,
) -> *mut NanoGraphHandle {
    let result: FfiResult<*mut NanoGraphHandle> = (|| {
        let db_path = parse_required_str("db_path", db_path)?;
        let schema_source = parse_required_str("schema_source", schema_source)?;
        let runtime = Runtime::new().map_err(|e| format!("failed to create runtime: {}", e))?;
        let db = runtime
            .block_on(Database::init(db_path.as_ref(), &schema_source))
            .map_err(to_ffi_err)?;
        let handle = NanoGraphHandle::with_runtime(runtime, db);
        Ok(Box::into_raw(Box::new(handle)))
    })();

    match result {
        Ok(handle) => {
            clear_last_error();
            handle
        }
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_open(db_path: *const c_char) -> *mut NanoGraphHandle {
    let result: FfiResult<*mut NanoGraphHandle> = (|| {
        let db_path = parse_required_str("db_path", db_path)?;
        let runtime = Runtime::new().map_err(|e| format!("failed to create runtime: {}", e))?;
        let db = runtime
            .block_on(Database::open(db_path.as_ref()))
            .map_err(to_ffi_err)?;
        let handle = NanoGraphHandle::with_runtime(runtime, db);
        Ok(Box::into_raw(Box::new(handle)))
    })();

    match result {
        Ok(handle) => {
            clear_last_error();
            handle
        }
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_close(handle: *mut NanoGraphHandle) -> c_int {
    to_status(with_handle(handle, |handle| {
        let mut guard = handle
            .db
            .lock()
            .map_err(|_| "database mutex is poisoned".to_string())?;
        *guard = None;
        Ok(())
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_destroy(handle: *mut NanoGraphHandle) {
    if handle.is_null() {
        return;
    }
    // SAFETY: pointer must originate from Box::into_raw in this library.
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_load(
    handle: *mut NanoGraphHandle,
    data_source: *const c_char,
    mode: *const c_char,
) -> c_int {
    let data_source = match parse_required_str("data_source", data_source) {
        Ok(v) => v,
        Err(err) => {
            set_last_error(err);
            return STATUS_ERR;
        }
    };
    let mode = match parse_required_str("mode", mode) {
        Ok(v) => v,
        Err(err) => {
            set_last_error(err);
            return STATUS_ERR;
        }
    };

    to_status(with_handle(handle, |handle| {
        let load_mode = parse_load_mode(&mode)?;
        let mut guard = handle
            .db
            .lock()
            .map_err(|_| "database mutex is poisoned".to_string())?;
        let db = guard
            .as_mut()
            .ok_or_else(|| "database is closed".to_string())?;
        handle
            .runtime
            .block_on(db.load_with_mode(&data_source, load_mode))
            .map_err(to_ffi_err)?;
        Ok(())
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_run(
    handle: *mut NanoGraphHandle,
    query_source: *const c_char,
    query_name: *const c_char,
    params_json: *const c_char,
) -> *mut c_char {
    let result = (|| {
        let query_source = parse_required_str("query_source", query_source)?;
        let query_name = parse_required_str("query_name", query_name)?;
        let params = parse_optional_json(params_json)?;
        with_handle(handle, |handle| {
            run_query_json(handle, &query_source, &query_name, params)
        })
    })();
    json_result_to_ptr(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_check(
    handle: *mut NanoGraphHandle,
    query_source: *const c_char,
) -> *mut c_char {
    let result = (|| {
        let query_source = parse_required_str("query_source", query_source)?;
        with_handle(handle, |handle| {
            let queries = parse_query(&query_source).map_err(to_ffi_err)?;
            let catalog = {
                let guard = handle
                    .db
                    .lock()
                    .map_err(|_| "database mutex is poisoned".to_string())?;
                let db = guard
                    .as_ref()
                    .ok_or_else(|| "database is closed".to_string())?;
                db.catalog().clone()
            };

            let mut checks = Vec::with_capacity(queries.queries.len());
            for q in &queries.queries {
                match typecheck_query_decl(&catalog, q) {
                    Ok(CheckedQuery::Read(_)) => checks.push(serde_json::json!({
                        "name": q.name,
                        "kind": "read",
                        "status": "ok",
                    })),
                    Ok(CheckedQuery::Mutation(_)) => checks.push(serde_json::json!({
                        "name": q.name,
                        "kind": "mutation",
                        "status": "ok",
                    })),
                    Err(e) => checks.push(serde_json::json!({
                        "name": q.name,
                        "kind": if q.mutation.is_some() { "mutation" } else { "read" },
                        "status": "error",
                        "error": e.to_string(),
                    })),
                }
            }
            Ok(serde_json::Value::Array(checks))
        })
    })();
    json_result_to_ptr(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_describe(handle: *mut NanoGraphHandle) -> *mut c_char {
    let result = with_handle(handle, |handle| {
        let guard = handle
            .db
            .lock()
            .map_err(|_| "database mutex is poisoned".to_string())?;
        let db = guard
            .as_ref()
            .ok_or_else(|| "database is closed".to_string())?;
        let ir = &db.schema_ir;

        let node_types: Vec<serde_json::Value> = ir
            .node_types()
            .map(|nt| {
                serde_json::json!({
                    "name": nt.name,
                    "typeId": nt.type_id,
                    "properties": nt.properties.iter().map(prop_def_to_json).collect::<Vec<_>>(),
                })
            })
            .collect();

        let edge_types: Vec<serde_json::Value> = ir
            .edge_types()
            .map(|et| {
                serde_json::json!({
                    "name": et.name,
                    "srcType": et.src_type_name,
                    "dstType": et.dst_type_name,
                    "typeId": et.type_id,
                    "properties": et.properties.iter().map(prop_def_to_json).collect::<Vec<_>>(),
                })
            })
            .collect();

        Ok(serde_json::json!({
            "nodeTypes": node_types,
            "edgeTypes": edge_types,
        }))
    });
    json_result_to_ptr(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_compact(
    handle: *mut NanoGraphHandle,
    options_json: *const c_char,
) -> *mut c_char {
    let result = (|| {
        let options = parse_optional_json(options_json)?;
        with_handle(handle, |handle| {
            let opts = parse_compact_options(options.as_ref())?;
            let mut guard = handle
                .db
                .lock()
                .map_err(|_| "database mutex is poisoned".to_string())?;
            let db = guard
                .as_mut()
                .ok_or_else(|| "database is closed".to_string())?;
            let result = handle
                .runtime
                .block_on(db.compact(opts))
                .map_err(to_ffi_err)?;
            Ok(serde_json::json!({
                "datasetsConsidered": result.datasets_considered,
                "datasetsCompacted": result.datasets_compacted,
                "fragmentsRemoved": result.fragments_removed,
                "fragmentsAdded": result.fragments_added,
                "filesRemoved": result.files_removed,
                "filesAdded": result.files_added,
                "manifestCommitted": result.manifest_committed,
            }))
        })
    })();
    json_result_to_ptr(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_cleanup(
    handle: *mut NanoGraphHandle,
    options_json: *const c_char,
) -> *mut c_char {
    let result = (|| {
        let options = parse_optional_json(options_json)?;
        with_handle(handle, |handle| {
            let opts = parse_cleanup_options(options.as_ref())?;
            let mut guard = handle
                .db
                .lock()
                .map_err(|_| "database mutex is poisoned".to_string())?;
            let db = guard
                .as_mut()
                .ok_or_else(|| "database is closed".to_string())?;
            let result = handle
                .runtime
                .block_on(db.cleanup(opts))
                .map_err(to_ffi_err)?;
            Ok(serde_json::json!({
                "txRowsRemoved": result.tx_rows_removed,
                "txRowsKept": result.tx_rows_kept,
                "cdcRowsRemoved": result.cdc_rows_removed,
                "cdcRowsKept": result.cdc_rows_kept,
                "datasetsCleaned": result.datasets_cleaned,
                "datasetOldVersionsRemoved": result.dataset_old_versions_removed,
                "datasetBytesRemoved": result.dataset_bytes_removed,
            }))
        })
    })();
    json_result_to_ptr(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn nanograph_db_doctor(handle: *mut NanoGraphHandle) -> *mut c_char {
    let result = with_handle(handle, |handle| {
        let guard = handle
            .db
            .lock()
            .map_err(|_| "database mutex is poisoned".to_string())?;
        let db = guard
            .as_ref()
            .ok_or_else(|| "database is closed".to_string())?;
        let report = handle.runtime.block_on(db.doctor()).map_err(to_ffi_err)?;
        Ok(serde_json::json!({
            "healthy": report.healthy,
            "issues": report.issues,
            "warnings": report.warnings,
            "datasetsChecked": report.datasets_checked,
            "txRows": report.tx_rows,
            "cdcRows": report.cdc_rows,
        }))
    });
    json_result_to_ptr(result)
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;
    use std::thread;

    use super::{clear_last_error, nanograph_db_open, nanograph_last_error_message};

    #[test]
    fn last_error_is_visible_across_threads() {
        clear_last_error();

        thread::spawn(|| {
            let _ = nanograph_db_open(ptr::null());
        })
        .join()
        .expect("error producer thread panicked");

        let msg = thread::spawn(|| {
            let ptr = nanograph_last_error_message();
            assert!(!ptr.is_null(), "expected error pointer");
            // SAFETY: pointer originates from `nanograph_last_error_message`.
            unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() }
        })
        .join()
        .expect("error reader thread panicked");

        assert!(
            msg.contains("db_path must not be null"),
            "unexpected error message: {}",
            msg
        );
    }
}
