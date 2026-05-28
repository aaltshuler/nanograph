//! PyO3 binding for nanograph (the chosen "B" path).
//!
//! Mirrors the structure of `nanograph-ffi` (an owned tokio `Runtime` that
//! `block_on`s the async core) but exposes a `#[pyclass]` instead of a C ABI.
//!
//! Crucially for the Hermes use case, every blocking core call runs inside
//! `Python::allow_threads(...)`, so the GIL is released for the duration of
//! the database work. That makes `sync_turn()`-style writes from a daemon
//! thread genuinely non-blocking with respect to the agent's main loop —
//! the requirement the Hermes MemoryProvider contract imposes.
//!
//! Methods return JSON strings; the Python wrapper json.loads them. A fully
//! productionized binding would project to native Python objects and could
//! expose true async via `pyo3-async-runtimes`, but the synchronous +
//! allow_threads shape already satisfies the non-blocking contract.

use std::sync::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use nanograph::store::database::{Database, EmbedOptions, LoadMode};
use nanograph::{JsonParamMode, find_named_query, json_params_to_param_map};

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

#[pyclass]
struct Db {
    rt: Runtime,
    db: Mutex<Option<Database>>,
}

impl Db {
    fn database(&self) -> PyResult<Database> {
        self.db
            .lock()
            .map_err(|_| PyRuntimeError::new_err("lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| PyRuntimeError::new_err("database is closed"))
    }
}

#[pymethods]
impl Db {
    #[staticmethod]
    fn open_in_memory(py: Python<'_>, schema: &str) -> PyResult<Db> {
        let rt = Runtime::new().map_err(err)?;
        let db = py
            .allow_threads(|| rt.block_on(Database::open_in_memory(schema)))
            .map_err(err)?;
        Ok(Db {
            rt,
            db: Mutex::new(Some(db)),
        })
    }

    #[staticmethod]
    fn init(py: Python<'_>, path: &str, schema: &str) -> PyResult<Db> {
        let rt = Runtime::new().map_err(err)?;
        let db = py
            .allow_threads(|| rt.block_on(Database::init(std::path::Path::new(path), schema)))
            .map_err(err)?;
        Ok(Db {
            rt,
            db: Mutex::new(Some(db)),
        })
    }

    #[staticmethod]
    fn open(py: Python<'_>, path: &str) -> PyResult<Db> {
        let rt = Runtime::new().map_err(err)?;
        let db = py
            .allow_threads(|| rt.block_on(Database::open(std::path::Path::new(path))))
            .map_err(err)?;
        Ok(Db {
            rt,
            db: Mutex::new(Some(db)),
        })
    }

    fn load(&self, py: Python<'_>, data: &str, mode: &str) -> PyResult<()> {
        let mode = match mode {
            "overwrite" => LoadMode::Overwrite,
            "append" => LoadMode::Append,
            "merge" => LoadMode::Merge,
            other => return Err(PyRuntimeError::new_err(format!("bad load mode: {other}"))),
        };
        let db = self.database()?;
        py.allow_threads(|| self.rt.block_on(db.load_with_mode(data, mode)))
            .map_err(err)
    }

    #[pyo3(signature = (query_source, query_name, params=None))]
    fn run(
        &self,
        py: Python<'_>,
        query_source: &str,
        query_name: &str,
        params: Option<&str>,
    ) -> PyResult<String> {
        let query = find_named_query(query_source, query_name).map_err(err)?;
        let params_val: Option<serde_json::Value> = match params {
            Some(s) if !s.trim().is_empty() => Some(serde_json::from_str(s).map_err(err)?),
            _ => None,
        };
        let param_map =
            json_params_to_param_map(params_val.as_ref(), &query.params, JsonParamMode::Standard)
                .map_err(err)?;
        let db = self.database()?;
        // GIL released for the duration of query planning + execution.
        let value = py
            .allow_threads(|| -> Result<serde_json::Value, String> {
                if query.mutation.is_some() {
                    self.rt
                        .block_on(db.run_query(&query, &param_map))
                        .map(|r| r.to_sdk_json())
                        .map_err(|e| e.to_string())
                } else {
                    let prepared = db.prepare_read_query(&query).map_err(|e| e.to_string())?;
                    self.rt
                        .block_on(prepared.execute(&param_map))
                        .map(|r| r.to_sdk_json())
                        .map_err(|e| e.to_string())
                }
            })
            .map_err(PyRuntimeError::new_err)?;
        serde_json::to_string(&value).map_err(err)
    }

    #[pyo3(signature = (options=None))]
    fn changes(&self, py: Python<'_>, options: Option<&str>) -> PyResult<String> {
        let (from, to) = match options {
            Some(s) if !s.trim().is_empty() => {
                let v: serde_json::Value = serde_json::from_str(s).map_err(err)?;
                let from = v
                    .get("since")
                    .or_else(|| v.get("from"))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0);
                (from, v.get("to").and_then(|x| x.as_u64()))
            }
            _ => (0, None),
        };
        let db = self.database()?;
        let rows = py
            .allow_threads(|| self.rt.block_on(db.changes(from, to)))
            .map_err(err)?;
        serde_json::to_string(&rows).map_err(err)
    }

    #[pyo3(signature = (options=None))]
    fn embed(&self, py: Python<'_>, options: Option<&str>) -> PyResult<String> {
        let mut opts = EmbedOptions::default();
        if let Some(s) = options {
            if !s.trim().is_empty() {
                let v: serde_json::Value = serde_json::from_str(s).map_err(err)?;
                if let Some(b) = v.get("onlyNull").and_then(|x| x.as_bool()) {
                    opts.only_null = b;
                }
                if let Some(b) = v.get("reindex").and_then(|x| x.as_bool()) {
                    opts.reindex = b;
                }
                if let Some(b) = v.get("dryRun").and_then(|x| x.as_bool()) {
                    opts.dry_run = b;
                }
                if let Some(t) = v.get("typeName").and_then(|x| x.as_str()) {
                    opts.type_name = Some(t.to_string());
                }
                if let Some(p) = v.get("property").and_then(|x| x.as_str()) {
                    opts.property = Some(p.to_string());
                }
                if let Some(l) = v.get("limit").and_then(|x| x.as_u64()) {
                    opts.limit = Some(l as usize);
                }
            }
        }
        let db = self.database()?;
        let r = py
            .allow_threads(|| self.rt.block_on(db.embed(opts)))
            .map_err(err)?;
        serde_json::to_string(&serde_json::json!({
            "embeddingsGenerated": r.embeddings_generated,
            "rowsSelected": r.rows_selected,
            "propertiesSelected": r.properties_selected,
            "reindexedTypes": r.reindexed_types,
            "dryRun": r.dry_run,
        }))
        .map_err(err)
    }

    fn describe(&self) -> PyResult<String> {
        let db = self.database()?;
        let ir = &db.schema_ir;
        let node_types: Vec<_> = ir
            .node_types()
            .map(|nt| serde_json::json!({ "name": nt.name }))
            .collect();
        let edge_types: Vec<_> = ir
            .edge_types()
            .map(|et| serde_json::json!({ "name": et.name }))
            .collect();
        serde_json::to_string(&serde_json::json!({
            "nodeTypes": node_types,
            "edgeTypes": edge_types,
        }))
        .map_err(err)
    }

    fn close(&self) -> PyResult<()> {
        *self
            .db
            .lock()
            .map_err(|_| PyRuntimeError::new_err("lock poisoned"))? = None;
        Ok(())
    }
}

#[pymodule]
fn nanograph_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Db>()?;
    Ok(())
}
