#![recursion_limit = "256"]

mod convert;

use std::path::PathBuf;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::Mutex;

use nanograph::error::NanoError;
use nanograph::json_output::record_batches_to_json_rows;
use nanograph::query::parser::parse_query;
use nanograph::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use nanograph::store::database::Database;
use nanograph::{ParamMap, execute_mutation, execute_query, lower_mutation_query, lower_query};

use convert::{
    js_object_to_param_map, parse_cleanup_options, parse_compact_options, parse_load_mode,
};

fn to_napi_err(e: NanoError) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

#[napi(js_name = "Database")]
pub struct JsDatabase {
    inner: Arc<Mutex<Option<Database>>>,
}

impl JsDatabase {
    fn db_path(path: &str) -> PathBuf {
        PathBuf::from(path)
    }
}

fn prop_def_to_json(prop: &nanograph::schema_ir::PropDef) -> serde_json::Value {
    let mut obj = serde_json::json!({
        "name": prop.name,
        "propId": prop.prop_id,
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

#[napi]
impl JsDatabase {
    /// Create a new database from a schema string.
    ///
    /// ```js
    /// const db = await Database.init("my.nano", schemaSource);
    /// ```
    #[napi(factory)]
    pub async fn init(db_path: String, schema_source: String) -> Result<Self> {
        let path = Self::db_path(&db_path);
        let db = Database::init(&path, &schema_source)
            .await
            .map_err(to_napi_err)?;
        Ok(JsDatabase {
            inner: Arc::new(Mutex::new(Some(db))),
        })
    }

    /// Open an existing database.
    ///
    /// ```js
    /// const db = await Database.open("my.nano");
    /// ```
    #[napi(factory)]
    pub async fn open(db_path: String) -> Result<Self> {
        let path = Self::db_path(&db_path);
        let db = Database::open(&path).await.map_err(to_napi_err)?;
        Ok(JsDatabase {
            inner: Arc::new(Mutex::new(Some(db))),
        })
    }

    /// Load JSONL data into the database.
    ///
    /// ```js
    /// await db.load(jsonlString, "overwrite");
    /// ```
    #[napi]
    pub async fn load(&self, data_source: String, mode: String) -> Result<()> {
        let load_mode = parse_load_mode(&mode)?;
        let mut guard = self.inner.lock().await;
        let db = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        db.load_with_mode(&data_source, load_mode)
            .await
            .map_err(to_napi_err)
    }

    /// Execute a named query from query source text.
    ///
    /// For read queries, returns an array of row objects.
    /// For mutation queries, returns `{ affectedNodes, affectedEdges }`.
    ///
    /// ```js
    /// const rows = await db.run(querySource, "findPeople", { minAge: 21 });
    /// ```
    #[napi]
    pub async fn run(
        &self,
        query_source: String,
        query_name: String,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let queries = parse_query(&query_source).map_err(to_napi_err)?;
        let query = queries
            .queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| napi::Error::from_reason(format!("query '{}' not found", query_name)))?
            .clone();

        let param_map: ParamMap = js_object_to_param_map(params.as_ref(), &query.params)?;

        if query.mutation.is_some() {
            // Mutation path — holds lock for the entire operation
            let mut guard = self.inner.lock().await;
            let db = guard
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("database is closed"))?;

            let checked = typecheck_query_decl(db.catalog(), &query).map_err(to_napi_err)?;
            if !matches!(checked, CheckedQuery::Mutation(_)) {
                return Err(napi::Error::from_reason("expected mutation query"));
            }

            let mutation_ir = lower_mutation_query(&query).map_err(to_napi_err)?;
            let result = execute_mutation(&mutation_ir, db, &param_map)
                .await
                .map_err(to_napi_err)?;

            Ok(serde_json::json!({
                "affectedNodes": result.affected_nodes,
                "affectedEdges": result.affected_edges,
            }))
        } else {
            // Read path — clone catalog + snapshot, then release lock
            let (catalog, storage) = {
                let guard = self.inner.lock().await;
                let db = guard
                    .as_ref()
                    .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
                (db.catalog().clone(), db.snapshot())
            };

            let type_ctx = typecheck_query(&catalog, &query).map_err(to_napi_err)?;
            let ir = lower_query(&catalog, &query, &type_ctx).map_err(to_napi_err)?;
            let results = execute_query(&ir, storage, &param_map)
                .await
                .map_err(to_napi_err)?;

            let rows = record_batches_to_json_rows(&results);
            Ok(serde_json::Value::Array(rows))
        }
    }

    /// Typecheck all queries in the source text against the database schema.
    ///
    /// Returns an array of `{ name, kind, status, error? }` objects.
    ///
    /// ```js
    /// const checks = await db.check(querySource);
    /// ```
    #[napi]
    pub async fn check(&self, query_source: String) -> Result<serde_json::Value> {
        let queries = parse_query(&query_source).map_err(to_napi_err)?;

        let guard = self.inner.lock().await;
        let db = guard
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let catalog = db.catalog().clone();
        drop(guard);

        let mut checks = Vec::with_capacity(queries.queries.len());
        for q in &queries.queries {
            match typecheck_query_decl(&catalog, q) {
                Ok(CheckedQuery::Read(_)) => {
                    checks.push(serde_json::json!({
                        "name": q.name,
                        "kind": "read",
                        "status": "ok",
                    }));
                }
                Ok(CheckedQuery::Mutation(_)) => {
                    checks.push(serde_json::json!({
                        "name": q.name,
                        "kind": "mutation",
                        "status": "ok",
                    }));
                }
                Err(e) => {
                    checks.push(serde_json::json!({
                        "name": q.name,
                        "kind": if q.mutation.is_some() { "mutation" } else { "read" },
                        "status": "error",
                        "error": e.to_string(),
                    }));
                }
            }
        }

        Ok(serde_json::Value::Array(checks))
    }

    /// Return schema introspection as a JSON object.
    ///
    /// ```js
    /// const info = await db.describe();
    /// ```
    #[napi]
    pub async fn describe(&self) -> Result<serde_json::Value> {
        let ir = {
            let guard = self.inner.lock().await;
            let db = guard
                .as_ref()
                .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
            db.schema_ir.clone()
        };

        let mut node_types = Vec::new();
        for nt in ir.node_types() {
            let props: Vec<serde_json::Value> =
                nt.properties.iter().map(prop_def_to_json).collect();
            node_types.push(serde_json::json!({
                "name": nt.name,
                "typeId": nt.type_id,
                "properties": props,
            }));
        }

        let mut edge_types = Vec::new();
        for et in ir.edge_types() {
            let props: Vec<serde_json::Value> =
                et.properties.iter().map(prop_def_to_json).collect();
            edge_types.push(serde_json::json!({
                "name": et.name,
                "srcType": et.src_type_name,
                "dstType": et.dst_type_name,
                "typeId": et.type_id,
                "properties": props,
            }));
        }

        Ok(serde_json::json!({
            "nodeTypes": node_types,
            "edgeTypes": edge_types,
        }))
    }

    /// Compact Lance datasets to reduce fragmentation.
    ///
    /// ```js
    /// const result = await db.compact({ targetRowsPerFragment: 1048576 });
    /// ```
    #[napi]
    pub async fn compact(&self, options: Option<serde_json::Value>) -> Result<serde_json::Value> {
        let opts = parse_compact_options(options.as_ref())?;
        let mut guard = self.inner.lock().await;
        let db = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let result = db.compact(opts).await.map_err(to_napi_err)?;
        Ok(serde_json::json!({
            "datasetsConsidered": result.datasets_considered,
            "datasetsCompacted": result.datasets_compacted,
            "fragmentsRemoved": result.fragments_removed,
            "fragmentsAdded": result.fragments_added,
            "filesRemoved": result.files_removed,
            "filesAdded": result.files_added,
            "manifestCommitted": result.manifest_committed,
        }))
    }

    /// Clean up old dataset versions and prune transaction/CDC logs.
    ///
    /// ```js
    /// const result = await db.cleanup({ retainTxVersions: 128 });
    /// ```
    #[napi]
    pub async fn cleanup(&self, options: Option<serde_json::Value>) -> Result<serde_json::Value> {
        let opts = parse_cleanup_options(options.as_ref())?;
        let mut guard = self.inner.lock().await;
        let db = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let result = db.cleanup(opts).await.map_err(to_napi_err)?;
        Ok(serde_json::json!({
            "txRowsRemoved": result.tx_rows_removed,
            "txRowsKept": result.tx_rows_kept,
            "cdcRowsRemoved": result.cdc_rows_removed,
            "cdcRowsKept": result.cdc_rows_kept,
            "datasetsCleaned": result.datasets_cleaned,
            "datasetOldVersionsRemoved": result.dataset_old_versions_removed,
            "datasetBytesRemoved": result.dataset_bytes_removed,
        }))
    }

    /// Run health checks on the database.
    ///
    /// ```js
    /// const report = await db.doctor();
    /// ```
    #[napi]
    pub async fn doctor(&self) -> Result<serde_json::Value> {
        let guard = self.inner.lock().await;
        let db = guard
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let report = db.doctor().await.map_err(to_napi_err)?;
        Ok(serde_json::json!({
            "healthy": report.healthy,
            "issues": report.issues,
            "warnings": report.warnings,
            "manifestDbVersion": report.manifest_db_version,
            "datasetsChecked": report.datasets_checked,
            "txRows": report.tx_rows,
            "cdcRows": report.cdc_rows,
        }))
    }

    /// Close the database, releasing resources.
    ///
    /// ```js
    /// await db.close();
    /// ```
    #[napi]
    pub async fn close(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        *guard = None;
        Ok(())
    }
}
