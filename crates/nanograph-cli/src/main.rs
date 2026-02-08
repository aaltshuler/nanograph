use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use clap::{Parser, Subcommand};

use nanograph::catalog::build_catalog;
use nanograph::ir::lower::lower_query;
use nanograph::ir::ParamMap;
use nanograph::plan::planner::execute_query;
use nanograph::query::ast::Literal;
use nanograph::query::parser::parse_query;
use nanograph::query::typecheck::typecheck_query;
use nanograph::schema::parser::parse_schema;
use nanograph::store::database::Database;
use nanograph::store::graph::GraphStorage;
use nanograph::store::loader::load_jsonl_data;

#[derive(Parser)]
#[command(name = "nanograph", about = "NanoGraph — embedded typed property graph DB")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new database
    Init {
        /// Path to the database directory
        db_path: PathBuf,
        #[arg(long)]
        schema: PathBuf,
    },
    /// Load data into an existing database
    Load {
        /// Path to the database directory
        db_path: PathBuf,
        #[arg(long)]
        data: PathBuf,
    },
    /// Parse and typecheck query files
    Check {
        /// Database directory (new mode)
        #[arg(long)]
        db: Option<PathBuf>,
        /// Schema file (legacy mode)
        #[arg(long)]
        schema: Option<PathBuf>,
        #[arg(long)]
        query: PathBuf,
    },
    /// Run a named query against data
    Run {
        /// Database directory (new mode)
        #[arg(long)]
        db: Option<PathBuf>,
        /// Schema file (legacy mode)
        #[arg(long)]
        schema: Option<PathBuf>,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        name: String,
        /// Data file (legacy mode)
        #[arg(long)]
        data: Option<PathBuf>,
        #[arg(long, default_value = "table")]
        format: String,
        /// Query parameters (repeatable), e.g. --param name="Alice"
        #[arg(long = "param", value_parser = parse_param)]
        params: Vec<(String, String)>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Init { db_path, schema } => cmd_init(&db_path, &schema).await,
        Commands::Load { db_path, data } => cmd_load(&db_path, &data).await,
        Commands::Check { db, schema, query } => cmd_check(db, schema, &query).await,
        Commands::Run {
            db,
            schema,
            query,
            name,
            data,
            format,
            params,
        } => cmd_run(db, schema, &query, &name, data, &format, params).await,
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn cmd_init(db_path: &PathBuf, schema_path: &PathBuf) -> Result<(), String> {
    let schema_src = std::fs::read_to_string(schema_path)
        .map_err(|e| format!("failed to read schema: {}", e))?;

    Database::init(db_path, &schema_src)
        .await
        .map_err(|e| format!("{}", e))?;

    println!("Initialized database at {}", db_path.display());
    Ok(())
}

async fn cmd_load(db_path: &PathBuf, data_path: &PathBuf) -> Result<(), String> {
    let data_src = std::fs::read_to_string(data_path)
        .map_err(|e| format!("failed to read data: {}", e))?;

    let mut db = Database::open(db_path)
        .await
        .map_err(|e| format!("{}", e))?;

    db.load(&data_src).await.map_err(|e| format!("{}", e))?;

    println!("Loaded data into {}", db_path.display());
    Ok(())
}

async fn cmd_check(
    db: Option<PathBuf>,
    schema: Option<PathBuf>,
    query_path: &PathBuf,
) -> Result<(), String> {
    let query_src = std::fs::read_to_string(query_path)
        .map_err(|e| format!("failed to read query: {}", e))?;

    let catalog = if let Some(db_path) = db {
        let db = Database::open(&db_path)
            .await
            .map_err(|e| format!("{}", e))?;
        db.catalog().clone()
    } else if let Some(schema_path) = schema {
        let schema_src = std::fs::read_to_string(&schema_path)
            .map_err(|e| format!("failed to read schema: {}", e))?;
        let schema_file = parse_schema(&schema_src).map_err(|e| format!("{}", e))?;
        build_catalog(&schema_file).map_err(|e| format!("{}", e))?
    } else {
        return Err("either --db or --schema is required".to_string());
    };

    let queries = parse_query(&query_src).map_err(|e| format!("{}", e))?;

    let mut error_count = 0;
    for q in &queries.queries {
        match typecheck_query(&catalog, q) {
            Ok(_) => println!("OK: query `{}`", q.name),
            Err(e) => {
                println!("ERROR: query `{}`: {}", q.name, e);
                error_count += 1;
            }
        }
    }

    println!(
        "Check complete: {} queries processed",
        queries.queries.len()
    );
    if error_count > 0 {
        return Err(format!("{} query(s) failed typecheck", error_count));
    }
    Ok(())
}

async fn cmd_run(
    db: Option<PathBuf>,
    schema: Option<PathBuf>,
    query_path: &PathBuf,
    query_name: &str,
    data: Option<PathBuf>,
    format: &str,
    raw_params: Vec<(String, String)>,
) -> Result<(), String> {
    let query_src = std::fs::read_to_string(query_path)
        .map_err(|e| format!("failed to read query: {}", e))?;

    let (catalog, storage) = if let Some(db_path) = db {
        // DB mode
        let db = Database::open(&db_path)
            .await
            .map_err(|e| format!("{}", e))?;
        let catalog = db.catalog().clone();
        let storage = db.snapshot();
        (catalog, storage)
    } else if let Some(schema_path) = schema {
        // Legacy mode
        let schema_src = std::fs::read_to_string(&schema_path)
            .map_err(|e| format!("failed to read schema: {}", e))?;
        let data_path = data
            .ok_or_else(|| "--data is required in legacy mode (with --schema)".to_string())?;
        let data_src = std::fs::read_to_string(&data_path)
            .map_err(|e| format!("failed to read data: {}", e))?;

        let schema_file = parse_schema(&schema_src).map_err(|e| format!("{}", e))?;
        let catalog = build_catalog(&schema_file).map_err(|e| format!("{}", e))?;

        let mut gs = GraphStorage::new(catalog.clone());
        load_jsonl_data(&mut gs, &data_src).map_err(|e| format!("{}", e))?;
        gs.build_indices().map_err(|e| format!("{}", e))?;
        (catalog, Arc::new(gs))
    } else {
        return Err("either --db or --schema is required".to_string());
    };

    // Parse queries and find the named one
    let queries = parse_query(&query_src).map_err(|e| format!("{}", e))?;
    let query = queries
        .queries
        .iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("query `{}` not found", query_name))?;

    // Typecheck
    let type_ctx = typecheck_query(&catalog, query).map_err(|e| format!("{}", e))?;

    // Lower to IR
    let ir = lower_query(&catalog, query, &type_ctx).map_err(|e| format!("{}", e))?;

    // Build param map from CLI args, using query param type info for inference
    let param_map = build_param_map(&ir.params, &raw_params)?;

    // Execute
    let results = execute_query(&ir, storage, &param_map)
        .await
        .map_err(|e| format!("{}", e))?;

    // Output
    match format {
        "table" => {
            if results.is_empty() {
                println!("(empty result)");
            } else {
                let formatted = arrow::util::pretty::pretty_format_batches(&results)
                    .map_err(|e| format!("{}", e))?;
                println!("{}", formatted);
            }
        }
        "csv" => {
            for batch in &results {
                print_csv(batch);
            }
        }
        "jsonl" => {
            for batch in &results {
                print_jsonl(batch);
            }
        }
        _ => return Err(format!("unknown format: {}", format)),
    }

    Ok(())
}

fn print_csv(batch: &RecordBatch) {
    let schema = batch.schema();
    let header: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    println!("{}", header.join(","));

    for row in 0..batch.num_rows() {
        let mut values = Vec::new();
        for col in 0..batch.num_columns() {
            let col_arr = batch.column(col);
            values.push(
                arrow::util::display::array_value_to_string(col_arr, row).unwrap_or_default(),
            );
        }
        println!("{}", values.join(","));
    }
}

fn print_jsonl(batch: &RecordBatch) {
    let schema = batch.schema();
    for row in 0..batch.num_rows() {
        let mut map = serde_json::Map::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col_arr = batch.column(col_idx);
            let val =
                arrow::util::display::array_value_to_string(col_arr, row).unwrap_or_default();
            map.insert(field.name().clone(), serde_json::Value::String(val));
        }
        println!("{}", serde_json::Value::Object(map));
    }
}

/// Parse a `key=value` CLI parameter.
fn parse_param(s: &str) -> std::result::Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid param '{}': expected key=value", s))?;
    let key = s[..pos].to_string();
    let value = s[pos + 1..].to_string();
    // Strip surrounding quotes from value if present
    let value = if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        value[1..value.len() - 1].to_string()
    } else {
        value
    };
    Ok((key, value))
}

/// Build a ParamMap from raw CLI strings using query param type declarations.
fn build_param_map(
    query_params: &[nanograph::query::ast::Param],
    raw: &[(String, String)],
) -> std::result::Result<ParamMap, String> {
    let mut map = ParamMap::new();
    for (key, value) in raw {
        // Find the declared type for this param
        let decl = query_params.iter().find(|p| p.name == *key);
        let lit = if let Some(decl) = decl {
            match decl.type_name.as_str() {
                "String" => Literal::String(value.clone()),
                "I32" | "I64" => {
                    let n: i64 = value
                        .parse()
                        .map_err(|_| format!("param '{}': expected integer, got '{}'", key, value))?;
                    Literal::Integer(n)
                }
                "F32" | "F64" => {
                    let f: f64 = value
                        .parse()
                        .map_err(|_| format!("param '{}': expected float, got '{}'", key, value))?;
                    Literal::Float(f)
                }
                "Bool" => {
                    let b: bool = value
                        .parse()
                        .map_err(|_| format!("param '{}': expected bool, got '{}'", key, value))?;
                    Literal::Bool(b)
                }
                _ => Literal::String(value.clone()),
            }
        } else {
            // No type declaration found — default to string
            Literal::String(value.clone())
        };
        map.insert(key.clone(), lit);
    }
    Ok(map)
}
