use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use ariadne::{Color, Label, Report, ReportKind, Source};
use arrow::array::RecordBatch;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Result, WrapErr};
use tracing::{debug, info, instrument};
use tracing_subscriber::EnvFilter;

use nanograph::catalog::build_catalog;
use nanograph::error::ParseDiagnostic;
use nanograph::ir::lower::lower_query;
use nanograph::ir::ParamMap;
use nanograph::plan::planner::execute_query;
use nanograph::query::ast::Literal;
use nanograph::query::parser::parse_query_diagnostic;
use nanograph::query::typecheck::typecheck_query;
use nanograph::schema::parser::parse_schema_diagnostic;
use nanograph::store::database::Database;
use nanograph::store::graph::GraphStorage;
use nanograph::store::loader::load_jsonl_data;
use nanograph::store::migration::{
    execute_schema_migration, MigrationExecution, MigrationPlan, MigrationStatus, MigrationStep,
};

#[derive(Parser)]
#[command(
    name = "nanograph",
    about = "NanoGraph — embedded typed property graph DB"
)]
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
    /// Diff and apply schema migration from <db>/schema.pg
    Migrate {
        /// Path to the database directory
        db_path: PathBuf,
        /// Show migration plan without applying writes
        #[arg(long)]
        dry_run: bool,
        /// Output format: table or json
        #[arg(long, default_value = "table")]
        format: String,
        /// Apply confirm-level steps without interactive prompts
        #[arg(long)]
        auto_approve: bool,
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
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { db_path, schema } => cmd_init(&db_path, &schema).await,
        Commands::Load { db_path, data } => cmd_load(&db_path, &data).await,
        Commands::Migrate {
            db_path,
            dry_run,
            format,
            auto_approve,
        } => cmd_migrate(&db_path, dry_run, &format, auto_approve).await,
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
    }?;

    Ok(())
}

#[instrument(skip(format), fields(db_path = %db_path.display(), dry_run = dry_run, format = format))]
async fn cmd_migrate(
    db_path: &PathBuf,
    dry_run: bool,
    format: &str,
    auto_approve: bool,
) -> Result<()> {
    let schema_path = db_path.join("schema.pg");
    let schema_src = std::fs::read_to_string(&schema_path)
        .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
    let _ = parse_schema_or_report(&schema_path, &schema_src)?;

    let execution = execute_schema_migration(db_path, dry_run, auto_approve).await?;
    render_migration_execution(&execution, format)?;

    match execution.status {
        MigrationStatus::Applied => {
            info!("schema migration completed");
            Ok(())
        }
        MigrationStatus::NeedsConfirmation => {
            std::process::exit(2);
        }
        MigrationStatus::Blocked => {
            std::process::exit(3);
        }
    }
}

fn render_migration_execution(execution: &MigrationExecution, format: &str) -> Result<()> {
    match format {
        "json" => {
            let out = serde_json::to_string_pretty(&execution.plan)
                .wrap_err("failed to serialize migration plan JSON")?;
            println!("{}", out);
        }
        "table" => print_migration_plan_table(&execution.plan),
        other => return Err(eyre!("unknown format: {}", other)),
    }

    Ok(())
}

fn print_migration_plan_table(plan: &MigrationPlan) {
    println!("Migration Plan");
    println!("  DB: {}", plan.db_path);
    println!("  Old schema hash: {}", plan.old_schema_hash);
    println!("  New schema hash: {}", plan.new_schema_hash);
    println!();

    if !plan.steps.is_empty() {
        println!("{:<10} {:<24} {}", "SAFETY", "STEP", "DETAIL");
        for planned in &plan.steps {
            let safety = match planned.safety {
                nanograph::store::migration::MigrationSafety::Safe => "safe",
                nanograph::store::migration::MigrationSafety::Confirm => "confirm",
                nanograph::store::migration::MigrationSafety::Blocked => "blocked",
            };
            let step = migration_step_kind(&planned.step);
            println!("{:<10} {:<24} {}", safety, step, planned.reason);
        }
    } else {
        println!("No migration steps.");
    }

    if !plan.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for w in &plan.warnings {
            println!("- {}", w);
        }
    }
    if !plan.blocked.is_empty() {
        println!();
        println!("Blocked:");
        for b in &plan.blocked {
            println!("- {}", b);
        }
    }
}

fn migration_step_kind(step: &MigrationStep) -> &'static str {
    match step {
        MigrationStep::AddNodeType { .. } => "AddNodeType",
        MigrationStep::AddEdgeType { .. } => "AddEdgeType",
        MigrationStep::DropNodeType { .. } => "DropNodeType",
        MigrationStep::DropEdgeType { .. } => "DropEdgeType",
        MigrationStep::RenameType { .. } => "RenameType",
        MigrationStep::AddProperty { .. } => "AddProperty",
        MigrationStep::DropProperty { .. } => "DropProperty",
        MigrationStep::RenameProperty { .. } => "RenameProperty",
        MigrationStep::AlterPropertyType { .. } => "AlterPropertyType",
        MigrationStep::AlterPropertyNullability { .. } => "AlterPropertyNullability",
        MigrationStep::RebindEdgeEndpoints { .. } => "RebindEdgeEndpoints",
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn normalize_span(span: Option<nanograph::error::SourceSpan>, source: &str) -> Range<usize> {
    if source.is_empty() {
        return 0..0;
    }
    let len = source.len();
    match span {
        Some(s) => {
            let start = s.start.min(len.saturating_sub(1));
            let end = s.end.max(start.saturating_add(1)).min(len);
            start..end
        }
        None => 0..1.min(len),
    }
}

fn render_parse_diagnostic(path: &Path, source: &str, diag: &ParseDiagnostic) {
    let file_id = path.display().to_string();
    let span = normalize_span(diag.span, source);
    let mut report = Report::build(ReportKind::Error, file_id.clone(), span.start)
        .with_message("parse error")
        .with_label(
            Label::new((file_id.clone(), span.clone()))
                .with_color(Color::Red)
                .with_message(diag.message.clone()),
        );
    if diag.span.is_none() {
        report = report.with_note(diag.message.clone());
    }
    let _ = report
        .finish()
        .eprint((file_id.clone(), Source::from(source)));
}

fn parse_schema_or_report(path: &Path, source: &str) -> Result<nanograph::schema::ast::SchemaFile> {
    parse_schema_diagnostic(source).map_err(|diag| {
        render_parse_diagnostic(path, source, &diag);
        eyre!("schema parse failed")
    })
}

fn parse_query_or_report(path: &Path, source: &str) -> Result<nanograph::query::ast::QueryFile> {
    parse_query_diagnostic(source).map_err(|diag| {
        render_parse_diagnostic(path, source, &diag);
        eyre!("query parse failed")
    })
}

#[instrument(skip(schema_path), fields(db_path = %db_path.display()))]
async fn cmd_init(db_path: &PathBuf, schema_path: &PathBuf) -> Result<()> {
    let schema_src = std::fs::read_to_string(schema_path)
        .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
    let _ = parse_schema_or_report(schema_path, &schema_src)?;

    Database::init(db_path, &schema_src).await?;

    info!("database initialized");
    println!("Initialized database at {}", db_path.display());
    Ok(())
}

#[instrument(skip(data_path), fields(db_path = %db_path.display()))]
async fn cmd_load(db_path: &PathBuf, data_path: &PathBuf) -> Result<()> {
    let data_src = std::fs::read_to_string(data_path)
        .wrap_err_with(|| format!("failed to read data: {}", data_path.display()))?;

    let mut db = Database::open(db_path).await?;

    db.load(&data_src).await?;

    info!("data load complete");
    println!("Loaded data into {}", db_path.display());
    Ok(())
}

#[instrument(skip(db, schema, query_path), fields(query_path = %query_path.display()))]
async fn cmd_check(
    db: Option<PathBuf>,
    schema: Option<PathBuf>,
    query_path: &PathBuf,
) -> Result<()> {
    let query_src = std::fs::read_to_string(query_path)
        .wrap_err_with(|| format!("failed to read query: {}", query_path.display()))?;

    let catalog = if let Some(db_path) = db {
        let db = Database::open(&db_path).await?;
        db.catalog().clone()
    } else if let Some(schema_path) = schema {
        let schema_src = std::fs::read_to_string(&schema_path)
            .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
        let schema_file = parse_schema_or_report(&schema_path, &schema_src)?;
        build_catalog(&schema_file)?
    } else {
        return Err(eyre!("either --db or --schema is required"));
    };

    let queries = parse_query_or_report(query_path, &query_src)?;

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
        return Err(eyre!("{} query(s) failed typecheck", error_count));
    }
    Ok(())
}

#[instrument(
    skip(db, schema, query_path, data, format, raw_params),
    fields(query_name = query_name, query_path = %query_path.display(), format = format)
)]
async fn cmd_run(
    db: Option<PathBuf>,
    schema: Option<PathBuf>,
    query_path: &PathBuf,
    query_name: &str,
    data: Option<PathBuf>,
    format: &str,
    raw_params: Vec<(String, String)>,
) -> Result<()> {
    let query_src = std::fs::read_to_string(query_path)
        .wrap_err_with(|| format!("failed to read query: {}", query_path.display()))?;

    let (catalog, storage) = if let Some(db_path) = db {
        // DB mode
        let db = Database::open(&db_path).await?;
        let catalog = db.catalog().clone();
        let storage = db.snapshot();
        (catalog, storage)
    } else if let Some(schema_path) = schema {
        // Legacy mode
        let schema_src = std::fs::read_to_string(&schema_path)
            .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
        let data_path =
            data.ok_or_else(|| eyre!("--data is required in legacy mode (with --schema)"))?;
        let data_src = std::fs::read_to_string(&data_path)
            .wrap_err_with(|| format!("failed to read data: {}", data_path.display()))?;

        let schema_file = parse_schema_or_report(&schema_path, &schema_src)?;
        let catalog = build_catalog(&schema_file)?;

        let mut gs = GraphStorage::new(catalog.clone());
        load_jsonl_data(&mut gs, &data_src)?;
        gs.build_indices()?;
        (catalog, Arc::new(gs))
    } else {
        return Err(eyre!("either --db or --schema is required"));
    };

    // Parse queries and find the named one
    let queries = parse_query_or_report(query_path, &query_src)?;
    let query = queries
        .queries
        .iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| eyre!("query `{}` not found", query_name))?;
    info!("executing query");

    // Typecheck
    let type_ctx = typecheck_query(&catalog, query)?;

    // Lower to IR
    let ir = lower_query(&catalog, query, &type_ctx)?;
    debug!(pipeline_len = ir.pipeline.len(), "query lowered");

    // Build param map from CLI args, using query param type info for inference
    let param_map = build_param_map(&ir.params, &raw_params)?;

    // Execute
    let results = execute_query(&ir, storage, &param_map).await?;

    // Output
    match format {
        "table" => {
            if results.is_empty() {
                println!("(empty result)");
            } else {
                let formatted = arrow::util::pretty::pretty_format_batches(&results)
                    .wrap_err("failed to render table output")?;
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
        _ => return Err(eyre!("unknown format: {}", format)),
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
            let val = arrow::util::display::array_value_to_string(col_arr, row).unwrap_or_default();
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
) -> Result<ParamMap> {
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
                        .map_err(|_| eyre!("param '{}': expected integer, got '{}'", key, value))?;
                    Literal::Integer(n)
                }
                "F32" | "F64" => {
                    let f: f64 = value
                        .parse()
                        .map_err(|_| eyre!("param '{}': expected float, got '{}'", key, value))?;
                    Literal::Float(f)
                }
                "Bool" => {
                    let b: bool = value
                        .parse()
                        .map_err(|_| eyre!("param '{}': expected bool, got '{}'", key, value))?;
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
