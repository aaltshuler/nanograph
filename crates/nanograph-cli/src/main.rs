use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use ariadne::{Color, Label, Report, ReportKind, Source};
use arrow::array::RecordBatch;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{Result, WrapErr, eyre};
use tracing::{debug, info, instrument};
use tracing_subscriber::EnvFilter;

use nanograph::error::{NanoError, ParseDiagnostic};
use nanograph::query::ast::Literal;
use nanograph::query::parser::parse_query_diagnostic;
use nanograph::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use nanograph::schema::parser::parse_schema_diagnostic;
use nanograph::store::database::{
    CdcAnalyticsMaterializeOptions, CleanupOptions, CompactOptions, Database, DeleteOp,
    DeletePredicate, LoadMode,
};
use nanograph::store::migration::{
    MigrationExecution, MigrationPlan, MigrationStatus, MigrationStep, execute_schema_migration,
};
use nanograph::store::txlog::{CdcLogEntry, read_visible_cdc_entries};
use nanograph::{
    MutationExecResult, ParamMap, execute_mutation, execute_query, lower_mutation_query,
    lower_query,
};

#[derive(Parser)]
#[command(
    name = "nanograph",
    about = "NanoGraph — embedded typed property graph DB",
    version
)]
struct Cli {
    /// Emit machine-readable JSON output.
    #[arg(long, global = true)]
    json: bool,
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
        /// Load mode: overwrite, append, or merge
        #[arg(long, value_enum)]
        mode: LoadModeArg,
    },
    /// Delete nodes by predicate, cascading incident edges
    Delete {
        /// Path to the database directory
        db_path: PathBuf,
        /// Node type name
        #[arg(long = "type")]
        type_name: String,
        /// Predicate expression, e.g. name=Alice or age>=30
        #[arg(long = "where")]
        predicate: String,
    },
    /// Stream CDC events from committed transactions
    Changes {
        /// Path to the database directory
        db_path: PathBuf,
        /// Return changes with db_version strictly greater than this value
        #[arg(long, conflicts_with_all = ["from_version", "to_version"])]
        since: Option<u64>,
        /// Inclusive lower bound for db_version (requires --to)
        #[arg(long = "from", requires = "to_version", conflicts_with = "since")]
        from_version: Option<u64>,
        /// Inclusive upper bound for db_version (requires --from)
        #[arg(long = "to", requires = "from_version", conflicts_with = "since")]
        to_version: Option<u64>,
        /// Output format: jsonl or json
        #[arg(long, default_value = "jsonl")]
        format: String,
    },
    /// Compact Lance datasets and commit updated pinned dataset versions
    Compact {
        /// Path to the database directory
        db_path: PathBuf,
        /// Target row count per compacted fragment
        #[arg(long, default_value_t = 1_048_576)]
        target_rows_per_fragment: usize,
        /// Whether to materialize deleted rows during compaction
        #[arg(long, default_value_t = true)]
        materialize_deletions: bool,
        /// Deletion fraction threshold for materialization
        #[arg(long, default_value_t = 0.1)]
        materialize_deletions_threshold: f32,
    },
    /// Prune old tx/CDC history and old Lance versions while keeping replay window
    Cleanup {
        /// Path to the database directory
        db_path: PathBuf,
        /// Keep this many latest tx versions for CDC replay
        #[arg(long, default_value_t = 128)]
        retain_tx_versions: u64,
        /// Keep at least this many latest versions per Lance dataset
        #[arg(long, default_value_t = 2)]
        retain_dataset_versions: usize,
    },
    /// Run consistency checks on manifest, datasets, logs, and graph integrity
    Doctor {
        /// Path to the database directory
        db_path: PathBuf,
    },
    /// Materialize visible CDC into a derived Lance analytics dataset
    CdcMaterialize {
        /// Path to the database directory
        db_path: PathBuf,
        /// Minimum number of new visible CDC rows required to run materialization
        #[arg(long, default_value_t = 0)]
        min_new_rows: usize,
        /// Force materialization regardless of threshold
        #[arg(long, default_value_t = false)]
        force: bool,
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
        /// Database directory
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        query: PathBuf,
    },
    /// Run a named query against data
    Run {
        /// Database directory
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "table")]
        format: String,
        /// Query parameters (repeatable), e.g. --param name="Alice"
        #[arg(long = "param", value_parser = parse_param)]
        params: Vec<(String, String)>,
    },
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum LoadModeArg {
    Overwrite,
    Append,
    Merge,
}

impl From<LoadModeArg> for LoadMode {
    fn from(value: LoadModeArg) -> Self {
        match value {
            LoadModeArg::Overwrite => LoadMode::Overwrite,
            LoadModeArg::Append => LoadMode::Append,
            LoadModeArg::Merge => LoadMode::Merge,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { db_path, schema } => cmd_init(&db_path, &schema, cli.json).await,
        Commands::Load {
            db_path,
            data,
            mode,
        } => cmd_load(&db_path, &data, mode, cli.json).await,
        Commands::Delete {
            db_path,
            type_name,
            predicate,
        } => cmd_delete(&db_path, &type_name, &predicate, cli.json).await,
        Commands::Changes {
            db_path,
            since,
            from_version,
            to_version,
            format,
        } => cmd_changes(&db_path, since, from_version, to_version, &format, cli.json).await,
        Commands::Compact {
            db_path,
            target_rows_per_fragment,
            materialize_deletions,
            materialize_deletions_threshold,
        } => {
            cmd_compact(
                &db_path,
                target_rows_per_fragment,
                materialize_deletions,
                materialize_deletions_threshold,
                cli.json,
            )
            .await
        }
        Commands::Cleanup {
            db_path,
            retain_tx_versions,
            retain_dataset_versions,
        } => {
            cmd_cleanup(
                &db_path,
                retain_tx_versions,
                retain_dataset_versions,
                cli.json,
            )
            .await
        }
        Commands::Doctor { db_path } => cmd_doctor(&db_path, cli.json).await,
        Commands::CdcMaterialize {
            db_path,
            min_new_rows,
            force,
        } => cmd_cdc_materialize(&db_path, min_new_rows, force, cli.json).await,
        Commands::Migrate {
            db_path,
            dry_run,
            format,
            auto_approve,
        } => cmd_migrate(&db_path, dry_run, &format, auto_approve, cli.json).await,
        Commands::Check { db, query } => cmd_check(db, &query, cli.json).await,
        Commands::Run {
            db,
            query,
            name,
            format,
            params,
        } => cmd_run(db, &query, &name, &format, params, cli.json).await,
    }?;

    Ok(())
}

#[instrument(skip(format), fields(db_path = %db_path.display(), dry_run = dry_run, format = format))]
async fn cmd_migrate(
    db_path: &PathBuf,
    dry_run: bool,
    format: &str,
    auto_approve: bool,
    json: bool,
) -> Result<()> {
    let schema_path = db_path.join("schema.pg");
    let schema_src = std::fs::read_to_string(&schema_path)
        .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
    let _ = parse_schema_or_report(&schema_path, &schema_src)?;

    let execution = execute_schema_migration(db_path, dry_run, auto_approve).await?;
    let effective_format = if json { "json" } else { format };
    render_migration_execution(&execution, effective_format)?;

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
            let payload = serde_json::json!({
                "status": migration_status_label(execution.status),
                "plan": execution.plan,
            });
            let out = serde_json::to_string_pretty(&payload)
                .wrap_err("failed to serialize migration JSON")?;
            println!("{}", out);
        }
        "table" => print_migration_plan_table(&execution.plan),
        other => return Err(eyre!("unknown format: {}", other)),
    }

    Ok(())
}

fn migration_status_label(status: MigrationStatus) -> &'static str {
    match status {
        MigrationStatus::Applied => "applied",
        MigrationStatus::NeedsConfirmation => "needs_confirmation",
        MigrationStatus::Blocked => "blocked",
    }
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
async fn cmd_init(db_path: &PathBuf, schema_path: &PathBuf, json: bool) -> Result<()> {
    let schema_src = std::fs::read_to_string(schema_path)
        .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
    let _ = parse_schema_or_report(schema_path, &schema_src)?;

    Database::init(db_path, &schema_src).await?;

    info!("database initialized");
    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "db_path": db_path.display().to_string(),
                "schema_path": schema_path.display().to_string(),
            })
        );
    } else {
        println!("Initialized database at {}", db_path.display());
    }
    Ok(())
}

#[instrument(skip(data_path), fields(db_path = %db_path.display(), mode = ?mode))]
async fn cmd_load(
    db_path: &PathBuf,
    data_path: &PathBuf,
    mode: LoadModeArg,
    json: bool,
) -> Result<()> {
    let data_src = std::fs::read_to_string(data_path)
        .wrap_err_with(|| format!("failed to read data: {}", data_path.display()))?;

    let mut db = Database::open(db_path).await?;

    if let Err(err) = db.load_with_mode(&data_src, mode.into()).await {
        render_load_error(db_path, &err, json);
        return Err(err.into());
    }

    info!("data load complete");
    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "db_path": db_path.display().to_string(),
                "data_path": data_path.display().to_string(),
                "mode": format!("{:?}", mode).to_lowercase(),
            })
        );
    } else {
        println!("Loaded data into {}", db_path.display());
    }
    Ok(())
}

fn render_load_error(db_path: &Path, err: &NanoError, json: bool) {
    if let NanoError::UniqueConstraint {
        type_name,
        property,
        value,
        first_row,
        second_row,
    } = err
    {
        if json {
            eprintln!(
                "{}",
                serde_json::json!({
                    "status": "error",
                    "error_kind": "unique_constraint",
                    "db_path": db_path.display().to_string(),
                    "type_name": type_name,
                    "property": property,
                    "value": value,
                    "first_row": first_row,
                    "second_row": second_row,
                })
            );
        } else {
            eprintln!("Load failed for {}.", db_path.display());
            eprintln!(
                "Unique constraint violation: {}.{} has duplicate value '{}'.",
                type_name, property, value
            );
            eprintln!(
                "Conflicting rows in loaded dataset: {} and {}.",
                first_row, second_row
            );
        }
    }
}

#[instrument(skip(type_name, predicate), fields(db_path = %db_path.display(), type_name = type_name))]
async fn cmd_delete(db_path: &PathBuf, type_name: &str, predicate: &str, json: bool) -> Result<()> {
    let pred = parse_delete_predicate(predicate)?;
    let mut db = Database::open(db_path).await?;
    let result = db.delete_nodes(type_name, &pred).await?;

    info!(
        deleted_nodes = result.deleted_nodes,
        deleted_edges = result.deleted_edges,
        "delete complete"
    );
    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "db_path": db_path.display().to_string(),
                "type_name": type_name,
                "deleted_nodes": result.deleted_nodes,
                "deleted_edges": result.deleted_edges,
            })
        );
    } else {
        println!(
            "Deleted {} node(s) and {} edge(s) in {}",
            result.deleted_nodes,
            result.deleted_edges,
            db_path.display()
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChangesWindow {
    from_db_version_exclusive: u64,
    to_db_version_inclusive: Option<u64>,
}

fn resolve_changes_window(
    since: Option<u64>,
    from_version: Option<u64>,
    to_version: Option<u64>,
) -> Result<ChangesWindow> {
    if since.is_some() && (from_version.is_some() || to_version.is_some()) {
        return Err(eyre!("use either --since or --from/--to, not both"));
    }

    if let Some(since) = since {
        return Ok(ChangesWindow {
            from_db_version_exclusive: since,
            to_db_version_inclusive: None,
        });
    }

    match (from_version, to_version) {
        (Some(from), Some(to)) => {
            if from > to {
                return Err(eyre!("--from must be <= --to"));
            }
            Ok(ChangesWindow {
                from_db_version_exclusive: from.saturating_sub(1),
                to_db_version_inclusive: Some(to),
            })
        }
        (None, None) => Ok(ChangesWindow {
            from_db_version_exclusive: 0,
            to_db_version_inclusive: None,
        }),
        _ => Err(eyre!("--from and --to must be provided together")),
    }
}

#[instrument(
    skip(format),
    fields(
        db_path = %db_path.display(),
        since = since,
        from = from_version,
        to = to_version,
        format = format
    )
)]
async fn cmd_changes(
    db_path: &PathBuf,
    since: Option<u64>,
    from_version: Option<u64>,
    to_version: Option<u64>,
    format: &str,
    json: bool,
) -> Result<()> {
    let window = resolve_changes_window(since, from_version, to_version)?;
    let rows = read_visible_cdc_entries(
        db_path,
        window.from_db_version_exclusive,
        window.to_db_version_inclusive,
    )?;

    let effective_format = if json { "json" } else { format };
    render_changes(effective_format, &rows)
}

fn render_changes(format: &str, rows: &[CdcLogEntry]) -> Result<()> {
    match format {
        "jsonl" => {
            for row in rows {
                let line = serde_json::to_string(row).wrap_err("failed to serialize CDC row")?;
                println!("{}", line);
            }
        }
        "json" => {
            let out =
                serde_json::to_string_pretty(rows).wrap_err("failed to serialize CDC rows")?;
            println!("{}", out);
        }
        other => {
            return Err(eyre!("unknown format: {} (supported: jsonl, json)", other));
        }
    }
    Ok(())
}

#[instrument(
    fields(
        db_path = %db_path.display(),
        target_rows_per_fragment = target_rows_per_fragment,
        materialize_deletions = materialize_deletions,
        materialize_deletions_threshold = materialize_deletions_threshold
    )
)]
async fn cmd_compact(
    db_path: &PathBuf,
    target_rows_per_fragment: usize,
    materialize_deletions: bool,
    materialize_deletions_threshold: f32,
    json: bool,
) -> Result<()> {
    let mut db = Database::open(db_path).await?;
    let result = db
        .compact(CompactOptions {
            target_rows_per_fragment,
            materialize_deletions,
            materialize_deletions_threshold,
        })
        .await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "db_path": db_path.display().to_string(),
                "datasets_considered": result.datasets_considered,
                "datasets_compacted": result.datasets_compacted,
                "fragments_removed": result.fragments_removed,
                "fragments_added": result.fragments_added,
                "files_removed": result.files_removed,
                "files_added": result.files_added,
                "manifest_committed": result.manifest_committed,
            })
        );
    } else {
        println!(
            "Compaction complete for {} (datasets compacted: {}, fragments -{} +{}, files -{} +{}, manifest committed: {})",
            db_path.display(),
            result.datasets_compacted,
            result.fragments_removed,
            result.fragments_added,
            result.files_removed,
            result.files_added,
            result.manifest_committed
        );
    }
    Ok(())
}

#[instrument(
    fields(
        db_path = %db_path.display(),
        retain_tx_versions = retain_tx_versions,
        retain_dataset_versions = retain_dataset_versions
    )
)]
async fn cmd_cleanup(
    db_path: &PathBuf,
    retain_tx_versions: u64,
    retain_dataset_versions: usize,
    json: bool,
) -> Result<()> {
    let mut db = Database::open(db_path).await?;
    let result = db
        .cleanup(CleanupOptions {
            retain_tx_versions,
            retain_dataset_versions,
        })
        .await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "db_path": db_path.display().to_string(),
                "tx_rows_removed": result.tx_rows_removed,
                "tx_rows_kept": result.tx_rows_kept,
                "cdc_rows_removed": result.cdc_rows_removed,
                "cdc_rows_kept": result.cdc_rows_kept,
                "datasets_cleaned": result.datasets_cleaned,
                "dataset_old_versions_removed": result.dataset_old_versions_removed,
                "dataset_bytes_removed": result.dataset_bytes_removed,
            })
        );
    } else {
        println!(
            "Cleanup complete for {} (tx removed {}, cdc removed {}, datasets cleaned {}, old versions removed {}, bytes removed {})",
            db_path.display(),
            result.tx_rows_removed,
            result.cdc_rows_removed,
            result.datasets_cleaned,
            result.dataset_old_versions_removed,
            result.dataset_bytes_removed
        );
    }

    Ok(())
}

#[instrument(
    fields(
        db_path = %db_path.display(),
        min_new_rows = min_new_rows,
        force = force
    )
)]
async fn cmd_cdc_materialize(
    db_path: &PathBuf,
    min_new_rows: usize,
    force: bool,
    json: bool,
) -> Result<()> {
    let db = Database::open(db_path).await?;
    let result = db
        .materialize_cdc_analytics(CdcAnalyticsMaterializeOptions {
            min_new_rows,
            force,
        })
        .await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "db_path": db_path.display().to_string(),
                "source_rows": result.source_rows,
                "previously_materialized_rows": result.previously_materialized_rows,
                "new_rows_since_last_run": result.new_rows_since_last_run,
                "materialized_rows": result.materialized_rows,
                "dataset_written": result.dataset_written,
                "skipped_by_threshold": result.skipped_by_threshold,
                "dataset_version": result.dataset_version,
            })
        );
    } else if result.skipped_by_threshold {
        println!(
            "CDC analytics materialization skipped for {} (new rows {}, threshold {})",
            db_path.display(),
            result.new_rows_since_last_run,
            min_new_rows
        );
    } else {
        println!(
            "CDC analytics materialized for {} (rows {}, dataset written {}, version {:?})",
            db_path.display(),
            result.materialized_rows,
            result.dataset_written,
            result.dataset_version
        );
    }

    Ok(())
}

#[instrument(fields(db_path = %db_path.display()))]
async fn cmd_doctor(db_path: &PathBuf, json: bool) -> Result<()> {
    let db = Database::open(db_path).await?;
    let report = db.doctor().await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": if report.healthy { "ok" } else { "error" },
                "db_path": db_path.display().to_string(),
                "healthy": report.healthy,
                "manifest_db_version": report.manifest_db_version,
                "datasets_checked": report.datasets_checked,
                "tx_rows": report.tx_rows,
                "cdc_rows": report.cdc_rows,
                "issues": report.issues,
                "warnings": report.warnings,
            })
        );
    } else {
        if report.healthy {
            println!(
                "Doctor OK for {} (db_version {}, datasets checked {}, tx rows {}, cdc rows {})",
                db_path.display(),
                report.manifest_db_version,
                report.datasets_checked,
                report.tx_rows,
                report.cdc_rows
            );
        } else {
            println!(
                "Doctor found issues for {} (db_version {}, datasets checked {}, tx rows {}, cdc rows {})",
                db_path.display(),
                report.manifest_db_version,
                report.datasets_checked,
                report.tx_rows,
                report.cdc_rows
            );
            for issue in &report.issues {
                println!("ISSUE: {}", issue);
            }
        }
        for warning in &report.warnings {
            println!("WARN: {}", warning);
        }
    }

    if report.healthy {
        Ok(())
    } else {
        Err(eyre!("doctor detected {} issue(s)", report.issues.len()))
    }
}

#[instrument(skip(query_path), fields(db_path = %db_path.display(), query_path = %query_path.display()))]
async fn cmd_check(db_path: PathBuf, query_path: &PathBuf, json: bool) -> Result<()> {
    let query_src = std::fs::read_to_string(query_path)
        .wrap_err_with(|| format!("failed to read query: {}", query_path.display()))?;
    let db = Database::open(&db_path).await?;
    let catalog = db.catalog().clone();

    let queries = parse_query_or_report(query_path, &query_src)?;

    let mut error_count = 0;
    let mut checks = Vec::with_capacity(queries.queries.len());
    for q in &queries.queries {
        match typecheck_query_decl(&catalog, q) {
            Ok(CheckedQuery::Read(_)) => {
                if !json {
                    println!("OK: query `{}` (read)", q.name);
                }
                checks.push(serde_json::json!({
                    "name": q.name,
                    "kind": "read",
                    "status": "ok",
                }));
            }
            Ok(CheckedQuery::Mutation(_)) => {
                if !json {
                    println!("OK: query `{}` (mutation)", q.name);
                }
                checks.push(serde_json::json!({
                    "name": q.name,
                    "kind": "mutation",
                    "status": "ok",
                }));
            }
            Err(e) => {
                if !json {
                    println!("ERROR: query `{}`: {}", q.name, e);
                }
                checks.push(serde_json::json!({
                    "name": q.name,
                    "status": "error",
                    "error": e.to_string(),
                }));
                error_count += 1;
            }
        }
    }

    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": if error_count == 0 { "ok" } else { "error" },
                "query_path": query_path.display().to_string(),
                "queries_processed": queries.queries.len(),
                "errors": error_count,
                "results": checks,
            })
        );
    } else {
        println!(
            "Check complete: {} queries processed",
            queries.queries.len()
        );
    }
    if error_count > 0 {
        return Err(eyre!("{} query(s) failed typecheck", error_count));
    }
    Ok(())
}

#[instrument(
    skip(query_path, format, raw_params),
    fields(db_path = %db_path.display(), query_name = query_name, query_path = %query_path.display(), format = format)
)]
async fn cmd_run(
    db_path: PathBuf,
    query_path: &PathBuf,
    query_name: &str,
    format: &str,
    raw_params: Vec<(String, String)>,
    json: bool,
) -> Result<()> {
    let query_src = std::fs::read_to_string(query_path)
        .wrap_err_with(|| format!("failed to read query: {}", query_path.display()))?;

    // Parse queries and find the named one
    let queries = parse_query_or_report(query_path, &query_src)?;
    let query = queries
        .queries
        .iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| eyre!("query `{}` not found", query_name))?;
    info!("executing query");

    // Build param map from CLI args, using query param type info for inference
    let param_map = build_param_map(&query.params, &raw_params)?;

    let effective_format = if json { "json" } else { format };

    if query.mutation.is_some() {
        let mut db = Database::open(&db_path).await?;
        let checked = typecheck_query_decl(db.catalog(), query)?;
        if !matches!(checked, CheckedQuery::Mutation(_)) {
            return Err(eyre!("expected mutation query"));
        }

        let mutation_ir = lower_mutation_query(query)?;
        let result = execute_mutation(&mutation_ir, &mut db, &param_map).await?;
        let result_batch = mutation_result_batch(result)?;
        return render_results(effective_format, &[result_batch]);
    }

    let db = Database::open(&db_path).await?;
    let catalog = db.catalog().clone();
    let storage = db.snapshot();

    let type_ctx = typecheck_query(&catalog, query)?;
    let ir = lower_query(&catalog, query, &type_ctx)?;
    debug!(pipeline_len = ir.pipeline.len(), "query lowered");
    let results = execute_query(&ir, storage, &param_map).await?;
    render_results(effective_format, &results)
}

fn render_results(format: &str, results: &[RecordBatch]) -> Result<()> {
    match format {
        "table" => {
            if results.is_empty() {
                println!("(empty result)");
            } else {
                let formatted = arrow::util::pretty::pretty_format_batches(results)
                    .wrap_err("failed to render table output")?;
                println!("{}", formatted);
            }
        }
        "csv" => {
            for batch in results {
                print_csv(batch);
            }
        }
        "jsonl" => {
            for batch in results {
                print_jsonl(batch);
            }
        }
        "json" => {
            print_json(results)?;
        }
        _ => return Err(eyre!("unknown format: {}", format)),
    }
    Ok(())
}

fn mutation_result_batch(result: MutationExecResult) -> Result<RecordBatch> {
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("affected_nodes", DataType::UInt64, false),
        Field::new("affected_edges", DataType::UInt64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![result.affected_nodes as u64])),
            Arc::new(UInt64Array::from(vec![result.affected_edges as u64])),
        ],
    )
    .map_err(|e| eyre!("failed to build mutation result batch: {}", e))
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

fn print_json(results: &[RecordBatch]) -> Result<()> {
    let rows = record_batches_to_json_rows(results);
    let out = serde_json::to_string_pretty(&rows).wrap_err("failed to serialize JSON output")?;
    println!("{}", out);
    Ok(())
}

fn record_batches_to_json_rows(results: &[RecordBatch]) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for batch in results {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col_arr = batch.column(col_idx);
                map.insert(field.name().clone(), cli_array_value_to_json(col_arr, row));
            }
            out.push(serde_json::Value::Object(map));
        }
    }
    out
}

fn cli_array_value_to_json(array: &arrow::array::ArrayRef, row: usize) -> serde_json::Value {
    use arrow::array::{
        Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;

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
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as u64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
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
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        _ => {
            let display =
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            serde_json::Value::String(display)
        }
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

fn parse_delete_predicate(input: &str) -> Result<DeletePredicate> {
    let ops = [
        (">=", DeleteOp::Ge),
        ("<=", DeleteOp::Le),
        ("!=", DeleteOp::Ne),
        ("=", DeleteOp::Eq),
        (">", DeleteOp::Gt),
        ("<", DeleteOp::Lt),
    ];

    for (token, op) in ops {
        if let Some(pos) = input.find(token) {
            let property = input[..pos].trim();
            let raw_value = input[pos + token.len()..].trim();
            if property.is_empty() || raw_value.is_empty() {
                return Err(eyre!(
                    "invalid --where predicate '{}': expected <property><op><value>",
                    input
                ));
            }

            let value = if (raw_value.starts_with('"') && raw_value.ends_with('"'))
                || (raw_value.starts_with('\'') && raw_value.ends_with('\''))
            {
                raw_value[1..raw_value.len() - 1].to_string()
            } else {
                raw_value.to_string()
            };

            return Ok(DeletePredicate {
                property: property.to_string(),
                op,
                value,
            });
        }
    }

    Err(eyre!(
        "invalid --where predicate '{}': supported operators are =, !=, >, >=, <, <=",
        input
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use nanograph::store::txlog::read_visible_cdc_entries;
    use tempfile::TempDir;

    fn write_file(path: &Path, content: &str) {
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn parse_load_mode_from_cli() {
        let cli = Cli::parse_from([
            "nanograph",
            "load",
            "/tmp/db",
            "--data",
            "/tmp/data.jsonl",
            "--mode",
            "append",
        ]);
        match cli.command {
            Commands::Load { mode, .. } => assert_eq!(mode, LoadModeArg::Append),
            _ => panic!("expected load command"),
        }
    }

    #[test]
    fn parse_changes_range_from_cli() {
        let cli = Cli::parse_from([
            "nanograph",
            "changes",
            "/tmp/db",
            "--from",
            "2",
            "--to",
            "4",
            "--format",
            "json",
        ]);
        match cli.command {
            Commands::Changes {
                from_version,
                to_version,
                format,
                ..
            } => {
                assert_eq!(from_version, Some(2));
                assert_eq!(to_version, Some(4));
                assert_eq!(format, "json");
            }
            _ => panic!("expected changes command"),
        }
    }

    #[test]
    fn parse_maintenance_commands_from_cli() {
        let compact = Cli::parse_from([
            "nanograph",
            "compact",
            "/tmp/db",
            "--target-rows-per-fragment",
            "1000",
        ]);
        match compact.command {
            Commands::Compact {
                target_rows_per_fragment,
                ..
            } => assert_eq!(target_rows_per_fragment, 1000),
            _ => panic!("expected compact command"),
        }

        let cleanup = Cli::parse_from([
            "nanograph",
            "cleanup",
            "/tmp/db",
            "--retain-tx-versions",
            "4",
            "--retain-dataset-versions",
            "3",
        ]);
        match cleanup.command {
            Commands::Cleanup {
                retain_tx_versions,
                retain_dataset_versions,
                ..
            } => {
                assert_eq!(retain_tx_versions, 4);
                assert_eq!(retain_dataset_versions, 3);
            }
            _ => panic!("expected cleanup command"),
        }

        let doctor = Cli::parse_from(["nanograph", "doctor", "/tmp/db"]);
        match doctor.command {
            Commands::Doctor { .. } => {}
            _ => panic!("expected doctor command"),
        }

        let materialize = Cli::parse_from([
            "nanograph",
            "cdc-materialize",
            "/tmp/db",
            "--min-new-rows",
            "50",
            "--force",
        ]);
        match materialize.command {
            Commands::CdcMaterialize {
                min_new_rows,
                force,
                ..
            } => {
                assert_eq!(min_new_rows, 50);
                assert!(force);
            }
            _ => panic!("expected cdc-materialize command"),
        }
    }

    #[test]
    fn resolve_changes_window_supports_since_and_range_modes() {
        let since = resolve_changes_window(Some(5), None, None).unwrap();
        assert_eq!(
            since,
            ChangesWindow {
                from_db_version_exclusive: 5,
                to_db_version_inclusive: None
            }
        );

        let range = resolve_changes_window(None, Some(2), Some(4)).unwrap();
        assert_eq!(
            range,
            ChangesWindow {
                from_db_version_exclusive: 1,
                to_db_version_inclusive: Some(4)
            }
        );
    }

    #[test]
    fn resolve_changes_window_rejects_invalid_ranges() {
        assert!(resolve_changes_window(Some(1), Some(1), Some(2)).is_err());
        assert!(resolve_changes_window(None, Some(4), Some(3)).is_err());
        assert!(resolve_changes_window(None, Some(2), None).is_err());
        assert!(resolve_changes_window(None, None, Some(2)).is_err());
    }

    #[tokio::test]
    async fn load_mode_merge_requires_keyed_schema() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let schema_path = dir.path().join("schema.pg");
        let data_path = dir.path().join("data.jsonl");

        write_file(
            &schema_path,
            r#"node Person {
    name: String
}"#,
        );
        write_file(&data_path, r#"{"type":"Person","data":{"name":"Alice"}}"#);

        cmd_init(&db_path, &schema_path, false).await.unwrap();
        let err = cmd_load(&db_path, &data_path, LoadModeArg::Merge, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires at least one node @key"));
    }

    #[tokio::test]
    async fn load_mode_append_and_merge_behave_as_expected() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let schema_path = dir.path().join("schema.pg");
        let data_initial = dir.path().join("initial.jsonl");
        let data_append = dir.path().join("append.jsonl");
        let data_merge = dir.path().join("merge.jsonl");

        write_file(
            &schema_path,
            r#"node Person {
    name: String @key
    age: I32?
}"#,
        );
        write_file(
            &data_initial,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}"#,
        );
        write_file(
            &data_append,
            r#"{"type":"Person","data":{"name":"Bob","age":22}}"#,
        );
        write_file(
            &data_merge,
            r#"{"type":"Person","data":{"name":"Alice","age":31}}"#,
        );

        cmd_init(&db_path, &schema_path, false).await.unwrap();
        cmd_load(&db_path, &data_initial, LoadModeArg::Overwrite, false)
            .await
            .unwrap();
        cmd_load(&db_path, &data_append, LoadModeArg::Append, false)
            .await
            .unwrap();
        cmd_load(&db_path, &data_merge, LoadModeArg::Merge, false)
            .await
            .unwrap();

        let db = Database::open(&db_path).await.unwrap();
        let batch = db.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut alice_age = None;
        let mut has_bob = false;
        for row in 0..batch.num_rows() {
            if names.value(row) == "Alice" {
                alice_age = Some(ages.value(row));
            }
            if names.value(row) == "Bob" {
                has_bob = true;
            }
        }
        assert_eq!(alice_age, Some(31));
        assert!(has_bob);
    }

    #[test]
    fn check_and_run_require_db_mode() {
        let check_err = Cli::try_parse_from(["nanograph", "check", "--query", "/tmp/q.gq"])
            .err()
            .expect("check without --db should fail");
        assert!(check_err.to_string().contains("--db"));

        let run_err =
            Cli::try_parse_from(["nanograph", "run", "--query", "/tmp/q.gq", "--name", "q"])
                .err()
                .expect("run without --db should fail");
        assert!(run_err.to_string().contains("--db"));
    }

    #[tokio::test]
    async fn run_mutation_insert_in_db_mode() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let schema_path = dir.path().join("schema.pg");
        let query_path = dir.path().join("mut.gq");

        write_file(
            &schema_path,
            r#"node Person {
    name: String
    age: I32?
}"#,
        );
        write_file(
            &query_path,
            r#"
query add_person($name: String, $age: I32) {
    insert Person {
        name: $name
        age: $age
    }
}
"#,
        );

        cmd_init(&db_path, &schema_path, false).await.unwrap();
        cmd_run(
            db_path.clone(),
            &query_path,
            "add_person",
            "table",
            vec![
                ("name".to_string(), "Eve".to_string()),
                ("age".to_string(), "29".to_string()),
            ],
            false,
        )
        .await
        .unwrap();

        let db = Database::open(&db_path).await.unwrap();
        let people = db.storage.get_all_nodes("Person").unwrap().unwrap();
        let names = people
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!((0..people.num_rows()).any(|row| names.value(row) == "Eve"));

        let cdc_rows = read_visible_cdc_entries(&db_path, 0, None).unwrap();
        assert_eq!(cdc_rows.len(), 1);
        assert_eq!(cdc_rows[0].op, "insert");
        assert_eq!(cdc_rows[0].type_name, "Person");
    }

    #[tokio::test]
    async fn maintenance_commands_work_on_real_db() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let schema_path = dir.path().join("schema.pg");
        let data_path = dir.path().join("data.jsonl");

        write_file(
            &schema_path,
            r#"node Person {
    name: String @key
}"#,
        );
        write_file(
            &data_path,
            r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}"#,
        );

        cmd_init(&db_path, &schema_path, false).await.unwrap();
        cmd_load(&db_path, &data_path, LoadModeArg::Overwrite, false)
            .await
            .unwrap();

        cmd_compact(&db_path, 1_024, true, 0.1, false)
            .await
            .unwrap();
        cmd_cleanup(&db_path, 1, 1, false).await.unwrap();
        cmd_cdc_materialize(&db_path, 0, true, false).await.unwrap();
        cmd_doctor(&db_path, false).await.unwrap();

        assert!(db_path.join("__cdc_analytics").exists());

        let db = Database::open(&db_path).await.unwrap();
        let report = db.doctor().await.unwrap();
        assert!(report.tx_rows <= 1);
    }
}
