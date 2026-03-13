use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, WrapErr, eyre};
use tracing::instrument;

use crate::ui::{StatusTone, format_status_line, stdout_supports_color, style_label};
use nanograph::store::migration::{
    MigrationExecution, MigrationPlan, MigrationStatus, MigrationStep, SchemaCompatibility,
    SchemaDiffReport, analyze_schema_diff, execute_schema_migration,
};

#[instrument(skip(format), fields(db_path = %db_path.display(), dry_run = dry_run, format = format))]
pub(crate) async fn cmd_migrate(
    db_path: &Path,
    dry_run: bool,
    format: &str,
    auto_approve: bool,
    json: bool,
    quiet: bool,
) -> Result<()> {
    let schema_path = db_path.join("schema.pg");
    let schema_src = std::fs::read_to_string(&schema_path)
        .wrap_err_with(|| format!("failed to read schema: {}", schema_path.display()))?;
    let _ = crate::parse_schema_or_report(&schema_path, &schema_src)?;

    let execution = execute_schema_migration(db_path, dry_run, auto_approve).await?;
    let effective_format = if json { "json" } else { format };
    if !(quiet && effective_format == "table") {
        render_migration_execution(&execution, effective_format)?;
    }

    match execution.status {
        MigrationStatus::Applied => Ok(()),
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
    let color = stdout_supports_color();
    println!("{}", style_label("Migration Plan", color));
    println!("  {} {}", style_label("DB:", color), plan.db_path);
    println!(
        "  {} {}",
        style_label("Old schema hash:", color),
        plan.old_schema_hash
    );
    println!(
        "  {} {}",
        style_label("New schema hash:", color),
        plan.new_schema_hash
    );
    println!();

    if !plan.steps.is_empty() {
        println!("{:<10} {:<24} DETAIL", "SAFETY", "STEP");
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
        println!(
            "{}",
            format_status_line(StatusTone::Warn, "Warnings:", color)
        );
        for w in &plan.warnings {
            println!("- {}", w);
        }
    }
    if !plan.blocked.is_empty() {
        println!();
        println!(
            "{}",
            format_status_line(StatusTone::Error, "Blocked:", color)
        );
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
        MigrationStep::AlterPropertyKey { .. } => "AlterPropertyKey",
        MigrationStep::AlterPropertyUnique { .. } => "AlterPropertyUnique",
        MigrationStep::AlterPropertyIndex { .. } => "AlterPropertyIndex",
        MigrationStep::AlterPropertyEnumValues { .. } => "AlterPropertyEnumValues",
        MigrationStep::AlterMetadata { .. } => "AlterMetadata",
        MigrationStep::RebindEdgeEndpoints { .. } => "RebindEdgeEndpoints",
    }
}

#[instrument(skip(format), fields(from_schema = %from_schema.display(), to_schema = %to_schema.display(), format = format))]
pub(crate) async fn cmd_schema_diff(
    from_schema: &PathBuf,
    to_schema: &PathBuf,
    format: &str,
    json: bool,
    quiet: bool,
) -> Result<()> {
    let old_source = std::fs::read_to_string(from_schema)
        .wrap_err_with(|| format!("failed to read schema: {}", from_schema.display()))?;
    let new_source = std::fs::read_to_string(to_schema)
        .wrap_err_with(|| format!("failed to read schema: {}", to_schema.display()))?;
    let old_schema = crate::parse_schema_or_report(from_schema, &old_source)?;
    let new_schema = crate::parse_schema_or_report(to_schema, &new_source)?;
    let report = analyze_schema_diff(&old_schema, &new_schema)?;
    let effective_format = if json { "json" } else { format };
    if !(quiet && effective_format == "table") {
        render_schema_diff_report(&report, effective_format)?;
    }
    Ok(())
}

fn render_schema_diff_report(report: &SchemaDiffReport, format: &str) -> Result<()> {
    match format {
        "json" => {
            let out = serde_json::to_string_pretty(report)
                .wrap_err("failed to serialize schema diff JSON")?;
            println!("{}", out);
        }
        "table" => {
            let color = stdout_supports_color();
            println!("{}", style_label("Schema Diff", color));
            println!(
                "  {} {}",
                style_label("Old schema hash:", color),
                report.old_schema_hash
            );
            println!(
                "  {} {}",
                style_label("New schema hash:", color),
                report.new_schema_hash
            );
            println!(
                "  {} {}",
                style_label("Compatibility:", color),
                schema_compatibility_label(report.compatibility)
            );
            println!(
                "  {} {}",
                style_label("Has breaking:", color),
                report.has_breaking
            );
            println!();

            if report.steps.is_empty() {
                println!("No schema changes.");
            } else {
                println!("{:<28} {:<30} DETAIL", "CLASSIFICATION", "STEP");
                for step in &report.steps {
                    println!(
                        "{:<28} {:<30} {}",
                        schema_compatibility_label(step.classification),
                        migration_step_kind(&step.step),
                        step.reason
                    );
                    if let Some(remediation) = &step.remediation {
                        println!("  remediation: {}", remediation);
                    }
                }
            }

            if !report.warnings.is_empty() {
                println!();
                println!(
                    "{}",
                    format_status_line(StatusTone::Warn, "Warnings:", color)
                );
                for warning in &report.warnings {
                    println!("- {}", warning);
                }
            }
            if !report.blocked.is_empty() {
                println!();
                println!(
                    "{}",
                    format_status_line(StatusTone::Error, "Blocked:", color)
                );
                for item in &report.blocked {
                    println!("- {}", item);
                }
            }
        }
        other => return Err(eyre!("unknown format: {} (supported: table, json)", other)),
    }
    Ok(())
}

pub(crate) fn schema_compatibility_label(value: SchemaCompatibility) -> &'static str {
    match value {
        SchemaCompatibility::Additive => "additive",
        SchemaCompatibility::CompatibleWithConfirmation => "compatible_with_confirmation",
        SchemaCompatibility::Breaking => "breaking",
        SchemaCompatibility::Blocked => "blocked",
    }
}
