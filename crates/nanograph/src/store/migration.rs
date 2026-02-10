use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, AsArray, RecordBatch, UInt64Array, new_null_array};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatchIterator;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{
    EdgeTypeDef, NodeTypeDef, PropDef, SchemaIR, TypeDef, build_catalog_from_ir,
};
use crate::error::{NanoError, Result};
use crate::schema::ast::{Annotation, PropDecl, SchemaDecl, SchemaFile};
use crate::schema::parser::parse_schema;
use crate::store::database::Database;
use crate::store::graph::GraphStorage;
use crate::store::manifest::{DatasetEntry, GraphManifest, hash_string};
use crate::types::ScalarType;

const SCHEMA_PG_FILENAME: &str = "schema.pg";
const SCHEMA_IR_FILENAME: &str = "schema.ir.json";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MigrationSafety {
    Safe,
    Confirm,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MigrationStep {
    AddNodeType {
        name: String,
        type_id: u32,
    },
    AddEdgeType {
        name: String,
        type_id: u32,
        src_type_id: u32,
        dst_type_id: u32,
    },
    DropNodeType {
        name: String,
        type_id: u32,
    },
    DropEdgeType {
        name: String,
        type_id: u32,
    },
    RenameType {
        type_kind: String,
        type_id: u32,
        old_name: String,
        new_name: String,
    },
    AddProperty {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        data_type: String,
        nullable: bool,
    },
    DropProperty {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
    },
    RenameProperty {
        type_name: String,
        type_id: u32,
        old_name: String,
        new_name: String,
        prop_id: u32,
    },
    AlterPropertyType {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        old_type: String,
        new_type: String,
    },
    AlterPropertyNullability {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        nullable: bool,
    },
    RebindEdgeEndpoints {
        edge_name: String,
        edge_type_id: u32,
        old_src_type_id: u32,
        old_dst_type_id: u32,
        new_src_type_id: u32,
        new_dst_type_id: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedStep {
    pub step: MigrationStep,
    pub safety: MigrationSafety,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub db_path: String,
    pub old_schema_hash: String,
    pub new_schema_hash: String,
    pub steps: Vec<PlannedStep>,
    pub warnings: Vec<String>,
    pub blocked: Vec<String>,
}

impl MigrationPlan {
    pub fn has_blocked(&self) -> bool {
        self.steps
            .iter()
            .any(|s| s.safety == MigrationSafety::Blocked)
            || !self.blocked.is_empty()
    }

    pub fn has_confirm(&self) -> bool {
        self.steps
            .iter()
            .any(|s| s.safety == MigrationSafety::Confirm)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStatus {
    Applied,
    NeedsConfirmation,
    Blocked,
}

#[derive(Debug, Clone)]
pub struct MigrationExecution {
    pub status: MigrationStatus,
    pub plan: MigrationPlan,
}

struct PlannedMigration {
    plan: MigrationPlan,
    schema_source: String,
    new_ir: SchemaIR,
    manifest: GraphManifest,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum JournalState {
    Prepared,
    Applying,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationJournal {
    version: u32,
    state: JournalState,
    db_path: String,
    backup_path: String,
    staging_path: String,
    old_schema_hash: String,
    new_schema_hash: String,
    created_at_unix: u64,
}

pub fn reconcile_migration_sidecars(db_path: &Path) -> Result<()> {
    let paths = migration_base_paths(db_path)?;

    if !paths.journal_path.exists() {
        if paths.backup_path.exists() {
            return Err(NanoError::Manifest(format!(
                "found orphaned migration backup at {} without a journal; inspect before proceeding",
                paths.backup_path.display()
            )));
        }
        return Ok(());
    }

    let raw = std::fs::read_to_string(&paths.journal_path)?;
    let journal: MigrationJournal = serde_json::from_str(&raw)
        .map_err(|e| NanoError::Manifest(format!("parse migration journal: {}", e)))?;

    let backup_path = PathBuf::from(&journal.backup_path);
    let staging_path = PathBuf::from(&journal.staging_path);

    match journal.state {
        JournalState::Committed => {
            if !db_path.exists() {
                return Err(NanoError::Manifest(format!(
                    "found committed migration journal at {} but database path {} is missing; manual recovery required",
                    paths.journal_path.display(),
                    db_path.display()
                )));
            }
            if backup_path.exists() {
                std::fs::remove_dir_all(&backup_path)?;
            }
            if staging_path.exists() {
                std::fs::remove_dir_all(&staging_path)?;
            }
            if paths.lock_path.exists() {
                let _ = std::fs::remove_file(&paths.lock_path);
            }
            std::fs::remove_file(&paths.journal_path)?;
            Ok(())
        }
        // TODO: implement deterministic roll-forward / rollback for PREPARED/APPLYING/ABORTED states.
        state => Err(NanoError::Manifest(format!(
            "incomplete migration journal at {} (state {}); manual recovery required",
            paths.journal_path.display(),
            journal_state_name(state)
        ))),
    }
}

fn journal_state_name(state: JournalState) -> &'static str {
    match state {
        JournalState::Prepared => "PREPARED",
        JournalState::Applying => "APPLYING",
        JournalState::Committed => "COMMITTED",
        JournalState::Aborted => "ABORTED",
    }
}

pub async fn execute_schema_migration(
    db_path: &Path,
    dry_run: bool,
    auto_approve: bool,
) -> Result<MigrationExecution> {
    reconcile_migration_sidecars(db_path)?;
    let planned = plan_schema_migration(db_path).await?;

    if planned.plan.has_blocked() {
        return Ok(MigrationExecution {
            status: MigrationStatus::Blocked,
            plan: planned.plan,
        });
    }

    if planned.plan.has_confirm() && !auto_approve {
        return Ok(MigrationExecution {
            status: MigrationStatus::NeedsConfirmation,
            plan: planned.plan,
        });
    }

    if dry_run {
        return Ok(MigrationExecution {
            status: MigrationStatus::Applied,
            plan: planned.plan,
        });
    }

    apply_planned_migration(db_path, &planned).await?;
    Ok(MigrationExecution {
        status: MigrationStatus::Applied,
        plan: planned.plan,
    })
}

async fn plan_schema_migration(db_path: &Path) -> Result<PlannedMigration> {
    let schema_path = db_path.join(SCHEMA_PG_FILENAME);
    let schema_source = std::fs::read_to_string(&schema_path).map_err(|e| {
        NanoError::Io(std::io::Error::new(
            e.kind(),
            format!("failed to read {}: {}", schema_path.display(), e),
        ))
    })?;
    let desired_schema = parse_schema(&schema_source)?;

    let old_ir_json = std::fs::read_to_string(db_path.join(SCHEMA_IR_FILENAME))?;
    let old_ir: SchemaIR = serde_json::from_str(&old_ir_json)
        .map_err(|e| NanoError::Manifest(format!("parse IR error: {}", e)))?;
    let mut manifest = GraphManifest::read(db_path)?;
    bootstrap_identity_counters(&old_ir, &mut manifest);

    let mut warnings = Vec::new();
    let mut blocked = Vec::new();
    let (new_ir, next_type_id, next_prop_id) = build_desired_schema_ir(
        &old_ir,
        &manifest,
        &desired_schema,
        &mut warnings,
        &mut blocked,
    )?;
    manifest.next_type_id = next_type_id;
    manifest.next_prop_id = next_prop_id;

    // Open once so planner can validate cast/nullability/endpoint rebind against existing data.
    let db = Database::open(db_path).await?;
    let mut steps = Vec::new();
    diff_schema(&old_ir, &new_ir, &db, &mut steps, &mut blocked)?;

    let new_ir_json = serde_json::to_string_pretty(&new_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
    let plan = MigrationPlan {
        db_path: db_path.display().to_string(),
        old_schema_hash: hash_string(&old_ir_json),
        new_schema_hash: hash_string(&new_ir_json),
        steps,
        warnings,
        blocked,
    };

    Ok(PlannedMigration {
        plan,
        schema_source,
        new_ir,
        manifest,
    })
}

fn annotation_value<'a>(annotations: &'a [Annotation], key: &str) -> Option<&'a str> {
    annotations
        .iter()
        .find(|a| a.name == key)
        .and_then(|a| a.value.as_deref())
}

fn build_desired_schema_ir(
    old_ir: &SchemaIR,
    manifest: &GraphManifest,
    desired_schema: &SchemaFile,
    warnings: &mut Vec<String>,
    blocked: &mut Vec<String>,
) -> Result<(SchemaIR, u32, u32)> {
    let mut next_type_id = manifest.next_type_id;
    let mut next_prop_id = manifest.next_prop_id;

    let mut old_nodes_by_name = HashMap::new();
    let mut old_edges_by_name = HashMap::new();
    for ty in &old_ir.types {
        match ty {
            TypeDef::Node(n) => {
                old_nodes_by_name.insert(n.name.clone(), n);
            }
            TypeDef::Edge(e) => {
                old_edges_by_name.insert(e.name.clone(), e);
            }
        }
    }

    let mut used_old_node_type_ids = HashSet::new();
    let mut used_old_edge_type_ids = HashSet::new();

    let mut node_defs = Vec::new();
    let mut edge_defs = Vec::new();
    let mut node_name_to_type_id = HashMap::new();

    for decl in &desired_schema.declarations {
        let SchemaDecl::Node(node) = decl else {
            continue;
        };

        let rename_from = annotation_value(&node.annotations, "rename_from");
        let old_node = match rename_from {
            Some(from) => {
                let found = old_nodes_by_name.get(from).copied();
                if found.is_none() {
                    blocked.push(format!(
                        "node `{}` has @rename_from(\"{}\") but source type does not exist",
                        node.name, from
                    ));
                }
                found
            }
            None => old_nodes_by_name.get(&node.name).copied(),
        };

        let type_id = if let Some(old) = old_node {
            if used_old_node_type_ids.insert(old.type_id) {
                old.type_id
            } else {
                blocked.push(format!(
                    "node `{}` maps to reused source type_id {}",
                    node.name, old.type_id
                ));
                alloc_next_id(&mut next_type_id)?
            }
        } else {
            alloc_next_id(&mut next_type_id)?
        };
        node_name_to_type_id.insert(node.name.clone(), type_id);

        let mut old_props_by_name = HashMap::new();
        if let Some(old) = old_node {
            for p in &old.properties {
                old_props_by_name.insert(p.name.clone(), p);
            }
        }
        let mut used_old_prop_ids = HashSet::new();
        let mut props = Vec::new();
        for prop in &node.properties {
            let old_prop = resolve_old_prop(prop, &old_props_by_name, &node.name, blocked);
            let prop_id = if let Some(old) = old_prop {
                if used_old_prop_ids.insert(old.prop_id) {
                    old.prop_id
                } else {
                    blocked.push(format!(
                        "node `{}` property `{}` maps to reused source prop_id {}",
                        node.name, prop.name, old.prop_id
                    ));
                    alloc_next_id(&mut next_prop_id)?
                }
            } else {
                alloc_next_id(&mut next_prop_id)?
            };
            props.push(PropDef {
                name: prop.name.clone(),
                prop_id,
                scalar_type: prop.prop_type.scalar.to_string(),
                nullable: prop.prop_type.nullable,
            });
        }

        if let Some(from) = rename_from {
            if from == node.name {
                warnings.push(format!(
                    "node `{}` uses redundant @rename_from(\"{}\")",
                    node.name, from
                ));
            }
        }

        node_defs.push(NodeTypeDef {
            name: node.name.clone(),
            type_id,
            properties: props,
        });
    }

    for decl in &desired_schema.declarations {
        let SchemaDecl::Edge(edge) = decl else {
            continue;
        };

        let rename_from = annotation_value(&edge.annotations, "rename_from");
        let old_edge = match rename_from {
            Some(from) => {
                let found = old_edges_by_name.get(from).copied();
                if found.is_none() {
                    blocked.push(format!(
                        "edge `{}` has @rename_from(\"{}\") but source type does not exist",
                        edge.name, from
                    ));
                }
                found
            }
            None => old_edges_by_name.get(&edge.name).copied(),
        };

        let src_type_id = *node_name_to_type_id.get(&edge.from_type).ok_or_else(|| {
            NanoError::Catalog(format!(
                "edge `{}` references unknown source node type `{}`",
                edge.name, edge.from_type
            ))
        })?;
        let dst_type_id = *node_name_to_type_id.get(&edge.to_type).ok_or_else(|| {
            NanoError::Catalog(format!(
                "edge `{}` references unknown target node type `{}`",
                edge.name, edge.to_type
            ))
        })?;

        let type_id = if let Some(old) = old_edge {
            if used_old_edge_type_ids.insert(old.type_id) {
                old.type_id
            } else {
                blocked.push(format!(
                    "edge `{}` maps to reused source type_id {}",
                    edge.name, old.type_id
                ));
                alloc_next_id(&mut next_type_id)?
            }
        } else {
            alloc_next_id(&mut next_type_id)?
        };

        let mut old_props_by_name = HashMap::new();
        if let Some(old) = old_edge {
            for p in &old.properties {
                old_props_by_name.insert(p.name.clone(), p);
            }
        }
        let mut used_old_prop_ids = HashSet::new();
        let mut props = Vec::new();
        for prop in &edge.properties {
            let old_prop = resolve_old_prop(prop, &old_props_by_name, &edge.name, blocked);
            let prop_id = if let Some(old) = old_prop {
                if used_old_prop_ids.insert(old.prop_id) {
                    old.prop_id
                } else {
                    blocked.push(format!(
                        "edge `{}` property `{}` maps to reused source prop_id {}",
                        edge.name, prop.name, old.prop_id
                    ));
                    alloc_next_id(&mut next_prop_id)?
                }
            } else {
                alloc_next_id(&mut next_prop_id)?
            };
            props.push(PropDef {
                name: prop.name.clone(),
                prop_id,
                scalar_type: prop.prop_type.scalar.to_string(),
                nullable: prop.prop_type.nullable,
            });
        }

        if let Some(from) = rename_from {
            if from == edge.name {
                warnings.push(format!(
                    "edge `{}` uses redundant @rename_from(\"{}\")",
                    edge.name, from
                ));
            }
        }

        edge_defs.push(EdgeTypeDef {
            name: edge.name.clone(),
            type_id,
            src_type_id,
            dst_type_id,
            src_type_name: edge.from_type.clone(),
            dst_type_name: edge.to_type.clone(),
            properties: props,
        });
    }

    let mut types = Vec::new();
    for n in node_defs {
        types.push(TypeDef::Node(n));
    }
    for e in edge_defs {
        types.push(TypeDef::Edge(e));
    }
    let ir = SchemaIR {
        ir_version: old_ir.ir_version.max(2),
        types,
    };
    // Validate resulting IR can build a catalog.
    let _ = build_catalog_from_ir(&ir)?;
    Ok((ir, next_type_id, next_prop_id))
}

fn resolve_old_prop<'a>(
    prop: &'a PropDecl,
    old_props_by_name: &'a HashMap<String, &'a PropDef>,
    type_name: &str,
    blocked: &mut Vec<String>,
) -> Option<&'a PropDef> {
    match annotation_value(&prop.annotations, "rename_from") {
        Some(from) => {
            let old = old_props_by_name.get(from).copied();
            if old.is_none() {
                blocked.push(format!(
                    "type `{}` property `{}` has @rename_from(\"{}\") but source property does not exist",
                    type_name, prop.name, from
                ));
            }
            old
        }
        None => old_props_by_name.get(&prop.name).copied(),
    }
}

fn alloc_next_id(next: &mut u32) -> Result<u32> {
    let id = (*next).max(1);
    *next = id
        .checked_add(1)
        .ok_or_else(|| NanoError::Manifest("identity counter overflow".to_string()))?;
    Ok(id)
}

fn diff_schema(
    old_ir: &SchemaIR,
    new_ir: &SchemaIR,
    db: &Database,
    steps: &mut Vec<PlannedStep>,
    blocked: &mut Vec<String>,
) -> Result<()> {
    let old_nodes = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(n) => Some((n.type_id, n)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let new_nodes = new_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(n) => Some((n.type_id, n)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let old_edges = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(e) => Some((e.type_id, e)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let new_edges = new_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(e) => Some((e.type_id, e)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    for new_node in new_nodes.values() {
        match old_nodes.get(&new_node.type_id) {
            None => {
                steps.push(PlannedStep {
                    step: MigrationStep::AddNodeType {
                        name: new_node.name.clone(),
                        type_id: new_node.type_id,
                    },
                    safety: MigrationSafety::Safe,
                    reason: "new node type".to_string(),
                });
            }
            Some(old_node) => {
                if old_node.name != new_node.name {
                    steps.push(PlannedStep {
                        step: MigrationStep::RenameType {
                            type_kind: "node".to_string(),
                            type_id: new_node.type_id,
                            old_name: old_node.name.clone(),
                            new_name: new_node.name.clone(),
                        },
                        safety: MigrationSafety::Safe,
                        reason: "id-preserving node rename".to_string(),
                    });
                }
                diff_properties(
                    &old_node.name,
                    &new_node.name,
                    old_node.type_id,
                    &old_node.properties,
                    &new_node.properties,
                    get_node_column,
                    db,
                    steps,
                    blocked,
                )?;
            }
        }
    }

    for old_node in old_nodes.values() {
        if !new_nodes.contains_key(&old_node.type_id) {
            steps.push(PlannedStep {
                step: MigrationStep::DropNodeType {
                    name: old_node.name.clone(),
                    type_id: old_node.type_id,
                },
                safety: MigrationSafety::Confirm,
                reason: "node type removed from desired schema".to_string(),
            });
        }
    }

    for new_edge in new_edges.values() {
        match old_edges.get(&new_edge.type_id) {
            None => {
                steps.push(PlannedStep {
                    step: MigrationStep::AddEdgeType {
                        name: new_edge.name.clone(),
                        type_id: new_edge.type_id,
                        src_type_id: new_edge.src_type_id,
                        dst_type_id: new_edge.dst_type_id,
                    },
                    safety: MigrationSafety::Safe,
                    reason: "new edge type".to_string(),
                });
            }
            Some(old_edge) => {
                if old_edge.name != new_edge.name {
                    steps.push(PlannedStep {
                        step: MigrationStep::RenameType {
                            type_kind: "edge".to_string(),
                            type_id: new_edge.type_id,
                            old_name: old_edge.name.clone(),
                            new_name: new_edge.name.clone(),
                        },
                        safety: MigrationSafety::Safe,
                        reason: "id-preserving edge rename".to_string(),
                    });
                }

                if old_edge.src_type_id != new_edge.src_type_id
                    || old_edge.dst_type_id != new_edge.dst_type_id
                {
                    let (safety, reason) = classify_endpoint_rebind(db, old_ir, old_edge, new_edge);
                    if safety == MigrationSafety::Blocked {
                        blocked.push(reason.clone());
                    }
                    steps.push(PlannedStep {
                        step: MigrationStep::RebindEdgeEndpoints {
                            edge_name: new_edge.name.clone(),
                            edge_type_id: new_edge.type_id,
                            old_src_type_id: old_edge.src_type_id,
                            old_dst_type_id: old_edge.dst_type_id,
                            new_src_type_id: new_edge.src_type_id,
                            new_dst_type_id: new_edge.dst_type_id,
                        },
                        safety,
                        reason,
                    });
                }

                diff_properties(
                    &old_edge.name,
                    &new_edge.name,
                    old_edge.type_id,
                    &old_edge.properties,
                    &new_edge.properties,
                    get_edge_column,
                    db,
                    steps,
                    blocked,
                )?;
            }
        }
    }

    for old_edge in old_edges.values() {
        if !new_edges.contains_key(&old_edge.type_id) {
            steps.push(PlannedStep {
                step: MigrationStep::DropEdgeType {
                    name: old_edge.name.clone(),
                    type_id: old_edge.type_id,
                },
                safety: MigrationSafety::Confirm,
                reason: "edge type removed from desired schema".to_string(),
            });
        }
    }

    Ok(())
}

fn diff_properties<F>(
    source_type_name: &str,
    display_type_name: &str,
    type_id: u32,
    old_props: &[PropDef],
    new_props: &[PropDef],
    column_getter: F,
    db: &Database,
    steps: &mut Vec<PlannedStep>,
    blocked: &mut Vec<String>,
) -> Result<()>
where
    F: Fn(&Database, &str, &str) -> Result<Option<ArrayRef>>,
{
    let old_by_id = old_props
        .iter()
        .map(|p| (p.prop_id, p))
        .collect::<HashMap<_, _>>();
    let new_by_id = new_props
        .iter()
        .map(|p| (p.prop_id, p))
        .collect::<HashMap<_, _>>();

    for new_p in new_props {
        match old_by_id.get(&new_p.prop_id) {
            None => {
                let safety = if new_p.nullable {
                    MigrationSafety::Safe
                } else {
                    blocked.push(format!(
                        "type `{}` adds non-nullable property `{}` without a default/backfill",
                        display_type_name, new_p.name
                    ));
                    MigrationSafety::Blocked
                };
                steps.push(PlannedStep {
                    step: MigrationStep::AddProperty {
                        type_name: display_type_name.to_string(),
                        type_id,
                        prop_name: new_p.name.clone(),
                        prop_id: new_p.prop_id,
                        data_type: new_p.scalar_type.clone(),
                        nullable: new_p.nullable,
                    },
                    safety,
                    reason: "new property".to_string(),
                });
            }
            Some(old_p) => {
                if old_p.name != new_p.name {
                    steps.push(PlannedStep {
                        step: MigrationStep::RenameProperty {
                            type_name: display_type_name.to_string(),
                            type_id,
                            old_name: old_p.name.clone(),
                            new_name: new_p.name.clone(),
                            prop_id: new_p.prop_id,
                        },
                        safety: MigrationSafety::Safe,
                        reason: "id-preserving property rename".to_string(),
                    });
                }
                if old_p.scalar_type != new_p.scalar_type {
                    let (safety, reason) = classify_type_change(
                        db,
                        source_type_name,
                        display_type_name,
                        &old_p.name,
                        &new_p.scalar_type,
                        &column_getter,
                    )?;
                    if safety == MigrationSafety::Blocked {
                        blocked.push(reason.clone());
                    }
                    steps.push(PlannedStep {
                        step: MigrationStep::AlterPropertyType {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_p.name.clone(),
                            prop_id: new_p.prop_id,
                            old_type: old_p.scalar_type.clone(),
                            new_type: new_p.scalar_type.clone(),
                        },
                        safety,
                        reason,
                    });
                }
                if old_p.nullable != new_p.nullable {
                    let (safety, reason) = classify_nullability_change(
                        db,
                        source_type_name,
                        display_type_name,
                        &old_p.name,
                        old_p.nullable,
                        new_p.nullable,
                        &column_getter,
                    )?;
                    if safety == MigrationSafety::Blocked {
                        blocked.push(reason.clone());
                    }
                    steps.push(PlannedStep {
                        step: MigrationStep::AlterPropertyNullability {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_p.name.clone(),
                            prop_id: new_p.prop_id,
                            nullable: new_p.nullable,
                        },
                        safety,
                        reason,
                    });
                }
            }
        }
    }

    for old_p in old_props {
        if !new_by_id.contains_key(&old_p.prop_id) {
            steps.push(PlannedStep {
                step: MigrationStep::DropProperty {
                    type_name: display_type_name.to_string(),
                    type_id,
                    prop_name: old_p.name.clone(),
                    prop_id: old_p.prop_id,
                },
                safety: MigrationSafety::Confirm,
                reason: "property removed from desired schema".to_string(),
            });
        }
    }

    Ok(())
}

fn classify_type_change<F>(
    db: &Database,
    source_type_name: &str,
    display_type_name: &str,
    old_prop_name: &str,
    target_scalar: &str,
    column_getter: &F,
) -> Result<(MigrationSafety, String)>
where
    F: Fn(&Database, &str, &str) -> Result<Option<ArrayRef>>,
{
    let target = ScalarType::from_str_name(target_scalar)
        .ok_or_else(|| NanoError::Catalog(format!("unknown scalar type: {}", target_scalar)))?
        .to_arrow();

    match column_getter(db, source_type_name, old_prop_name)? {
        None => Ok((
            MigrationSafety::Confirm,
            "type change requires data cast".to_string(),
        )),
        Some(col) => {
            if arrow::compute::cast(col.as_ref(), &target).is_ok() {
                Ok((
                    MigrationSafety::Confirm,
                    "type change requires data cast".to_string(),
                ))
            } else {
                Ok((
                    MigrationSafety::Blocked,
                    format!(
                        "cannot cast existing data for `{}`.`{}` to {}",
                        display_type_name, old_prop_name, target_scalar
                    ),
                ))
            }
        }
    }
}

fn classify_nullability_change<F>(
    db: &Database,
    source_type_name: &str,
    display_type_name: &str,
    old_prop_name: &str,
    old_nullable: bool,
    new_nullable: bool,
    column_getter: &F,
) -> Result<(MigrationSafety, String)>
where
    F: Fn(&Database, &str, &str) -> Result<Option<ArrayRef>>,
{
    if old_nullable && !new_nullable {
        if let Some(col) = column_getter(db, source_type_name, old_prop_name)? {
            if col.null_count() > 0 {
                return Ok((
                    MigrationSafety::Blocked,
                    format!(
                        "cannot make `{}`.`{}` non-nullable: {} null value(s) exist",
                        display_type_name,
                        old_prop_name,
                        col.null_count()
                    ),
                ));
            }
        }
        return Ok((
            MigrationSafety::Confirm,
            "nullable -> non-nullable requires data check".to_string(),
        ));
    }

    Ok((
        MigrationSafety::Safe,
        if new_nullable {
            "non-nullable -> nullable".to_string()
        } else {
            "nullability changed".to_string()
        },
    ))
}

fn classify_endpoint_rebind(
    db: &Database,
    old_ir: &SchemaIR,
    old_edge: &EdgeTypeDef,
    new_edge: &EdgeTypeDef,
) -> (MigrationSafety, String) {
    let Some(seg) = db.storage.edge_segments.get(&old_edge.name) else {
        return (
            MigrationSafety::Blocked,
            format!("edge segment for `{}` not found", old_edge.name),
        );
    };
    if seg.edge_ids.is_empty() {
        return (
            MigrationSafety::Safe,
            "edge endpoint rebind has no existing rows".to_string(),
        );
    }

    let node_sets = build_node_id_sets_by_type_id(old_ir, &db.storage);
    let new_src = node_sets.get(&new_edge.src_type_id);
    let new_dst = node_sets.get(&new_edge.dst_type_id);
    if new_src.is_none() || new_dst.is_none() {
        return (
            MigrationSafety::Blocked,
            format!(
                "edge `{}` endpoint rebind targets unknown node type id(s)",
                old_edge.name
            ),
        );
    }
    let new_src = new_src.expect("checked above");
    let new_dst = new_dst.expect("checked above");

    let mut invalid = 0usize;
    for i in 0..seg.edge_ids.len() {
        if !new_src.contains(&seg.src_ids[i]) || !new_dst.contains(&seg.dst_ids[i]) {
            invalid += 1;
        }
    }

    if invalid > 0 {
        (
            MigrationSafety::Blocked,
            format!(
                "edge `{}` endpoint rebind would invalidate {} existing edge row(s)",
                old_edge.name, invalid
            ),
        )
    } else {
        (
            MigrationSafety::Confirm,
            "edge endpoint rebind requires data rewrite validation".to_string(),
        )
    }
}

fn build_node_id_sets_by_type_id(
    old_ir: &SchemaIR,
    storage: &GraphStorage,
) -> HashMap<u32, HashSet<u64>> {
    let mut out = HashMap::new();
    for ty in &old_ir.types {
        let TypeDef::Node(n) = ty else {
            continue;
        };
        let mut set = HashSet::new();
        if let Ok(Some(batch)) = storage.get_all_nodes(&n.name) {
            if let Some(col) = batch.column_by_name("id") {
                if let Some(ids) = col.as_any().downcast_ref::<UInt64Array>() {
                    for i in 0..ids.len() {
                        set.insert(ids.value(i));
                    }
                }
            }
        }
        out.insert(n.type_id, set);
    }
    out
}

fn get_node_column(db: &Database, type_name: &str, prop_name: &str) -> Result<Option<ArrayRef>> {
    let Some(batch) = db.storage.get_all_nodes(type_name)? else {
        return Ok(None);
    };
    Ok(batch.column_by_name(prop_name).cloned())
}

fn get_edge_column(db: &Database, type_name: &str, prop_name: &str) -> Result<Option<ArrayRef>> {
    let Some(batch) = db.storage.edge_batch_for_save(type_name)? else {
        return Ok(None);
    };
    Ok(batch.column_by_name(prop_name).cloned())
}

async fn apply_planned_migration(db_path: &Path, planned: &PlannedMigration) -> Result<()> {
    let lock = acquire_migration_lock(db_path)?;
    let names = migration_sidecar_paths(db_path)?;

    if names.journal_path.exists() {
        drop(lock);
        return Err(NanoError::Manifest(format!(
            "migration journal already exists at {}; resolve it before retrying",
            names.journal_path.display()
        )));
    }
    if names.backup_path.exists() {
        drop(lock);
        return Err(NanoError::Manifest(format!(
            "refusing to overwrite existing migration backup at {}",
            names.backup_path.display()
        )));
    }

    let old_db = Database::open(db_path).await?;

    let mut journal = MigrationJournal {
        version: 1,
        state: JournalState::Prepared,
        db_path: db_path.display().to_string(),
        backup_path: names.backup_path.display().to_string(),
        staging_path: names.staging_path.display().to_string(),
        old_schema_hash: planned.plan.old_schema_hash.clone(),
        new_schema_hash: planned.plan.new_schema_hash.clone(),
        created_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };
    write_journal(&names.journal_path, &journal)?;

    let new_catalog = build_catalog_from_ir(&planned.new_ir)?;
    let new_storage = transform_storage_for_new_schema(&old_db, &planned.new_ir, &new_catalog)?;

    if names.staging_path.exists() {
        std::fs::remove_dir_all(&names.staging_path)?;
    }
    write_staged_db(
        &names.staging_path,
        &planned.schema_source,
        &planned.new_ir,
        &new_storage,
        &planned.manifest,
    )
    .await?;

    journal.state = JournalState::Applying;
    write_journal(&names.journal_path, &journal)?;

    std::fs::rename(db_path, &names.backup_path)?;
    if let Err(e) = std::fs::rename(&names.staging_path, db_path) {
        let _ = std::fs::rename(&names.backup_path, db_path);
        journal.state = JournalState::Aborted;
        let _ = write_journal(&names.journal_path, &journal);
        drop(lock);
        return Err(NanoError::Io(e));
    }

    journal.state = JournalState::Committed;
    write_journal(&names.journal_path, &journal)?;
    std::fs::remove_dir_all(&names.backup_path)?;
    std::fs::remove_file(&names.journal_path)?;

    drop(lock);
    Ok(())
}

struct SidecarPaths {
    journal_path: PathBuf,
    backup_path: PathBuf,
    staging_path: PathBuf,
}

struct BaseSidecarPaths {
    journal_path: PathBuf,
    backup_path: PathBuf,
    lock_path: PathBuf,
}

fn migration_base_paths(db_path: &Path) -> Result<BaseSidecarPaths> {
    let parent = db_path
        .parent()
        .ok_or_else(|| NanoError::Manifest(format!("invalid db path: {}", db_path.display())))?;
    let name = db_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| NanoError::Manifest("db path must be valid unicode".to_string()))?;

    Ok(BaseSidecarPaths {
        journal_path: parent.join(format!("{}.migration.journal.json", name)),
        backup_path: parent.join(format!("{}.migration.backup", name)),
        lock_path: parent.join(format!("{}.migration.lock", name)),
    })
}

fn migration_sidecar_paths(db_path: &Path) -> Result<SidecarPaths> {
    let base = migration_base_paths(db_path)?;
    let parent = db_path
        .parent()
        .ok_or_else(|| NanoError::Manifest(format!("invalid db path: {}", db_path.display())))?;
    let name = db_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| NanoError::Manifest("db path must be valid unicode".to_string()))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    Ok(SidecarPaths {
        journal_path: base.journal_path,
        backup_path: base.backup_path,
        staging_path: parent.join(format!("{}.migration.staging.{}", name, nonce)),
    })
}

struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn acquire_migration_lock(db_path: &Path) -> Result<LockGuard> {
    let path = migration_base_paths(db_path)?.lock_path;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|e| {
            NanoError::Manifest(format!(
                "migration lock already held or cannot be created at {}: {}",
                path.display(),
                e
            ))
        })?;
    Ok(LockGuard { path })
}

fn write_journal(path: &Path, journal: &MigrationJournal) -> Result<()> {
    let json = serde_json::to_string_pretty(journal)
        .map_err(|e| NanoError::Manifest(format!("serialize migration journal: {}", e)))?;
    std::fs::write(path, json)?;
    Ok(())
}

fn transform_storage_for_new_schema(
    old_db: &Database,
    new_ir: &SchemaIR,
    new_catalog: &Catalog,
) -> Result<GraphStorage> {
    let mut out = GraphStorage::new(new_catalog.clone());

    let old_nodes_by_id = old_db
        .schema_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(n) => Some((n.type_id, n)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let old_edges_by_id = old_db
        .schema_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(e) => Some((e.type_id, e)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    for ty in &new_ir.types {
        let TypeDef::Node(new_node) = ty else {
            continue;
        };
        let Some(old_node) = old_nodes_by_id.get(&new_node.type_id) else {
            continue;
        };
        let Some(old_batch) = old_db.storage.get_all_nodes(&old_node.name)? else {
            continue;
        };
        let old_props_by_id = old_node
            .properties
            .iter()
            .map(|p| (p.prop_id, p))
            .collect::<HashMap<_, _>>();

        let mut cols: Vec<ArrayRef> = Vec::new();
        cols.push(
            old_batch
                .column_by_name("id")
                .ok_or_else(|| NanoError::Storage("node batch missing id".to_string()))?
                .clone(),
        );
        for new_prop in &new_node.properties {
            let arr = if let Some(old_prop) = old_props_by_id.get(&new_prop.prop_id) {
                let old_col = old_batch.column_by_name(&old_prop.name).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "node batch missing property `{}` for type `{}`",
                        old_prop.name, old_node.name
                    ))
                })?;
                cast_array_if_needed(old_col.clone(), &new_prop.scalar_type)?
            } else {
                if !new_prop.nullable {
                    return Err(NanoError::Manifest(format!(
                        "cannot apply migration: new non-nullable node property `{}` on `{}` has no value source",
                        new_prop.name, new_node.name
                    )));
                }
                let dt = scalar_to_arrow(&new_prop.scalar_type)?;
                new_null_array(&dt, old_batch.num_rows())
            };
            if !new_prop.nullable && arr.null_count() > 0 {
                return Err(NanoError::Manifest(format!(
                    "cannot apply migration: `{}`.`{}` contains nulls but target is non-nullable",
                    new_node.name, new_prop.name
                )));
            }
            cols.push(arr);
        }

        let schema = new_catalog
            .node_types
            .get(&new_node.name)
            .ok_or_else(|| {
                NanoError::Storage(format!("new catalog missing node type `{}`", new_node.name))
            })?
            .arrow_schema
            .clone();
        let batch = RecordBatch::try_new(schema, cols)
            .map_err(|e| NanoError::Storage(format!("migrated node batch error: {}", e)))?;
        out.load_node_batch(&new_node.name, batch)?;
    }

    for ty in &new_ir.types {
        let TypeDef::Edge(new_edge) = ty else {
            continue;
        };
        let Some(old_edge) = old_edges_by_id.get(&new_edge.type_id) else {
            continue;
        };
        let Some(old_batch) = old_db.storage.edge_batch_for_save(&old_edge.name)? else {
            continue;
        };
        let old_props_by_id = old_edge
            .properties
            .iter()
            .map(|p| (p.prop_id, p))
            .collect::<HashMap<_, _>>();

        let mut cols: Vec<ArrayRef> = Vec::new();
        let id_col = old_batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("edge batch missing id".to_string()))?
            .clone();
        let src_col = old_batch
            .column_by_name("src")
            .ok_or_else(|| NanoError::Storage("edge batch missing src".to_string()))?
            .clone();
        let dst_col = old_batch
            .column_by_name("dst")
            .ok_or_else(|| NanoError::Storage("edge batch missing dst".to_string()))?
            .clone();

        validate_edge_endpoints_during_apply(
            &out,
            new_ir,
            new_edge.src_type_id,
            new_edge.dst_type_id,
            src_col.as_ref(),
            dst_col.as_ref(),
            &new_edge.name,
        )?;

        cols.push(id_col);
        cols.push(src_col);
        cols.push(dst_col);

        for new_prop in &new_edge.properties {
            let arr = if let Some(old_prop) = old_props_by_id.get(&new_prop.prop_id) {
                let old_col = old_batch.column_by_name(&old_prop.name).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "edge batch missing property `{}` for type `{}`",
                        old_prop.name, old_edge.name
                    ))
                })?;
                cast_array_if_needed(old_col.clone(), &new_prop.scalar_type)?
            } else {
                if !new_prop.nullable {
                    return Err(NanoError::Manifest(format!(
                        "cannot apply migration: new non-nullable edge property `{}` on `{}` has no value source",
                        new_prop.name, new_edge.name
                    )));
                }
                let dt = scalar_to_arrow(&new_prop.scalar_type)?;
                new_null_array(&dt, old_batch.num_rows())
            };
            if !new_prop.nullable && arr.null_count() > 0 {
                return Err(NanoError::Manifest(format!(
                    "cannot apply migration: `{}`.`{}` contains nulls but target is non-nullable",
                    new_edge.name, new_prop.name
                )));
            }
            cols.push(arr);
        }

        let schema = out
            .edge_segments
            .get(&new_edge.name)
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "new storage missing edge segment `{}`",
                    new_edge.name
                ))
            })?
            .schema
            .clone();

        let batch = RecordBatch::try_new(schema, cols)
            .map_err(|e| NanoError::Storage(format!("migrated edge batch error: {}", e)))?;
        out.load_edge_batch(&new_edge.name, batch)?;
    }

    out.build_indices()?;
    Ok(out)
}

fn validate_edge_endpoints_during_apply(
    storage: &GraphStorage,
    ir: &SchemaIR,
    src_type_id: u32,
    dst_type_id: u32,
    src_col: &dyn arrow::array::Array,
    dst_col: &dyn arrow::array::Array,
    edge_name: &str,
) -> Result<()> {
    let src_name = ir.type_name(src_type_id).ok_or_else(|| {
        NanoError::Manifest(format!(
            "edge `{}` source type id {} not found in target schema",
            edge_name, src_type_id
        ))
    })?;
    let dst_name = ir.type_name(dst_type_id).ok_or_else(|| {
        NanoError::Manifest(format!(
            "edge `{}` destination type id {} not found in target schema",
            edge_name, dst_type_id
        ))
    })?;
    let src_ids = src_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge src column must be UInt64".to_string()))?;
    let dst_ids = dst_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge dst column must be UInt64".to_string()))?;

    let src_set = collect_node_ids(storage, src_name)?;
    let dst_set = collect_node_ids(storage, dst_name)?;

    for i in 0..src_ids.len() {
        let s = src_ids.value(i);
        let d = dst_ids.value(i);
        if !src_set.contains(&s) || !dst_set.contains(&d) {
            return Err(NanoError::Manifest(format!(
                "edge `{}` endpoint rebind invalidates row {} (src={}, dst={})",
                edge_name, i, s, d
            )));
        }
    }
    Ok(())
}

fn collect_node_ids(storage: &GraphStorage, type_name: &str) -> Result<HashSet<u64>> {
    let mut set = HashSet::new();
    if let Some(batch) = storage.get_all_nodes(type_name)? {
        let ids = batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("missing id column".to_string()))?
            .as_primitive::<UInt64Type>();
        for i in 0..ids.len() {
            set.insert(ids.value(i));
        }
    }
    Ok(set)
}

fn scalar_to_arrow(name: &str) -> Result<arrow::datatypes::DataType> {
    let scalar = ScalarType::from_str_name(name)
        .ok_or_else(|| NanoError::Catalog(format!("unknown scalar type: {}", name)))?;
    Ok(scalar.to_arrow())
}

fn cast_array_if_needed(col: ArrayRef, target_scalar: &str) -> Result<ArrayRef> {
    let target = scalar_to_arrow(target_scalar)?;
    if col.data_type() == &target {
        Ok(col)
    } else {
        arrow::compute::cast(col.as_ref(), &target)
            .map_err(|e| NanoError::Execution(format!("cast error: {}", e)))
    }
}

async fn write_staged_db(
    path: &Path,
    schema_source: &str,
    schema_ir: &SchemaIR,
    storage: &GraphStorage,
    manifest_seed: &GraphManifest,
) -> Result<()> {
    std::fs::create_dir_all(path)?;
    std::fs::create_dir_all(path.join("nodes"))?;
    std::fs::create_dir_all(path.join("edges"))?;
    std::fs::write(path.join(SCHEMA_PG_FILENAME), schema_source)?;

    let ir_json = serde_json::to_string_pretty(schema_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
    std::fs::write(path.join(SCHEMA_IR_FILENAME), &ir_json)?;

    for node_def in schema_ir.node_types() {
        if let Some(batch) = storage.get_all_nodes(&node_def.name)? {
            let dataset_path = path
                .join("nodes")
                .join(SchemaIR::dir_name(node_def.type_id));
            write_lance_batch(&dataset_path, batch).await?;
        }
    }
    for edge_def in schema_ir.edge_types() {
        if let Some(batch) = storage.edge_batch_for_save(&edge_def.name)? {
            let dataset_path = path
                .join("edges")
                .join(SchemaIR::dir_name(edge_def.type_id));
            write_lance_batch(&dataset_path, batch).await?;
        }
    }

    let mut manifest = GraphManifest::new(hash_string(&ir_json));
    manifest.next_node_id = storage.next_node_id();
    manifest.next_edge_id = storage.next_edge_id();
    manifest.next_type_id = manifest_seed.next_type_id;
    manifest.next_prop_id = manifest_seed.next_prop_id;
    manifest.schema_identity_version = manifest_seed.schema_identity_version.saturating_add(1);

    for node_def in schema_ir.node_types() {
        if let Some(batch) = storage.get_all_nodes(&node_def.name)? {
            manifest.datasets.push(DatasetEntry {
                type_id: node_def.type_id,
                type_name: node_def.name.clone(),
                kind: "node".to_string(),
                row_count: batch.num_rows() as u64,
            });
        }
    }
    for edge_def in schema_ir.edge_types() {
        if let Some(batch) = storage.edge_batch_for_save(&edge_def.name)? {
            manifest.datasets.push(DatasetEntry {
                type_id: edge_def.type_id,
                type_name: edge_def.name.clone(),
                kind: "edge".to_string(),
                row_count: batch.num_rows() as u64,
            });
        }
    }
    manifest.write_atomic(path)?;
    Ok(())
}

async fn write_lance_batch(path: &Path, batch: RecordBatch) -> Result<()> {
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let uri = path.to_string_lossy().to_string();
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };
    Dataset::write(reader, &uri, Some(write_params))
        .await
        .map_err(|e| NanoError::Lance(format!("write error: {}", e)))?;
    Ok(())
}

fn bootstrap_identity_counters(ir: &SchemaIR, manifest: &mut GraphManifest) {
    if manifest.next_type_id > 0 && manifest.next_prop_id > 0 {
        return;
    }
    let mut max_type_id = 0u32;
    let mut max_prop_id = 0u32;
    for ty in &ir.types {
        match ty {
            TypeDef::Node(n) => {
                max_type_id = max_type_id.max(n.type_id);
                for p in &n.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
            TypeDef::Edge(e) => {
                max_type_id = max_type_id.max(e.type_id);
                for p in &e.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
        }
    }
    if manifest.next_type_id == 0 {
        manifest.next_type_id = max_type_id.saturating_add(1).max(1);
    }
    if manifest.next_prop_id == 0 {
        manifest.next_prop_id = max_prop_id.saturating_add(1).max(1);
    }
    manifest.schema_identity_version = manifest.schema_identity_version.max(1);
}
