mod common;

use std::fs;

use common::{ExampleProject, ExampleWorkspace};

fn warning_contains(value: &serde_json::Value, needle: &str) -> bool {
    value["warnings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|warning| warning.as_str())
        .any(|warning| warning.contains(needle))
}

#[test]
fn changes_ignore_legacy_graph_mirror_files_in_namespace_lineage() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    workspace.delete_file("omni.nano/__graph_commits");
    workspace.delete_file("omni.nano/__graph_changes");

    let changes = workspace.run_ok(&["changes", "--since", "0", "--format", "jsonl"]);
    assert!(changes.stdout.contains("\"graph_version\":"));
    assert!(changes.stdout.contains("\"type_name\":\"Signal\""));

    let doctor = workspace.json_value(&["--json", "doctor"]);
    assert_eq!(doctor["status"], "ok");
    assert_eq!(doctor["healthy"], true);
    assert!(!warning_contains(&doctor, "graph mirror"));
}

#[test]
fn cleanup_does_not_recreate_legacy_graph_mirror_files() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    workspace.delete_file("omni.nano/__graph_commits");
    workspace.delete_file("omni.nano/__graph_changes");
    assert!(!workspace.file("omni.nano/__graph_commits").exists());
    assert!(!workspace.file("omni.nano/__graph_changes").exists());

    let cleanup = workspace.json_value(&[
        "--json",
        "cleanup",
        "--retain-tx-versions",
        "2",
        "--retain-dataset-versions",
        "1",
    ]);
    assert_eq!(cleanup["status"], "ok");
    assert!(!workspace.file("omni.nano/__graph_commits").exists());
    assert!(!workspace.file("omni.nano/__graph_changes").exists());

    let doctor = workspace.json_value(&["--json", "doctor"]);
    assert_eq!(doctor["status"], "ok");
    assert_eq!(doctor["healthy"], true);
    assert_eq!(
        doctor["warnings"].as_array().map(|warnings| warnings.len()),
        Some(0)
    );
}

#[test]
fn doctor_ignores_unreadable_legacy_graph_mirror_artifacts() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    workspace.delete_file("omni.nano/__graph_commits");
    workspace.delete_file("omni.nano/__graph_changes");
    fs::write(
        workspace.file("omni.nano/__graph_commits"),
        "not a lance dataset",
    )
    .unwrap();
    fs::write(
        workspace.file("omni.nano/__graph_changes"),
        "not a lance dataset",
    )
    .unwrap();

    let doctor = workspace.json_value(&["--json", "doctor"]);
    assert_eq!(doctor["status"], "ok");
    assert_eq!(doctor["healthy"], true);
    assert!(!warning_contains(&doctor, "graph mirror"));

    let changes = workspace.run_ok(&["changes", "--since", "0", "--format", "jsonl"]);
    assert!(changes.stdout.contains("\"graph_version\":"));
}
