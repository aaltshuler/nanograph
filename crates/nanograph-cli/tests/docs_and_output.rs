mod common;

use common::{ExampleProject, ExampleWorkspace};

#[test]
fn starwars_example_doc_commands_stay_green() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Starwars);
    workspace.init();
    workspace.load();
    workspace.check();

    let describe = workspace.json_value(&["describe", "--type", "Character", "--format", "json"]);
    assert_eq!(describe["nodes"][0]["name"], "Character");

    let search = workspace
        .run_ok(&["run", "search", "who turned evil"])
        .stdout;
    assert!(search.contains("Query: semantic_search"));

    let hybrid = workspace.json_rows(&[
        "run",
        "hybrid",
        "father and son conflict",
        "--format",
        "json",
    ]);
    assert!(!hybrid.is_empty());

    let family = workspace.json_rows(&[
        "run",
        "family",
        "luke-skywalker",
        "chosen one prophecy",
        "--format",
        "json",
    ]);
    assert!(!family.is_empty());

    let debut = workspace
        .run_ok(&["run", "debut", "anakin-skywalker"])
        .stdout;
    assert!(debut.contains("The Phantom Menace"));

    let same_debut = workspace.json_rows(&[
        "run",
        "--query",
        "starwars.gq",
        "--name",
        "same_debut",
        "--format",
        "json",
        "--param",
        "film=a-new-hope",
    ]);
    assert!(same_debut.len() >= 4);
}

#[test]
fn revops_example_doc_commands_stay_green() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();
    workspace.check();

    let describe = workspace.json_value(&["describe", "--type", "Signal", "--format", "json"]);
    assert_eq!(describe["nodes"][0]["name"], "Signal");

    let why = workspace
        .run_ok(&["run", "why", "opp-stripe-migration"])
        .stdout;
    assert!(why.contains("Query: decision_trace"));

    let trace = workspace.json_rows(&["run", "trace", "sig-hates-vendor", "--format", "json"]);
    assert_eq!(trace.len(), 1);

    let value = workspace.json_rows(&["run", "value", "sig-hates-vendor", "--format", "json"]);
    assert_eq!(value.len(), 1);

    let pipeline = workspace.run_ok(&["run", "pipeline"]).stdout;
    assert!(pipeline.contains("won"));

    let signals = workspace.json_rows(&[
        "run",
        "signals",
        "cli-priya-shah",
        "procurement approval timing",
        "--format",
        "json",
    ]);
    assert!(!signals.is_empty());
}

#[test]
fn human_oriented_command_outputs_smoke_cleanly() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    let version = workspace.run_ok(&["version"]).stdout;
    assert!(version.contains("nanograph "));
    assert!(version.contains("Database:"));
    assert!(version.contains("Manifest: format v"));

    let describe = workspace.run_ok(&["describe", "--type", "Signal"]).stdout;
    assert!(describe.contains("Database:"));
    assert!(describe.contains("Summary:"));
    assert!(describe.contains("Node Types"));
    assert!(describe.contains("Signal"));
    assert!(!describe.contains("dataset_version="));
    assert!(!describe.contains("type_id="));

    let check = workspace.run_ok(&["check", "--query", "revops.gq"]).stdout;
    assert!(check.contains("OK: query `decision_trace` (read)"));
    assert!(check.contains("INFO: Check complete:"));
    assert!(check.contains("Check complete:"));

    let compact = workspace
        .run_ok(&["compact", "--target-rows-per-fragment", "1024"])
        .stdout;
    assert!(compact.contains("Compaction complete"));

    let cleanup = workspace
        .run_ok(&[
            "cleanup",
            "--retain-tx-versions",
            "2",
            "--retain-dataset-versions",
            "1",
        ])
        .stdout;
    assert!(cleanup.contains("Cleanup complete"));

    let doctor = workspace.run_ok(&["doctor"]).stdout;
    assert!(doctor.contains("OK: Doctor OK"));
}

#[test]
fn describe_verbose_shows_manifest_and_dataset_internals() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    let output = workspace
        .run_ok(&["describe", "--type", "Signal", "--verbose"])
        .stdout;
    assert!(output.contains("Manifest:"));
    assert!(output.contains("Schema hash:"));
    assert!(output.contains("dataset_version="));
    assert!(output.contains("type_id="));
}

#[test]
fn quiet_suppresses_human_output_but_not_machine_formats() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    let version = workspace.run_ok(&["--quiet", "version"]).stdout;
    assert!(version.trim().is_empty());

    let describe = workspace
        .run_ok(&["--quiet", "describe", "--type", "Signal"])
        .stdout;
    assert!(describe.trim().is_empty());

    let run = workspace.run_ok(&["--quiet", "run", "pipeline"]).stdout;
    assert!(run.trim().is_empty());

    let json_rows = workspace
        .run_ok(&["--quiet", "run", "pipeline", "--format", "json"])
        .stdout;
    assert!(json_rows.contains('['));
}
