mod common;

use std::thread::sleep;
use std::time::Duration;

use common::{ExampleProject, ExampleWorkspace, scalar_string};

fn now_schema() -> &'static str {
    r#"
node Event {
    slug: String @key
    created_at: DateTime
    updated_at: DateTime?
}
"#
}

fn now_data() -> &'static str {
    r#"
{"type":"Event","data":{"slug":"launch","created_at":"2026-03-01T12:00:00Z","updated_at":null}}
"#
}

fn now_queries() -> &'static str {
    r#"
query current_time() {
    match {
        $e: Event
        $e.slug = "launch"
        $e.created_at <= now()
    }
    return { $e.slug, now() as ts }
}

query stamp_now() {
    update Event set { updated_at: now() } where slug = "launch"
}

query event_timestamps() {
    match {
        $e: Event
        $e.slug = "launch"
    }
    return { $e.slug, $e.created_at, $e.updated_at }
}
"#
}

fn init_now_db(workspace: &ExampleWorkspace) {
    workspace.write_file("now.pg", now_schema());
    workspace.write_file("now.jsonl", now_data());
    workspace.write_file("now.gq", now_queries());

    let init = workspace.json_value(&["--json", "init", "now.nano", "--schema", "now.pg"]);
    assert_eq!(init["status"], "ok");

    let load = workspace.json_value(&[
        "--json",
        "load",
        "now.nano",
        "--data",
        "now.jsonl",
        "--mode",
        "overwrite",
    ]);
    assert_eq!(load["status"], "ok");
}

#[test]
fn now_is_resolved_fresh_for_each_cli_execution() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_now_db(&workspace);

    let first = workspace.json_rows(&[
        "run",
        "--db",
        "now.nano",
        "--query",
        "now.gq",
        "--name",
        "current_time",
        "--format",
        "json",
    ]);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["slug"], "launch");
    let first_ts = scalar_string(&first[0]["ts"]);
    assert!(first_ts.contains('T'));

    // Sleep long enough to make the execution-local timestamp differ even if
    // the JSON datetime renderer rounds to second precision.
    sleep(Duration::from_millis(1100));

    let second = workspace.json_rows(&[
        "run",
        "--db",
        "now.nano",
        "--query",
        "now.gq",
        "--name",
        "current_time",
        "--format",
        "json",
    ]);
    assert_eq!(second.len(), 1);
    let second_ts = scalar_string(&second[0]["ts"]);
    assert!(second_ts.contains('T'));
    assert_ne!(first_ts, second_ts);
}

#[test]
fn now_can_be_assigned_through_cli_mutation_queries() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_now_db(&workspace);

    let mutation = workspace.json_rows(&[
        "run",
        "--db",
        "now.nano",
        "--query",
        "now.gq",
        "--name",
        "stamp_now",
        "--format",
        "json",
    ]);
    assert_eq!(mutation.len(), 1);
    assert_eq!(scalar_string(&mutation[0]["affected_nodes"]), "1");
    assert_eq!(scalar_string(&mutation[0]["affected_edges"]), "0");

    let rows = workspace.json_rows(&[
        "run",
        "--db",
        "now.nano",
        "--query",
        "now.gq",
        "--name",
        "event_timestamps",
        "--format",
        "json",
    ]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["slug"], "launch");
    let created_at = scalar_string(&rows[0]["created_at"]);
    assert!(created_at.starts_with("2026-03-01T12:00:00"));
    let updated_at = scalar_string(&rows[0]["updated_at"]);
    assert!(updated_at.contains('T'));
    assert_ne!(updated_at, "null");
    assert_ne!(updated_at, created_at);
}
