mod common;

use std::fs;

use common::{parse_jsonl_rows, ExampleProject, ExampleWorkspace};

fn bug_schema() -> &'static str {
    r#"
node Person {
    slug: String @key
    name: String
    email: String @unique
    age: I32?
    role: enum(admin, member, guest)
    joinedAt: Date
}

node Project {
    slug: String @key
    name: String
    createdAt: DateTime
}

edge WorksOn: Person -> Project {
    role: enum(lead, contributor, reviewer)
    startedAt: Date
}
"#
}

fn bug_schema_with_display_name() -> &'static str {
    r#"
node Person {
    slug: String @key
    name: String
    displayName: String?
    email: String @unique
    age: I32?
    role: enum(admin, member, guest)
    joinedAt: Date
}

node Project {
    slug: String @key
    name: String
    createdAt: DateTime
}

edge WorksOn: Person -> Project {
    role: enum(lead, contributor, reviewer)
    startedAt: Date
}
"#
}

fn bug_seed_data() -> &'static str {
    r#"
{"type":"Person","data":{"slug":"alice","name":"Alice","email":"alice@example.com","age":30,"role":"admin","joinedAt":"2024-01-10"}}
{"type":"Project","data":{"slug":"apollo","name":"Apollo","createdAt":"2024-01-10T12:00:00Z"}}
{"edge":"worksOn","from":"alice","to":"apollo","data":{"role":"lead","startedAt":"2024-02-01"}}
"#
}

fn bug_append_data() -> &'static str {
    r#"
{"type":"Person","data":{"slug":"bob","name":"Bob","email":"bob@example.com","age":null,"role":"member","joinedAt":"2024-01-11"}}
{"type":"Project","data":{"slug":"zeus","name":"Zeus","createdAt":"2024-01-11T12:00:00Z"}}
{"edge":"worksOn","from":"bob","to":"zeus","data":{"role":"contributor","startedAt":"2024-02-02"}}
"#
}

fn embed_merge_schema() -> &'static str {
    r#"
node Chunk {
    slug: String @key
    chunk_type: String @index
    content: String
    context: String?
    embedding: Vector(1536) @embed(content)
}
"#
}

fn embed_merge_initial_data() -> &'static str {
    r#"
{"type":"Chunk","data":{"slug":"abc123","chunk_type":"teaching_point","content":"Some text content here...","context":"Context info"}}
"#
}

fn embed_merge_delta_data() -> &'static str {
    r#"
{"type":"Chunk","data":{"slug":"abc123","chunk_type":"teaching_point","content":"Some text content here...","context":"Context info"}}
{"type":"Chunk","data":{"slug":"def456","chunk_type":"teaching_point","content":"More text content here...","context":"More context info"}}
"#
}

fn write_bug_fixture(workspace: &ExampleWorkspace) {
    workspace.write_file("bug.pg", bug_schema());
    workspace.write_file("bug.jsonl", bug_seed_data());
    workspace.write_file("bug-append.jsonl", bug_append_data());
}

fn init_bug_db(workspace: &ExampleWorkspace) {
    write_bug_fixture(workspace);
    let init = workspace.json_value(&["--json", "init", "bug.nano", "--schema", "bug.pg"]);
    assert_eq!(init["status"], "ok");
}

fn write_embed_merge_fixture(workspace: &ExampleWorkspace) {
    workspace.write_file(".env.nano", "NANOGRAPH_EMBEDDINGS_MOCK=1\n");
    workspace.write_file("embed.pg", embed_merge_schema());
    workspace.write_file("embed-initial.jsonl", embed_merge_initial_data());
    workspace.write_file("embed-merge.jsonl", embed_merge_delta_data());
    workspace.write_file(
        "embed.gq",
        r#"
query all_chunks() {
    match { $c: Chunk }
    return { $c.slug, $c.embedding }
    order { $c.slug asc }
}
"#,
    );
}

#[test]
fn load_rejects_invalid_enum_value_end_to_end() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_bug_db(&workspace);
    workspace.write_file(
        "bad-enum.jsonl",
        r#"{"type":"Person","data":{"slug":"bad","name":"Bad","email":"bad@example.com","age":22,"role":"superadmin","joinedAt":"2024-01-12"}}
"#,
    );

    let failure = workspace.run_fail(&[
        "--json",
        "load",
        "bug.nano",
        "--data",
        "bad-enum.jsonl",
        "--mode",
        "append",
    ]);
    assert!(failure.stderr.trim().is_empty());
    assert!(failure.stdout.contains("\"status\":\"error\""));
    assert!(failure.stdout.contains("invalid enum value 'superadmin'"));
}

#[test]
fn load_rejects_wrong_type_for_nullable_field_end_to_end() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_bug_db(&workspace);
    workspace.write_file(
        "bad-type.jsonl",
        r#"{"type":"Person","data":{"slug":"bad","name":"Bad","email":"bad@example.com","age":"not-a-number","role":"member","joinedAt":"2024-01-12"}}
"#,
    );

    let failure = workspace.run_fail(&[
        "--json",
        "load",
        "bug.nano",
        "--data",
        "bad-type.jsonl",
        "--mode",
        "append",
    ]);
    assert!(failure.stderr.trim().is_empty());
    assert!(failure.stdout.contains("\"status\":\"error\""));
    assert!(failure.stdout.contains("type mismatch for Person.age"));
}

#[test]
fn append_mode_preserves_edge_date_and_enum_properties_end_to_end() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_bug_db(&workspace);

    let load = workspace.json_value(&[
        "--json",
        "load",
        "bug.nano",
        "--data",
        "bug.jsonl",
        "--mode",
        "overwrite",
    ]);
    assert_eq!(load["status"], "ok");

    let append = workspace.json_value(&[
        "--json",
        "load",
        "bug.nano",
        "--data",
        "bug-append.jsonl",
        "--mode",
        "append",
    ]);
    assert_eq!(append["status"], "ok");

    let exported = workspace
        .run_ok(&["export", "--db", "bug.nano", "--format", "jsonl"])
        .stdout;
    let rows = parse_jsonl_rows(&exported);
    assert!(rows.iter().any(|row| {
        row["edge"] == "WorksOn"
            && row["from"] == "alice"
            && row["to"] == "apollo"
            && row["data"]["role"] == "lead"
            && row["data"]["startedAt"] == "2024-02-01"
    }));
    assert!(rows.iter().any(|row| {
        row["edge"] == "WorksOn"
            && row["from"] == "bob"
            && row["to"] == "zeus"
            && row["data"]["role"] == "contributor"
            && row["data"]["startedAt"] == "2024-02-02"
    }));
}

#[test]
fn additive_migration_with_edge_date_properties_succeeds_end_to_end() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_bug_db(&workspace);

    let load = workspace.json_value(&[
        "--json",
        "load",
        "bug.nano",
        "--data",
        "bug.jsonl",
        "--mode",
        "overwrite",
    ]);
    assert_eq!(load["status"], "ok");

    workspace.write_file("bug.nano/schema.pg", bug_schema_with_display_name());

    let plan = workspace.run_ok(&["migrate", "bug.nano", "--dry-run"]).stdout;
    assert!(plan.contains("AddProperty"));

    let apply = workspace.run_ok(&["migrate", "bug.nano", "--auto-approve"]).stdout;
    assert!(apply.contains("AddProperty") || apply.contains("Applied"));

    let doctor = workspace.run_ok(&["doctor", "bug.nano"]).stdout;
    assert!(doctor.contains("Doctor OK"));

    let exported = workspace
        .run_ok(&["export", "--db", "bug.nano", "--format", "jsonl"])
        .stdout;
    let rows = parse_jsonl_rows(&exported);
    assert!(rows.iter().any(|row| {
        row["edge"] == "WorksOn"
            && row["from"] == "alice"
            && row["to"] == "apollo"
            && row["data"]["role"] == "lead"
            && row["data"]["startedAt"] == "2024-02-01"
    }));
}

#[test]
fn prepared_journal_is_cleaned_when_database_is_intact() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    init_bug_db(&workspace);

    let load = workspace.json_value(&[
        "--json",
        "load",
        "bug.nano",
        "--data",
        "bug.jsonl",
        "--mode",
        "overwrite",
    ]);
    assert_eq!(load["status"], "ok");

    let journal_path = workspace.file("bug.nano.migration.journal.json");
    let staging_path = workspace.file("bug.nano.migration.staging.test");
    fs::create_dir_all(&staging_path).expect("create staging dir");
    fs::write(staging_path.join("marker.txt"), "staged").expect("write staging marker");
    let journal = serde_json::json!({
        "version": 1,
        "state": "PREPARED",
        "db_path": workspace.file("bug.nano").display().to_string(),
        "backup_path": workspace.file("bug.nano.migration.backup").display().to_string(),
        "staging_path": staging_path.display().to_string(),
        "old_schema_hash": "old",
        "new_schema_hash": "new",
        "created_at_unix": 0
    });
    fs::write(
        &journal_path,
        serde_json::to_string_pretty(&journal).expect("serialize journal"),
    )
    .expect("write journal");

    let doctor = workspace.run_ok(&["doctor", "bug.nano"]).stdout;
    assert!(doctor.contains("Doctor OK"));
    assert!(!journal_path.exists());
    assert!(!staging_path.exists());
}

#[test]
fn merge_mode_materializes_missing_embeddings_end_to_end() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    write_embed_merge_fixture(&workspace);

    let init = workspace.json_value(&["--json", "init", "embed.nano", "--schema", "embed.pg"]);
    assert_eq!(init["status"], "ok");

    let overwrite = workspace.json_value(&[
        "--json",
        "load",
        "embed.nano",
        "--data",
        "embed-initial.jsonl",
        "--mode",
        "overwrite",
    ]);
    assert_eq!(overwrite["status"], "ok");

    let merge = workspace.json_value(&[
        "--json",
        "load",
        "embed.nano",
        "--data",
        "embed-merge.jsonl",
        "--mode",
        "merge",
    ]);
    assert_eq!(merge["status"], "ok");

    let rows = workspace.json_rows(&[
        "--json",
        "run",
        "--db",
        "embed.nano",
        "--query",
        "embed.gq",
        "--name",
        "all_chunks",
    ]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["slug"], "abc123");
    assert_eq!(rows[1]["slug"], "def456");
    assert_eq!(rows[0]["embedding"].as_array().map(Vec::len), Some(1536));
    assert_eq!(rows[1]["embedding"].as_array().map(Vec::len), Some(1536));
}
