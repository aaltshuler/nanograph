use std::path::Path;
use std::process::Command;

fn write_file(path: &Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

fn run_nanograph(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_nanograph"))
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    if !output.status.success() {
        panic!(
            "nanograph command failed\nargs: {:?}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            args,
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn semantic_search_uses_string_query_with_dotenv_loaded_embeddings() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("demo.nano");
    let schema_path = dir.path().join("schema.pg");
    let data_path = dir.path().join("data.jsonl");
    let query_path = dir.path().join("search.gq");
    let dotenv_path = dir.path().join(".env");

    write_file(
        &schema_path,
        r#"node Doc {
    slug: String @key
    title: String
    embedding: Vector(8) @embed(title) @index
}"#,
    );
    write_file(
        &data_path,
        r#"{"type":"Doc","data":{"slug":"space_doc","title":"galactic rebellion story"}}
{"type":"Doc","data":{"slug":"finance_doc","title":"quarterly revenue planning"}}
{"type":"Doc","data":{"slug":"baking_doc","title":"sourdough starter guide"}}"#,
    );
    write_file(
        &query_path,
        r#"query semantic($q: String) {
    match { $d: Doc }
    return { $d.slug as slug, $d.title as title }
    order { nearest($d.embedding, $q) }
    limit 2
}"#,
    );
    write_file(&dotenv_path, "NANOGRAPH_EMBEDDINGS_MOCK=1\n");

    run_nanograph(
        dir.path(),
        &[
            "init",
            db_path.to_str().unwrap(),
            "--schema",
            schema_path.to_str().unwrap(),
        ],
    );
    run_nanograph(
        dir.path(),
        &[
            "load",
            db_path.to_str().unwrap(),
            "--data",
            data_path.to_str().unwrap(),
            "--mode",
            "overwrite",
        ],
    );
    let out = run_nanograph(
        dir.path(),
        &[
            "run",
            "--db",
            db_path.to_str().unwrap(),
            "--query",
            query_path.to_str().unwrap(),
            "--name",
            "semantic",
            "--format",
            "json",
            "--param",
            "q=galactic rebellion story",
        ],
    );

    let json_start = out
        .find('[')
        .unwrap_or_else(|| panic!("run output did not contain JSON array start:\n{}", out));
    let json_tail = &out[json_start..];
    let json_end = json_tail
        .rfind(']')
        .map(|idx| idx + 1)
        .unwrap_or_else(|| panic!("run output did not contain JSON array end:\n{}", out));
    let rows: serde_json::Value = serde_json::from_str(&json_tail[..json_end])
        .unwrap_or_else(|e| panic!("failed to parse run output as JSON: {}\n{}", e, out));
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["slug"],
        serde_json::Value::String("space_doc".to_string())
    );
    assert_eq!(
        rows[0]["title"],
        serde_json::Value::String("galactic rebellion story".to_string())
    );
}
