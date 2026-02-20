use std::path::Path;
use std::process::Command;

fn write_file(path: &Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

fn run_nanograph(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_nanograph"))
        .current_dir(cwd)
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
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
    return {
        $d.slug as slug,
        $d.title as title,
        nearest($d.embedding, $q) as score
    }
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
    assert!(rows[0]["score"].is_number());
    assert!(rows[1]["score"].is_number());
    let score0 = rows[0]["score"].as_f64().unwrap();
    let score1 = rows[1]["score"].as_f64().unwrap();
    assert!(score0 <= score1);
}

#[test]
fn semantic_search_string_query_does_not_panic_when_openai_unreachable() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("demo.nano");
    let schema_path = dir.path().join("schema.pg");
    let data_path = dir.path().join("data.jsonl");
    let query_path = dir.path().join("search.gq");

    write_file(
        &schema_path,
        r#"node Doc {
    slug: String @key
    embedding: Vector(8) @index
}"#,
    );
    write_file(
        &data_path,
        r#"{"type":"Doc","data":{"slug":"a","embedding":[1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]}}
{"type":"Doc","data":{"slug":"b","embedding":[0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0]}}"#,
    );
    write_file(
        &query_path,
        r#"query semantic($q: String) {
    match { $d: Doc }
    return { $d.slug as slug }
    order { nearest($d.embedding, $q) }
    limit 1
}"#,
    );

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

    let output = Command::new(env!("CARGO_BIN_EXE_nanograph"))
        .current_dir(dir.path())
        .env_remove("NANOGRAPH_EMBEDDINGS_MOCK")
        .env("OPENAI_API_KEY", "dummy-test-key")
        .env("OPENAI_BASE_URL", "http://127.0.0.1:1")
        .env("NANOGRAPH_EMBED_RETRY_ATTEMPTS", "1")
        .env("NANOGRAPH_EMBED_TIMEOUT_MS", "25")
        .args([
            "run",
            "--db",
            db_path.to_str().unwrap(),
            "--query",
            query_path.to_str().unwrap(),
            "--name",
            "semantic",
            "--param",
            "q=who turned evil",
        ])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("The application panicked"),
        "stderr should not report panic:\n{}",
        stderr
    );
    assert!(
        !stderr.contains("Cannot drop a runtime in a context where blocking is not allowed"),
        "stderr should not contain tokio runtime drop panic:\n{}",
        stderr
    );
    assert!(
        stderr.contains("embedding request failed"),
        "stderr should contain normal embedding failure message:\n{}",
        stderr
    );
}
