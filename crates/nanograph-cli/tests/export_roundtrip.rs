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

fn parse_jsonl_rows(output: &str) -> Vec<serde_json::Value> {
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|err| {
                panic!("failed to parse export JSONL line: {}\n{}", err, line)
            })
        })
        .collect()
}

#[test]
fn export_roundtrip_preserves_keyed_edges_end_to_end() {
    let dir = tempfile::TempDir::new().unwrap();
    let schema_path = dir.path().join("schema.pg");
    let input_path = dir.path().join("input.jsonl");
    let export_path = dir.path().join("export.jsonl");
    let db_a_path = dir.path().join("graph-a.nano");
    let db_b_path = dir.path().join("graph-b.nano");

    write_file(
        &schema_path,
        r#"node ActionItem {
    slug: String @key
    title: String
}
node Person {
    id: String @key
    name: String
}
edge MadeBy: ActionItem -> Person {
    id: String
    note: String
}"#,
    );
    write_file(
        &input_path,
        r#"{"type":"ActionItem","data":{"slug":"dec-build-mcp","title":"Build MCP"}}
{"type":"Person","data":{"id":"usr_andrew","name":"Andrew"}}
{"edge":"MadeBy","from":"dec-build-mcp","to":"usr_andrew","data":{"id":"rel_madeby_1","note":"owner"}}"#,
    );

    run_nanograph(
        dir.path(),
        &[
            "init",
            db_a_path.to_str().unwrap(),
            "--schema",
            schema_path.to_str().unwrap(),
        ],
    );
    run_nanograph(
        dir.path(),
        &[
            "load",
            db_a_path.to_str().unwrap(),
            "--data",
            input_path.to_str().unwrap(),
            "--mode",
            "overwrite",
        ],
    );

    let exported = run_nanograph(
        dir.path(),
        &[
            "export",
            "--db",
            db_a_path.to_str().unwrap(),
            "--format",
            "jsonl",
        ],
    );
    let exported_rows = parse_jsonl_rows(&exported);
    assert!(exported_rows.iter().any(|row| {
        row["type"] == "ActionItem"
            && row["data"]["slug"] == "dec-build-mcp"
            && row["data"]["title"] == "Build MCP"
            && row.get("id").is_none()
    }));
    assert!(exported_rows.iter().any(|row| {
        row["type"] == "Person"
            && row["data"]["id"] == "usr_andrew"
            && row["data"]["name"] == "Andrew"
            && row.get("id").is_none()
    }));
    assert!(exported_rows.iter().any(|row| {
        row["edge"] == "MadeBy"
            && row["from"] == "dec-build-mcp"
            && row["to"] == "usr_andrew"
            && row["data"]["id"] == "rel_madeby_1"
            && row["data"]["note"] == "owner"
            && row.get("id").is_none()
            && row.get("src").is_none()
            && row.get("dst").is_none()
    }));

    write_file(&export_path, &exported);

    run_nanograph(
        dir.path(),
        &[
            "init",
            db_b_path.to_str().unwrap(),
            "--schema",
            schema_path.to_str().unwrap(),
        ],
    );
    run_nanograph(
        dir.path(),
        &[
            "load",
            db_b_path.to_str().unwrap(),
            "--data",
            export_path.to_str().unwrap(),
            "--mode",
            "overwrite",
        ],
    );

    let roundtrip_export = run_nanograph(
        dir.path(),
        &[
            "export",
            "--db",
            db_b_path.to_str().unwrap(),
            "--format",
            "jsonl",
        ],
    );
    let roundtrip_rows = parse_jsonl_rows(&roundtrip_export);
    assert!(roundtrip_rows.iter().any(|row| {
        row["type"] == "ActionItem"
            && row["data"]["slug"] == "dec-build-mcp"
            && row["data"]["title"] == "Build MCP"
            && row.get("id").is_none()
    }));
    assert!(roundtrip_rows.iter().any(|row| {
        row["type"] == "Person"
            && row["data"]["id"] == "usr_andrew"
            && row["data"]["name"] == "Andrew"
            && row.get("id").is_none()
    }));
    assert!(roundtrip_rows.iter().any(|row| {
        row["edge"] == "MadeBy"
            && row["from"] == "dec-build-mcp"
            && row["to"] == "usr_andrew"
            && row["data"]["id"] == "rel_madeby_1"
            && row["data"]["note"] == "owner"
            && row.get("id").is_none()
            && row.get("src").is_none()
            && row.get("dst").is_none()
    }));
}
