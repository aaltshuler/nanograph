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

fn run_query_rows(
    cwd: &Path,
    db_path: &Path,
    query_path: &Path,
    query_name: &str,
    params: &[(&str, &str)],
) -> Vec<serde_json::Value> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nanograph"));
    cmd.current_dir(cwd)
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .arg("run")
        .arg("--db")
        .arg(db_path)
        .arg("--query")
        .arg(query_path)
        .arg("--name")
        .arg(query_name)
        .arg("--format")
        .arg("json");
    for (key, value) in params {
        cmd.arg("--param").arg(format!("{}={}", key, value));
    }
    let output = cmd.output().unwrap();
    if !output.status.success() {
        panic!(
            "query '{}' failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            query_name,
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    let out = String::from_utf8(output.stdout).unwrap();
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
    rows.as_array().cloned().unwrap_or_default()
}

#[test]
fn text_search_and_hybrid_rrf_work_end_to_end() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("demo.nano");
    let schema_path = dir.path().join("schema.pg");
    let data_path = dir.path().join("data.jsonl");
    let query_path = dir.path().join("search.gq");

    write_file(
        &schema_path,
        r#"node Signal {
    slug: String @key
    summary: String
    embedding: Vector(3) @index
}"#,
    );
    write_file(
        &data_path,
        r#"{"type":"Signal","data":{"slug":"sig-billing-delay","summary":"billing reconciliation delay due to missing invoice data","embedding":[1.0,0.0,0.0]}}
{"type":"Signal","data":{"slug":"sig-referral-analytics","summary":"warm referral for analytics migration project","embedding":[0.0,1.0,0.0]}}
{"type":"Signal","data":{"slug":"sig-procurement","summary":"enterprise procurement questionnaire backlog and mitigation owner tracking","embedding":[0.0,0.0,1.0]}}"#,
    );
    write_file(
        &query_path,
        r#"query keyword($q: String) {
    match {
        $s: Signal
        search($s.summary, $q)
    }
    return { $s.slug as slug }
    order { $s.slug asc }
}

query lexical($q: String) {
    match {
        $s: Signal
        match_text($s.summary, $q)
    }
    return { $s.slug as slug }
    order { $s.slug asc }
}

query fuzzy_q($q: String) {
    match {
        $s: Signal
        fuzzy($s.summary, $q)
    }
    return { $s.slug as slug }
    order { $s.slug asc }
}

query bm25_q($q: String) {
    match { $s: Signal }
    return { $s.slug as slug, bm25($s.summary, $q) as score }
    order { bm25($s.summary, $q) desc }
    limit 3
}

query hybrid_rrf($vq: Vector(3), $tq: String) {
    match { $s: Signal }
    return {
        $s.slug as slug,
        rrf(nearest($s.embedding, $vq), bm25($s.summary, $tq), 60) as score
    }
    order { rrf(nearest($s.embedding, $vq), bm25($s.summary, $tq), 60) desc }
    limit 3
}

query projection_exprs($q: String, $vq: Vector(3), $tq: String) {
    match { $s: Signal }
    return {
        $s.slug as slug,
        search($s.summary, $q) as kw,
        match_text($s.summary, "missing invoice") as mt,
        fuzzy($s.summary, "reconciliaton delay") as fz,
        bm25($s.summary, $tq) as bm,
        rrf(nearest($s.embedding, $vq), bm25($s.summary, $tq), 60) as rr
    }
    order { $s.slug asc }
    limit 3
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
    run_nanograph(
        dir.path(),
        &[
            "check",
            "--db",
            db_path.to_str().unwrap(),
            "--query",
            query_path.to_str().unwrap(),
        ],
    );

    let keyword = run_query_rows(
        dir.path(),
        &db_path,
        &query_path,
        "keyword",
        &[("q", "billing missing")],
    );
    assert_eq!(keyword[0]["slug"], "sig-billing-delay");

    let lexical = run_query_rows(
        dir.path(),
        &db_path,
        &query_path,
        "lexical",
        &[("q", "missing invoice")],
    );
    assert_eq!(lexical[0]["slug"], "sig-billing-delay");

    let fuzzy = run_query_rows(
        dir.path(),
        &db_path,
        &query_path,
        "fuzzy_q",
        &[("q", "reconciliaton delay")],
    );
    assert_eq!(fuzzy[0]["slug"], "sig-billing-delay");

    let bm25 = run_query_rows(
        dir.path(),
        &db_path,
        &query_path,
        "bm25_q",
        &[("q", "billing missing invoice")],
    );
    assert_eq!(bm25[0]["slug"], "sig-billing-delay");
    assert_eq!(bm25.len(), 3);

    let hybrid = run_query_rows(
        dir.path(),
        &db_path,
        &query_path,
        "hybrid_rrf",
        &[("vq", "[1.0, 0.0, 0.0]"), ("tq", "billing missing invoice")],
    );
    assert_eq!(hybrid[0]["slug"], "sig-billing-delay");
    assert_eq!(hybrid.len(), 3);

    let projection = run_query_rows(
        dir.path(),
        &db_path,
        &query_path,
        "projection_exprs",
        &[
            ("q", "billing missing"),
            ("vq", "[1.0, 0.0, 0.0]"),
            ("tq", "billing missing invoice"),
        ],
    );
    assert_eq!(projection.len(), 3);
    assert_eq!(projection[0]["slug"], "sig-billing-delay");
    assert_eq!(projection[0]["kw"], true);
    assert_eq!(projection[0]["mt"], true);
    assert_eq!(projection[0]["fz"], true);
    assert!(projection[0]["bm"].is_number());
    assert!(projection[0]["rr"].is_number());
    assert_eq!(projection[2]["kw"], false);
}
