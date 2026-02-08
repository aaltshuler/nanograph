use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use nanograph::catalog::build_catalog;
use nanograph::ir::lower::lower_query;
use nanograph::ir::ParamMap;
use nanograph::plan::planner::execute_query;
use nanograph::query::parser::parse_query;
use nanograph::query::typecheck::typecheck_query;
use nanograph::schema::parser::parse_schema;
use nanograph::store::graph::GraphStorage;

fn test_schema() -> &'static str {
    r#"
node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#
}

fn setup_storage() -> Arc<GraphStorage> {
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let mut storage = GraphStorage::new(catalog);

    // Insert people
    let person_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));
    let people = RecordBatch::try_new(
        person_schema,
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana"])),
            Arc::new(Int32Array::from(vec![Some(30), Some(25), Some(35), Some(28)])),
        ],
    )
    .unwrap();
    let person_ids = storage.insert_nodes("Person", people).unwrap();

    // Insert companies
    let company_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
    ]));
    let companies = RecordBatch::try_new(
        company_schema,
        vec![Arc::new(StringArray::from(vec!["Acme", "Globex"]))],
    )
    .unwrap();
    let company_ids = storage.insert_nodes("Company", companies).unwrap();

    // Edges: Alice->knows->Bob, Alice->knows->Charlie, Bob->knows->Diana
    storage
        .insert_edges(
            "Knows",
            &[person_ids[0], person_ids[0], person_ids[1]],
            &[person_ids[1], person_ids[2], person_ids[3]],
            None,
        )
        .unwrap();

    // Edges: Alice->worksAt->Acme, Bob->worksAt->Globex
    storage
        .insert_edges(
            "WorksAt",
            &[person_ids[0], person_ids[1]],
            &[company_ids[0], company_ids[1]],
            None,
        )
        .unwrap();

    storage.build_indices().unwrap();
    Arc::new(storage)
}

async fn run_query_test(query_str: &str, storage: Arc<GraphStorage>) -> Vec<RecordBatch> {
    run_query_test_with_params(query_str, storage, &ParamMap::new()).await
}

async fn run_query_test_with_params(
    query_str: &str,
    storage: Arc<GraphStorage>,
    params: &ParamMap,
) -> Vec<RecordBatch> {
    let catalog = &storage.catalog;
    let qf = parse_query(query_str).unwrap();
    let query = &qf.queries[0];
    let tc = typecheck_query(catalog, query).unwrap();
    let ir = lower_query(catalog, query, &tc).unwrap();
    execute_query(&ir, storage, params).await.unwrap()
}

fn extract_string_column(batches: &[RecordBatch], col_name: &str) -> Vec<String> {
    let mut result = Vec::new();
    for batch in batches {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(col_idx);
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                result.push(arr.value(i).to_string());
            }
        }
    }
    result
}

#[allow(dead_code)]
fn extract_i32_column(batches: &[RecordBatch], col_name: &str) -> Vec<Option<i32>> {
    let mut result = Vec::new();
    for batch in batches {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(col_idx);
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                result.push(None);
            } else {
                result.push(Some(arr.value(i)));
            }
        }
    }
    result
}

#[tokio::test]
async fn test_bind_by_property() {
    let storage = setup_storage();
    let mut params = ParamMap::new();
    params.insert(
        "name".to_string(),
        nanograph::query::ast::Literal::String("Alice".to_string()),
    );
    let results = run_query_test_with_params(
        r#"
query q($name: String) {
    match { $p: Person { name: $name } }
    return { $p.name, $p.age }
}
"#,
        storage,
        &params,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice"]);
}

#[tokio::test]
async fn test_filter_age() {
    let storage = setup_storage();
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p.age > 30
    }
    return { $p.name, $p.age }
}
"#,
        storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Charlie"]);
}

#[tokio::test]
async fn test_one_hop_traversal() {
    let storage = setup_storage();
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
    }
    return { $f.name }
}
"#,
        storage,
    )
    .await;

    let mut names = extract_string_column(&results, "name");
    names.sort();
    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[tokio::test]
async fn test_two_hop_traversal() {
    let storage = setup_storage();
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $mid
        $mid knows $fof
    }
    return { $fof.name }
}
"#,
        storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    // Alice->Bob->Diana, Alice->Charlie->nobody
    assert_eq!(names, vec!["Diana"]);
}

#[tokio::test]
async fn test_negation_unemployed() {
    let storage = setup_storage();
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#,
        storage,
    )
    .await;

    let mut names = extract_string_column(&results, "name");
    names.sort();
    // Charlie and Diana don't work anywhere
    assert_eq!(names, vec!["Charlie", "Diana"]);
}

#[tokio::test]
async fn test_aggregation_friend_counts() {
    let storage = setup_storage();
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return {
        $p.name
        count($f) as friends
    }
    order { friends desc }
}
"#,
        storage,
    )
    .await;

    assert!(!results.is_empty());
    let names = extract_string_column(&results, "name");
    // Alice has 2 friends, Bob has 1
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
}

#[tokio::test]
async fn test_order_and_limit() {
    let storage = setup_storage();
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
    }
    return { $p.name, $p.age }
    order { $p.age desc }
    limit 2
}
"#,
        storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names.len(), 2);
    assert_eq!(names[0], "Charlie"); // age 35
    assert_eq!(names[1], "Alice"); // age 30
}

#[tokio::test]
async fn test_type_error_unknown_type() {
    let storage = setup_storage();
    let catalog = &storage.catalog;
    let qf = parse_query(
        r#"
query q() {
    match { $p: Foo }
    return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(catalog, &qf.queries[0]).unwrap_err();
    assert!(err.to_string().contains("T1"));
}

#[tokio::test]
async fn test_type_error_bad_endpoints() {
    let storage = setup_storage();
    let catalog = &storage.catalog;
    let qf = parse_query(
        r#"
query q() {
    match {
        $c: Company
        $c knows $f
    }
    return { $c.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(catalog, &qf.queries[0]).unwrap_err();
    assert!(err.to_string().contains("T5"));
}

#[tokio::test]
async fn test_schema_typecheck_all_valid() {
    // Test that all valid queries from test.gq parse and typecheck
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();

    let query_src = std::fs::read_to_string("tests/fixtures/test.gq").unwrap();
    let qf = parse_query(&query_src).unwrap();

    for q in &qf.queries {
        let result = typecheck_query(&catalog, q);
        assert!(
            result.is_ok(),
            "query {} failed typecheck: {:?}",
            q.name,
            result.err()
        );
    }
}
