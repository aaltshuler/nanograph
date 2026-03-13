mod common;

use std::fs;

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nanograph::store::database::{CleanupOptions, CompactOptions, Database, LoadMode};
use nanograph::store::migration::execute_schema_migration;
use tempfile::TempDir;

const BASE_SCHEMA: &str = r#"
node Person {
    slug: String @key
    name: String
    age: I32?
}

edge Knows: Person -> Person
"#;

const TARGET_SCHEMA: &str = r#"
node Person {
    slug: String @key
    name: String
    age: I32?
    email: String? @index
}

edge Knows: Person -> Person
"#;

const MIGRATION_SEED_DATA: &str = r#"
{"type":"Person","data":{"slug":"alice","name":"Alice","age":30}}
{"type":"Person","data":{"slug":"bob","name":"Bob","age":35}}
{"edge":"knows","from":"alice","to":"bob"}
"#;

fn bench_migration_additive_apply(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let mut group = c.benchmark_group("migration_additive_apply");
    group.bench_function(BenchmarkId::from_parameter("small"), |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().expect("tempdir");
                let db_path = dir.path().join("db");
                runtime
                    .block_on(Database::init(&db_path, BASE_SCHEMA))
                    .expect("init migration db");
                let db = runtime
                    .block_on(Database::open(&db_path))
                    .expect("open migration db");
                runtime
                    .block_on(db.load_with_mode(MIGRATION_SEED_DATA, LoadMode::Overwrite))
                    .expect("seed migration db");
                fs::write(db_path.join("schema.pg"), TARGET_SCHEMA).expect("write target schema");
                (dir, db_path)
            },
            |(_dir, db_path)| {
                let execution = runtime
                    .block_on(execute_schema_migration(&db_path, false, false))
                    .expect("apply migration");
                black_box(execution.status);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_compact_cleanup(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let rows_per_batch = if common::smoke_mode() { 1_000 } else { 5_000 };
    let batch_count = if common::smoke_mode() { 3 } else { 5 };
    let mut payloads = Vec::with_capacity(batch_count);
    for i in 0..batch_count {
        payloads.push(common::build_people_rows(
            i * rows_per_batch,
            rows_per_batch,
        ));
    }

    let mut group = c.benchmark_group("compact_cleanup");
    group.bench_function(
        BenchmarkId::from_parameter(batch_count * rows_per_batch),
        |b| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().expect("tempdir");
                    let db_path = dir.path().join("db");
                    runtime
                        .block_on(Database::init(&db_path, common::keyed_people_schema(true)))
                        .expect("init maintenance db");
                    let db = runtime
                        .block_on(Database::open(&db_path))
                        .expect("open maintenance db");
                    for (idx, payload) in payloads.iter().enumerate() {
                        let mode = if idx == 0 {
                            LoadMode::Overwrite
                        } else {
                            LoadMode::Append
                        };
                        runtime
                            .block_on(db.load_with_mode(payload, mode))
                            .expect("seed maintenance db");
                    }
                    (dir, db)
                },
                |(_dir, db)| {
                    let compact = runtime
                        .block_on(db.compact(CompactOptions::default()))
                        .expect("compact");
                    let cleanup = runtime
                        .block_on(db.cleanup(CleanupOptions::default()))
                        .expect("cleanup");
                    black_box((compact.datasets_compacted, cleanup.datasets_cleaned));
                },
                BatchSize::SmallInput,
            );
        },
    );
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = bench_migration_additive_apply, bench_compact_cleanup
}
criterion_main!(benches);
