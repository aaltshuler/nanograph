mod common;

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use nanograph::store::database::{Database, LoadMode};

fn bench_load_overwrite(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let rows = common::load_overwrite_rows();
    let payload = common::build_people_rows(0, rows);
    let mut group = c.benchmark_group("load_overwrite_jsonl");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function(BenchmarkId::from_parameter(rows), |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(Database::open_in_memory(common::keyed_people_schema(true)))
                    .expect("open overwrite db")
            },
            |db| {
                runtime
                    .block_on(db.load_with_mode(&payload, LoadMode::Overwrite))
                    .expect("overwrite load");
                black_box(db.path().to_path_buf());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_load_append(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let rows = common::load_merge_rows();
    let initial = common::build_people_rows(0, rows);
    let incoming = common::build_people_rows(rows, rows);
    let mut group = c.benchmark_group("load_append_jsonl");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function(BenchmarkId::from_parameter(rows), |b| {
        b.iter_batched(
            || {
                let db = runtime
                    .block_on(Database::open_in_memory(common::keyed_people_schema(true)))
                    .expect("open append db");
                runtime
                    .block_on(db.load_with_mode(&initial, LoadMode::Overwrite))
                    .expect("seed append db");
                db
            },
            |db| {
                runtime
                    .block_on(db.load_with_mode(&incoming, LoadMode::Append))
                    .expect("append load");
                black_box(db.path().to_path_buf());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_load_merge(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let rows = common::load_merge_rows();
    let initial = common::build_people_rows(0, rows);
    let incoming = common::build_people_merge_payload(rows / 2, rows, rows / 2);
    let mut group = c.benchmark_group("load_merge_jsonl");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function(BenchmarkId::from_parameter(rows), |b| {
        b.iter_batched(
            || {
                let db = runtime
                    .block_on(Database::open_in_memory(common::keyed_people_schema(true)))
                    .expect("open merge db");
                runtime
                    .block_on(db.load_with_mode(&initial, LoadMode::Overwrite))
                    .expect("seed merge db");
                db
            },
            |db| {
                runtime
                    .block_on(db.load_with_mode(&incoming, LoadMode::Merge))
                    .expect("merge load");
                black_box(db.path().to_path_buf());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_load_merge_embed(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let rows = common::load_merge_embed_rows();
    let initial = common::build_chunk_rows(0, rows);
    let incoming = common::build_chunk_merge_payload(rows / 2, rows, rows / 2);
    let _mock = common::enable_mock_embeddings();

    let mut group = c.benchmark_group("load_merge_embed");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function(BenchmarkId::from_parameter(rows), |b| {
        b.iter_batched(
            || {
                let db = runtime
                    .block_on(Database::open_in_memory(common::embed_chunk_schema()))
                    .expect("open embed db");
                runtime
                    .block_on(db.load_with_mode(&initial, LoadMode::Overwrite))
                    .expect("seed embed db");
                db
            },
            |db| {
                runtime
                    .block_on(db.load_with_mode(&incoming, LoadMode::Merge))
                    .expect("merge embed load");
                black_box(db.path().to_path_buf());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = bench_load_overwrite, bench_load_append, bench_load_merge, bench_load_merge_embed
}
criterion_main!(benches);
