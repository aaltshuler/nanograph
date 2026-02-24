---
title: Architecture
slug: architecture
audience: dev
status: active
updated: 2026-02-12
---

# NanoGraph Architecture Review

System architecture overview and optimization targets.

## 1) System at a glance

NanoGraph is a local-first, embedded graph database with a schema-first workflow:

- Command surface: `crates/nanograph-cli/src/main.rs`
- Core engine: `crates/nanograph/src`
- Storage model: Arrow batches in memory + Lance datasets on disk
- Query model: parse -> typecheck -> IR lowering -> custom physical execution

## 2) Key components

| Component | Responsibility | Main files |
|---|---|---|
| CLI Orchestrator | Handles `init`, `load`, `check`, `run`, `migrate`, `delete` flows | `crates/nanograph-cli/src/main.rs` |
| Schema Frontend | Parses schema DSL into AST | `schema/parser.rs`, `schema/ast.rs`, `schema/schema.pest` |
| Catalog + Schema IR | Builds runtime catalog and stable hashed type/property IDs | `catalog/mod.rs`, `catalog/schema_ir.rs` |
| Query Frontend | Parses query DSL into AST | `query/parser.rs`, `query/ast.rs`, `query/query.pest` |
| Query Typechecker | Resolves bindings/traversals, validates type constraints (T1–T14) | `query/typecheck.rs` |
| IR Lowering | Converts typed AST to `QueryIR` ops (`NodeScan`, `Expand`, `Filter`, `AntiJoin`) or `MutationIR` ops (`Insert`, `Update`, `Delete`) | `ir/lower.rs`, `ir/mod.rs` |
| Physical Planner + Runtime | Builds DataFusion `ExecutionPlan` tree with custom operators; executes mutations | `plan/planner.rs`, `plan/node_scan.rs`, `plan/physical.rs` |
| In-memory Graph Store | Owns node/edge segments, IDs, and CSR/CSC indices | `store/graph.rs`, `store/csr.rs` |
| Loader | JSONL parsing, load modes (overwrite/append/merge), `@key`/`@unique` enforcement | `store/loader.rs`, `store/loader/jsonl.rs`, `store/loader/constraints.rs`, `store/loader/merge.rs` |
| Persistence + Metadata | Writes/reads schema IR, manifest, and Lance datasets; delete API | `store/database.rs`, `store/manifest.rs` |
| Indexing | Lance scalar (B-tree) index lifecycle for `@index`/`@key`/`@unique` properties | `store/indexing.rs` |
| Schema Migration | Diff-based schema evolution with safety levels and `@rename_from` tracking | `store/migration.rs` |

All source paths relative to `crates/nanograph/src/`.

## 3) Command and data lifecycle

```text
                                 +------------------+
                                 |   User / CLI     |
                                 +---------+--------+
                                           |
   +------------+------------+-------------+-------------+------------+
   |            |            |             |             |            |
  init        load        run/check     migrate       delete
   |            |            |             |             |
   v            v            v             v             v
read .pg    DB::open()   parse .gq     DB::open()    DB::open()
parse       load_jsonl   typecheck     diff old/new  delete_nodes()
build IR    (mode:        lower/exec    schema IR     cascade edges
write .pg    overwrite    or exec       plan steps    compact Lance
write IR     append       mutation      apply safe
write mfst   merge)                     confirm/block
```

## 4) Query pipeline (logical -> physical)

```text
Query text (.gq)
    |
    v
parse_query()
    |
    v
typecheck_query()  (T1–T9 for reads, T10–T14 for mutations)
  - resolves variable bindings/types
  - resolves traversal direction
  - validates filter compatibility
    |
    +------ read query ------+------ mutation query ------+
    |                                                      |
    v                                                      v
lower_query() -> QueryIR                    lower_mutation_query() -> MutationIR
  pipeline: [NodeScan|Expand|Filter|AntiJoin]  op: Insert | Update | Delete
  return_exprs/order/limit                     assignments + predicates
    |                                                      |
    v                                                      v
build_physical_plan()                         execute_mutation()
    |                                           - insert: append to Lance
    +--> NodeScanExec  (scan + Lance pushdown)  - update: merge via @key
    +--> CrossJoinExec (combine bindings)        - delete: cascade edges
    +--> ExpandExec    (CSR/CSC expansion)              |
    +--> AntiJoinExec  (negation)                       v
    |                                         MutationExecResult
    v                                           (rows affected)
execute_query()
  - fast path: stream when no agg/order/limit
  - otherwise: collect, filter/project/agg/order/limit
    |
    v
Arrow RecordBatch result(s)
```

## 5) Storage architecture

```text
On disk (db directory)                     In memory (GraphStorage)
----------------------                     ------------------------
schema.pg                                  catalog
schema.ir.json                             node_segments[type]:
graph.manifest.json                          - schema
nodes/                                        - batches[]
  <type_id_hex>/                              - id_to_row map
edges/                                      edge_segments[type]:
  <type_id_hex>/                              - src_ids[], dst_ids[], edge_ids[]
                                              - edge property batches[]
                                              - csr (outgoing), csc (incoming)
```

Manifest is the authoritative dataset inventory. `Database::open()` validates the schema hash, restores the listed datasets, then rebuilds CSR/CSC indices.

## 6) Architectural strengths

- Clear separation of frontend (parse/typecheck/lower) and backend (plan/execute/store).
- Deterministic schema IDs and manifest-gated dataset loading.
- Strong typing checks before execution (query safety).
- Graph-native expansion with CSR/CSC gives good adjacency traversal behavior.

## 7) Optimization hotspots

### P1: Cross joins can explode intermediate cardinality

- Location: `plan/planner.rs` (`CrossJoinExec`, `cross_join_batches`).
- Issue: independent bindings are combined with full cartesian replication (`O(left * right)` rows).
- Impact: memory/time blowups on multi-binding queries, especially before filters narrow rows.
- Partial mitigation: `NodeScanExec` now pushes filters into Lance scans, reducing input cardinality.
- Optimize:
  - Reorder bindings by estimated selectivity.
  - Introduce keyed joins when clauses imply equality constraints.

### P1: Global collect path for agg/order/limit is memory-heavy

- Location: `execute_query`, `apply_aggregation`, `apply_order_and_limit` in `plan/planner.rs`.
- Issue: non-fast-path queries collect all batches and often concatenate them.
- Impact: high peak RAM and latency spikes on large results.
- Optimize:
  - Push projection/filter/sort/limit/aggregation into DataFusion physical operators.
  - Keep streaming for top-k and partial aggregates where possible.

### P1: Expand currently buffers full child stream

- Location: `plan/physical.rs` (`ExpandExec::execute`).
- Issue: collects all input batches before expansion instead of processing incrementally.
- Impact: weak backpressure behavior and higher memory use.
- Optimize:
  - Convert to streaming transform (expand one input batch -> emit one output batch).
  - Keep per-batch state only, avoid whole-input materialization.

### P2: Repeated destination lookup map build in expand

- Location: `ExpandExec::expand_batch` in `plan/physical.rs`.
- Issue: builds `dst_id_to_row` hash map from concatenated destination nodes.
- Impact: repeated CPU/hash allocation for repeated expands on same type.
- Optimize:
  - Cache per-type `id -> row` projection for concatenated view.
  - Or avoid concat path by reusing `id_to_row` with direct batch/row fetches.

### P2: Snapshot clones entire graph storage for each run

- Location: `Database::snapshot()` in `store/database.rs`.
- Issue: `Arc::new(self.storage.clone())` duplicates storage structures.
- Impact: extra memory and clone cost on repeated query execution.
- Optimize:
  - Make storage segments immutable behind `Arc`.
  - Snapshot by sharing references, not deep cloning.

### P2: Database open eagerly loads all manifest datasets

- Location: `Database::open()` in `store/database.rs`.
- Issue: every open reads all listed node/edge datasets and rebuilds indices.
- Impact: startup cost scales with full dataset size.
- Optimize:
  - Lazy-load per type on first touch.
  - Persist/load precomputed index metadata when feasible.

## 8) Suggested optimization sequence

1. Make `ExpandExec` and global post-processing paths streaming-first.
2. Reduce cardinality early: filter pushdown + smarter binding/join ordering.
3. Remove deep-clone snapshots by moving graph storage to shared immutable segments.
4. Add lazy dataset loading and index reuse to reduce open-time costs.

