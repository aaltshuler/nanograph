---
audience: dev
status: active
updated: 2026-02-14
---

# NanoGraph — Database Features

Current version: `v0.6.0`

---

## Feature Status

### Schema & Types

| Feature | Status | Notes |
|---------|--------|-------|
| Typed schema (`.pg`) | Done | Parsed + lowered to `SchemaIR`/`Catalog` |
| `@key` annotation | Done | Single per node type, stable IDs across loads, auto-indexed |
| `@unique` annotation | Done | Enforced on load/upsert; nullable unique allows multiple nulls |
| `@index` annotation | Done | Lance scalar (B-tree) index on annotated properties |
| `@rename_from` annotation | Done | Tracks type/property renames for migration |
| Schema migration | Done | `nanograph migrate` with safety levels (safe/confirm/blocked), journal + backup |
| Pretty parse errors (Ariadne) | Done | Source-span diagnostics in CLI |

### Data Loading

| Feature | Status | Notes |
|---------|--------|-------|
| JSONL loading | Done | Via `nanograph load` |
| Load modes | Done | `--mode overwrite\|append\|merge` — explicit, no implicit defaults |
| Keyed upsert (`@key` merge) | Done | Stable ID preservation, edge endpoint remap, edge dedup by `(src,dst)` |
| Duplicate key detection | Done | Rejects duplicate `@key` values within and across batches |
| Null key rejection | Done | `@key` properties must be non-null |
| `@unique` enforcement on load | Done | Checked in `loader/constraints.rs` |

### Persistence

| Feature | Status | Notes |
|---------|--------|-------|
| Lance + Arrow storage | Done | Per-type Lance datasets + manifest |
| Secondary indexes | Done | `indexing.rs` — builds Lance scalar indexes for `@key`/`@unique`/`@index` properties |
| Delete API + edge cascade | Done | `nanograph delete --type --where` with incident edge cascade |

### Query — Read Path

| Feature | Status | Notes |
|---------|--------|-------|
| Read queries (`.gq`) | Done | Parse -> typecheck -> IR -> physical plan -> execute |
| Compile-time typechecking | Done | T1–T9 for reads, T15 for traversal bounds |
| Edge traversal (single hop) | Done | `$a edge $b` — direction inferred from schema |
| Bounded expansion | Done | `knows{1,3}` — BFS-based multi-hop with min/max bounds |
| Negation (`not { ... }`) | Done | Anti-join lowering/execution |
| Aggregation | Done | `count`, `sum`, `avg`, `min`, `max` |
| Query parameters (`--param`) | Done | CLI `--param key=value` + runtime binding |
| Output formats | Done | `--format table\|csv\|jsonl` |
| Filter pushdown (DataFusion) | Done | `NodeScanExec` pushes supported predicates into Lance scans |
| Projection pushdown | Done | Only requested columns scanned from Lance |
| Limit pushdown | Done | Pushed to Lance when all filters are pushdown-eligible |

### Query — Mutation Path

| Feature | Status | Notes |
|---------|--------|-------|
| `insert` mutations | Done | Append mode, typechecked (T10) |
| `update` mutations | Done | Merge via `@key`, typechecked (T11–T12) |
| `delete` mutations | Done | Edge cascade, typechecked (T13–T14) |
| Mutation typechecking | Done | T10–T14 rules in `typecheck.rs` |

### CLI

| Feature | Status | Notes |
|---------|--------|-------|
| `nanograph init` | Done | Create DB from schema |
| `nanograph load` | Done | Load JSONL with explicit mode |
| `nanograph check` | Done | Validate query files against DB catalog |
| `nanograph run` | Done | Execute queries (read + mutation) in DB mode |
| `nanograph migrate` | Done | Schema evolution with `--dry-run`, `--auto-approve`, `--format` |
| `nanograph delete` | Done | Predicate-based node deletion with edge cascade |
| `nanograph cdc-materialize` | Done | Optional CDC analytics materializer (`__cdc_analytics`) with threshold/force controls; correctness path stays on JSONL |

---

## Remaining Work

### Storage & Performance

| Item | Priority | Notes |
|------|----------|-------|
| Lance-native merge path | Medium | Use `MergeInsertBuilder` for storage-level upsert instead of in-memory merge + overwrite |
| Cross-join cardinality control | Medium | Binding reorder by selectivity, keyed joins for equality constraints |
| Streaming ExpandExec | Medium | Currently buffers full input; convert to per-batch streaming |
| Snapshot without deep clone | Low | `Database::snapshot()` clones entire `GraphStorage`; move to `Arc`-shared segments |
| Lazy dataset loading | Low | `Database::open()` eagerly loads all datasets; lazy-load per type on first touch |

### Query Language

| Item | Priority | Notes |
|------|----------|-------|
| Variable-length paths | High | Unbounded traversal disabled at typecheck (T15); needs iterative fixed-point operator |
| Richer delete predicates | Low | CLI delete uses simple `prop = value`; could align with query filter grammar |

### Specialized Capabilities

| Item | Priority | Notes |
|------|----------|-------|
| Vector search | Future | Lance has native vector index support; strong differentiator once core is stable |
| Full-text search | Future | Candidate via inverted index path |
| Additional scalar types | Future | Timestamp with timezone, collections |
| Transaction semantics | Future | Migration is transactional; general write path lacks explicit transactions |
