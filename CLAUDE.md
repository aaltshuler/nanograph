# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is NanoGraph?

Embedded local-first typed property graph DB in Rust. Arrow-native columnar execution, Lance storage, DataFusion query engine. Think SQLite for graphs. Two custom DSLs: schema (.pg) and query (.gq), both parsed with Pest grammars.

## Build & Test Commands

```bash
cargo build                              # full workspace build
cargo build -p nanograph                 # library only
cargo build -p nanograph-cli             # CLI only
cargo test                               # all tests (unit + e2e + migration)
cargo test -p nanograph                  # library tests only
cargo test --test e2e                    # e2e integration tests only
cargo test --test schema_migration       # migration tests only
cargo test test_bind_by_property         # single test by name
cargo test -- --nocapture                # show stdout
cargo clippy                             # lint
cargo fmt                                # format
RUST_LOG=debug cargo run -p nanograph-cli -- run ...  # enable tracing
```

**Requires `protoc`** (Protocol Buffers compiler) at build time for the Lance dependency. Rust edition 2024; `debug = 0` in dev profile (no debuginfo — builds are faster but backtraces are address-only).

## Architecture

### Workspace

Two crates: `nanograph` (library) and `nanograph-cli` (binary named `nanograph`). The CLI depends on the library.

### Query Execution Pipeline

```
.gq text → parse_query() → QueryAST
         → typecheck_query() → TypeContext (validates against catalog)
         → lower_query() → QueryIR (pipeline of operators)
         → build_physical_plan() → DataFusion ExecutionPlan
         → execute_query() → Vec<RecordBatch>
```

### Module Map (`crates/nanograph/src/`)

| Module | Role |
|--------|------|
| `schema/` | `schema.pest` grammar + parser for `.pg` schema files → schema AST |
| `query/` | `query.pest` grammar + parser for `.gq` query files → query AST; `typecheck.rs` validates queries against catalog |
| `catalog/` | `schema_ir.rs` — compiled schema representation used at runtime |
| `ir/` | `lower.rs` — lowers typed query AST into flat IR (NodeScan, Expand, Filter, AntiJoin operators) |
| `plan/` | `planner.rs` — converts IR to DataFusion physical plans; `node_scan.rs` — custom NodeScanExec; `physical.rs` — ExpandExec, CrossJoinExec, AntiJoinExec |
| `store/` | `graph.rs` — in-memory GraphStorage with CSR/CSC indices; `database.rs` — Lance-backed persistence; `loader.rs` — JSONL data loading; `manifest.rs` — dataset inventory; `migration.rs` — schema evolution |
| `types.rs` | Core type definitions, Arrow type mappings |
| `error.rs` | `NanoError` error type; `ariadne` crate used for pretty source-span diagnostics |

### Key Design Details

- **Variables are Arrow Struct columns**: `$p: Person` becomes `Struct<id: U64, name: Utf8, age: Int32?>`. Property access is struct field access.
- **Edge traversal is a Datalog predicate**: `$p knows $f` — no arrows, no Cypher syntax. Direction inferred from schema endpoint types.
- **Custom ExecutionPlans**: NodeScanExec, ExpandExec (CSR/CSC traversal), CrossJoinExec, AntiJoinExec. Stock DataFusion operators used for filter, sort, limit, aggregation.
- **Reverse traversal**: When source is unbound and destination is bound, the planner swaps direction and uses CSC instead of CSR.
- **Negation**: `not {}` compiles to AntiJoinExec. The inner pipeline must be seeded with the outer plan's input.
- **Bounded expansion**: `knows{1,3}` compiles to a finite union of 1-hop, 2-hop, 3-hop — no recursion.

### Persistence Layout

```
<name>.nanograph/
├── schema.pg              # source schema
├── schema.ir.json         # compiled schema IR
├── graph.manifest.json    # dataset inventory
├── nodes/<type_id_hex>/   # Lance dataset per node type
└── edges/<type_id_hex>/   # Lance dataset per edge type
```

Type IDs are FNV-1a hashes of `"node:TypeName"` / `"edge:TypeName"` → u32 hex.

### CLI Commands

`nanograph init` / `load` / `migrate` / `check` / `run`. Supports both DB mode (`--db path.nanograph`) and legacy mode (`--schema`/`--data` flags for in-memory operation).

- `run` supports `--format table|csv|jsonl` and `--param key=value` for parameterized queries.
- `migrate` supports `--dry-run`, `--auto-approve`, and `--format table|json`.

### Schema Migration

Schema evolution via `nanograph migrate`. Edit `<db>/schema.pg` then run migrate. Uses `@rename_from("old_name")` annotations on types/properties to track renames. Migration steps have safety levels: `safe` (auto-apply), `confirm` (needs `--auto-approve` or interactive confirmation), `blocked` (e.g. adding non-nullable property to populated type).

## Version Constraints

Arrow 57, DataFusion 52, Lance 2.0 — these must stay compatible with each other. Pest 2 for both grammars.

## Design Documents

- `grammar.ebnf` — formal grammar for both DSLs, includes 12 type rules (T1-T12)
- `docs/specs.md` — product spec (v0)
- `docs/datafusion_mapping.md` — how each query construct maps to DataFusion execution

## Test Fixtures

Test schemas, queries, and data live in `crates/nanograph/tests/fixtures/` (test.pg, test.gq, test.jsonl). Star Wars example in `examples/starwars/`. Migration tests in `tests/schema_migration.rs`.

## Known Pitfalls

- `literal_to_array` must cast to match LHS column type, not default to Int64.
- ExpandExec id→row mapping must be rebuilt from the concatenated batch, not reuse segment.id_to_row.
- AntiJoin inner pipeline needs outer input seeded via `build_physical_plan_with_input`.
- When a query variable isn't in the batch schema, skip the filter rather than erroring.
