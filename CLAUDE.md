# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is nanograph?

Embedded local-first typed property graph DB in Rust. Arrow-native columnar execution, Lance storage, DataFusion query engine. Think SQLite for graphs. Two custom DSLs: schema (.pg) and query (.gq), both parsed with Pest grammars.

## Build & Test Commands

```bash
cargo build                              # full workspace build
cargo build -p nanograph                 # library only
cargo build -p nanograph-cli             # CLI only
cargo test                               # all tests (unit + e2e + migration)
cargo test -p nanograph                  # library tests only
cargo test -p nanograph --test e2e       # e2e integration tests only
cargo test -p nanograph --test schema_migration  # migration tests only
cargo test test_bind_by_property         # single test by name
cargo test -- --nocapture                # show stdout
bash tests/cli/run-cli-e2e.sh            # all CLI shell scenarios
bash tests/cli/run-cli-e2e.sh lifecycle  # single CLI scenario by name
cargo clippy                             # lint
cargo fmt                                # format
RUST_LOG=debug cargo run -p nanograph-cli -- run ...  # enable tracing
```

**Requires `protoc`** (Protocol Buffers compiler) at build time for the Lance dependency. Rust edition 2024; `debug = 0` in dev profile (no debuginfo — builds are faster but backtraces are address-only). Dependencies are compiled with `opt-level = 2` even in dev profile so tests run at reasonable speed while the nanograph crate itself stays unoptimized for fast rebuilds.

## Architecture

### Workspace

Two crates: `nanograph` (library) and `nanograph-cli` (binary named `nanograph`). The CLI depends on the library. All domain logic lives in the library; the CLI is a thin clap wrapper that calls library functions.

### Dual-Mode Execution

The system supports two execution modes that affect many code paths:
- **DB mode** (`--db path.nanograph`): Lance-backed persistence, supports mutations, CDC, migration, maintenance commands.
- **Legacy mode** (`--schema`/`--data` flags): In-memory GraphStorage, read-only queries. Useful for quick checks without a DB.

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
| `schema/` | `schema.pest` grammar + parser → schema AST |
| `query/` | `query.pest` grammar + parser → query AST; `typecheck.rs` validates against catalog |
| `catalog/` | `schema_ir.rs` — compiled schema representation used at runtime |
| `ir/` | `lower.rs` — lowers typed AST into flat IR operators (NodeScan, Expand, Filter, AntiJoin, mutations) |
| `plan/planner.rs` | Converts IR to DataFusion physical plans |
| `plan/node_scan.rs` | Custom NodeScanExec with Lance filter pushdown |
| `plan/physical.rs` | Custom ExpandExec, CrossJoinExec, AntiJoinExec, mutation execution |
| `store/database.rs` | Lance-backed persistence, delete API, load modes, compact/cleanup/doctor |
| `store/graph.rs` | In-memory GraphStorage with CSR/CSC indices |
| `store/csr.rs` | CSR/CSC adjacency structure — core graph index for traversal |
| `store/loader/` | Load orchestration: `jsonl.rs` (parsing + Arrow builders), `constraints.rs`, `merge.rs` |
| `store/indexing.rs` | Lance scalar index lifecycle |
| `store/migration.rs` | Schema evolution engine |
| `store/manifest.rs` | Dataset inventory (`graph.manifest.json`) — tracks which node/edge types have Lance datasets |
| `store/txlog.rs` | Transaction catalog + CDC log (`_tx_catalog.jsonl`, `_cdc_log.jsonl`) |
| `types.rs` | Core type definitions, `PropType`, Arrow type mappings |
| `error.rs` | `NanoError` error type |

### Error Handling

All library errors go through `NanoError` (in `error.rs`). Source-span diagnostics use the `ariadne` crate for pretty error rendering with source locations (used in schema/query parse errors and type errors).

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
├── _tx_catalog.jsonl      # transaction log
├── _cdc_log.jsonl         # CDC event log
├── nodes/<type_id_hex>/   # Lance dataset per node type
└── edges/<type_id_hex>/   # Lance dataset per edge type
```

Type IDs are FNV-1a hashes of `"node:TypeName"` / `"edge:TypeName"` → u32 hex.

### CLI Commands

All commands support `--json` global flag for structured output. Core: `init`, `load` (requires `--mode overwrite|append|merge`), `check`, `run` (`--format table|csv|jsonl|json`, `--param key=value`), `delete`, `migrate`. Inspection: `version`, `describe`, `export`. Maintenance: `compact`, `cleanup`, `doctor`, `cdc-materialize`, `changes`. Run `nanograph <command> --help` for full flag details.

### Type System

Scalar types: `String`, `I32`, `I64`, `U64`, `F32`, `F64`, `Bool`, `Date`, `DateTime`. Enum types: `enum(val1, val2, ...)`. List types: `[String]`, `[I32]`, etc. All property types are nullable by appending `?`. Query literals include `date("2026-01-15")`, `datetime("2026-01-15T10:00:00Z")`, and list literals `[1, 2, 3]`.

### Schema Annotations

- `@key` — single property per node type, used for keyed merge. Auto-indexed.
- `@unique` — enforced on load/upsert. Nullable unique allows multiple nulls.
- `@index` — Lance scalar (B-tree) index. Enables index-backed filtering.
- `@rename_from("old")` — tracks type/property renames for migration.

List properties cannot have `@key`, `@unique`, or `@index`.

### Schema Migration

Edit `<db>/schema.pg` then `nanograph migrate`. Uses `@rename_from("old_name")` for renames. Safety levels: `safe` (auto-apply), `confirm` (needs `--auto-approve`), `blocked` (e.g. adding non-nullable property to populated type).

### Query Mutations

```
insert Person { name: $name, age: $age }
update Person set { age: $age } where name = $name
delete Person where name = $name
```

`insert` = append. `update` requires `@key`, uses merge. `delete` cascades edges. Typechecked at compile time (T10-T14).

## Common Change Patterns

**Adding a new scalar type**: `types.rs` (PropType + Arrow mapping) → `schema.pest` + `schema/parser.rs` → `query.pest` + `query/parser.rs` (literal syntax) → `query/typecheck.rs` → `store/loader/jsonl.rs` (Arrow builder) → `store/database.rs` (predicate handling if needed).

**Adding a new IR operator**: `ir/lower.rs` (emit new op) → `plan/planner.rs` (convert to ExecutionPlan) → `plan/physical.rs` (implement ExecutionPlan trait).

**Adding a new CLI command**: `crates/nanograph-cli/src/main.rs` (clap subcommand + handler). Library logic goes in `crates/nanograph/src/`.

**Modifying Pest grammars**: Edit `.pest` file → update corresponding `parser.rs` → update `typecheck.rs` if the change affects type rules → update `grammar.ebnf` to keep it in sync.

## Version Constraints

Arrow 57, DataFusion 52, Lance 2.0 + lance-index 2.0 — these must stay compatible with each other. Pest 2 for both grammars.

## Design Documents

- `grammar.ebnf` — formal grammar for both DSLs, includes type rules (T1-T14; T10-T14 cover mutations)
- `docs/dev/architecture.md` — system overview, module map, data flow
- `docs/dev/db-features.md` — feature roadmap with implementation status
- `docs/dev/test-framework.md` — test tiers, commands, fixture policy, CI guidance
- `docs/dev/backlog.md` — current backlog and priorities

Source of truth for behavior is code. Update docs in the same PR when behavior changes.

## Test Fixtures

Test schemas, queries, and data live in `crates/nanograph/tests/fixtures/` (test.pg, test.gq, test.jsonl). Star Wars example in `examples/starwars/`. Migration tests in `crates/nanograph/tests/schema_migration.rs`. Index performance harness in `crates/nanograph/tests/index_perf.rs` (run with `--ignored`). Write amplification harness in `crates/nanograph/tests/write_amp_perf.rs` (run with `--ignored`).

CLI scenario scripts live in `tests/cli/scenarios/` and use shared helpers from `tests/cli/lib/common.sh` (build helpers, assertion macros, query runners). Scenarios: `lifecycle`, `migration`, `query_mutations`, `maintenance`, `revops_typed_cdc`. Run all via `bash tests/cli/run-cli-e2e.sh` or one via `bash tests/cli/run-cli-e2e.sh <scenario>`.

## Known Pitfalls

- `literal_to_array` must cast to match LHS column type, not default to Int64.
- ExpandExec id→row mapping must be rebuilt from the concatenated batch, not reuse segment.id_to_row.
- AntiJoin inner pipeline needs outer input seeded via `build_physical_plan_with_input`.
- When a query variable isn't in the batch schema, skip the filter rather than erroring.
- Enum values are auto-sorted and deduplicated; duplicate values are rejected at parse time.
- List properties cannot have `@key`, `@unique`, or `@index` annotations.
