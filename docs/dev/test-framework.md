---
audience: dev
status: active
updated: 2026-02-15
---

# NanoGraph Test Framework

Status: active (updated 2026-02-15)

## Overview

The repository uses a tiered test framework:

- Rust unit/integration tests stay crate-local for normal Cargo discovery.
- Root-level shell scenarios provide black-box CLI end-to-end coverage.
- A single runner script executes all CLI scenarios consistently.

## Layout

```text
crates/nanograph/src/**              # unit tests
crates/nanograph/tests/*.rs          # integration/e2e/migration/perf
crates/nanograph-cli/src/main.rs     # CLI unit tests

tests/cli/lib/common.sh              # shared shell helpers
tests/cli/scenarios/*.sh             # CLI black-box scenarios
tests/cli/run-cli-e2e.sh             # scenario runner

crates/nanograph/tests/fixtures/*    # canonical fixtures
tests/fixtures/README.md             # fixture policy only (no duplicate data)
```

## Test Tiers

- Tier 0: unit tests (`crates/*/src/**`)
- Tier 1: Rust integration/e2e (`crates/nanograph/tests/*.rs`)
- Tier 2: CLI black-box scenarios (`tests/cli/scenarios/*.sh`)
- Tier 3: performance harness (`crates/nanograph/tests/index_perf.rs`, ignored/manual)

## Standard Commands

Fast local checks:

```bash
cargo test -p nanograph --lib
cargo test -p nanograph --test e2e
```

Note: `cargo test -p nanograph --lib` is a unit-only fast pass; it excludes integration tests and doctests.

Full engine checks:

```bash
cargo test -p nanograph
cargo test -p nanograph-cli
```

Targeted migration checks:

```bash
cargo test -p nanograph --test schema_migration
```

Note: `schema_migration` is also included in `cargo test -p nanograph`.

CLI scenario checks:

```bash
bash tests/cli/run-cli-e2e.sh --list
bash tests/cli/run-cli-e2e.sh
bash tests/cli/run-cli-e2e.sh migration
```

Recommended comprehensive pass before release:

```bash
cargo test -p nanograph
cargo test -p nanograph-cli
bash tests/cli/run-cli-e2e.sh
```

## Scenario Framework Rules

- Put reusable logic in `tests/cli/lib/common.sh`.
- Keep scenario scripts focused on domain assertions only.
- Use explicit load mode flags (`overwrite|append|merge`) in all CLI tests.
- Use `--format jsonl` for stable script parsing when validating row content.
- For feature E2E, anchor tests to canonical example schemas (`examples/starwars`, `examples/revops`) instead of adding ad-hoc test-only schemas.

## CLI Scenario Ownership

| Scenario | Fixture schema(s) | Primary coverage |
|----------|-------------------|------------------|
| `lifecycle.sh` | `examples/starwars/starwars.pg` | Merge upsert behavior, edge dedup, delete cascade, reopen/readback consistency |
| `migration.sh` | `examples/starwars/starwars.pg` | Schema migration plan/apply flow, rename/add/drop semantics, post-migration query parity |
| `query_mutations.sh` | `examples/starwars/starwars.pg` | Query mutation syntax (insert/update/delete for nodes/edges) and CLI `--json` response contract |
| `revops_typed_cdc.sh` | `examples/revops/revops.pg` | Typed fields (enum/list/DateTime/Bool/F64), mutation events, CDC range/since windows, version/describe/export/doctor |
| `maintenance.sh` | `examples/revops/revops.pg` | Compact/cleanup/cdc-materialize maintenance commands and replay window integrity |

## Fixtures Policy

- Engine integration fixtures are `crates/nanograph/tests/fixtures/`.
- CLI E2E fixtures come from `examples/starwars/` and `examples/revops/`.
- Do not add standalone feature schemas under `tests/cli/scenarios`; extend one of the canonical examples instead.
- If fixtures change, update references and tests in one place only.

## CI Guidance

PR gate (required):

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p nanograph
cargo test -p nanograph-cli
bash tests/cli/run-cli-e2e.sh
```

PR gate should cover Tier 0-2 confidence checks.

Nightly or pre-release:

```bash
bash tests/cli/run-cli-e2e.sh
cargo test -p nanograph --test index_perf -- --ignored --nocapture
```

## Adding a New CLI Scenario

1. Create `tests/cli/scenarios/<name>.sh`.
2. Source `tests/cli/lib/common.sh`.
3. Reuse shared helpers for build/assert/query execution.
4. Validate with:
   - `bash -n tests/cli/scenarios/<name>.sh`
   - `bash tests/cli/run-cli-e2e.sh <name>`
5. Update the CLI ownership table in this document.
