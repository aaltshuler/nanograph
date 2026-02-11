---
audience: dev
status: active
updated: 2026-02-12
---

# NanoGraph Implementation Checklist

Source roadmap: `docs/dev/db-features.md`
Authoritative status source: this checklist (the roadmap is directional and may lag implementation details).

## Current Snapshot

- [x] Phases 1-5 are implemented (`C1`/`C2`/`C3`/`H2`/`H3`).
- [x] `H4` bounded traversal is implemented end-to-end.
- [x] Unbounded traversal (`{m,}`) is intentionally disabled in typecheck for v0 safety.
- [x] CLI E2E test framework is consolidated under `tests/cli/`.

## Priority Order

1. Harden and battle-test completed features with comprehensive E2E coverage.
2. Keep `H4` unbounded traversal deferred until production-like validation justifies enabling it.
3. Medium backlog (vector/full-text/transaction model/scalar expansion).

## Phase 1: Complete C1 (Storage-Aware Node Scans)

### Tasks

- [x] Add Lance-native predicate/projection scan adapter in `crates/nanograph/src/plan/node_scan.rs` (uses dataset paths tracked in storage).
- [x] Expand pushdown translation (IR/DataFusion expr -> storage predicate) in `crates/nanograph/src/plan/node_scan.rs`.
- [x] Skip re-applying pushed predicates in the single-scan planner path in `crates/nanograph/src/plan/planner.rs`.
- [x] Generalize consumed-filter tracking for multi-scan and anti-join plans in `crates/nanograph/src/plan/planner.rs`.
- [x] Keep fallback to current in-memory filter path for unsupported predicates in `crates/nanograph/src/plan/node_scan.rs`.
- [x] Add projection pushdown (column pruning) into scan execution in `crates/nanograph/src/plan/node_scan.rs`.

### Tests / Validation

- [x] Add/extend pushdown tests in `crates/nanograph/tests/e2e.rs`.
- [x] Verify correctness parity for pushed vs non-pushed plans.
- [x] Confirm limit + filter + projection combinations.

### Done Criteria

- Eligible equality/range filters no longer require full row materialization.
- Planner does not re-apply filters already consumed by storage pushdown.

## Phase 2: Complete C2 + H1 (Merge Semantics + Uniqueness)

### Tasks

- [x] Add storage-native keyed merge path with Lance merge APIs in `crates/nanograph/src/store/database.rs`.
- [x] Refactor merge/load orchestration in `crates/nanograph/src/store/loader.rs`.
- [x] Split loader implementation into focused modules in `crates/nanograph/src/store/loader/jsonl.rs`, `crates/nanograph/src/store/loader/constraints.rs`, and `crates/nanograph/src/store/loader/merge.rs`, with `crates/nanograph/src/store/loader.rs` as orchestration facade.
- [x] Introduce/extend uniqueness metadata in schema IR in `crates/nanograph/src/catalog/schema_ir.rs`.
- [x] Wire parser support/validation for `@unique` semantics in `crates/nanograph/src/schema/parser.rs`.
- [x] Enforce `@unique` checks in load/upsert path in `crates/nanograph/src/store/database.rs`.
- [x] Add typed uniqueness diagnostics surfaced to CLI in `crates/nanograph-cli/src/main.rs`.

### Tests / Validation

- [x] Add tests for duplicate `@unique` collisions (existing-existing, existing-incoming, incoming-incoming).
- [x] Add tests for nullable vs non-nullable unique columns.
- [x] Ensure idempotent repeated keyed loads still preserve node IDs and edge consistency.
- [x] Add focused loader module tests in `crates/nanograph/src/store/loader/jsonl.rs`, `crates/nanograph/src/store/loader/constraints.rs`, and `crates/nanograph/src/store/loader/merge.rs`.

### Done Criteria

- Keyed merge is storage-native, not only in-memory rewrite.
- `@key` and `@unique` rules are both enforced with deterministic errors.

## Phase 3: Implement C3 (Secondary Index Foundation)

### Tasks

- [x] Add index metadata support (`@index`, auto-index `@key`) in `crates/nanograph/src/catalog/schema_ir.rs`.
- [x] Parse and validate index annotations in `crates/nanograph/src/schema/parser.rs`.
- [x] Add index lifecycle hooks (build/rebuild) in `crates/nanograph/src/store/database.rs`.
- [x] Extend migration handling for index creation/backfill in `crates/nanograph/src/store/migration.rs`.
- [x] Teach planner scan path to exploit index-eligible predicates in `crates/nanograph/src/plan/planner.rs`.

### Tests / Validation

- [x] Add index annotation tests in `crates/nanograph/tests/schema_migration.rs`.
- [x] Add query tests demonstrating index-backed point lookup in `crates/nanograph/tests/e2e.rs`.
- [x] Add performance regression harness (baseline scan vs indexed lookup) in `crates/nanograph/tests/index_perf.rs` (ignored/manual run).

### Done Criteria

- Index metadata persists and survives reopen/migration.
- Planner selects index-backed execution for eligible predicates.

## Phase 4: Implement H2 (Incremental Load Modes)

### Tasks

- [x] Add load mode enum (`overwrite|append|merge`) in `crates/nanograph/src/store/database.rs`.
- [x] Update loader logic for explicit mode behavior in `crates/nanograph/src/store/loader.rs`.
- [x] Add CLI flag wiring in `crates/nanograph-cli/src/main.rs`.
- [x] Add unsafe-combo guards (for example keyed append without uniqueness checks).

### Tests / Validation

- [x] Table-driven mode tests in `crates/nanograph/src/store/database.rs`.
- [x] CLI integration tests for mode parsing and behavior.

### Done Criteria

- Load semantics are explicit and deterministic across all schemas.

## Phase 5: Implement H3 (Query Mutation Surface)

### Tasks

- [x] Extend grammar for `insert/update/delete` in `crates/nanograph/src/query/query.pest`.
- [x] Add AST nodes in `crates/nanograph/src/query/ast.rs`.
- [x] Parse mutations in `crates/nanograph/src/query/parser.rs`.
- [x] Add typechecking rules in `crates/nanograph/src/query/typecheck.rs`.
- [x] Lower mutation IR in `crates/nanograph/src/ir/lower.rs`.
- [x] Add physical mutation execution path in `crates/nanograph/src/plan/physical.rs`.
- [x] Expose via CLI query runner in `crates/nanograph-cli/src/main.rs`.

### Tests / Validation

- [x] Add parse + typecheck mutation tests.
- [x] Add end-to-end mutation query tests in `crates/nanograph/tests/e2e.rs`.

### Done Criteria

- Mutation queries have same safety guarantees as CLI load/delete paths.

## Phase 6: Implement H4 (Bounded/Variable-Length Traversal)

Status: Partial by design (`bounded` complete, `unbounded` deferred/disabled).

### Tasks

- [x] Add bounded syntax (`{m,n}`) in `crates/nanograph/src/query/query.pest`.
- [x] Extend traversal AST representation in `crates/nanograph/src/query/ast.rs`.
- [x] Add parser coverage in `crates/nanograph/src/query/parser.rs`.
- [x] Add typechecking rules for bounds and endpoint compatibility in `crates/nanograph/src/query/typecheck.rs`.
- [x] Implement bounded expansion execution in `crates/nanograph/src/plan/physical.rs`.
- [x] Explicitly reject unbounded traversal (`{m,}`) in `crates/nanograph/src/query/typecheck.rs` for current release safety.
- [ ] Add iterative fixed-point path for unbounded traversal in `crates/nanograph/src/plan/physical.rs` (deferred).

### Tests / Validation

- [x] Bounded path correctness tests.
- [x] Parser/typecheck/lowering tests for bounded and unbounded syntax handling.
- [x] Unbounded traversal rejection tests.
- [ ] Cycle and termination tests for unbounded traversal (deferred with unbounded disabled).
- [ ] Performance sanity checks on sparse and dense graphs.

### Done Criteria

- Bounded traversals are type-safe and deterministic.
- Unbounded traversal remains disabled until explicit enablement criteria are met.

## Phase 7: Test Framework Coherence

### Tasks

- [x] Move CLI scenario scripts to `tests/cli/scenarios/`.
- [x] Add shared scenario helper library at `tests/cli/lib/common.sh`.
- [x] Add scenario runner `tests/cli/run-cli-e2e.sh`.
- [x] Remove stale root `tests/e2e.rs`.
- [x] Remove duplicate root fixture copies; keep canonical fixtures under `crates/nanograph/tests/fixtures/`.

### Tests / Validation

- [x] Validate scenario runner (`--list`, single scenario, full suite).
- [x] Validate migrated CLI scenario scripts with current CLI flags (`--mode` required).

### Done Criteria

- CLI E2E tests have one canonical entrypoint and shared harness logic.
- No duplicate fixture copies in root `/tests`.

## Medium Backlog (After Critical/High)

- [ ] Vector search integration (likely planner + scan extensions and new operators).
- [ ] Full-text search path (inverted index-backed predicates).
- [ ] Unified transaction model for general writes (load/upsert/delete/query-mutations).
- [ ] Additional scalar types (timestamps with timezone, collections).

## Suggested Milestone Gates

1. `M1`: C1 complete + measurable reduction in full-scan execution for eligible predicates.
2. `M2`: C2/H1 complete + storage-native merge and full uniqueness semantics.
3. `M3`: C3 complete + index-backed plans observable in explain/debug path.
4. `M4`: H2/H3 complete + stable write API (CLI + query).
5. `M5`: H4 bounded traversal complete + unbounded traversal explicitly gated/disabled pending hardening.

## Comprehensive E2E Test Cadence

Use this cadence so new features are validated at the right times without running the full suite on every edit.

### Run targeted tests (fast path)

- Run on every local feature slice and every PR update.
- Scope tests to touched modules first, then run package e2e.
- Typical command set:
  - `cargo test -p nanograph --lib`
  - `cargo test -p nanograph --test e2e`
  - `cargo test -p nanograph --test schema_migration`
  - module-targeted tests for touched areas (for example parser/typecheck/storage migration tests).

### Run comprehensive e2e (full confidence checkpoints)

- Run when any of these happens:
  - A phase gate is claimed complete (`M1`..`M5`).
  - Changes span multiple layers (parser/typecheck/IR/planner/physical/storage).
  - Persistence or compatibility behavior changes (manifest, migration, load/upsert/delete semantics).
  - Before cutting a release candidate or version tag.
  - After fixing a high-severity bug in execution or storage semantics.
- Recommended command set:
  - `cargo test -p nanograph`
  - `cargo test -p nanograph-cli`
  - `bash tests/cli/run-cli-e2e.sh`

### Release hardening pass

- Run at release-candidate time and before final tag.
- Include at least one repeat run after a clean build to catch flakiness in e2e and migration paths.
