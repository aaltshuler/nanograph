# NanoGraph Implementation Checklist

Last updated: 2026-02-10
Source roadmap: `/Users/andrew/code/hyperdb/docs/db-features.md`

## Priority Order

1. Finish `C1` storage-native read pushdown.
2. Finish `C2` storage-native merge and `H1` uniqueness.
3. Implement `C3` secondary indexes.
4. Implement `H2` incremental load modes.
5. Implement `H3` query mutation surface.
6. Implement `H4` bounded/variable-length traversal.
7. Medium backlog (vector/full-text/transaction model/scalar expansion).

## Phase 1: Complete C1 (Storage-Aware Node Scans)

### Tasks

- [x] Add Lance-native predicate/projection scan adapter in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/node_scan.rs` (uses dataset paths tracked in storage).
- [x] Expand pushdown translation (IR/DataFusion expr -> storage predicate) in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/node_scan.rs`.
- [x] Skip re-applying pushed predicates in the single-scan planner path in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/planner.rs`.
- [x] Generalize consumed-filter tracking for multi-scan and anti-join plans in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/planner.rs`.
- [x] Keep fallback to current in-memory filter path for unsupported predicates in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/node_scan.rs`.
- [x] Add projection pushdown (column pruning) into scan execution in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/node_scan.rs`.

### Tests / Validation

- [x] Add/extend pushdown tests in `/Users/andrew/code/hyperdb/crates/nanograph/tests/e2e.rs`.
- [ ] Verify correctness parity for pushed vs non-pushed plans.
- [x] Confirm limit + filter + projection combinations.

### Done Criteria

- Eligible equality/range filters no longer require full row materialization.
- Planner does not re-apply filters already consumed by storage pushdown.

## Phase 2: Complete C2 + H1 (Merge Semantics + Uniqueness)

### Tasks

- [ ] Add storage-native keyed merge path with Lance merge APIs in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/database.rs`.
- [ ] Refactor merge/load orchestration in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/loader.rs`.
- [ ] Introduce/extend uniqueness metadata in schema IR in `/Users/andrew/code/hyperdb/crates/nanograph/src/catalog/schema_ir.rs`.
- [x] Wire parser support/validation for `@unique` semantics in `/Users/andrew/code/hyperdb/crates/nanograph/src/schema/parser.rs`.
- [x] Enforce `@unique` checks in load/upsert path in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/database.rs`.
- [ ] Add typed uniqueness diagnostics surfaced to CLI in `/Users/andrew/code/hyperdb/crates/nanograph-cli/src/main.rs`.

### Tests / Validation

- [x] Add tests for duplicate `@unique` collisions (existing-existing, existing-incoming, incoming-incoming).
- [x] Add tests for nullable vs non-nullable unique columns.
- [x] Ensure idempotent repeated keyed loads still preserve node IDs and edge consistency.

### Done Criteria

- Keyed merge is storage-native, not only in-memory rewrite.
- `@key` and `@unique` rules are both enforced with deterministic errors.

## Phase 3: Implement C3 (Secondary Index Foundation)

### Tasks

- [ ] Add index metadata support (`@index`, auto-index `@key`) in `/Users/andrew/code/hyperdb/crates/nanograph/src/catalog/schema_ir.rs`.
- [ ] Parse and validate index annotations in `/Users/andrew/code/hyperdb/crates/nanograph/src/schema/parser.rs`.
- [ ] Add index lifecycle hooks (build/rebuild) in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/database.rs`.
- [ ] Extend migration handling for index creation/backfill in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/migration.rs`.
- [ ] Teach planner scan path to exploit index-eligible predicates in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/planner.rs`.

### Tests / Validation

- [ ] Add index annotation tests in `/Users/andrew/code/hyperdb/crates/nanograph/tests/schema_migration.rs`.
- [ ] Add query tests demonstrating index-backed point lookup in `/Users/andrew/code/hyperdb/crates/nanograph/tests/e2e.rs`.
- [ ] Add performance regression harness (baseline scan vs indexed lookup).

### Done Criteria

- Index metadata persists and survives reopen/migration.
- Planner selects index-backed execution for eligible predicates.

## Phase 4: Implement H2 (Incremental Load Modes)

### Tasks

- [ ] Add load mode enum (`overwrite|append|merge`) in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/database.rs`.
- [ ] Update loader logic for explicit mode behavior in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/loader.rs`.
- [ ] Add CLI flag wiring in `/Users/andrew/code/hyperdb/crates/nanograph-cli/src/main.rs`.
- [ ] Add unsafe-combo guards (for example keyed append without uniqueness checks).

### Tests / Validation

- [ ] Table-driven mode tests in `/Users/andrew/code/hyperdb/crates/nanograph/src/store/database.rs`.
- [ ] CLI integration tests for mode parsing and behavior.

### Done Criteria

- Load semantics are explicit and deterministic across all schemas.

## Phase 5: Implement H3 (Query Mutation Surface)

### Tasks

- [ ] Extend grammar for `insert/update/delete` in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/query.pest`.
- [ ] Add AST nodes in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/ast.rs`.
- [ ] Parse mutations in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/parser.rs`.
- [ ] Add typechecking rules in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/typecheck.rs`.
- [ ] Lower mutation IR in `/Users/andrew/code/hyperdb/crates/nanograph/src/ir/lower.rs`.
- [ ] Add physical mutation execution path in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/physical.rs`.
- [ ] Expose via CLI query runner in `/Users/andrew/code/hyperdb/crates/nanograph-cli/src/main.rs`.

### Tests / Validation

- [ ] Add parse + typecheck mutation tests.
- [ ] Add end-to-end mutation query tests in `/Users/andrew/code/hyperdb/crates/nanograph/tests/e2e.rs`.

### Done Criteria

- Mutation queries have same safety guarantees as CLI load/delete paths.

## Phase 6: Implement H4 (Bounded/Variable-Length Traversal)

### Tasks

- [ ] Add bounded syntax (`{m,n}`) in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/query.pest`.
- [ ] Extend traversal AST representation in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/ast.rs`.
- [ ] Add parser coverage in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/parser.rs`.
- [ ] Add typechecking rules for bounds and endpoint compatibility in `/Users/andrew/code/hyperdb/crates/nanograph/src/query/typecheck.rs`.
- [ ] Implement bounded expansion execution in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/physical.rs`.
- [ ] Add iterative fixed-point path for unbounded traversal in `/Users/andrew/code/hyperdb/crates/nanograph/src/plan/physical.rs`.

### Tests / Validation

- [ ] Bounded path correctness tests.
- [ ] Cycle and termination tests for unbounded traversal.
- [ ] Performance sanity checks on sparse and dense graphs.

### Done Criteria

- Bounded and unbounded traversals are type-safe and terminate correctly.

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
5. `M5`: H4 complete + traversal language expansion.

## Comprehensive E2E Test Cadence

Use this cadence so new features are validated at the right times without running the full suite on every edit.

### Run targeted tests (fast path)

- Run on every local feature slice and every PR update.
- Scope tests to touched modules first, then run package e2e.
- Typical command set:
  - `cargo test -p nanograph --test e2e`
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

### Release hardening pass

- Run at release-candidate time and before final tag.
- Include at least one repeat run after a clean build to catch flakiness in e2e and migration paths.
