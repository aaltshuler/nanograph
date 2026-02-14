# HyperDB Refactor Plan (Breaking Change, Safety-First, Lance-First Core)

Updated: 2026-02-14

## Compatibility contract (explicit)

This refactor intentionally breaks old on-disk compatibility:

1. No backward compatibility with V1 manifest format.
2. Existing `.nanograph` databases do not need migration support.
3. No downgrade support to older binaries.

## CDC storage decision

Decision: use JSONL for the authoritative CDC log on the write path.

Rationale:

1. CDC is a sequential append-log workload, not a columnar analytics-first workload.
2. Single-row append to Lance per mutation can create high version/fragment churn.
3. JSONL gives simpler durability and lower write amplification for high-frequency agent mutations.
4. Lance is still used where it is strongest: graph tables, scans, filters, merges, compaction.

Optional optimization later:

1. Add a derived/materialized Lance CDC view (batch-built from JSONL) for heavy analytics.
2. Keep JSONL as the source of truth even if Lance materialization exists.

## Non-negotiable invariants

Every phase must preserve these invariants:

1. `Database::open` reconstructs exactly one globally consistent snapshot.
2. A crash never exposes a partially committed logical mutation.
3. Logical commits are totally ordered by monotonically increasing `db_version`.
4. CDC output order is exactly (`db_version`, `seq_in_tx`).
5. CDC visibility is commit-gated (no orphan/uncommitted events can leak).

## Target architecture

## Core state

1. `graph.manifest.json` (V2 only):
1. `manifest_format_version = 2`
2. `db_version`
3. per-dataset pinned Lance `dataset_version`
4. `last_tx_id`
5. `committed_at`
2. Lance datasets for graph tables remain canonical data storage.

## Commit metadata and CDC

1. `_tx_catalog.jsonl` (append-only):
1. one row per committed logical transaction
2. `tx_id`, `db_version`, `dataset_versions`, `committed_at`, `op_summary`
3. optional `cdc_start_offset` and `cdc_end_offset` for fast mapping
2. `_cdc_log.jsonl` (append-only):
1. one row per CDC event
2. `tx_id`, `db_version`, `seq_in_tx`, `op`, `entity_kind`, `type_name`, `entity_key`, `payload`, `committed_at`

## Recovery model

1. Manifest is the visibility gate and source of truth for current state.
2. Any tx catalog rows with `db_version > manifest.db_version` are ignored on startup.
3. Any CDC rows not referenced by visible tx rows are ignored.
4. Trailing partial JSONL records are truncated during startup repair.

## Progress status (as of 2026-02-14)

1. Phase 0: DONE
2. Phase 1: DONE
3. Phase 2: DONE
4. Phase 3: DONE
5. Phase 4: DONE
6. Phase 5: DONE
7. Phase 6: DONE
8. Phase 7: DONE

## Phase 0: Baseline and safety harness

Status: DONE

### Goal

Lock in current behavior and add crash safety tests before major changes.

### Steps

1. Add storage safety test suite:
1. reopen consistency
2. id counter monotonicity
3. edge cascade correctness
2. Add deterministic crash injection points around write path.
3. Add perf baselines:
1. append-heavy
2. merge-heavy
3. delete-heavy
4. high-frequency single-row mutation workload

### Exit criteria

1. Baseline suite is green in CI.
2. Crash harness can deterministically simulate mid-commit failures.

### Phase 0 completion notes (2026-02-14)

1. Baseline safety suites are green in CI:
1. reopen consistency
2. id counter monotonicity and persistence
3. cascade correctness for node and edge mutation paths
2. Deterministic commit crash failpoints are implemented in tx log commit flow:
1. `before_cdc_append`
2. `after_cdc_append`
3. `after_tx_append`
4. `after_manifest_write`
3. Crash matrix test is implemented and passing:
1. `commit_failpoint_matrix_preserves_visible_state`
2. validates manifest-gated visibility and log reconciliation for each failpoint stage

## Phase 1: Manifest V2 only (no compatibility layer)

Status: DONE

### Goal

Make global snapshot reconstruction deterministic with pinned dataset versions.

### Steps

1. Replace manifest schema with V2 structure only.
2. Update `Database::open` to:
1. read V2 manifest
2. open each dataset path
3. `checkout_version(dataset_version)` for every dataset
3. Remove V1 reader and any compatibility branches.
4. Fail fast on:
1. missing pinned version
2. missing dataset path
3. malformed manifest

### Exit criteria

1. Open path only accepts V2 manifests.
2. Snapshot reconstruction is deterministic and version-pinned.

## Phase 2: Global commit coordinator

Status: DONE

### Goal

Introduce transaction ordering and commit metadata independent of table scans.

### Steps

1. Add append writer for `_tx_catalog.jsonl` with:
1. newline-delimited records
2. fsync after append
3. startup truncation of trailing partial line
2. Define strict logical commit order:
1. apply data mutations to touched Lance datasets
2. read resulting dataset versions
3. append CDC rows to `_cdc_log.jsonl` (fsync)
4. append one tx row to `_tx_catalog.jsonl` (fsync)
5. write V2 manifest with same `db_version` and dataset versions (atomic rename + fsync)
3. Add startup reconciliation:
1. if tx/db versions exceed manifest, ignore tail rows
2. if CDC rows exist past last visible tx, ignore as orphan tail

### Exit criteria

1. Manifest and tx catalog are consistent for all visible commits.
2. Crash at any step does not expose partial logical commit.

## Phase 3: Unified mutation pipeline

Status: DONE

### Goal

Route all writes through one transaction pipeline and remove ad-hoc persistence paths.

### Steps

1. Add `MutationPlan` that contains:
1. touched datasets
2. concrete Lance operations
3. CDC event list
2. Route all mutation entry points through this pipeline:
1. `load_with_mode`
2. query `insert/update/delete`
3. CLI `delete`
3. Remove legacy ad-hoc persistence entrypoints.
4. Defer touched-dataset commit minimization to Phase 4 (Lance-native operations).
4. Keep existing logical semantics identical:
1. uniqueness and key checks
2. cascade behavior
3. deterministic ids

### Exit criteria

1. All mutation entry points commit through one pipeline.
2. Behavior parity tests pass vs baseline semantics.

## Phase 4: Lance-native data operations

Status: DONE

### Goal

Use Lance primitives directly for graph data where they are strongest.

### Steps

1. Append path:
1. use `WriteMode::Append` where valid
2. avoid read/concat/rewrite for unchanged datasets
2. Merge path:
1. use `MergeInsertBuilder` directly on target dataset
2. eliminate temp-dataset roundtrip in steady state
3. Delete path:
1. use Lance delete/update operations where feasible
2. retain graph-level edge cascade orchestration
4. Overwrite path:
1. keep explicit
2. do not use as hidden fallback for routine mutations
5. Add compaction command and policy for graph datasets.

### Exit criteria

1. Load modes map cleanly to Lance operations.
2. Write amplification drops on single-row mutation workloads.

### Phase 4 completion notes (2026-02-14)

1. Fallback-audit correctness checks are in place:
1. node-only keyed update keeps edge datasets unchanged
2. edge-delete path mutates only the targeted edge dataset
2. `merge_edge_batches` now has a no-incoming fast-path to avoid incidental edge rewrites on keyed node updates.
3. Write-amplification harness is implemented:
1. test: `benchmark_write_amplification_mutation_paths` (ignored by default)
2. scenarios: append single-row, keyed update single-row, delete-edge-heavy
3. metrics: touched dataset count, per-type version deltas, version-history deltas, fragment deltas, tombstone deltas, bytes delta, elapsed ms
4. Default harness scale and guardrails:
1. `NANOGRAPH_WRITE_AMP_ROWS` default: `2000`
2. optional enforced latency SLO env defaults: append `250ms`, update `400ms`, delete `450ms`
3. for larger stress runs (`5000+` rows), use `RUST_MIN_STACK=33554432`
5. Baseline measurement (2026-02-14, default rows=2000, enforce enabled):
1. append: `39.246ms`
2. keyed update: `44.604ms`
3. delete-heavy edge mutation: `93.043ms`

## Phase 5: CDC V1 (authoritative JSONL log)

Status: DONE

### Goal

Ship safe, deterministic CDC with strict commit-order semantics.

### Steps

1. Implement `_cdc_log.jsonl` writer and record schema.
2. Generate CDC events from `MutationPlan` before commit finalization.
3. Commit-gate CDC visibility through manifest `db_version`.
4. Add CLI:
1. `nanograph changes --since <db_version>`
2. `nanograph changes --from <db_version> --to <db_version>`
3. output formats `jsonl|json`
5. Reader semantics:
1. iterate visible tx range only
2. stream referenced CDC rows in order
3. never scan raw CDC rows without tx gating

### Exit criteria

1. CDC replay is exact and ordered.
2. No duplicate or partial tx emission in crash tests.

## Phase 6: Optional CDC analytics acceleration

Status: DONE

### Goal

Enable fast analytical CDC queries without changing correctness source.

### Steps

1. Add optional materializer:
1. batch-ingest `_cdc_log.jsonl` into a Lance `__cdc_analytics` dataset
2. run on threshold or explicit command
2. Keep JSONL authoritative for `changes` command.
3. Expose analytics command separately if needed.

### Exit criteria

1. Analytics acceleration does not alter correctness path.
2. Turning materializer off does not change CDC results.

## Phase 7: Retention, cleanup, compaction

Status: DONE

### Goal

Control storage growth safely across graph data and logs.

### Steps

1. Add retention config:
1. graph dataset version retention
2. tx catalog retention
3. cdc log retention
2. Add maintenance commands:
1. `nanograph compact`
2. `nanograph cleanup`
3. `nanograph doctor`
3. Cleanup rules:
1. never remove data required for visible manifest state
2. never remove CDC data inside configured replay window
3. cleanup always manifest/tx-aware

### Exit criteria

1. Cleanup cannot break open, replay, or CDC contract.
2. Storage growth is bounded by policy.

## Cross-phase test matrix

Run at each phase gate:

1. `cargo test -p nanograph`
2. `cargo test -p nanograph-cli`
3. `bash tests/cli/run-cli-e2e.sh`
4. Crash matrix:
1. crash before CDC append
2. crash after CDC append, before tx append
3. crash after tx append, before manifest write
4. crash after manifest write
5. Log repair matrix:
1. trailing partial line in `_cdc_log.jsonl`
2. trailing partial line in `_tx_catalog.jsonl`
3. orphan CDC rows and orphan tx rows beyond visible manifest

## Rollout plan

1. PR1: DONE - Manifest V2-only + pinned open semantics.
2. PR2: DONE - `_tx_catalog.jsonl` and `_cdc_log.jsonl` append/recovery utilities.
3. PR3: DONE - Unified commit coordinator + crash-safe ordering.
4. PR4: DONE - Route append mutations through new pipeline.
5. PR5: DONE - Route merge/delete + remove legacy snapshot persistence entrypoints.
6. PR6: DONE - `nanograph changes` CLI with tx/manifest-gated CDC reads.
7. PR7: DONE - compaction/cleanup/doctor commands + retention window flags.
8. PR8: DONE - touched-dataset minimization + append-mode Lance append + keyed merge commit-path upsert + removed loader temp merge roundtrip.
9. PR9: DONE - route delete-heavy mutations to Lance-native dataset ops while preserving cascade semantics.
10. PR10: DONE - route update-heavy mutations to Lance-native keyed upsert path beyond load:merge.
11. PR11: DONE - Phase 4 exit validation (write amplification/perf gates + fallback audit).
12. PR12: DONE - deterministic commit crash failpoints + crash matrix safety harness.
13. PR13: DONE - dead-code cleanup pass (moved failpoint injection scaffolding to test-only scope).
14. PR14: DONE - removed legacy CLI `check/run` schema+data mode; DB-only execution path.
15. PR15: DONE - tightened `store::txlog` visibility to crate-only internals; removed CLI test dependency on internal tx-catalog reader.
16. PR16: DONE - hardened module exports (`lib.rs`/`store/mod.rs`) to curated public re-exports; made planner/loader/indexing internals crate-private.
17. PR17: DONE - removed unused `NodeTypeTable` DataFusion table-provider path from `plan/node_scan`; retained `NodeScanExec`-based scan path only.
18. PR18: DONE - added optional `cdc-materialize` flow: JSONL-visible CDC -> derived `__cdc_analytics` Lance dataset with threshold/force controls; `changes` correctness path unchanged.
19. PR19: DONE - full dead-code sweep: removed last `#[allow(dead_code)]` helper in `tests/e2e.rs`; workspace strict unused/dead/unreachable checks clean.

## Do-not-do list

1. Do not use Lance as authoritative CDC log on per-mutation append path.
2. Do not expose CDC by scanning raw `_cdc_log` without tx/manifest gating.
3. Do not reintroduce hidden full-snapshot rewrite behavior.
4. Do not add compatibility shims for old manifest formats.
