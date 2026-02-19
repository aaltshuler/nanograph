---
audience: dev
status: draft
updated: 2026-02-19
---

# Search Implementation Plan

Status: **draft** (updated 2026-02-19)

## Goals

- Ship a coherent search stack for NanoGraph with correct semantics in graph queries.
- Deliver a usable vector-search MVP first, then optimize with ANN indexing.
- Keep implementation aligned with current engine contracts and test framework.
- Use only canonical example schemas for E2E coverage: `examples/starwars` and `examples/revops`.

## Non-Goals (for initial delivery)

- No new ad-hoc test-only schema files under `tests/cli/scenarios`.
- No full-text/BM25 implementation in the vector MVP phase.
- No model-selection/config matrix in v0 (single embedding model when auto-embed is enabled).

## Hard Constraints

- Database path convention is `.nano`.
- Loader JSONL contract remains:
  - Node row: `{"type":"TypeName","data":{...}}`
  - Edge row: `{"edge":"EdgeName","from":"key","to":"key","data":{...}}`
- Existing `@index` behavior stays compatible for scalar properties.
- Graph-structural filtering and traversal cannot be pushed into Lance scalar filter strings.
- Current Lance target in this repo is `lance = 2.0.1`; plan must use APIs available in that release.
- Do not assume public scanner row-id bitmap APIs (`RowIdSet` / `RowIdMask`) are available in this release.

## Lance Status (Resolved + Pending)

Resolved for `lance = 2.0.1` (as of 2026-02-17):

- There is no public scanner row-id bitmap pushdown API in this release; do not plan on `RowIdSet` / `RowIdMask` in 2.0.1.
- For graph-generated candidate sets, default to chunked `take_rows` + exact scoring for correctness and predictable latency.
- There is no official old->new row-id mapping API emitted by `update` / `merge_insert`; use a stable user PK for identity and re-resolve row ids after mutations.
- Treat scanner `_rowid IN (...)` prefilter as optional and bounded only (feature-flagged), not as the primary execution path.

Pending for `lance` v3 beta/GA adoption:

- Verify the exact Rust API surface for row-id bitmap pushdown in v3 (`RowIdSet` / `RowIdMask`) and lock the planner integration point.
- Establish exact-vs-ANN routing thresholds under row-id mask pushdown with benchmarks.
- Confirm the normative row-id stability contract in v3 docs for `delete`, `optimize`, `update`, and `merge_insert`, then codify it in migration/sync rules.

## Coherent v0 Search Semantics

1. `nearest(...)` is an ordering expression, not a filter.
2. `nearest(...)` is ascending-distance in v0; `asc` / `desc` modifiers on `nearest` are not part of v0 syntax.
3. `nearest` requires `limit`.
4. Query-time candidate generation is split:
   - Local scalar-only prefilter on one node type: push down to Lance where possible.
   - Any traversal/join/graph-structural filter: evaluate graph first, then produce a node candidate subset.
5. Correctness path always exists:
   - If query already has a graph-generated candidate subset, use chunked `take_rows` and compute exact distances in-memory (default v0 path).
   - Optional optimization (feature-flagged): for bounded candidate sets only, attempt scanner prefilter (`_rowid` expression + `nearest` + `prefilter(true)`), and auto-fallback to exact chunked `take_rows` on parser/planner/runtime failure.
   - If query is single-node and no index exists, use Lance exact vector scan (flat KNN) when available.
6. Top-k output is deterministic: primary sort by distance ascending, stable tie-break by node id ascending.
7. `OPENAI_API_KEY` is required only for operations that need embedding generation (`@embed` paths or string query embedding). Manual vector workflows must run without API key.

## Phase Plan

## Phase 0: Spec Lock and Drift Cleanup

Deliverables:

- Reconcile docs with engine contracts:
  - `.nano` examples only.
  - Correct JSONL shape (`type` + `data`).
  - Clarify API key behavior by operation.
  - Remove stale `.nanograph` references where present.
  - Replace stale JSONL examples that use `_type` with the `type` / `data` shape.
  - Keep `@embed` requirements in Phase 3 scope (not Phase 1).
- Freeze v0 metric behavior:
  - Use cosine only in v0 unless metric support is implemented end-to-end.
  - Defer metric-specific schema syntax until parser + IR changes are ready.

Exit criteria:

- `docs/dev/vector.md` is marked superseded and points to `docs/dev/search-plan.md` as canonical.
- One canonical source of truth for v0 semantics.
- Phase 0 drift checklist items are explicitly resolved (or deferred with rationale).

## Phase 1: Vector Core MVP (Manual Vectors First)

Deliverables:

- Schema/type system:
  - Add `Vector(dim)` support.
  - Validate `dim > 0`.
- Query language:
  - Add `nearest(property, query)` expression support in `order`.
  - Support vector query params (`Vector(N)`) and list literal decoding for CLI params.
  - Define/implement CLI vector syntax: `--param 'q=[0.1, 0.2, ...]'`.
- Typechecker:
  - `nearest` requires `limit`.
  - `nearest` operand type rules:
    - left side must be vector property
    - right side must be `Vector(N)` or `String` (when embedding path is enabled)
    - dimensions must match
- Loader/runtime:
  - Parse vector columns from JSON arrays into `FixedSizeList<Float32, N>`.
  - Strict length validation (dimension mismatch is an error).
- Execution:
  - Path A (single-node exact in Phase 1): use Lance native exact vector scan (`nearest` with `use_index(false)`); do not implement custom full-dataset distance loops.
  - Path B (graph-structured exact in Phase 1): generate candidate row ids first, then chunked `take_rows` + exact scoring.
  - Path C (optional experimental): scanner prefilter (`_rowid` filter expression + `nearest` + `prefilter(true)`) only when candidate count is below `SEARCH_ROWID_IN_MAX`, with automatic fallback to Path B.
  - Exact distance kernels for Path B:
    - prefer `lance_linalg::distance` with an explicit direct dependency pin;
    - if API compatibility blocks this, keep a local exact-kernel adapter with identical semantics.
  - Define and expose:
    - `SEARCH_TAKE_ROWS_BATCH` (row batch size for chunked exact path).
    - `SEARCH_ROWID_IN_MAX` (hard ceiling for optional `_rowid IN (...)` scanner prefilter path).

Exit criteria:

- Manual-vector queries return deterministic top-k results (including tie cases).
- Graph + vector combined queries return correct constrained results.
- No OpenAI dependency required for this phase.

## Phase 2: Vector Index Lifecycle (ANN Optimization)

Deliverables:

- Build/rebuild vector indexes for vector properties marked indexable.
- Default ANN index type for v0 is `IVF_PQ` (cosine); defer alternate index-type tuning to post-v0 optimization.
- Planner chooses indexed ANN path when valid and beneficial; otherwise falls back automatically.
- Keep exact path (`use_index(false)` and/or `take_rows` exact scoring) as correctness fallback.

Exit criteria:

- When scanner prefilter experimental path is enabled, its results are equivalent to exact `take_rows` in membership and order for bounded deterministic fixtures.
- ANN path meets agreed recall target against exact baseline (e.g. recall@k) for deterministic fixtures.
- Performance improves on larger fixtures without behavior changes.

## Phase 3: Auto-Embedding (`@embed`) and Query String Embedding

Deliverables:

- `@embed(source_prop)` parsing and validation.
- Load-time embedding generation pipeline:
  - batching with backoff
  - resumable cache (`content_hash -> vector`) to prevent re-paying after partial failures
- Query-time string embedding path for `nearest(..., $q: String)`.
- Offline/manual mode remains fully supported:
  - no key required for manual vectors
  - key required only when an embedding call is needed

Exit criteria:

- Load retries are resumable and idempotent for embeddings.
- Manual vector workflows still pass with no `OPENAI_API_KEY`.

## Phase 4: FTS and Hybrid Search (Separate Track)

Deliverables:

- Keep baseline exact text predicates (`search`, `fuzzy`) documented in `docs/dev/search.md`.
- Separate RFC/design doc for `match_text`, `bm25`, and hybrid ranking (`rrf`):
  - `docs/dev/fts-rfc.md`
- Verify Lance API surface before committing syntax or execution details.

Exit criteria:

- FTS plan is validated independently and does not block vector MVP.

## Implementation Workstreams by Area

- Schema and types:
  - `crates/nanograph/src/schema/schema.pest`
  - `crates/nanograph/src/schema/parser.rs`
  - `crates/nanograph/src/types.rs`
  - `crates/nanograph/src/catalog/schema_ir.rs`
  - Phase scope note: `Vector(dim)` parser/type support is Phase 1; `@embed(...)` parsing/validation is Phase 3.
- Query frontend:
  - `crates/nanograph/src/query/query.pest`
  - `crates/nanograph/src/query/ast.rs`
  - `crates/nanograph/src/query/parser.rs`
  - `crates/nanograph/src/query/typecheck.rs`
- IR/lowering:
  - `crates/nanograph/src/ir/mod.rs`
  - `crates/nanograph/src/ir/lower.rs`
- Planner/execution:
  - `crates/nanograph/src/plan/planner.rs`
  - `crates/nanograph/src/plan/node_scan.rs`
  - (new) `crates/nanograph/src/plan/vector_search.rs` if needed
- Loader and indexing:
  - `crates/nanograph/src/store/loader/jsonl.rs`
  - `crates/nanograph/src/store/indexing.rs`
  - CLI command wiring for index lifecycle
- CLI/runtime param decoding:
  - `crates/nanograph-cli/src/main.rs`

## E2E Test Strategy (Canonical Schemas Only)

Rules:

- Extend only `examples/starwars/*` and `examples/revops/*` fixtures.
- Do not add new standalone schema fixtures for search.
- Keep vector fixtures deterministic for CI by using manual vectors in MVP tests.

Coverage plan:

1. Star Wars search coverage:
   - Add vector property to one stable node type and deterministic vectors in JSONL.
   - Add `nearest` queries in `examples/starwars/starwars.gq`.
   - Validate constrained search with traversal + nearest (graph-first candidate subset).
2. RevOps search coverage:
   - Add vector property on text-heavy entity.
   - Add exact top-k search query and filtered search query.
3. CLI scenario integration:
   - Add/extend scenario under `tests/cli/scenarios` that runs search queries against both examples.
   - Validate result ordering and membership invariants.
4. Rust integration checks:
   - Parser/typechecker tests for vector typing and `nearest` rules.
   - Planner tests that verify path selection:
     - single-node local filter path
     - graph-structured `take_rows` exact path
     - graph-structured scanner-prefilter path (feature flag on + bounded candidate set)

## Rollout Gates

1. Gate A (Phase 1 complete):
   - `cargo test -p nanograph`
   - `cargo test -p nanograph-cli`
   - `bash tests/cli/run-cli-e2e.sh`
2. Gate B (Phase 2 complete):
   - Add index-specific integration tests with exact baseline comparison and ANN recall checks.
3. Gate C (Phase 3 complete):
   - Add embedding retry/resume tests and no-key manual-vector tests.

## Priority Order

1. Phase 0 + Phase 1 (correctness-first vector core, manual vectors).
2. Phase 2 (ANN/index acceleration).
3. Phase 3 (auto-embedding and query string embedding).
4. Phase 4 (FTS/hybrid as separate RFC and implementation stream).
