---
audience: dev
status: draft
updated: 2026-02-19
---

# FTS and Hybrid Search RFC (Phase 4)

Status: **draft** (updated 2026-02-19)

## Why this RFC

NanoGraph now supports:

- vector search (`nearest(...)`)
- exact text predicates (`search(...)`, `fuzzy(...)`)

What is missing is ranked lexical retrieval and fused lexical+semantic ranking for graph-constrained queries. This RFC defines that Phase 4 contract.

## Goals

- Add ranked lexical retrieval with BM25 semantics.
- Add hybrid ranking using Reciprocal Rank Fusion (RRF).
- Preserve graph-first correctness constraints and deterministic output.
- Keep a correctness fallback path when pushdown/index paths are unavailable.

## Non-goals

- No breaking changes to existing `search(...)` and `fuzzy(...)`.
- No requirement to expose all Lance tokenizer knobs in v1 of Phase 4.
- No requirement to block vector MVP/ANN work.

## Current Baseline

- Exact text predicates are documented in `docs/dev/search.md`.
- Phase 4 in `docs/dev/search-plan.md` currently requires a separate RFC for:
  - `match_text`
  - `bm25`
  - hybrid ranking (`rrf`)

## Proposed Query Surface

### 1) Boolean lexical filter

```gq
match_text($s.summary, $q)
```

- Returns `Bool`.
- Intended as a filter expression in `match`.
- Semantics: analyzer/tokenizer-based lexical match (not fuzzy).

### 2) BM25 score expression

```gq
order { bm25($s.summary, $q) desc }
```

- Returns numeric score (`F64`).
- Valid in `return` and `order`.
- Higher score is better (`desc` for relevance order).

### 3) Hybrid fusion expression

```gq
order { rrf(nearest($s.summaryEmbedding, $vq), bm25($s.summary, $tq), 60) desc }
limit 10
```

- Returns numeric fused score (`F64`).
- Inputs are rank-producing expressions.
- Third arg is optional `rrf_k` (default `60`).

## Type Rules (v1)

- `match_text(field, query)`:
  - `field`: scalar `String`
  - `query`: scalar `String`
  - returns `Bool`
- `bm25(field, query)`:
  - `field`: scalar `String`
  - `query`: scalar `String`
  - returns `F64`
- `rrf(a, b[, k])`:
  - `a` / `b`: rank-producing expressions (`nearest`, `bm25`)
  - optional `k`: integer scalar, `k > 0`
  - returns `F64`
- `rrf(...)` in `order` requires `limit` (same operational reason as `nearest`).

## Execution Contract

### A) Single-node local search

If query is one node binding with only local scalar filters:

- planner may push lexical/vector scoring into Lance scanner/index paths.
- fallback: in-memory exact scoring path (correctness-first).

### B) Graph-structured search

If traversal/join/negation exists:

- evaluate graph constraints first.
- produce candidate node set.
- score only candidates:
  - BM25 path: lexical scoring over candidate set.
  - nearest path: existing exact/indexed candidate scoring path.
  - hybrid path: compute both ranks then fuse via RRF.

This preserves current graph-first correctness.

## Scoring Semantics

### BM25

- Canonical BM25 scoring.
- v1 defaults:
  - `k1 = 1.2`
  - `b = 0.75`
- tokenizer/analyzer defaults must be fixed and documented for determinism.

### RRF

- For each scorer rank `r_i` (1-based):
  - `rrf_i = 1 / (k + r_i)`
- Combined score:
  - `rrf = sum(rrf_i)`
- Tie-break:
  - primary: fused score descending
  - secondary: node id ascending

## Index and Schema Considerations

Current `@index` behavior should remain backward-compatible for scalar/vector indexes.

For Phase 4, adopt explicit lexical index intent:

- Option A (preferred): add `@fts` annotation for `String` properties.
- Option B: reuse `@index` for string FTS (riskier due to overloaded semantics).

Recommendation: **Option A** (`@fts`) to avoid behavior ambiguity and migration risk.

## Pushdown Matrix (v1)

- `match_text` + single-node local filters: pushdown eligible.
- `bm25` single-node ranking: pushdown eligible when lexical index exists.
- graph traversal filters: not pushdown into Lance scalar SQL; graph-first candidate generation required.
- hybrid (`rrf`): partial pushdown allowed, but must preserve exact candidate membership and deterministic ordering.

## Failure and Fallback Rules

On parser/planner/runtime pushdown failure:

- auto-fallback to correctness path.
- never return partial/approximate membership silently.
- emit debug log with fallback reason.

## Lance Version Gating

This RFC does **not** assume a specific unverified FTS API shape for `lance = 2.0.1` vs v3 beta.

Before implementation:

1. lock target Lance version for Phase 4;
2. verify Rust API surface for:
   - lexical index creation
   - lexical query/scoring
   - combined lexical+vector execution hooks;
3. record exact API calls in implementation notes.

## Test Plan

### Parser/Typechecker

- accept `match_text`, `bm25`, `rrf` syntax.
- reject invalid operand types and invalid `rrf_k`.
- enforce `limit` when `rrf(...)` is used in ordering.

### Planner/Execution

- deterministic ordering and tie-break checks.
- graph-constrained hybrid query returns only graph-valid nodes.
- pushdown path result equivalence vs fallback path on deterministic fixtures.

### CLI E2E

Using canonical examples only (`examples/starwars`, `examples/revops`):

- BM25 top-k query.
- traversal-constrained BM25 query.
- hybrid query (`nearest` + `bm25` via `rrf`).

## Rollout

1. Phase 4A:
   - `match_text` + `bm25` syntax/typecheck + correctness path
2. Phase 4B:
   - lexical index lifecycle + pushdown
3. Phase 4C:
   - `rrf` hybrid ranking + planner routing
4. Phase 4D:
   - optimization + benchmark gates

## Open Questions

- Should `bm25` support multi-field input in v1 (`bm25([$s.title, $s.summary], $q)`) or remain single-field?
- Do we expose analyzer/tokenizer config in schema annotations in v1, or keep engine defaults only?
- Should hybrid allow weighted RRF in v1 (`rrf_weighted`) or defer to v2?
