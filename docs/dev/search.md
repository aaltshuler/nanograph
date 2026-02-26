---
audience: dev
status: draft
updated: 2026-02-24
---

# Search Semantics (Current)

This document summarizes the current NanoGraph search behavior across text, vector, hybrid, and graph-constrained queries.

## Scope

Implemented now:

- `search(field, query)`
- `fuzzy(field, query[, max_edits])`
- `match_text(field, query)`
- `bm25(field, query)`
- `nearest(vector_field, query_vector_or_text)`
- `rrf(rank_expr_a, rank_expr_b[, k])`

Still pending / future optimization work:

- Lance FTS pushdown for text predicates and BM25
- Broader ANN/hybrid pushdown coverage beyond current fast paths

## Query Syntax

```gq
query keyword($q: String) {
    match {
        $s: Signal
        search($s.summary, $q)
    }
    return { $s.slug }
}

query phrase($q: String) {
    match {
        $s: Signal
        match_text($s.summary, $q)
    }
    return { $s.slug }
}

query semantic($q: String) {
    match { $s: Signal }
    return { $s.slug, nearest($s.embedding, $q) as distance }
    order { nearest($s.embedding, $q) }
    limit 5
}

query hybrid($q: String) {
    match { $s: Signal }
    return { $s.slug, rrf(nearest($s.embedding, $q), bm25($s.summary, $q)) as score }
    order { rrf(nearest($s.embedding, $q), bm25($s.summary, $q)) desc }
    limit 5
}
```

## Semantics

### Text predicates (boolean filters)

- `search(field, query)`: case-insensitive token search; query and field are tokenized by non-alphanumeric boundaries; all query tokens must exist as exact tokens; empty token sets evaluate to `false`.
- `fuzzy(field, query[, max_edits])`: case-insensitive token search with typo tolerance; each query token must fuzzy-match a field token via Levenshtein distance; `max_edits` must be integer >= 0; default per-token edits are `0` (len <= 2), `1` (len <= 5), `2` (len > 5).
- `match_text(field, query)`: tokenized contiguous phrase match (query tokens must appear in order and adjacent).

### Ranking functions

- `bm25(field, query)`: returns lexical relevance score; higher is better; use `desc` ordering.
- `nearest(vector_field, query)`: returns cosine distance; lower is better; default ordering is ascending; ordering with `nearest(...)` requires `limit`.
- `rrf(a, b[, k])`: reciprocal rank fusion over rank-producing expressions (`nearest` / `bm25` / nested `rrf`); higher is better; use `desc`; ordering with `rrf(...)` requires `limit`; `k` must be integer > 0 (default `60`).

## Type Rules

- `search` / `fuzzy` require String field/query (`T19`).
- `match_text` / `bm25` require String field/query (`T20`).
- `rrf` operands must be rank expressions and `k > 0` (`T21`).
- `nearest` validates vector property and query dimension compatibility (`T15`).
- Standalone `nearest` and `rrf` ordering require `limit` (`T17`, `T21`).

## Execution Notes

- Text predicate evaluation, BM25 scoring, and RRF fusion run in planner execution (correctness-first path).
- String queries for `nearest(..., $q: String)` are resolved to embeddings before execution.
- Single-node `nearest` queries can use an ANN fast path when an index is available.
- Graph traversal constraints are applied before ranking, enabling graph-scoped search.

## Roadmap

- Continue improving pushdown/index utilization for text and hybrid ranking paths.
- Keep semantics aligned with `docs/user/search.md` and `docs/user/queries.md`.
