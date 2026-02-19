---
audience: dev
status: draft
updated: 2026-02-19
---

# Traditional and Fuzzy Search (NanoGraph v0)

This document describes the **current NanoGraph behavior** for traditional text search and fuzzy text search in graph queries.

## Scope

Implemented now:

- `search(field, query)`
- `fuzzy(field, query[, max_edits])`

Not implemented in this phase:

- BM25 ranking
- Phrase/proximity scoring
- Lance FTS index pushdown
- Hybrid vector + BM25 ranking (RRF)

## Query Syntax

These functions are boolean expressions and can be used directly inside `match` blocks:

```gq
query keyword_signals($q: String) {
    match {
        $s: Signal
        search($s.summary, $q)
    }
    return { $s.slug, $s.summary }
}

query fuzzy_signals($q: String, $m: I64) {
    match {
        $s: Signal
        fuzzy($s.summary, $q, $m)
    }
    return { $s.slug, $s.summary }
}
```

## Semantics

### `search(field, query)`

- Case-insensitive token search.
- Both `field` and `query` are tokenized by non-alphanumeric boundaries.
- **All query tokens must exist as exact tokens** in the field text.
- Empty token sets evaluate to `false`.

### `fuzzy(field, query[, max_edits])`

- Case-insensitive token search with typo tolerance.
- Each query token must fuzzy-match at least one field token.
- Matching uses Levenshtein edit distance.
- `max_edits` is optional and must be an integer scalar (`I32`, `I64`, `U32`, `U64`) >= 0.
- If `max_edits` is omitted, per-token defaults are:
  - length <= 2: `0`
  - length <= 5: `1`
  - length > 5: `2`

## Type Rules

- `field` must be scalar `String`.
- `query` must be scalar `String`.
- `max_edits` (if provided) must be integer scalar.

Type errors are reported as `T19` violations.

## Execution Notes

- Current execution is correctness-first and in-memory at filter evaluation time.
- These predicates are **not** pushed into Lance SQL scanner filters in this phase.
- This path is deterministic and works with graph traversal constraints.

## Roadmap

- FTS/BM25 and hybrid ranking remain Phase 4 (`docs/dev/search-plan.md`).
- Phase 4 design contract is in `docs/dev/fts-rfc.md`.
- Lance-native inverted-index pushdown and hybrid scoring implementation should follow `docs/dev/fts-rfc.md`.
