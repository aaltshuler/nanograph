# NanoGraph — Database Features Roadmap (Upgraded)

Current target: `v0.2.x` to `v0.3.0`
Last updated: `2026-02-10`

This proposal is validated against the current codebase and updates priorities to match actual architecture and APIs.

Detailed execution checklist: `/Users/andrew/code/hyperdb/docs/execution-checklist.md`

---

## Validated Current State

| Feature | Status | Notes |
|---------|--------|-------|
| Typed schema (`.pg`) | Done | Parsed + lowered to `SchemaIR`/`Catalog` |
| JSONL loading | Done (overwrite + keyed merge) | Overwrite by default; keyed node types merge on `@key` with ID preservation |
| Persistence (Lance + Arrow) | Done | Data stored per type in Lance datasets + manifest |
| Read queries (`.gq`) | Done | Parse -> typecheck -> IR -> physical plan |
| Compile-time typechecking | Done (T1-T9) | Enforced in `query/typecheck.rs` |
| Edge traversal (single hop) | Done | Grammar supports `$a edge $b` |
| Negation (`not { ... }`) | Done | Anti-join lowering/execution path exists |
| Aggregation (`count/sum/avg/min/max`) | Done | Supported in parser + planner |
| Query parameters (`--param`) | Done | CLI + runtime binding implemented |
| Schema migration (`nanograph migrate`) | Done | Planned steps + transactional apply (journal + backup) |
| Pretty parse errors (Ariadne) | Done | Used in CLI diagnostics |
| Keyed upsert (`@key`) | Done | Keyed merge, duplicate key detection, null-key rejection, endpoint remap |
| Edge dedup (no multigraph) | Done | Deduplicated by `(src,dst)` per edge type in load/merge paths |
| Delete API (CLI-level) + edge cascade | Done | `nanograph delete` removes matching nodes and cascades incident edges |
| DataFusion scan contract pushdown | Partial | `supports_filters_pushdown` + `scan` filter/limit handling implemented for supported predicates |
| Single-scan filter + limit pushdown | Partial | Planner pushes supported `Filter` ops into `NodeScanExec`; limit pushdown when all filters are pushdown-eligible |
| Bounded expansion (`knows{1,3}`) | Not implemented | Not in grammar |
| Variable-length paths | Not implemented | No iterative reachability operator |
| Storage-level read filter pushdown | Partial | In-memory batch pushdown exists; no Lance-native predicate pushdown yet |
| Secondary indexes integration | Not implemented | No index build/use in runtime |
| Query-language mutations | Not implemented | No `insert/update/delete` grammar clauses |

---

## Architectural Constraints To Address First (Status)

1. Resolved: `NodeTypeTable::scan` now accepts DataFusion filters/limits for supported predicates.
2. Resolved: `NodeScanExec` iterates segment batches and applies pushdown filters per batch.
3. Partial: planner still has post-filter path, but skips predicates that were pushed down.
4. Partial: keyed upsert and delete cascade exist; index pipeline is still missing.

Read-path pushdown groundwork now exists, but full storage-native pushdown and indexing are still pending.

---

## Upgraded Priorities

### Critical (implement first)

### C1. Storage-aware Node Scans (Filter/Projection Pushdown)
**Status:** Partial

**Why critical:** Without this, indexes and larger datasets do not pay off.

**Approach:**
- Done:
  - DataFusion `TableProvider` filter/limit pushdown contract integration.
  - Planner pushdown of supported scan/explicit filters into `NodeScanExec`.
  - Single-scan limit pushdown when all filters are pushdown-eligible.
- Remaining:
  - Lance-backed predicate/projection pushdown path.
  - Storage-native filtering beyond in-memory batch filtering.

**Implementation sketch (remaining):**
- Add a storage-backed scanner adapter with Lance-native predicate/projection pushdown.
- Keep current in-memory scan path as fallback.

**Effort:** Large

---

### C2. Idempotent Load + Upsert Semantics (`@key` first)
**Status:** Partial (core behavior done)

**Why critical:** Needed for real ingest workflows and duplicate prevention.

**Approach:**
- Done:
  - `@key`-driven node merge with stable IDs.
  - Existing non-keyed type preservation when absent from incoming keyed load.
  - Edge endpoint remap + edge dedup by `(src,dst)` (no multigraph).
  - Duplicate key and null key validation.
- Remaining:
  - Lance `MergeInsertBuilder`-based storage-native merge path.
  - `@unique` semantics beyond current keyed-path checks.

**API alignment (remaining):**
- Use `lance::dataset::MergeInsertBuilder` / `WhenMatched` / `WhenNotMatched` once storage-native merge is added.

**Effort:** Medium-Large

---

### C3. Secondary Index Foundation
**Status:** Not started

**Why critical:** Enables point lookups/traversal speedups once C1 is in place.

**Approach:**
- Add index policy in schema:
  - auto-index `@key`
  - optional `@index`
- Build indexes during load/migration for participating columns.
- Ensure scan path can benefit from index-backed predicate execution.

**Dependency note:**
- Index creation APIs rely on `lance-index` types (`IndexType`, scalar params). Keep explicit dependency wiring clear in workspace.

**Effort:** Medium

---

### C4. Delete API (CLI-level) + Edge Cascade Safety
**Status:** Done (v0 CLI + storage path)

**Why critical:** Completes minimum write lifecycle (insert/upsert/delete).

**Approach:**
- Implemented:
  - CLI delete command: `nanograph delete <db_path> --type <NodeType> --where <predicate>`
  - Node delete with edge cascade (`src_id`/`dst_id`) in storage path.
  - Manifest and Lance dataset rewrite after delete.
- Remaining:
  - richer predicate grammar / query-aligned delete expressions.

**Effort:** Medium

---

### High

### H1. Uniqueness Enforcement (`@key`, `@unique`)
**Status:** Partial

**Approach:**
- Done:
  - Duplicate `@key` detection for existing and incoming keyed batches.
  - Null `@key` rejection.
- Remaining:
  - `@unique` support.
  - Cross-type/index-backed enforcement.
  - Full typed diagnostics for all uniqueness violations.

**Effort:** Medium

---

### H2. Incremental Load Modes
**Status:** Not started

**Approach:**
- Add explicit load mode flags: `overwrite` (default), `append`, `merge`.
- Guard unsafe combos (e.g., append on keyed datasets without uniqueness checks).

**Effort:** Small-Medium

---

### H3. Query Mutation Surface (Phase after CLI writes)
**Status:** Not started

**Approach:**
- Start with CLI writes first (C2/C4).
- Then add query-level `insert/update/delete` grammar and typed lowering.
- Reuse existing storage mutation primitives.

**Effort:** Large

---

### H4. Bounded and Variable-Length Traversal
**Status:** Not started

**Approach:**
- Add bounded quantifier syntax first (e.g., `{1,3}`).
- Then add iterative fixed-point execution for unbounded reachability.

**Effort:** Large

---

## Medium

| Feature | Notes |
|---------|-------|
| Vector search | Strong differentiator once write + index foundations are stable |
| Full-text search | Candidate via inverted index path |
| Transaction model for general writes | Migration flow is transactional; general load/delete/upsert still needs explicit transaction semantics |
| Additional scalar types | Timestamp with timezone and collections are most practical next |

---

## Recommended Implementation Order

```
Phase 1 — Read-path foundation (partially complete)
  C1. Storage-aware node scans (pushdown)

Phase 2 — Core write lifecycle (mostly complete)
  C2. Idempotent load + upsert
  C4. Delete + edge cascade
  H1. Uniqueness enforcement

Phase 3 — Index and scale
  C3. Secondary indexes (key/index annotations)
  H2. Incremental load modes

Phase 4 — Language expansion
  H3. Query mutations
  H4. Bounded/variable-length traversal

Phase 5 — Specialized capabilities
  Vector search
  Full-text search
  Broader transaction semantics
```

---

## Success Criteria (per phase)

- **Phase 1:** equality-filter queries avoid full table materialization for eligible predicates. (Partially met)
- **Phase 2:** repeated keyed loads are idempotent; delete cascades preserve graph consistency. (Met for current CLI/storage scope)
- **Phase 3:** key lookups show measurable speedup vs baseline scans.
- **Phase 4:** mutation queries typecheck and execute with same safety guarantees as CLI writes.
- **Phase 5:** optional advanced search features integrate without regressing core query latency.

---

## Explicit Corrections From Previous Proposal

- Load is now **overwrite + keyed merge** (depending on schema annotations/data shape).
- Bounded expansion syntax is **not implemented** today.
- Typechecking coverage is **T1-T9**, not T1-T12.
- Index integration is **not a medium bolt-on** without read-path pushdown.
- Write-query examples should use actual Lance APIs (`MergeInsertBuilder`, `Dataset::delete`, write builders), not non-existent shortcuts.
