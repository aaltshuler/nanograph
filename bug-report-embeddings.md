# Bug: migrate fails on empty vector columns (IVF index on zero vectors)

**Date:** 2026-03-13
**Component:** `nanograph migrate` / `schema_ops.rs`
**Severity:** High — blocks all new vector column additions to existing databases

## Summary

`nanograph migrate --auto-approve` fails when adding `Vector(1536)` columns with `@index` to node types that have existing rows but no embedding data yet. The migration attempts to build an IVF-PQ index immediately, which requires k-means training — impossible with 0 vectors.

## Reproduction

```bash
# schema.pg defines: embedding: Vector(1536)? @embed(description) @index
# DB has 30 Opportunity rows, none with embeddings yet

ngdev migrate --dry-run
# Shows 5 safe AddProperty steps + 1 AlterPropertyNullability

ngdev migrate --auto-approve
# FAILS:
# lance error: create vector index `nano_927217ca_embedding_ivfpq_idx`
# on Opportunity.embedding failed: Unprocessable: KMeans cannot train
# 1 centroids with 0 vectors; choose a smaller K (< 0)
```

The migration is atomic — all 5 embedding columns roll back, none persist.

## Root Cause

In `crates/nanograph-cli/src/schema_ops.rs:26`, after adding a `Vector` column with `@index`, nanograph immediately tries to create an IVF-PQ index. Lance's k-means implementation requires at least K vectors to train centroids. With 0 populated vectors, this is impossible.

## Expected Behavior

Migration should succeed in two phases:

1. **Add column** — nullable `Vector(1536)` column added, no index yet
2. **Deferred indexing** — index created later when vectors are backfilled, either:
   - Automatically during `nanograph compact`
   - Via explicit `nanograph index` command
   - On first `nanograph load` that populates embeddings

The `--dry-run` output correctly identifies the steps as "safe" — the failure is unexpected.

## Workaround

Remove `@index` from vector fields in `schema.pg`, migrate (adds nullable columns without index), backfill embeddings, then re-add `@index` and migrate again to build the index.

## Feature Request: `nanograph embed` command

There is no built-in way to backfill embeddings for existing rows. nanograph needs a command like:

```bash
nanograph embed                          # Backfill all empty embedding columns
nanograph embed --type Signal            # Backfill a specific node type
nanograph embed --type Signal --reindex  # Backfill + rebuild vector index
```

This would:
1. Read the `@embed(field)` annotation to know which source field to embed
2. Find rows where the embedding column is null
3. Call the configured provider (from `nanograph.toml` — OpenAI text-embedding-3-small)
4. Write vectors back, respecting `batch_size` config
5. Optionally rebuild the IVF-PQ index after backfill

Without this, there's no clean path from "add vector column" to "searchable embeddings."

## Impact

Blocks semantic search (`search_signals`, `search_opportunities`, `search_projects`, `search_tasks`, `search_decisions`) on the Omni database. All 5 node types with `@embed` annotations are affected: Opportunity, Project, ActionItem, Decision, Signal.

## Bug 2: migrate silently skips Vector column additions

Even after clearing the cached schema state (`omni.nano/schema.pg`), `nanograph migrate` does not generate `AddProperty` steps for `Vector(1536)` columns. The diff engine appears to ignore Vector-typed fields entirely.

```bash
# spec/schema.pg has: embedding: Vector(1536)? @embed(description)
# DB has no embedding column on any node type

ngdev migrate --dry-run
# Only shows: AlterPropertyNullability — no AddProperty for vectors
```

The migration engine needs to support `Vector` as a first-class property type in schema diffs.

## Environment

```
nanograph build: dev (alias ngdev -> /Users/andrew/code/nanograph/target/debug/nanograph)
lance-index: 2.0.1
DB: omni.nano (db_version 42)
Schema hash: fc7d2702c9ec3007 -> 05d8c27c61f11461
```
