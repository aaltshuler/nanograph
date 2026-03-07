# Backlog

## Embeddable API

See [docs/dev/embeddable-api.md](embeddable-api.md) for full design.

- [x] Core embeddable API and shareable `Database` handle
- [x] Tempdir-backed in-memory open
- [x] Streaming ingest and large-embedding load path
- [x] TypeScript SDK on the current embeddable surface
- [x] Swift SDK on the current embeddable surface

### Deferred
- [ ] `on_change()` callback registration (broadcast channel)
- [ ] Custom UDF registration via DataFusion (separate design doc)
- [ ] Pure in-memory backend that bypasses Lance / filesystem metadata

## CLI Ergonomics

- [ ] Structured JSON errors — when `--json` is set, emit `{"error": "...", "kind": "..."}` instead of stderr text. Critical for agent consumption.
- [ ] `--dry-run` on `load` and mutation `run` — show type-checked plan, affected types, estimated row counts without writing. (`migrate --dry-run` already exists.)
- [ ] Consistent `--json` vs `--format` — pick one mechanism. Currently `--json` is global and `--format` is per-command; some commands check both. Unify so `--json` globally switches all output (including errors) to structured JSON.
- [ ] Schema introspection — `nanograph describe --type Person` to dump a single type's properties, annotations, and Arrow schema. Current `describe` shows manifest-level info only.
- [ ] Inline query — `--query-inline 'insert Person { name: $name }'` or `--query -` for stdin, so one-liners don't need a `.gq` file.
- [ ] Stdin for load — `--data -` reads JSONL from stdin for pipeline composition (`cat data.jsonl | nanograph load my.nano --data - --mode append`).

## CLI (existing)

- [x] `nanograph describe` — print schema definition for a type or all types
- [x] `--format json` output (array of objects, in addition to existing `table|csv|jsonl`)
- [x] `--json` on all commands (`check`, `migrate`, `load`, `init`, `delete`) — machine-readable output for agents
- [x] `nanograph export --format jsonl` — dump full graph to stdout for git-friendly snapshots
- [x] `nanograph version --db` — print current Lance version per dataset type
- [ ] Progress bars on `nanograph load` for large datasets — stream row counts via indicatif during JSONL parse and Lance write phases
- [ ] Locked node types — mark types as read-only in schema (`@locked`?) to prevent mutation/delete via CLI or query mutations
- [ ] Encryption at rest — encrypt Lance datasets and JSONL logs on disk, key management via env var or keyring

## Query

- [x] Vector search — `Vector(dim)` type, `@embed(prop)`, `nearest()` ordering, text predicates (`search`, `fuzzy`, `match_text`)
- [x] Hybrid search — `bm25()` + `nearest()` + `rrf()` fusion, graph-scoped retrieval

## Storage

- [x] Content-addressed node IDs — derive from `@key` hash instead of auto-increment u64
- [ ] Multimedia content — store binary blobs (images, PDFs, audio) alongside graph nodes, auto-embed via multimodal models

## Schema / Types

- [x] Enum types (e.g. `status: enum(open, closed, blocked)`)
- [x] Array/list properties (e.g. `tags: [String]`)
- [x] Date/DateTime types (chrono — date literals, filtering, ordering)

## Performance

- [ ] Benchmark suite — criterion harnesses for NodeScan 10K, 2-hop expansion 10K, JSONL load 100K; track regressions in CI

## UX

- [ ] Agent nudges — proactive suggestions for agent how to make most of nanograph
- [ ] ASCII graph visualization — render subgraph topology in terminal output for `nanograph describe` or query results

## SDK

- [x] TypeScript SDK — `nanograph-db` npm package, napi-rs bindings for Node.js
- [x] Swift SDK — `nanograph-ffi` C ABI + Swift Package wrapper
- [ ] Python SDK — PyO3 bindings for Python applications
- [ ] Go SDK — CGo bindings for Go applications
