# Backlog

## CLI

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
