# Backlog

## CLI

- [x] `nanograph describe` — print schema definition for a type or all types
- [x] `--format json` output (array of objects, in addition to existing `table|csv|jsonl`)
- [x] `--json` on all commands (`check`, `migrate`, `load`, `init`, `delete`) — machine-readable output for agents
- [x] `nanograph export --format jsonl` — dump full graph to stdout for git-friendly snapshots
- [ ] Progress bars on `nanograph load` for large datasets (indicatif)
- [x] `nanograph version --db` — print current Lance version per dataset type
- [ ] locked node types
- [ ] Encryption

## Query

- [ ] Vector search — `Vector(dim)` type, `@embed(prop)`, `nearest()` ordering, Lance ANN (see docs/archive/vector.md)
- [ ] Hybrid search — combine vector similarity + full-text search (BM25) + graph traversal, Lance FTS index + reranking

## Storage

- [x] Content-addressed node IDs — derive from `@key` hash instead of auto-increment u64
- [ ] Multimedia content + embeddings

## Schema / Types

- [x] Enum types (e.g. `status: enum(open, closed, blocked)`)
- [x] Array/list properties (e.g. `tags: [String]`)
- [x] Date/DateTime types (chrono — needed for date literals, filtering, ordering)

## Performance

- [ ] Benchmark suite + targets (criterion) — NodeScan 10K, 2-hop expansion 10K, JSONL load 100K

## UX

- [ ] Nudges for agents (proactivity)
- [ ] ASCII Graphs for visualizing data

## SDK

- [ ] Rust SDK — `nanograph-rs` crate for Rust applications
- [ ] Python SDK — `nanograph-py` package for Python applications
- [ ] TypeScript SDK — `nanograph-ts` package for TypeScript/JavaScript applications
- [ ] Go SDK — `nanograph-go` package for Go applications
