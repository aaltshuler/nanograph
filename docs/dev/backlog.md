# Backlog

## CLI

- [ ] `nanograph describe` — print schema definition for a type or all types
- [ ] `--format json` output (array of objects, in addition to existing `table|csv|jsonl`)
- [ ] `--json` on all commands (`check`, `migrate`, `load`, `init`, `delete`) — machine-readable output for agents
- [ ] `nanograph export --format jsonl` — dump full graph to stdout for git-friendly snapshots
- [ ] Progress bars on `nanograph load` for large datasets (indicatif)
- [ ] `nanograph version --db` — print current Lance version per dataset type

## Query

- [ ] Vector search — `Vector(dim)` type, `@embed(prop)`, `nearest()` ordering, Lance ANN (see docs/archive/vector.md)
- [ ] Hybrid search — combine vector similarity + full-text search (BM25) + graph traversal, Lance FTS index + reranking

## Storage

- [ ] Single-file database format (like SQLite/DuckDB) — currently directory-based due to Lance
- [ ] Content-addressed node IDs — derive from `@key` hash instead of auto-increment u64

## Schema / Types

- [ ] Enum types (e.g. `status: enum(open, closed, blocked)`)
- [ ] Array/list properties (e.g. `tags: [String]`)
- [ ] Date/DateTime types (chrono — needed for date literals, filtering, ordering)

## Performance

- [ ] Benchmark suite + targets (criterion) — NodeScan 10K, 2-hop expansion 10K, JSONL load 100K
