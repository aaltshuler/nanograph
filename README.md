# nanograph

[![Crates.io](https://img.shields.io/crates/v/nanograph)](https://crates.io/crates/nanograph)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

On-device graph database for agents and humans. One CLI. One folder. Schema-as-code. No server.

Built on Rust, Lance, Arrow, and DataFusion.

**[Website](https://nanograph.io)** | **[Docs](https://nanograph.io/docs)** | **[Examples](examples/starwars/)**

## Why nanograph

- **On-device** -- no server, no cloud, no Docker. Everything stays on your machine.
- **Schema-as-code** -- `.pg` files version-controlled in git, enforced at query time. No "property not found" at runtime.
- **Built for agents** -- Claude reads, writes, and traverses the graph natively.
- **Fast** -- Rust + Lance + Arrow columnar execution. Sub-millisecond opens, ACID, time-travel.
- **Full search stack** -- full-text, semantic, fuzzy, BM25, hybrid, graph-constrained reranking.
- **Built-in CDC** -- every mutation logged to a ledger. Replay, audit, sync from any version.
- **Zero setup** -- create a graph and start querying. Delete and recreate in seconds.

## Install

```bash
brew install nanograph/tap/nanograph
```

Or from source (requires [Rust](https://www.rust-lang.org/tools/install) 1.85+ and `protoc`):

```bash
cargo install nanograph-cli
```

### SDKs

- **TypeScript/Node.js** -- `npm install nanograph-db`
- **Swift** -- Swift Package via the [nanograph-ffi](crates/nanograph-ffi/swift/) wrapper

## Quick start

```bash
nanograph init my.nano --schema schema.pg
nanograph load my.nano --data data.jsonl --mode overwrite
nanograph check --db my.nano --query queries.gq
nanograph run --db my.nano --query queries.gq --name my_query
```

See [`examples/starwars/`](examples/starwars/) for a ready-to-run demo with 66 nodes, 146 edges, and 30 queries.

## Use cases

- Context graphs and decision traces for AI agents
- Agentic memory with typed, sub-100ms local queries
- Personal knowledge graphs with schema enforcement
- Dependency and lineage modeling
- Feature generation for ML pipelines

## License

MIT
