# NanoGraph

A local-first, embedded, strongly-typed property graph database in Rust. Think SQLite/DuckDB, but for graphs.

Arrow-native execution, Lance/Parquet storage, schema-first design.

## Key ideas

- **Schema-first** -- define node/edge types and properties up front. Queries are validated before they run.
- **Embedded** -- no server. A library you link or a CLI you run. Single-machine, offline-capable.
- **CLI workflow** -- `nanograph init` / `load` / `check` / `run`. DB as a build artifact, not a service.
- **Graph-native operators** -- traversal, neighbor expansion, and path primitives are first-class, not bolted onto relational joins.
- **Columnar execution** -- batch/vectorized through Arrow RecordBatches. Results integrate naturally with the Arrow ecosystem.
- **Lance + Parquet storage** -- Lance as the operational format, Parquet as the interchange format.

## Quick start

```bash
# Build
cargo build

# Initialize a database from a schema
nanograph init my.nanograph --schema schema.pg

# Load data
nanograph load my.nanograph --data data.jsonl

# Typecheck queries
nanograph check --db my.nanograph --query queries.gq

# Run a query
nanograph run --db my.nanograph --query queries.gq --name my_query
```

## Query language

Typed Datalog with GraphQL-shaped syntax. Edge traversal as predicates, compile-time type checking.

```
query friends_of($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $f
    }
    return { $f.name, $f.age }
}
```

## Origin

Born from using DuckDB as a ledger for Claude Code projects -- the same instant-open, single-file, zero-infra experience, applied to property graphs and agentic memory.
