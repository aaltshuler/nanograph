# nanograph

On-device property graph database. No server, no cloud, no connection strings. Think DuckDB, but for graphs.

Schema-as-code, compile-time validated, Arrow-native.

## Use cases

- **Local first context graph**
- **Memory layer for AI agents**
- **Dependency & lineage graph**
- **Personal knowledge graph (PKM)**
- **Indexing & ontology layer for Obsidian**
- **Analytics & domain exploration**

## Key benefits

- **Schema-as-code** -- schema files live in your repo, version with git, review in PRs. No "property not found" at runtime. Ever.
- **Local-first** -- no server. A library you link or a CLI you run. Works offline, works in CI, works on a plane.
- **CLI workflow** -- `nanograph init` / `load` / `check` / `run`. DB as a build artifact, not a service.
- **Graph-native operators** -- traversal, neighbor expansion, and path primitives are first-class, not bolted onto relational joins.
- **Columnar execution** -- batch/vectorized through Arrow RecordBatches. Results integrate naturally with the Arrow ecosystem.
- **Built-in CDC** -- every mutation is logged to a commit-gated journal. Replay decision traces, sync downstream, or audit changes from any version.
- **Fast** -- Rust, Arrow columnar execution, Lance storage with scalar indexes. Sub-millisecond opens, vectorized scans, zero cold-start.

## Quick start

```bash
# Build
cargo build

# Initialize a database from a schema
nanograph init my.nano --schema schema.pg

# Load data
nanograph load my.nano --data data.jsonl --mode overwrite

# Typecheck queries
nanograph check --db my.nano --query queries.gq

# Run a query
nanograph run --db my.nano --query queries.gq --name my_query
```

See `examples/starwars/` for a ready-to-run demo (66 nodes, 146 edges, 27 queries).

## Schema language

Schema files are source code — version them with git, review them in PRs, catch relationship bugs at compile time.

```
node Character {
    slug: String @key
    name: String
    alignment: enum(hero, villain, neutral)
    era: enum(prequel, original, sequel)
    tags: [String]?
}

node Film {
    slug: String @key
    name: String
    release_date: Date
}

edge DebutsIn: Character -> Film
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

Mutations are first-class:

```
query add_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query update_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}

query remove_person($name: String) {
    delete Person where name = $name
}
```

## Change tracking

Every mutation is logged to a commit-gated CDC journal. Replay changes since any version:

```bash
nanograph changes my.nano --since 3 --format jsonl
nanograph changes my.nano --from 1 --to 5 --format json
```

## Testing

```bash
# Engine + library tests
cargo test -p nanograph
cargo test -p nanograph-cli

# CLI end-to-end scenarios
bash tests/cli/run-cli-e2e.sh
```

## Origin

Inspired by using DuckDB as a ledger for Claude Code projects -- the same instant-open, zero-infra experience, applied to property graphs and agentic memory.
