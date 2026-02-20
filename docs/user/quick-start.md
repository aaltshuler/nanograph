# Quick Start

Build a Star Wars knowledge graph in under 5 minutes.

## Prerequisites

- Rust toolchain (edition 2024)
- `protoc` (Protocol Buffers compiler)

```bash
cargo build -p nanograph-cli
```

## 1. Define a schema

Create `starwars.pg`:

```
node Character {
    name: String @key
    species: String
    alignment: String
}

node Faction {
    name: String @key
}

edge AffiliatedWith: Character -> Faction
edge HasMentor: Character -> Character
```

## 2. Prepare data

Create `starwars.jsonl`:

```jsonl
{"type": "Faction", "data": {"name": "Jedi Order"}}
{"type": "Faction", "data": {"name": "Sith Order"}}
{"type": "Character", "data": {"name": "Yoda", "species": "Unknown", "alignment": "Hero"}}
{"type": "Character", "data": {"name": "Obi-Wan Kenobi", "species": "Human", "alignment": "Hero"}}
{"type": "Character", "data": {"name": "Anakin Skywalker", "species": "Human", "alignment": "Hero"}}
{"type": "Character", "data": {"name": "Luke Skywalker", "species": "Human", "alignment": "Hero"}}
{"edge": "AffiliatedWith", "from": "Yoda", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Obi-Wan Kenobi", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Anakin Skywalker", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Luke Skywalker", "to": "Jedi Order"}
{"edge": "HasMentor", "from": "Obi-Wan Kenobi", "to": "Yoda"}
{"edge": "HasMentor", "from": "Anakin Skywalker", "to": "Obi-Wan Kenobi"}
{"edge": "HasMentor", "from": "Luke Skywalker", "to": "Obi-Wan Kenobi"}
```

Nodes use `"type"` + `"data"`. Edges use `"edge"` + `"from"` + `"to"` (matched by `@key` value).

## 3. Initialize and load

```bash
nanograph init starwars.nano --schema starwars.pg
nanograph load starwars.nano --data starwars.jsonl --mode overwrite
```

## 4. Write queries

Create `starwars.gq`:

```
query jedi() {
    match {
        $c: Character
        $c affiliatedWith $f
        $f.name = "Jedi Order"
    }
    return { $c.name }
    order { $c.name asc }
}

query students_of($name: String) {
    match {
        $m: Character { name: $name }
        $s hasMentor $m
    }
    return { $s.name }
    order { $s.name asc }
}
```

## 5. Run queries

```bash
# Validate
nanograph check --db starwars.nano --query starwars.gq

# Run a query
nanograph run --db starwars.nano --query starwars.gq --name jedi

# With parameters
nanograph run --db starwars.nano --query starwars.gq --name students_of --param name="Yoda"

# Different output formats
nanograph run --db starwars.nano --query starwars.gq --name jedi --format csv
```

## Next steps

- [Schema Language Reference](schema.md) — types, annotations, naming conventions
- [Query Language Reference](queries.md) — match, return, traversal, mutations
- [Search Guide](search.md) — text search, vector search, hybrid search
- [CLI Reference](cli-reference.md) — all commands and options
- [Star Wars Example](starwars-example.md) — worked example with search queries and real output
- [Context Graph Example](context-graph-example.md) — CRM/RevOps context graph case study
