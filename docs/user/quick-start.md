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
    name: String
    species: String
    alignment: String
    rank: String?
}

node Faction {
    name: String
}

edge AffiliatedWith: Character -> Faction
edge HasMentor: Character -> Character
```

## 2. Prepare data

Create `starwars.jsonl`:

```jsonl
{"type": "Faction", "data": {"name": "Jedi Order"}}
{"type": "Faction", "data": {"name": "Sith Order"}}
{"type": "Character", "data": {"name": "Yoda", "species": "Unknown", "alignment": "Hero", "rank": "Jedi Grand Master"}}
{"type": "Character", "data": {"name": "Obi-Wan Kenobi", "species": "Human", "alignment": "Hero", "rank": "Jedi Master"}}
{"type": "Character", "data": {"name": "Anakin Skywalker", "species": "Human", "alignment": "Hero", "rank": "Jedi Knight"}}
{"type": "Character", "data": {"name": "Luke Skywalker", "species": "Human", "alignment": "Hero", "rank": "Jedi Knight"}}
{"type": "Character", "data": {"name": "Emperor Palpatine", "species": "Human", "alignment": "Villain", "rank": "Emperor"}}
{"edge": "AffiliatedWith", "from": "Yoda", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Obi-Wan Kenobi", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Anakin Skywalker", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Luke Skywalker", "to": "Jedi Order"}
{"edge": "AffiliatedWith", "from": "Emperor Palpatine", "to": "Sith Order"}
{"edge": "HasMentor", "from": "Obi-Wan Kenobi", "to": "Yoda"}
{"edge": "HasMentor", "from": "Anakin Skywalker", "to": "Obi-Wan Kenobi"}
{"edge": "HasMentor", "from": "Luke Skywalker", "to": "Obi-Wan Kenobi"}
{"edge": "HasMentor", "from": "Luke Skywalker", "to": "Yoda"}
```

## 3. Initialize and load

```bash
nanograph init starwars.nanograph --schema starwars.pg
nanograph load starwars.nanograph --data starwars.jsonl --mode overwrite
```

## 4. Write queries

Create `starwars.gq`:

```
// All Jedi
query jedi() {
    match {
        $c: Character
        $c affiliatedWith $f
        $f.name = "Jedi Order"
    }
    return { $c.name, $c.rank }
    order { $c.name asc }
}

// Who trained whom
query students_of($name: String) {
    match {
        $m: Character { name: $name }
        $s hasMentor $m
    }
    return { $s.name }
    order { $s.name asc }
}

// Characters not in any faction
query unaffiliated() {
    match {
        $c: Character
        not { $c affiliatedWith $_ }
    }
    return { $c.name }
}

// Faction sizes
query faction_sizes() {
    match {
        $c: Character
        $c affiliatedWith $f
    }
    return { $f.name, count($c) as members }
    order { members desc }
}
```

## 5. Run queries

```bash
# Validate first
nanograph check --db starwars.nanograph --query starwars.gq

# Run a query
nanograph run --db starwars.nanograph --query starwars.gq --name jedi

# With parameters
nanograph run --db starwars.nanograph --query starwars.gq --name students_of --param name="Yoda"

# Different output formats
nanograph run --db starwars.nanograph --query starwars.gq --name faction_sizes --format csv
```

## 6. Mutate data

Add mutation queries to `starwars.gq`:

```
query add_character($name: String, $species: String, $alignment: String) {
    insert Character { name: $name, species: $species, alignment: $alignment }
}
```

```bash
nanograph run --db starwars.nanograph --query starwars.gq --name add_character \
    --param name="Ahsoka Tano" --param species="Togruta" --param alignment="Hero"
```

## Next steps

- See [cli-reference.md](cli-reference.md) for all commands and options
- See `examples/starwars/` in the repo for a full Star Wars graph (59 nodes, 120 edges, 25 queries)
