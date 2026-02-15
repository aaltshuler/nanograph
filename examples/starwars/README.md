# Star Wars Knowledge Graph (Merged)

Typed property graph of the Star Wars universe with a rich relationship model and realistic temporal anchor via films.

## Files

| File | Description |
|------|-------------|
| `starwars.pg` | Merged schema (9 node types, 25 edge types, slug `@key` identity) |
| `starwars.jsonl` | Merged data (~66 nodes, ~146 edges) |
| `starwars.gq` | Read + mutation query suite (legacy reads + film/date + mutations) |

## Notable Modeling Choices

- `slug: String @key` on every node type for stable identity.
- Enums on categorical fields (`alignment`, `era`, `side`, `climate`).
- `Film.release_date: Date` is the only date field in the example.
- Character and battle timeline context is expressed through edges:
  - `DebutsIn: Character -> Film`
  - `DepictedIn: Battle -> Film`

## Quick Start

```bash
nanograph init sw.nanograph --schema examples/starwars/starwars.pg
nanograph load sw.nanograph --data examples/starwars/starwars.jsonl --mode overwrite
nanograph check --db sw.nanograph --query examples/starwars/starwars.gq
nanograph run --db sw.nanograph --query examples/starwars/starwars.gq --name jedi
nanograph run --db sw.nanograph --query examples/starwars/starwars.gq --name film_timeline
```

## Mutation + CDC Walkthrough

```bash
# mutations
nanograph run --db sw.nanograph --query examples/starwars/starwars.gq --name add_character
nanograph run --db sw.nanograph --query examples/starwars/starwars.gq --name update_character --param slug=ezra-bridger
nanograph run --db sw.nanograph --query examples/starwars/starwars.gq --name delete_character --param slug=ezra-bridger

# inspect changes
nanograph changes sw.nanograph --from 2 --to 4 --format json
nanograph changes sw.nanograph --since 3 --format jsonl
```

## Introspection

```bash
nanograph version --db sw.nanograph
nanograph describe --db sw.nanograph --format table
nanograph export --db sw.nanograph --format jsonl
```
