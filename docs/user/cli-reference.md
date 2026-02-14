# CLI Reference

```
nanograph <command> [options]
```

## Commands

### `init`

Create a new database from a schema file.

```bash
nanograph init <db_path> --schema <schema.pg>
```

Creates the `<db_path>.nanograph/` directory with `schema.pg`, `schema.ir.json`, and an empty manifest.

### `load`

Load JSONL data into an existing database.

```bash
nanograph load <db_path> --data <data.jsonl> --mode <overwrite|append|merge>
```

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace all data for types present in the file |
| `append` | Add rows without deduplication |
| `merge` | Upsert by `@key` — update existing rows, insert new ones |

`merge` requires at least one node type in the schema to have a `@key` property.

### `check`

Parse and typecheck a query file without executing.

```bash
nanograph check --db <db_path> --query <queries.gq>
```

### `run`

Execute a named query.

```bash
nanograph run --db <db_path> --query <queries.gq> --name <query_name> [options]
```

| Option | Description |
|--------|-------------|
| `--format table\|csv\|jsonl` | Output format (default: `table`) |
| `--param key=value` | Query parameter (repeatable) |

Supports both read queries and mutation queries (`insert`, `update`, `delete`) in DB mode.

### `delete`

Delete nodes by predicate with automatic edge cascade.

```bash
nanograph delete <db_path> --type <NodeType> --where <predicate>
```

Predicate format: `property=value` or `property>=value`, etc.

All edges where the deleted node is a source or destination are automatically removed.

### `changes`

Read commit-gated CDC rows from the authoritative JSONL log.

```bash
nanograph changes <db_path> [--since <db_version> | --from <db_version> --to <db_version>] [--format jsonl|json]
```

### `compact`

Compact manifest-tracked Lance datasets and commit updated pinned versions.

```bash
nanograph compact <db_path> [--target-rows-per-fragment <n>] [--materialize-deletions <bool>] [--materialize-deletions-threshold <f32>]
```

### `cleanup`

Prune tx/CDC history and old Lance dataset versions while preserving replay/manifest correctness.

```bash
nanograph cleanup <db_path> [--retain-tx-versions <n>] [--retain-dataset-versions <n>]
```

### `doctor`

Validate manifest/dataset/log consistency and graph integrity.

```bash
nanograph doctor <db_path>
```

### `cdc-materialize`

Materialize visible CDC rows into derived Lance dataset `__cdc_analytics` for analytics acceleration.
This does not change `changes` semantics; JSONL remains authoritative.

```bash
nanograph cdc-materialize <db_path> [--min-new-rows <n>] [--force]
```

### `migrate`

Apply schema changes to an existing database.

```bash
nanograph migrate <db_path> [options]
```

Edit `<db_path>/schema.pg` first, then run migrate. The command diffs the old and new schema IR and generates a migration plan.

| Option | Description |
|--------|-------------|
| `--dry-run` | Show plan without applying |
| `--auto-approve` | Apply `confirm`-level steps without prompting |
| `--format table\|json` | Output format (default: `table`) |

Migration steps have safety levels:

| Level | Behavior |
|-------|----------|
| `safe` | Applied automatically (add nullable property, add new type) |
| `confirm` | Requires `--auto-approve` or interactive confirmation (drop property, rename) |
| `blocked` | Cannot be auto-applied (add non-nullable property to populated type) |

Use `@rename_from("old_name")` in the schema to track type/property renames.

## Schema annotations

| Annotation | Description |
|------------|-------------|
| `@key` | Primary key for merge/upsert. One per node type. Auto-indexed. |
| `@unique` | Uniqueness constraint. Enforced on load. Multiple per type. |
| `@index` | Creates a Lance scalar index for faster filtering. |
| `@rename_from("old")` | Tracks renames for schema migration. |

## Data format

JSONL with one record per line.

Nodes:
```json
{"type": "Person", "data": {"name": "Alice", "age": 30}}
```

Edges (matched by node name within source/destination types):
```json
{"edge": "Knows", "from": "Alice", "to": "Bob"}
```

Edges with properties:
```json
{"edge": "Knows", "from": "Alice", "to": "Bob", "data": {"since": "2020-01-01"}}
```

## Debug logging

```bash
RUST_LOG=debug nanograph run --db mydb.nanograph --query q.gq --name my_query
```
