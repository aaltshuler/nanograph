# CLI Reference

```
nanograph <command> [options]
```

## Global options

| Option | Description |
|--------|-------------|
| `--json` | Emit machine-readable JSON output for supported commands |
| `--help` | Show help |
| `--version` | Show CLI version |

## Commands

### `version`

Show CLI version and optional database manifest/dataset version info.

```bash
nanograph version [--db <db_path>]
```

With `--db`, includes current manifest `db_version` and per-dataset Lance versions.

### `describe`

Describe schema + manifest summary for a database.

```bash
nanograph describe --db <db_path> [--format table|json]
```

### `export`

Export the full graph (nodes first, then edges) to stdout.

```bash
nanograph export --db <db_path> [--format jsonl|json]
```

### `init`

Create a new database from a schema file.

```bash
nanograph init <db_path> --schema <schema.pg>
```

Creates the `<db_path>/` directory with `schema.pg`, `schema.ir.json`, and an empty manifest.

### `load`

Load JSONL data into an existing database.

```bash
nanograph load <db_path> --data <data.jsonl> --mode <overwrite|append|merge>
```

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace the entire current graph snapshot with the loaded data |
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
| `--format table\|csv\|jsonl\|json` | Output format (default: `table`) |
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

## CDC semantics (time machine)

- Source of truth: CDC is read from `_cdc_log.jsonl`, gated by `_tx_catalog.jsonl` and manifest `db_version`.
- Commit visibility: only fully committed transactions at or below current manifest `db_version` are visible.
- Ordering: rows are emitted in logical commit order by `(db_version, seq_in_tx)`.
- Windowing:
  - `--since X` returns rows with `db_version > X`
  - `--from A --to B` returns rows in inclusive range `[A, B]`
- Crash/recovery safety: trailing partial JSONL lines are truncated on open/read reconciliation; orphan tx/cdc tail rows beyond manifest visibility are ignored/truncated.
- Retention impact: `nanograph cleanup --retain-tx-versions N` prunes old tx/cdc history, so time-machine replay is guaranteed only within retained versions.
- Analytics materialization: `nanograph cdc-materialize` builds derived Lance dataset `__cdc_analytics` for analytics acceleration, but does not change CDC correctness semantics.

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

Returns non-zero when issues are detected.

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

## Data format

JSONL with one record per line. **Nodes and edges use different key names** — mixing them up is a common error.

Nodes use `"type"` + `"data"`:
```json
{"type": "Person", "data": {"name": "Alice", "age": 30}}
```

Edges use `"edge"` + `"from"` + `"to"` (matched by node `@key` value within source/destination types):
```json
{"edge": "Knows", "from": "Alice", "to": "Bob"}
```

> **Common mistake:** Using `{"type": "Knows", "src": "Alice", "dst": "Bob"}` for edges. This is parsed as a node record and will fail with "unknown node type".

For each edge endpoint type, `@key` is required so `from`/`to` can be resolved.

Edges with properties:
```json
{"edge": "Knows", "from": "Alice", "to": "Bob", "data": {"since": "2020-01-01"}}
```

## Debug logging

```bash
RUST_LOG=debug nanograph run --db mydb.nano --query q.gq --name my_query
```

## See also

- [Schema Language Reference](schema.md) — types, annotations, naming conventions
- [Query Language Reference](queries.md) — match, return, traversal, mutations
- [Search Guide](search.md) — text search, vector search, embedding env vars
