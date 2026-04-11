---
title: TypeScript SDK
slug: typescript-sdk
---

# TypeScript SDK

`nanograph-db` is a Node.js SDK that embeds nanograph directly in your process via napi-rs. Same engine as the CLI — no server, no IPC.

## Requirements

- Node.js >= 18
- Supported native targets: macOS `arm64` / `x64`, Linux `arm64` / `x64` with glibc, Windows `x64` MSVC
- Rust toolchain + `protoc` only if you are rebuilding from monorepo sources or targeting an unsupported platform

## Install

```bash
npm install nanograph-db
```

Published packages use bundled native binaries on supported targets. Linux musl is not supported.

## Quick start

```typescript
import { Database, decodeArrow } from "nanograph-db";

const schema = `
node Person {
  name: String @key
  age: I32?
}

edge Knows: Person -> Person
`;

const data = [
  '{"type":"Person","data":{"name":"Alice","age":30}}',
  '{"type":"Person","data":{"name":"Bob","age":25}}',
  '{"edge":"Knows","from":"Alice","to":"Bob"}',
].join("\n");

const queries = `
query allPeople() {
  match { $p: Person }
  return { $p.name as name, $p.age as age }
  order { $p.name asc }
}

query byName($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name as name, $p.age as age }
}

query addPerson($name: String, $age: I32) {
  insert Person { name: $name, age: $age }
}
`;

const db = await Database.openInMemory(schema);
await db.load(data, "overwrite");

// Read query
const rows = await db.run(queries, "allPeople");
// [{ name: "Alice", age: 30 }, { name: "Bob", age: 25 }]

// Parameterized query
const alice = await db.run(queries, "byName", { name: "Alice" });
// [{ name: "Alice", age: 30 }]

// Mutation
const result = await db.run(queries, "addPerson", { name: "Carol", age: 28 });
// { affectedNodes: 1, affectedEdges: 0 }

// Columnar read path for vector-heavy or large result sets
const arrow = await db.runArrow(queries, "allPeople");
const table = decodeArrow(arrow);
const arrowRows = table.toArray();

await db.close();
```

## API

### `Database.init(dbPath, schemaSource)`

Create a new database from a schema string. Returns `Promise<Database>`.

### `Database.open(dbPath)`

Open an existing database. Returns `Promise<Database>`.

### `Database.openInMemory(schemaSource)`

Create a tempdir-backed database with automatic cleanup when the last handle closes. Returns `Promise<Database>`.

### `db.load(dataSource, mode)`

Load JSONL data into the database.

- `"overwrite"` — replace all data
- `"append"` — add rows
- `"merge"` — upsert by `@key`

### `db.loadFile(dataPath, mode)`

Load JSONL data from a file path.

- uses the reader-based streaming ingest path
- preferred for large datasets and embedding-heavy loads
- supports the same `"overwrite"`, `"append"`, and `"merge"` modes

### `db.loadRows(rows, mode)`

Load programmatic node/edge rows without building JSONL yourself. This is the preferred SDK path when you are constructing rows in memory or using media helper values.

```typescript
import { mediaFile, mediaUri } from "nanograph-db";

await db.loadRows(
  [
    {
      type: "PhotoAsset",
      data: {
        slug: "hero",
        uri: mediaFile("/absolute/path/hero.jpg", "image/jpeg"),
      },
    },
    {
      edge: "HasPhoto",
      from: "rocket",
      to: "hero",
    },
  ],
  "overwrite",
);
```

Helper exports:

- `mediaFile(path, mimeType?)`
- `mediaBase64(base64, mimeType?)`
- `mediaUri(uri, mimeType?)`

### `db.run(querySource, queryName, params?)`

Execute a named query. Returns `Promise<Record<string, unknown>[]>` for read queries or `Promise<MutationResult>` for mutations.

```typescript
// Read
const rows = await db.run(queries, "allPeople");

// With params
const rows = await db.run(queries, "byName", { name: "Alice" });

// Mutation
const result = await db.run(queries, "addPerson", { name: "Dave", age: 40 });
// { affectedNodes: 1, affectedEdges: 0 }
```

### `db.runArrow(querySource, queryName, params?)`

Execute a named read query and return an Arrow IPC stream as a `Buffer`.

```typescript
const arrow = await db.runArrow(queries, "allPeople");
const table = decodeArrow(arrow);
const rows = table.toArray();
```

Use this path for large result sets and especially for vector-heavy reads. JSON remains the ergonomic default, but Arrow is much faster for large returned embeddings.

### `db.check(querySource)`

Typecheck all queries against the database schema.

```typescript
const checks = await db.check(queries);
// [{ name: "allPeople", kind: "read", status: "ok" }, ...]
```

### `db.describe()`

Return schema introspection.

```typescript
const schema = await db.describe();
// {
//   nodeTypes: [{
//     typeId: 1,
//     name: "Person",
//     description: "...",
//     instruction: "...",
//     keyProperty: "name",
//     uniqueProperties: [],
//     outgoingEdges: [...],
//     incomingEdges: [...],
//     properties: [{ propId: 1, name: "name", type: "String", ... }]
//   }],
//   edgeTypes: [{
//     typeId: 2,
//     name: "Knows",
//     description: "...",
//     instruction: "...",
//     endpointKeys: { src: "name", dst: "name" },
//     properties: [...]
//   }]
// }
```

The describe payload includes schema `@description(...)` / `@instruction(...)` metadata, stable `typeId` / `propId` identifiers, derived key-property summaries, relationship hints, and `mediaMimeProp` for `@media_uri(...)` fields. This is the preferred machine-readable introspection surface for agents and SDK consumers.

### `db.embed(options?)`

Materialize `@embed(...)` properties using the same provider/env configuration as the CLI.

```typescript
const result = await db.embed({
  typeName: "PhotoAsset",
  property: "embedding",
  onlyNull: true,
  dryRun: false,
});
```

Options: `typeName`, `property`, `onlyNull`, `limit`, `reindex`, `dryRun`.

### `db.compact(options?)`

Compact Lance datasets to reduce fragmentation.

```typescript
const result = await db.compact({ targetRowsPerFragment: 1024 });
```

Options: `targetRowsPerFragment` (number), `materializeDeletions` (boolean), `materializeDeletionsThreshold` (number 0.0-1.0).

### `db.cleanup(options?)`

Prune old dataset versions and log entries.

```typescript
const result = await db.cleanup({ retainTxVersions: 10 });
```

Options:

- `retainTxVersions` (number) — primary retention control for new `NamespaceLineage` graphs
- `retainDatasetVersions` (number) — legacy/advanced override; mostly relevant for older storage generations

### `db.doctor()`

Run health checks on the database.

```typescript
const report = await db.doctor();
// {
//   healthy: true,
//   issues: [],
//   warnings: [],
//   manifestDbVersion: 1,
//   datasetsChecked: 2,
//   txRows: 1,
//   cdcRows: 3
// }
```

`healthy` stays `true` when only warnings are present. On new `NamespaceLineage` graphs, `lineageShadow` is `null` because lineage is the real CDC rail. Structured `lineageShadow` details are only reported for legacy `V4Namespace` databases.

### `db.changes(options?)`

Read committed lineage-native change rows.

```typescript
const rows = await db.changes({ since: 0 });
// [{
//   graph_version: 1,
//   tx_id: "manifest-1",
//   committed_at: "1700000000",
//   change_kind: "insert",
//   entity_kind: "node",
//   type_name: "Person",
//   table_id: "nodes/00000001",
//   rowid: 1,
//   entity_id: 1,
//   logical_key: "id=1",
//   row: { name: "Alice", age: 30 },
//   previous_graph_version: null
// }]
```

Options:

- `since` — shorthand for `graph_version > since`
- `from` / `to` — explicit open/closed window `(from, to]`

Returned rows use the public CDC contract directly:

- `graph_version` instead of legacy `db_version`
- no `seq_in_tx`
- deterministic ordering by `graph_version`, `entity_kind`, `type_name`, `rowid`/`logical_key`, `change_kind`
- insert/update rows include the current row image
- delete rows include the tombstoned last-visible row image

### `db.isInMemory()`

Return `true` when the handle was created with `Database.openInMemory(...)`.

### `db.close()`

Close the database and release resources. Safe to call multiple times.

## Parameter types

| Schema type | JavaScript type |
|------------|----------------|
| `String` | `string` |
| `I32`, `I64` | `number` |
| `U32`, `U64` | `number` (or `string` for values > 2^53) |
| `F32`, `F64` | `number` |
| `Bool` | `boolean` |
| `Date` | `string` (`"2026-01-15"`) |
| `DateTime` | `string` (`"2026-01-15T10:00:00Z"`) |
| `Vector(dim)` | `number[]` |
| `enum(...)` | `string` |

## Reopening a database

```typescript
const db = await Database.init("my.nano", schema);
await db.load(data, "overwrite");
await db.close();

// Later
const db2 = await Database.open("my.nano");
const rows = await db2.run(queries, "allPeople");
await db2.close();
```

## Large embedding workloads

For large graph loads, prefer `loadFile(...)` over building one giant JSONL string in JS. For programmatic media-heavy loads, prefer `loadRows(...)`. For large returned vectors, prefer `runArrow(...)` plus `decodeArrow(...)` over `run(...)`.
