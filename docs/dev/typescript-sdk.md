---
title: TypeScript SDK
slug: typescript-sdk
---

# TypeScript SDK

`nanograph-db` is a Node.js SDK that embeds NanoGraph directly in your process via napi-rs. Same engine as the CLI — no server, no IPC.

## Requirements

- Node.js >= 18
- Rust toolchain (builds from source on `npm install`)
- `protoc` (`brew install protobuf`)

## Install

```bash
npm install nanograph-db
```

## Quick start

```typescript
import { Database } from "nanograph-db";

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
  '{"type":"Knows","data":{"src":"Alice","dst":"Bob"}}',
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

const db = await Database.init("my.nano", schema);
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

await db.close();
```

## API

### `Database.init(dbPath, schemaSource)`

Create a new database from a schema string. Returns `Promise<Database>`.

### `Database.open(dbPath)`

Open an existing database. Returns `Promise<Database>`.

### `db.load(dataSource, mode)`

Load JSONL data into the database.

- `"overwrite"` — replace all data
- `"append"` — add rows
- `"merge"` — upsert by `@key`

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
// { nodeTypes: [{ name: "Person", typeId: ..., properties: [...] }], edgeTypes: [...] }
```

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

Options: `retainTxVersions` (number), `retainDatasetVersions` (number).

### `db.doctor()`

Run health checks on the database.

```typescript
const report = await db.doctor();
// { healthy: true, issues: [], warnings: [], datasetsChecked: 2, txRows: 1, cdcRows: 3 }
```

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
