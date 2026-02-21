# nanograph-ts — TypeScript SDK

napi-rs bindings that wrap the `nanograph` Rust library crate. Same query pipeline, storage engine, and type system as the CLI — no logic duplication.

## Architecture

Single `Database` class exposed to JS/TS via napi-rs v2. Interior mutability (`Arc<tokio::sync::Mutex<Database>>`) for safe async mutable access. Results returned as plain JS objects via `serde_json::Value` bridge. Query and schema source passed as strings (user controls file I/O).

**Read path**: lock held only to clone catalog + take snapshot, then released before query execution so reads don't block mutations.

**Mutation path**: lock held for the entire operation (typecheck → lower → execute → persist).

## Crate layout

```
crates/nanograph-ts/
├── Cargo.toml              # cdylib crate, depends on nanograph + napi
├── build.rs                # napi_build::setup()
├── package.json            # @nanograph/sdk, @napi-rs/cli v3
├── index.js                # Platform-aware native binding loader
├── index.d.ts              # TypeScript type definitions
├── src/
│   ├── lib.rs              # JsDatabase struct + all #[napi] methods
│   └── convert.rs          # JS↔Rust type conversion (params, options)
└── __test__/
    └── index.spec.mjs      # Node.js integration tests (node:test)
```

## API

```typescript
export class Database {
  static init(dbPath: string, schemaSource: string): Promise<Database>
  static open(dbPath: string): Promise<Database>
  load(dataSource: string, mode: string): Promise<void>
  run(querySource: string, queryName: string, params?: unknown): Promise<unknown>
  check(querySource: string): Promise<unknown>
  describe(): Promise<unknown>
  compact(options?: unknown): Promise<unknown>
  cleanup(options?: unknown): Promise<unknown>
  doctor(): Promise<unknown>
  close(): void
}
```

### `Database.init(dbPath, schemaSource)`

Create a new database directory from a schema string. Returns a `Database` instance.

### `Database.open(dbPath)`

Open an existing `.nano` database. Returns a `Database` instance.

### `db.load(dataSource, mode)`

Load JSONL data. `mode` is `"overwrite"`, `"append"`, or `"merge"`. The `dataSource` is the JSONL content as a string (same format as `nanograph load`):

```jsonl
{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
```

### `db.run(querySource, queryName, params?)`

Execute a named query from query source text. Params is an optional object mapping parameter names to values.

**Read queries** return an array of row objects:
```js
const rows = await db.run(queries, "findPeople", { minAge: 21 });
// [{ name: "Alice", age: 30 }, { name: "Bob", age: 25 }]
```

**Mutation queries** return `{ affectedNodes, affectedEdges }`:
```js
const result = await db.run(queries, "insertPerson", { name: "Eve", age: 28 });
// { affectedNodes: 1, affectedEdges: 0 }
```

Parameter type conversion is guided by query param declarations:
- `String` → JS string
- `I32`, `I64`, `U32` → JS number (integer) or decimal string
- `U64` → JS number (integer) or decimal string (currently capped at `i64::MAX`)
- `F32`, `F64` → JS number (float)
- `Bool` → JS boolean
- `Date`, `DateTime` → JS string (ISO format)
- `Vector(N)` → JS array of numbers
- Untyped params → inferred from JS value type

For exact 64-bit integers, pass decimal strings when values exceed `Number.MAX_SAFE_INTEGER`.
Query result cells for `Int64`/`UInt64` beyond JS safe integer range are returned as strings.

### `db.check(querySource)`

Typecheck all queries against the schema. Returns:
```js
[
  { name: "findPeople", kind: "read", status: "ok" },
  { name: "broken", status: "error", error: "..." }
]
```

### `db.describe()`

Schema introspection:
```js
{
  nodeTypes: [{ name: "Person", typeId: 123, properties: [...] }],
  edgeTypes: [{ name: "Knows", srcType: "Person", dstType: "Person", ... }]
}
```

### `db.compact(options?)`

Compact Lance datasets. Options: `{ targetRowsPerFragment, materializeDeletions, materializeDeletionsThreshold }`.

### `db.cleanup(options?)`

Prune old dataset versions and transaction logs. Options: `{ retainTxVersions, retainDatasetVersions }`.

### `db.doctor()`

Health check: `{ healthy: true, issues: [], warnings: [], datasetsChecked, txRows, cdcRows }`.

### `db.close()`

Synchronously release the database. Fails if another operation is in progress.

## Build

```bash
# Rust compilation
cargo build -p nanograph-ts

# Native addon (produces .node file + index.js/index.d.ts)
cd crates/nanograph-ts
npm install
npm run build:debug    # dev
npm run build          # release

# Tests
npm test
```

Requires `protoc` (for Lance) and Node.js >= 18.

## Shared code

`nanograph::json_output` module (extracted from CLI) provides `record_batches_to_json_rows` and `array_value_to_json` — used by both the CLI and the TS SDK to convert Arrow RecordBatches to JSON.

## Error handling

All `NanoError` variants map to `napi::Error` via `to_string()`. Schema parse errors, type errors, and storage errors all surface as rejected promises with descriptive messages.

## napi-rs notes

- Uses `napi` v2 + `napi-derive` v2 (workspace deps), `@napi-rs/cli` v3 (npm dev dep)
- `#![recursion_limit = "256"]` required due to deep type chains in Lance/moka dependencies
- `crate-type = ["cdylib"]` — mandatory for `.node` addon output
- Targets: darwin-arm64, darwin-x64, linux-gnu-x64, linux-gnu-arm64, win32-msvc-x64
