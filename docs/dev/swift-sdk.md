---
title: Swift SDK
slug: swift-sdk
---

# Swift SDK

Swift Package that embeds nanograph via C ABI (`nanograph-ffi`). Same engine as the CLI — no server, no IPC.

## Requirements

- macOS 13+
- Swift 6.0+
- Rust toolchain
- `protoc` (`brew install protobuf`)

## Build

```bash
# 1. Build the Rust FFI library
cargo build -p nanograph-ffi

# 2. Build or test the Swift package
cd crates/nanograph-ffi/swift
swift build
swift test
```

The Swift package links `target/debug` for debug/test builds and `target/release` for release builds.

## Quick start

```swift
import NanoGraph

let schema = """
node Person {
  name: String @key
  age: I32?
}

edge Knows: Person -> Person
"""

let data = [
    #"{"type":"Person","data":{"name":"Alice","age":30}}"#,
    #"{"type":"Person","data":{"name":"Bob","age":25}}"#,
    #"{"edge":"Knows","from":"Alice","to":"Bob"}"#,
].joined(separator: "\n")

let queries = """
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
"""

let db = try Database.openInMemory(schemaSource: schema)
try db.load(dataSource: data, mode: .overwrite)

// Untyped read
let raw = try db.run(querySource: queries, queryName: "allPeople")
let rows = raw as! [[String: Any]]
// [["name": "Alice", "age": 30], ["name": "Bob", "age": 25]]

// Typed read
struct PersonRow: Decodable {
    let name: String
    let age: Int?
}
let people = try db.run([PersonRow].self, querySource: queries, queryName: "allPeople")
// [PersonRow(name: "Alice", age: 30), PersonRow(name: "Bob", age: 25)]

// Parameterized query
let alice = try db.run(querySource: queries, queryName: "byName", params: ["name": "Alice"])

// Mutation
let result = try db.run(querySource: queries, queryName: "addPerson",
                        params: ["name": "Carol", "age": 28])
// ["affectedNodes": 1, "affectedEdges": 0]

// Arrow IPC bytes for large read results
let arrow = try db.runArrow(querySource: queries, queryName: "allPeople")
let arrowRows = try decodeArrow(arrow) as! [[String: Any]]

try db.close()
```

## API

### `Database.create(dbPath:schemaSource:)`

Create a new database from a schema string. Throws on invalid schema.

### `Database.open(dbPath:)`

Open an existing database. Throws if path doesn't exist.

### `Database.openInMemory(schemaSource:)`

Create a tempdir-backed database with automatic cleanup when the handle is released.

### `db.load(dataSource:mode:)`

Load JSONL data into the database.

```swift
try db.load(dataSource: jsonlString, mode: .overwrite)
```

`LoadMode`: `.overwrite`, `.append`, `.merge`.

### `db.loadFile(dataPath:mode:)`

Load JSONL data from a file path using the reader-based streaming ingest path.

```swift
try db.loadFile(dataPath: "/tmp/data.jsonl", mode: .overwrite)
```

### `db.loadRows(_:mode:)`

Load programmatic node/edge rows without building JSONL yourself. This is the preferred Swift path for in-memory row construction and media references.

```swift
try db.loadRows([
    .node(type: "PhotoAsset", data: [
        "slug": "hero",
        "uri": MediaRef.file("/absolute/path/hero.jpg", mimeType: "image/jpeg"),
    ]),
    .edge(type: "HasPhoto", from: "rocket", to: "hero"),
], mode: .overwrite)
```

Supported helpers:

- `MediaRef.file(...)`
- `MediaRef.base64(...)`
- `MediaRef.uri(...)`

### `db.run(querySource:queryName:params:)`

Execute a named query. Returns `Any` — array of dicts for reads, dict for mutations.

```swift
// Untyped
let rows = try db.run(querySource: queries, queryName: "allPeople")

// With params
let rows = try db.run(querySource: queries, queryName: "byName", params: ["name": "Alice"])
```

### `db.runArrow(querySource:queryName:params:)`

Execute a named read query and return Arrow IPC bytes as `Data`.

```swift
let arrow = try db.runArrow(querySource: queries, queryName: "allPeople")
```

Use this for large result sets and vector-heavy reads.

### `decodeArrow(_ data: Data)`

Decode Arrow IPC bytes into Foundation values.

```swift
let arrow = try db.runArrow(querySource: queries, queryName: "allPeople")
let rows = try decodeArrow(arrow) as! [[String: Any]]
```

### `decodeArrow(_:from:)`

Typed decode overload for Arrow IPC bytes.

```swift
struct PersonRow: Decodable {
    let name: String
    let age: Int?
}

let arrow = try db.runArrow(querySource: queries, queryName: "allPeople")
let rows = try decodeArrow([PersonRow].self, from: arrow)
```

This helper is a convenience path back to Swift values. If you need direct columnar Arrow consumption, keep using the raw `Data` payload with your own Arrow reader.

### `db.run(_:querySource:queryName:params:)`

Typed overload — decodes result directly into a `Decodable` type.

```swift
struct PersonRow: Decodable {
    let name: String
    let age: Int?
}
let people = try db.run([PersonRow].self, querySource: queries, queryName: "allPeople")
```

### `db.check(querySource:)`

Typecheck all queries against the database schema.

```swift
let checks = try db.check(querySource: queries) as! [[String: Any]]
// [["name": "allPeople", "kind": "read", "status": "ok"], ...]

// Typed
struct CheckRow: Decodable {
    let name: String
    let kind: String
    let status: String
    let error: String?
}
let checks = try db.check([CheckRow].self, querySource: queries)
```

### `db.describe()`

Return schema introspection.

```swift
let schema = try db.describe() as! [String: Any]
// ["nodeTypes": [...], "edgeTypes": [...]]

// Typed
struct DescribeResult: Decodable {
    struct NodeType: Decodable {
        let name: String
        let description: String?
        let instruction: String?
        let keyProperty: String?
    }
    struct EdgeType: Decodable {
        let name: String
        let description: String?
        let instruction: String?
    }
    let nodeTypes: [NodeType]
    let edgeTypes: [EdgeType]
}
let schema = try db.describe(DescribeResult.self)
```

The describe payload includes schema `@description(...)` / `@instruction(...)` metadata, stable `typeId` / `propId` identifiers, derived key-property summaries, endpoint-key metadata, relationship hints, and `mediaMimeProp` for `@media_uri(...)` fields. Use this as the canonical machine-readable schema surface from Swift.

### `db.embed(options:)`

Materialize `@embed(...)` properties using the same provider/env configuration as the CLI.

```swift
let result = try db.embed(options: [
    "typeName": "PhotoAsset",
    "property": "embedding",
    "onlyNull": true,
])
```

### `db.embed(_:options:)`

Typed overload for embed results.

```swift
let result = try db.embed(EmbedResult.self, options: EmbedOptions(
    typeName: "PhotoAsset",
    property: "embedding",
    onlyNull: true
))
```

### `db.compact(options:)`

Compact Lance datasets.

```swift
let result = try db.compact(options: ["targetRowsPerFragment": 1024])
```

Options: `targetRowsPerFragment` (Int), `materializeDeletions` (Bool), `materializeDeletionsThreshold` (Double 0.0-1.0).

### `db.cleanup(options:)`

Prune old dataset versions and log entries.

```swift
let result = try db.cleanup(options: ["retainTxVersions": 10])
```

Options: `retainTxVersions` (Int), `retainDatasetVersions` (Int).

### `db.doctor()`

Run health checks.

```swift
let report = try db.doctor() as! [String: Any]
// [
//   "healthy": true,
//   "issues": [],
//   "warnings": [],
//   "manifestDbVersion": 1,
//   "datasetsChecked": 2,
//   ...
// ]
```

`healthy` remains `true` when only warnings are present. Rebuildable graph-mirror conditions are surfaced as warnings rather than hard failures.

### `db.isInMemory()`

Return `true` when the handle was created with `Database.openInMemory(...)`.

### `db.close()`

Close the database and release resources. Idempotent — safe to call multiple times. Using the database after close throws `NanoGraphError.message("Database is closed")`.

The database handle is also cleaned up automatically on `deinit`.

## Error handling

All methods throw `NanoGraphError.message(String)`.

```swift
do {
    let _ = try Database.open(dbPath: "nonexistent.nano")
} catch let error as NanoGraphError {
    print(error.errorDescription!) // prints the error message
}
```

## Thread safety

`Database` uses `NSLock` internally to serialize all handle operations. Safe to use from multiple threads, but calls are serialized — not concurrent.

## Large embedding workloads

For large graph loads, prefer `loadFile(...)` over building one giant JSONL string in Swift. For programmatic media-heavy loads, prefer `loadRows(...)`. For large returned vectors, prefer `runArrow(...)` over `run(...)`.

## Reopening a database

```swift
let db = try Database.create(dbPath: "my.nano", schemaSource: schema)
try db.load(dataSource: data, mode: .overwrite)
try db.close()

// Later
let db2 = try Database.open(dbPath: "my.nano")
let rows = try db2.run(querySource: queries, queryName: "allPeople")
try db2.close()
```
