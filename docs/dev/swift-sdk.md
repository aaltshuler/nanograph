---
title: Swift SDK
slug: swift-sdk
---

# Swift SDK

Swift Package that embeds NanoGraph via C ABI (`nanograph-ffi`). Same engine as the CLI — no server, no IPC.

## Requirements

- macOS 13+
- Swift 6.0+
- Rust toolchain
- `protoc` (`brew install protobuf`)

## Build

```bash
# 1. Build the Rust FFI library
cargo build -p nanograph-ffi --release

# 2. Build the Swift package
cd crates/nanograph-ffi/swift
swift build
```

Produces `libnanograph_ffi.a` (static) and `libnanograph_ffi.dylib` (dynamic) in `target/release/`.

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
    #"{"type":"Knows","data":{"src":"Alice","dst":"Bob"}}"#,
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

let db = try Database.create(dbPath: "my.nano", schemaSource: schema)
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

try db.close()
```

## API

### `Database.create(dbPath:schemaSource:)`

Create a new database from a schema string. Throws on invalid schema.

### `Database.open(dbPath:)`

Open an existing database. Throws if path doesn't exist.

### `db.load(dataSource:mode:)`

Load JSONL data into the database.

```swift
try db.load(dataSource: jsonlString, mode: .overwrite)
```

`LoadMode`: `.overwrite`, `.append`, `.merge`.

### `db.run(querySource:queryName:params:)`

Execute a named query. Returns `Any` — array of dicts for reads, dict for mutations.

```swift
// Untyped
let rows = try db.run(querySource: queries, queryName: "allPeople")

// With params
let rows = try db.run(querySource: queries, queryName: "byName", params: ["name": "Alice"])
```

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
    struct TypeDef: Decodable { let name: String }
    let nodeTypes: [TypeDef]
    let edgeTypes: [TypeDef]
}
let schema = try db.describe(DescribeResult.self)
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
// ["healthy": true, "issues": [], "warnings": [], "datasetsChecked": 2, ...]
```

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
