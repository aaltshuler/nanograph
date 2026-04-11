# nanograph Swift Wrapper (Local)

This package wraps the `nanograph-ffi` C ABI and links against the Rust library built in this repo.

## Prerequisites

Build the Rust FFI library first:

```bash
cd /Users/andrew/code/nanograph
cargo build -p nanograph-ffi
```

Expected artifacts:

- `target/debug/libnanograph_ffi.a`
- `target/debug/libnanograph_ffi.dylib`

## Build the Swift package

```bash
cd /Users/andrew/code/nanograph/crates/nanograph-ffi/swift
swift build
swift test
```

Debug/test builds link `../../../target/debug`; Swift release builds link `../../../target/release`.

## API shape

`Database` provides:

- `create(dbPath:schemaSource:)`
- `open(dbPath:)`
- `openInMemory(schemaSource:)`
- `load(dataSource:mode:)`
- `loadFile(dataPath:mode:)`
- `loadRows(_:mode:)`
- `run(querySource:queryName:params:)`
- `runArrow(querySource:queryName:params:)`
- `check(querySource:)`
- `describe()`
- `embed(options:)`
- `compact(options:)`
- `cleanup(options:)`
- `doctor()`
- `changes(options:)`
- `isInMemory()`
- `close()`

All read APIs decode C-returned JSON into Foundation values (`Any`) by default.
Typed decode overloads are available for `run/check/describe/doctor/changes`. `runArrow(...)` returns Arrow IPC as `Data`, and the package also exports:

- `decodeArrow(_ data: Data) -> Any`
- `decodeArrow(_:from:) -> T`

## Media nodes and external assets

nanograph stores media as external URIs. The Swift wrapper now includes:

- `MediaRef.file(...)`
- `MediaRef.base64(...)`
- `MediaRef.uri(...)`
- `LoadRow.node(...)`
- `LoadRow.edge(...)`
- `Database.loadRows(_:mode:)`

Example:

```swift
import NanoGraph

let schema = """
node PhotoAsset {
  slug: String @key
  uri: String @media_uri(mime)
  mime: String
}

node Product {
  slug: String @key
  name: String
}

edge HasPhoto: Product -> PhotoAsset
"""

let queries = """
query products_from_image_search($q: String) {
  match {
    $product: Product
    $product hasPhoto $img
  }
  return { $product.slug as product, $img.slug as image, $img.uri as uri }
}
"""

let db = try Database.create(dbPath: "/tmp/app.nano", schemaSource: schema)

try db.loadRows([
  .node(type: "PhotoAsset", data: [
    "slug": "space",
    "uri": MediaRef.file("/absolute/path/space.jpg", mimeType: "image/jpeg"),
  ]),
  .node(type: "Product", data: [
    "slug": "rocket",
    "name": "Rocket Poster",
  ]),
  .edge(type: "HasPhoto", from: "rocket", to: "space", data: [:]),
], mode: .overwrite)

let rows = try db.run(
  querySource: queries,
  queryName: "products_from_image_search",
  params: ["q": "space scene"]
)
```

`describe()` now includes `mediaMimeProp` for `@media_uri(...)` properties, so SDK callers can detect media URI fields and their sibling mime properties.
`changes()` returns the lineage-native CDC contract for new `NamespaceLineage` graphs.
OpenAI embeddings remain text-only today. Use Gemini when you want nanograph to generate media embeddings from `@media_uri(...)` sources.
