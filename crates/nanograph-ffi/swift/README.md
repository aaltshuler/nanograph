# NanoGraph Swift Wrapper (Local)

This package wraps the `nanograph-ffi` C ABI and links against the Rust library built in this repo.

## Prerequisites

Build the Rust FFI library first:

```bash
cd /Users/andrew/code/nanograph
cargo build -p nanograph-ffi --release
```

Expected artifacts:

- `target/release/libnanograph_ffi.a`
- `target/release/libnanograph_ffi.dylib`

## Build the Swift package

```bash
cd /Users/andrew/code/nanograph/crates/nanograph-ffi/swift
swift build
```

## API shape

`Database` provides:

- `create(dbPath:schemaSource:)`
- `open(dbPath:)`
- `load(dataSource:mode:)`
- `run(querySource:queryName:params:)`
- `check(querySource:)`
- `describe()`
- `compact(options:)`
- `cleanup(options:)`
- `doctor()`
- `close()`

All read APIs decode C-returned JSON into Foundation values (`Any`) by default.
Typed decode overloads are available for `run/check/describe`.
