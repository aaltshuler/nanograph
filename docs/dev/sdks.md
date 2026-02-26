---
title: SDKs
slug: sdks
---

# SDK Strategy

Expose the Rust core to JS/TS and Swift via FFI. Keep SDKs embedded and local-first: no service process, no GraphQL adapter.

## Direction

- Keep TypeScript SDK **in this repo** (`crates/nanograph-ts`) as a first-party optional package.
- Build Swift SDK as **in-repo C ABI** (`crates/nanograph-ffi`) and layer Swift wrappers on top.
- Avoid duplicating database/query logic in SDKs. Bindings call `nanograph` crate internals directly.

This matches usage reality: most users use CLI/core, while SDKs remain tightly versioned and tested against the same commit.

## Workspace Layout

```text
crates/
  nanograph/            # core library
  nanograph-cli/        # CLI
  nanograph-ts/         # Node/Bun SDK (napi-rs)
  nanograph-ffi/        # C ABI for Swift and native clients
```

## TypeScript SDK (Current)

`crates/nanograph-ts` is a napi-rs binding over `nanograph`.

### API surface

```ts
Database.init(dbPath: string, schemaSource: string): Promise<Database>
Database.open(dbPath: string): Promise<Database>
db.load(dataSource: string, mode: "overwrite" | "append" | "merge"): Promise<void>
db.run(querySource: string, queryName: string, params?: Record<string, unknown>): Promise<RunResult>
db.check(querySource: string): Promise<CheckResult[]>
db.describe(): Promise<DescribeResult>
db.compact(options?: CompactOptions): Promise<CompactResult>
db.cleanup(options?: CleanupOptions): Promise<CleanupResult>
db.doctor(): Promise<DoctorResult>
db.close(): void
```

`run()` returns:
- read query: array of row objects
- mutation query: `{ affectedNodes, affectedEdges }`

### Packaging model

- npm package: `nanograph-db`
- prebuilt `.node` artifacts per target with `@napi-rs/cli`
- typed surface is maintained in `index.d.ts` (not generated from Rust signatures)

## Swift SDK (Current)

`crates/nanograph-ffi` exposes a C ABI so Swift can call NanoGraph without adding a new Rust binding framework dependency.
`crates/nanograph-ffi/swift` provides a minimal Swift Package wrapper over that ABI.

### Exported C API

Declared in `crates/nanograph-ffi/include/nanograph_ffi.h`:

- lifecycle:
  - `nanograph_db_init`
  - `nanograph_db_open`
  - `nanograph_db_close`
  - `nanograph_db_destroy`
- operations:
  - `nanograph_db_load`
  - `nanograph_db_run`
  - `nanograph_db_check`
  - `nanograph_db_describe`
  - `nanograph_db_compact`
  - `nanograph_db_cleanup`
  - `nanograph_db_doctor`
- helpers:
  - `nanograph_last_error_message`
  - `nanograph_string_free`

Conventions:
- `*_run/check/describe/compact/cleanup/doctor` return heap-allocated JSON C strings (`char *`) on success.
- caller must free returned strings with `nanograph_string_free`.
- status-returning calls return `0` on success, `-1` on failure.
- on failure, read error text via `nanograph_last_error_message()`.

### Why C ABI first

- Works immediately with Swift via bridging headers/module maps.
- No extra Rust dependencies beyond current workspace stack.
- Keeps SDK path stable in restricted/offline build environments.

## Shared Backend Flow

Both SDK layers call the same core pipeline:

- parse query/schema
- typecheck against catalog
- lower to IR
- execute against `Database` storage/snapshot
- serialize rows to JSON via `nanograph::json_output`

No duplicate planner/storage semantics between CLI and SDKs.

## Build

### TypeScript SDK

```bash
cargo build -p nanograph-ts
cd crates/nanograph-ts
npm run build:debug
npm test
```

### Swift FFI SDK

```bash
cargo build -p nanograph-ffi
```

Artifacts are produced under `target/debug/`:
- macOS static lib: `libnanograph_ffi.a`
- macOS dynamic lib: `libnanograph_ffi.dylib`

### Swift Package Wrapper

```bash
cargo build -p nanograph-ffi --release
cd crates/nanograph-ffi/swift
swift build
```

## Next Phase (Optional)

If Swift API ergonomics becomes the bottleneck, add a higher-level generator layer (for example UniFFI/cargo-swift) on top of `nanograph-ffi` semantics. Keep the core contract stable first.
