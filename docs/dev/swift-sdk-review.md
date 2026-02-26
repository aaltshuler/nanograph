# Swift SDK Review

## Overall Assessment

The SDK is well-built and follows the same thin-wrapper strategy as the TS SDK. The code is clean, the FFI boundary is handled correctly, and the test suite is comprehensive.

## Correctness Issues

### 1. `nanograph_db_init` creates two tokio runtimes (lib.rs:518-523)

```rust
let runtime = Runtime::new()?;          // runtime #1 — used for init
let db = runtime.block_on(Database::init(...))?;
drop(runtime);                          // dropped
let handle = NanoGraphHandle::new(db)?;  // creates runtime #2
```

Same pattern in `nanograph_db_open` (line 543-548). A temporary runtime is created just for the async `init`/`open` call, then dropped, and `NanoGraphHandle::new` creates a second one. This is wasteful — two runtime startups per database open. Fix: create the runtime once and reuse it:

```rust
let runtime = Runtime::new()?;
let db = runtime.block_on(Database::init(...))?;
Ok(Box::into_raw(Box::new(NanoGraphHandle { runtime, db: Mutex::new(Some(db)) })))
```

### 2. Thread-local `LAST_ERROR` is unsound for multi-threaded Swift callers (lib.rs:22-24)

`nanograph_last_error_message()` returns a raw pointer into the thread-local `RefCell`. If the Swift caller reads the error from thread A, but the Rust error was set on thread B, the pointer is null/stale. The current Swift wrapper is single-threaded (NSLock serializes), so this works in practice, but the C API's contract is fragile. The TS SDK avoids this by returning errors inline via napi `Result`.

Known limitation of thread-local error patterns in C APIs — not a bug per se, but worth documenting in the header.

## Safety Concerns

### 3. `@unchecked Sendable` is optional but worth considering (NanoGraph.swift:21)

`Database` is a reference type with internal locking and is usable as-is, but it does not conform to `Sendable`. In strict-concurrency contexts, callers can still see warnings/errors when moving `Database` across actor/task boundaries. Marking it `@unchecked Sendable` can improve ergonomics for highly-concurrent clients:

```swift
public final class Database: @unchecked Sendable {
```

This is not a correctness bug in the SDK itself; it is an API ergonomics choice depending on whether you want to support cross-actor usage cleanly.

### 4. `with_handle` has dead code (lib.rs:424-425)

```rust
let handle = unsafe { handle.as_ref() }.ok_or_else(...)?;
```

`as_ref()` on a non-null raw pointer returns `Some` by definition (never `None`). The `.ok_or_else()` is dead code. Not harmful, just unnecessary.

## API Design

### 5. `run()` returns `Any` — loses type safety

The untyped `run()` → `Any` forces callers into `as?` casting. The typed `run<T: Decodable>` overloads are good, but the `Any` return type is the primary API surface. Consider making the typed version the default and the `Any` version the escape hatch (e.g., `runRaw()`).

### 6. Missing `export`, `changes`, `migrate`, `delete` commands

Both the FFI and Swift SDK lack these CLI commands that are available in the binary. This is consistent with the TS SDK (also missing these), so this appears to be a deliberate scope decision. Worth noting if you plan to expand the SDK surface.

## Code Quality

### 7. Duplicate header file (drift risk)

`include/nanograph_ffi.h` and `swift/Sources/CNanoGraph/include/nanograph_ffi.h` are identical copies. If the C API changes, both must be updated. Consider a symlink or a build step that copies one from the other.

### 8. `NSLock.withLock` extension reimplements Foundation API (NanoGraph.swift:237-242)

`NSLock` already provides `withLock(_:)` on this deployment target, so the custom extension is redundant. This is a cleanup/refactor opportunity, not a functional issue.

## Summary

| Priority | Issue | Location |
|----------|-------|----------|
| Medium | Double runtime creation in init/open | lib.rs:518-523, 543-548 |
| Low | Optional `@unchecked Sendable` for actor-boundary ergonomics | NanoGraph.swift:21 |
| Low | Thread-local error pattern fragile for multi-threaded callers | lib.rs:22-24 |
| Low | Duplicate header file (drift risk) | two `nanograph_ffi.h` copies |
| Low | Custom `NSLock.withLock` extension (redundant with Foundation API) | NanoGraph.swift:237-242 |
| Low | Dead `.ok_or_else()` after `as_ref()` on non-null ptr | lib.rs:425 |
| Nit | `Any` return types as primary API | NanoGraph.swift |
