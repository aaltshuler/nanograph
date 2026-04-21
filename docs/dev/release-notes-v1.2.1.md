v1.2.1

- Patch release for the `NamespaceLineage` line. `v1.2.1` hardens namespace-backed storage, fixes schema-migration CDC continuity, and closes a sparse edge-delete identity bug.

- Namespace-backed local databases are more robust:
  - fixed local DB roots whose paths contain spaces
  - fixed local DB roots whose paths contain URI-reserved characters like `#` and `%`
  - removed the remaining ad hoc `file://` string handling in the namespace-open path

- `NamespaceLineage` schema migration no longer writes impossible CDC windows:
  - touched tables now start a fresh CDC epoch after namespace-backed schema migration
  - `nanograph changes`, `doctor`, and `cleanup` continue to work after migration
  - post-migration lineage reads no longer point at unreachable pre-swap table versions

- Namespace commit publishing now includes the managed blob store:
  - fresh `NamespaceLineage` graphs correctly publish `__blob_store`
  - managed media imports continue to work on newly initialized databases

- Sparse edge deletes now resolve endpoint identities correctly:
  - delete predicates on `from` / `to` now resolve node endpoints by the declared node `@key`, not a hardcoded `name` column
  - missing edge endpoints now behave as a 0-affected no-op instead of an execution error for equality deletes

- Release metadata updated:
  - version bumped to `1.2.1` across Rust crates, npm package metadata, and Swift packaging examples
