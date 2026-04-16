v1.2.0

- Canonical stable release for the new Lance 4 / `NamespaceLineage` storage line. `v1.2.0` makes lineage-native CDC the default for new graphs, upgrades the storage engine to Lance 4, replaces `check` with `lint`, and aligns the CLI and SDK surfaces around the new graph version model.

- New default storage generation for fresh graphs:
  - new databases now initialize as `NamespaceLineage`
  - committed graph state is stored in Lance, not `graph.manifest.json`
  - managed imported media is stored in `__blob_store`
  - legacy v3 databases are no longer opened directly for normal reads and writes

- Lineage-native CDC is now the primary rail for new graphs:
  - new graphs commit through `__graph_tx`, `__graph_deletes`, and `__graph_snapshot`
  - insert and update change rows are reconstructed from Lance lineage
  - delete payloads are preserved in `__graph_deletes`
  - public CDC now uses `graph_version` as the version unit for new storage

- Lance 4 storage alignment is now in production:
  - table writes use namespace-backed Lance 4 datasets
  - storage is pinned to Lance data format `2.2`
  - stable row ids are enabled for new tables
  - cleanup and doctor understand retained lineage windows and namespace-backed snapshots

- Storage migration paths are now first-class:
  - `nanograph storage migrate --target lance-v4` remains available for the intermediate namespace rail
  - `nanograph storage migrate --target lineage-native` creates a fresh `NamespaceLineage` graph with a new CDC epoch
  - migrated graphs keep a backup of the previous storage root

- Query tooling changed:
  - `nanograph check` has been replaced by `nanograph lint`
  - `lint` combines strict per-query validation with whole-file query coverage checks
  - when `--schema` is provided, lint validates against that schema directly instead of the current DB schema

- SDK and CLI surfaces were aligned to the new CDC model:
  - TypeScript and Swift SDKs now expose the current `changes()` contract
  - CLI `changes` and docs use `graph_version` language for new storage
  - migration and doctor output were updated to reflect `NamespaceLineage`

- Coverage and validation expanded:
  - namespace-lineage CLI e2e coverage now checks migration, CDC row shape, deterministic ordering, cleanup retention, and delete tombstones
  - JS SDK open path was verified against migrated graphs
  - release docs, operator docs, and user docs were updated to the current storage and lint behavior

- Release/distribution cleanup:
  - version bumped to `1.2.0` across Rust crates, npm package metadata, and Swift packaging examples
  - this release supersedes `v1.1.2` as the canonical stable release line
