# nanograph — Bug Report

Tested on macOS Darwin 25.1.0 (Apple Silicon).
Originally reproduced on v0.9.1 (homebrew) and v0.10.0 (debug build). The current branch now contains fixes for BUG-1 through BUG-5; this document is kept as a historical validation record plus fix summary.

All bugs can be replicated from `~/tests` using the provided scripts. The test database is created from `test.pg` (3 node types, 3 edge types with Date fields) and `combined.jsonl` (9 people, 4 projects, 6 skills, edges).

---

## BUG-1: migrate --auto-approve crashed for several additive schema changes

**Severity**: High
**Versions affected**: 0.9.1, 0.10.0
**Status**: Fixed on the current branch

### Description

`migrate --dry-run` reported all steps as `safe`, but `migrate --auto-approve` failed for several additive migrations with an Arrow column type mismatch during edge dataset rewrite.

The confirmed root cause was unstable edge property ordering during migration/storage reconstruction. Edge schemas were sometimes rebuilt from unordered property maps, which could swap edge property column positions and produce type mismatches during re-serialization. The specific error varied by schema:

| Schema | Error |
|--------|-------|
| test.pg (edges with `Date` fields) | `expected Date32 but found Utf8 at column index 3` |
| omni/revops (edges with `enum?` fields) | `Column 'outcome' is declared as non-nullable but contains null values` |
| omni/revops (after making enum nullable) | `expected List(Utf8) but found Utf8 at column index 3` |

Observed from the repro runs, the failures are all in the same family: migrated edge batch construction produces a schema/type mismatch during re-serialization.

### Original failing migrations on test.pg

| Change | Dry-run | Apply (before) | Apply (current branch) |
|--------|---------|----------------|------------------------|
| Add nullable field to node | safe | CRASH | OK |
| Expand node enum | safe | CRASH | OK |
| Add new node type | safe | CRASH | OK |
| Add new edge type | safe | OK | OK |
| Expand edge enum | safe | OK | OK |
| Multiple additive changes | safe | CRASH | OK |
| Add multiple nullable fields | safe | CRASH | OK |

### Original failing migrations on omni schema

| Change | Dry-run | Apply (before) | Apply (current branch) |
|--------|---------|----------------|------------------------|
| Add nullable `Vector(1536)?` to node types | safe | CRASH | OK |

### Replicate

```bash
cd ~/tests && ./bug-migrate.sh
```

Or manually:

```bash
cd ~/tests
rm -rf test.nano test.nano.migration.journal.json
ngdev init --schema test.pg test.nano
ngdev load --data combined.jsonl --mode overwrite test.nano

# Simplest failing case — add one nullable field:
sed 's/name: String$/name: String\n    displayName: String?/' test.pg > test.nano/schema.pg

ngdev migrate --dry-run test.nano    # says "safe"
ngdev migrate --auto-approve test.nano  # crashes
```

### Fix summary

The migration path now preserves edge property order deterministically by carrying Arrow edge schemas in declaration/IR order instead of reconstructing them from unordered property maps. A focused regression test now covers additive node-property migration on a schema with `edge WorksOn { role: enum(...), startedAt: Date }`, and the full external `bug-migrate.sh` matrix is green.

### Secondary issue: failed migration leaves the database blocked

After a crash, a `*.migration.journal.json` file could be left in `PREPARED` state. All subsequent operations failed with:

```
manifest error: incomplete migration journal at test.nano.migration.journal.json
  (state PREPARED); manual recovery required
```

This is partially fixed on the current branch:
- `PREPARED` and `ABORTED` journals are now auto-cleaned when the live DB is intact and no backup sidecar remains
- `APPLYING` still requires manual recovery, because that state may reflect an interrupted rename/commit boundary

---

## BUG-2: Enum values were not validated on load

**Severity**: Medium — silent data corruption.
**Versions affected**: 0.10.0
**Status**: Fixed on the current branch

### Description

Loading a record with an enum value not defined in the schema succeeds silently. The invalid value is stored and returned by queries and exports without any error or warning.

The schema defines `role: enum(admin, member, guest)`. Loading a person with `role: "superadmin"` succeeds with exit 0. The value `superadmin` is stored verbatim.

### Replicate

```bash
cd ~/tests && ./bug-enum.sh
```

Or manually:

```bash
cd ~/tests
rm -rf test.nano test.nano.migration.journal.json
ngdev init --schema test.pg test.nano
ngdev load --data combined.jsonl --mode overwrite test.nano

echo '{"type":"Person","data":{"slug":"bad","name":"Bad","email":"bad@test.com","bio":"x","role":"superadmin","joinedAt":"2025-01-01"}}' > bad.jsonl
ngdev load --data bad.jsonl --mode append test.nano
# exit 0 — no error

ngdev export --db test.nano --format jsonl | grep '"slug":"bad"'
# shows "role":"superadmin" — invalid value stored
```

### Fix summary

Loads now validate enum membership before Arrow array construction and reject invalid values with an explicit schema-aware error, e.g.:

`storage error: invalid enum value 'superadmin' for Person.role (expected: admin, member, guest)`

---

## BUG-3: Wrong field types were silently coerced to null

**Severity**: Medium — silent data loss.
**Versions affected**: 0.10.0
**Status**: Fixed on the current branch

### Description

Loading a record where a field value has the wrong type succeeds silently when the field is nullable. The value is dropped and the field becomes `null`.

The schema defines `age: I32?`. Loading `age: "not-a-number"` (string instead of integer) succeeds with exit 0. The string is silently discarded and `age` becomes `null`.

### Replicate

```bash
cd ~/tests
rm -rf test.nano test.nano.migration.journal.json
ngdev init --schema test.pg test.nano
ngdev load --data combined.jsonl --mode overwrite test.nano

echo '{"type":"Person","data":{"slug":"bad","name":"Bad","email":"bad@test.com","bio":"x","age":"not-a-number","role":"member","joinedAt":"2025-01-01"}}' > bad.jsonl
ngdev load --data bad.jsonl --mode append test.nano
# exit 0 — no error

ngdev export --db test.nano --format jsonl | grep '"slug":"bad"'
# shows "age":null — string silently dropped
```

### Fix summary

The loader now rejects non-null values whose JSON type does not match the declared schema type, even when the field itself is nullable. Actual `null` values are still accepted for nullable fields.

---

## BUG-4: --json flag did not emit JSON on error paths

**Severity**: Low — breaks machine-readable contract.
**Versions affected**: 0.10.0
**Status**: Fixed on the current branch

### Description

The `--json` global flag guarantees machine-readable output on success, but on failure most subcommands fall back to plain-text errors on stderr with nothing on stdout.

**4a.** No JSON emitted on failure (stdout empty, stderr has plain text):
- `run --json` with missing query name
- `load --json` with bad data
- `describe --json` with non-existent db

**4b.** JSON emitted on stdout but plain-text also leaked to stderr:
- `check --json` with invalid queries

### Replicate

```bash
cd ~/tests

# 4a: stdout is empty
ngdev run --db test.nano --query test.gq --name nonexistent --json 2>/dev/null
# expected: {"status":"error","message":"query `nonexistent` not found"}

# 4b: stderr leaks plain text
ngdev check --db test.nano --query bad-queries.gq --json 2>&1 1>/dev/null
# shows "Error: 4 query(s) failed typecheck" on stderr
```

### Fix summary

The CLI now emits structured JSON on stdout for command failures in JSON mode, with no plain-text stderr leakage. `check --json` preserves its richer summary payload while still avoiding duplicate stderr/plain-text output.

---

## BUG-5: Append-mode load failed on edge datasets with Date fields

**Severity**: Medium — append mode broken for edges with Date properties.
**Versions affected**: 0.10.0
**Status**: Fixed on the current branch

### Description

`--mode append` fails when the data contains edges with `Date`-typed properties. The same data loads fine with `--mode overwrite`. On the repro fixture, the failure is:

`storage error: invalid Date literal 'lead'`

That strongly suggests append-mode edge batch reconstruction is misaligning the `role` enum column with the `startedAt: Date` column.

### Replicate

```bash
cd ~/tests
rm -rf test.nano test.nano.migration.journal.json
ngdev init --schema test.pg test.nano
ngdev load --data test.jsonl --mode overwrite test.nano   # works
ngdev load --data stress-data.jsonl --mode append test.nano  # fails
# storage error: invalid Date literal 'lead'
```

### Fix summary

Append-mode edge merges now match edge property columns by field name instead of column position. That prevents `role` / `startedAt` swaps when existing and incoming edge batches carry the same properties in different orders. The provided append repro now succeeds.

---

## Relation between BUG-1 + BUG-5

These two bugs were related but not identical:

- BUG-1 came from unstable edge schema/property ordering during migration/storage reconstruction
- BUG-5 came from append-mode edge property merging by column position instead of field name

Both manifested as edge-batch type confusion, especially on schemas mixing `enum(Utf8)` and `Date32` edge properties.

---

## Scripts

| Script | What it tests |
|--------|---------------|
| `~/tests/bug-migrate.sh` | BUG-1: migration crash (7 variants) |
| `~/tests/bug-enum.sh` | BUG-2: enum validation bypass |
| `~/tests/bugs.sh` | All bugs in one run |

## Resolved

- **Edge export uses slugs** — earlier versions exported edges with internal numeric IDs instead of schema `@key` references. Fixed in the current CLI line; `export --format jsonl` now emits portable edge endpoints via `from` / `to`.
