#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"

DB="/tmp/maintenance_e2e.nanograph"
TMP_DIR="$(mktemp -d /tmp/maintenance_e2e.XXXXXX)"
SCHEMA="$TMP_DIR/schema.pg"
DATA_INIT="$TMP_DIR/init.jsonl"
DATA_APPEND="$TMP_DIR/append.jsonl"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

build_nanograph_binary "$ROOT"

cat > "$SCHEMA" << 'SCHEMA'
node Person {
    name: String @key
    age: I32?
}
SCHEMA

cat > "$DATA_INIT" << 'DATA'
{"type":"Person","data":{"name":"Alice","age":30}}
DATA

cat > "$DATA_APPEND" << 'DATA'
{"type":"Person","data":{"name":"Bob","age":29}}
DATA

rm -rf "$DB"

info "Initializing and loading maintenance test database..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$SCHEMA")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

LOAD1_JSON=$("$NG" --json load "$DB" --data "$DATA_INIT" --mode overwrite)
assert_contains "$LOAD1_JSON" '"status":"ok"' "overwrite load --json status"

LOAD2_JSON=$("$NG" --json load "$DB" --data "$DATA_APPEND" --mode append)
assert_contains "$LOAD2_JSON" '"status":"ok"' "append load --json status"

info "Running compact command..."
COMPACT_JSON=$("$NG" --json compact "$DB" --target-rows-per-fragment 1024)
assert_contains "$COMPACT_JSON" '"status":"ok"' "compact --json status"

info "Running doctor command..."
DOCTOR_JSON=$("$NG" --json doctor "$DB")
assert_contains "$DOCTOR_JSON" '"status":"ok"' "doctor --json status before cleanup"
assert_contains "$DOCTOR_JSON" '"healthy":true' "doctor reports healthy before cleanup"

info "Running cleanup command..."
CLEANUP_JSON=$("$NG" --json cleanup "$DB" --retain-tx-versions 2 --retain-dataset-versions 1)
assert_contains "$CLEANUP_JSON" '"status":"ok"' "cleanup --json status"
assert_contains "$CLEANUP_JSON" '"tx_rows_kept"' "cleanup reports tx retention stats"

info "Running CDC analytics materializer..."
CDC_MAT_JSON=$("$NG" --json cdc-materialize "$DB" --min-new-rows 1)
assert_contains "$CDC_MAT_JSON" '"status":"ok"' "cdc-materialize --json status"
assert_contains "$CDC_MAT_JSON" '"dataset_written":true' "cdc-materialize writes analytics dataset"
[ -d "$DB/__cdc_analytics" ] || fail "expected $DB/__cdc_analytics to exist"
pass "cdc analytics dataset exists"

CDC_MAT_SKIP_JSON=$("$NG" --json cdc-materialize "$DB" --min-new-rows 99999)
assert_contains "$CDC_MAT_SKIP_JSON" '"status":"ok"' "cdc-materialize threshold run status"
assert_contains "$CDC_MAT_SKIP_JSON" '"skipped_by_threshold":true' "cdc-materialize threshold skip"

DOCTOR_AFTER_JSON=$("$NG" --json doctor "$DB")
assert_contains "$DOCTOR_AFTER_JSON" '"status":"ok"' "doctor --json status after cleanup"
assert_contains "$DOCTOR_AFTER_JSON" '"healthy":true' "doctor reports healthy after cleanup"

CHANGES_JSON=$("$NG" changes "$DB" --since 0 --format json)
assert_contains "$CHANGES_JSON" '"db_version": 2' "changes keeps replay window events after cleanup"
assert_contains "$CHANGES_JSON" '"name": "Bob"' "changes includes append payload after cleanup"

echo ""
pass "maintenance CLI e2e passed"
