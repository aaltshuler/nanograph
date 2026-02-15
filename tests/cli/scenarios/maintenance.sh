#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/revops"

TMP_DIR="$(mktemp -d /tmp/maintenance_e2e.XXXXXX)"
DB="$TMP_DIR/maintenance_e2e.nano"
DATA_APPEND="$TMP_DIR/append.jsonl"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

build_nanograph_binary "$ROOT"

cat > "$DATA_APPEND" << 'DATA'
{"type":"Signal","data":{"slug":"sig-maint-append","observedAt":"2026-02-16T00:00:00Z","summary":"Maintenance append event","urgency":"low","sourceType":"observation","assertion":"fact","createdAt":"2026-02-16T00:00:00Z"}}
DATA

info "Initializing and loading maintenance test database..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$EXAMPLES/revops.pg")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

LOAD1_JSON=$("$NG" --json load "$DB" --data "$EXAMPLES/revops.jsonl" --mode overwrite)
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

info "Running describe command..."
DESCRIBE_JSON=$("$NG" describe --db "$DB" --format json)
assert_contains "$DESCRIBE_JSON" '"schema_ir_version"' "describe includes schema version"
assert_contains "$DESCRIBE_JSON" '"name": "Opportunity"' "describe includes Opportunity node type"

CHANGES_JSON=$("$NG" changes "$DB" --since 0 --format json)
assert_contains "$CHANGES_JSON" '"db_version": 2' "changes keeps replay window events after cleanup"
assert_contains "$CHANGES_JSON" '"sig-maint-append"' "changes includes append payload after cleanup"

echo ""
pass "maintenance CLI e2e passed"
