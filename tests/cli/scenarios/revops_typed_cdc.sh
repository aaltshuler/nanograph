#!/usr/bin/env bash
set -euo pipefail

# RevOps Typed + CDC CLI E2E
#
# Exercises:
#   1) RevOps example schema/data as typed baseline (enum/list/DateTime/Bool/F64)
#   2) query typecheck + typed filters (DateTime params, bool filter)
#   3) mutation queries (insert/update/delete) against canonical example schema
#   4) enum validation failure surfaced by `check`
#   5) CDC range/since reads across typed mutation events
#   6) introspection commands (version/describe/export/doctor)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/revops"

TMP_DIR="$(mktemp -d /tmp/revops_typed_cdc_e2e.XXXXXX)"
DB="$TMP_DIR/revops_typed_cdc_e2e.nano"
MUTATION_QUERY_FILE="$TMP_DIR/revops_mutations.gq"
BAD_QUERY_FILE="$TMP_DIR/invalid_enum.gq"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_jsonl() {
    local query_file="$1"
    local query_name="$2"
    shift 2
    run_query_jsonl "$DB" "$query_file" "$query_name" "$@"
}

run_count() {
    local query_file="$1"
    local query_name="$2"
    shift 2
    run_query_count "$DB" "$query_file" "$query_name" "$@"
}

build_nanograph_binary "$ROOT"

info "Initializing RevOps database..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$EXAMPLES/revops.pg")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

info "Loading RevOps seed data..."
LOAD_JSON=$("$NG" --json load "$DB" --data "$EXAMPLES/revops.jsonl" --mode overwrite)
assert_contains "$LOAD_JSON" '"status":"ok"' "load --json status"

BASE_VERSION_JSON=$("$NG" --json version --db "$DB")
assert_contains "$BASE_VERSION_JSON" '"db_version"' "version --json reports baseline db version"

info "Typechecking canonical RevOps query suite..."
CHECK_JSON=$("$NG" --json check --db "$DB" --query "$EXAMPLES/revops.gq")
assert_contains "$CHECK_JSON" '"status":"ok"' "check revops query suite"

info "Validating typed read queries..."
PIPELINE_CSV=$("$NG" run --db "$DB" --query "$EXAMPLES/revops.gq" --name pipeline_summary --format csv)
PIPELINE_ROWS=$(count_csv_data_rows "$PIPELINE_CSV")
assert_int_eq "$PIPELINE_ROWS" 1 "pipeline_summary returns one stage bucket"
assert_contains "$PIPELINE_CSV" "25000" "pipeline_summary includes F64 amount aggregation"

RECENT_SIGNALS=$(run_count "$EXAMPLES/revops.gq" recent_signals --param "since=2026-02-08T00:00:00Z")
assert_int_eq "$RECENT_SIGNALS" 1 "DateTime param filter works"

SUCCESSFUL_ACTIONS=$(run_count "$EXAMPLES/revops.gq" successful_actions)
assert_int_eq "$SUCCESSFUL_ACTIONS" 2 "Bool filter works on successful actions"

CLIENTS_JSONL=$(run_jsonl "$EXAMPLES/revops.gq" all_clients)
assert_contains "$CLIENTS_JSONL" '"tags":"[' "list field is projected in query output"

cat > "$MUTATION_QUERY_FILE" << 'QUERIES'
query signal_rows($slug: String) {
    match { $s: Signal { slug: $slug } }
    return { $s.slug, $s.urgency, $s.observedAt }
}

query opportunity_stage() {
    match { $o: Opportunity { slug: "opp-stripe-migration" } }
    return { $o.stage }
}

query task_rows() {
    match { $t: ActionItem { slug: "ai-draft-proposal" } }
    return { $t.slug, $t.status }
}

query insert_test_signal() {
    insert Signal {
        slug: "sig-test-renewal"
        observedAt: datetime("2026-02-15T15:00:00Z")
        summary: "Renewal risk identified during support exchange"
        urgency: "critical"
        sourceType: "message"
        assertion: "fact"
        createdAt: datetime("2026-02-15T15:00:00Z")
    }
}

query update_stage_to_hold() {
    update Opportunity set {
        stage: "hold"
        updatedAt: datetime("2026-02-15T15:10:00Z")
    } where slug = "opp-stripe-migration"
}

query set_task_in_progress() {
    update ActionItem set {
        status: "in_progress"
        updatedAt: datetime("2026-02-15T15:12:00Z")
    } where slug = "ai-draft-proposal"
}

query delete_task() {
    delete ActionItem where slug = "ai-draft-proposal"
}
QUERIES

CHECK_MUT_JSON=$("$NG" --json check --db "$DB" --query "$MUTATION_QUERY_FILE")
assert_contains "$CHECK_MUT_JSON" '"status":"ok"' "check revops mutation test queries"

info "Running typed mutation sequence..."
INSERT_OUT=$(run_jsonl "$MUTATION_QUERY_FILE" insert_test_signal)
assert_contains "$INSERT_OUT" '"affected_nodes":"1"' "insert mutation affected_nodes"
INSERT_VERSION_JSON=$("$NG" --json version --db "$DB")
INSERT_DB_VERSION=$(echo "$INSERT_VERSION_JSON" | sed -n 's/.*"db_version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
[ -n "$INSERT_DB_VERSION" ] || fail "failed to parse insert db_version"

UPDATE_OPP_OUT=$(run_jsonl "$MUTATION_QUERY_FILE" update_stage_to_hold)
assert_contains "$UPDATE_OPP_OUT" '"affected_nodes":"1"' "opportunity update affected_nodes"
UPDATE_OPP_VERSION_JSON=$("$NG" --json version --db "$DB")
UPDATE_OPP_DB_VERSION=$(echo "$UPDATE_OPP_VERSION_JSON" | sed -n 's/.*"db_version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
[ -n "$UPDATE_OPP_DB_VERSION" ] || fail "failed to parse opportunity update db_version"

UPDATE_TASK_OUT=$(run_jsonl "$MUTATION_QUERY_FILE" set_task_in_progress)
assert_contains "$UPDATE_TASK_OUT" '"affected_nodes":"1"' "action item update affected_nodes"
UPDATE_TASK_VERSION_JSON=$("$NG" --json version --db "$DB")
UPDATE_TASK_DB_VERSION=$(echo "$UPDATE_TASK_VERSION_JSON" | sed -n 's/.*"db_version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
[ -n "$UPDATE_TASK_DB_VERSION" ] || fail "failed to parse action item update db_version"

TASK_AFTER_UPDATE=$(run_jsonl "$MUTATION_QUERY_FILE" task_rows)
assert_contains "$TASK_AFTER_UPDATE" '"status":"in_progress"' "task status updated"

DELETE_TASK_OUT=$(run_jsonl "$MUTATION_QUERY_FILE" delete_task)
assert_contains "$DELETE_TASK_OUT" '"affected_nodes":"1"' "delete mutation affected_nodes"
DELETE_TASK_VERSION_JSON=$("$NG" --json version --db "$DB")
DELETE_TASK_DB_VERSION=$(echo "$DELETE_TASK_VERSION_JSON" | sed -n 's/.*"db_version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
[ -n "$DELETE_TASK_DB_VERSION" ] || fail "failed to parse delete db_version"

INSERTED_SIGNAL_COUNT=$(run_count "$MUTATION_QUERY_FILE" signal_rows --param "slug=sig-test-renewal")
assert_int_eq "$INSERTED_SIGNAL_COUNT" 1 "inserted signal is queryable"
INSERTED_SIGNAL_ROW=$(run_jsonl "$MUTATION_QUERY_FILE" signal_rows --param "slug=sig-test-renewal")
assert_contains "$INSERTED_SIGNAL_ROW" '"urgency":"critical"' "inserted enum value preserved"

OPP_STAGE_AFTER_UPDATE=$(run_jsonl "$MUTATION_QUERY_FILE" opportunity_stage)
assert_contains "$OPP_STAGE_AFTER_UPDATE" '"stage":"hold"' "opportunity stage updated"

TASK_COUNT_AFTER_DELETE=$(run_count "$MUTATION_QUERY_FILE" task_rows)
assert_int_eq "$TASK_COUNT_AFTER_DELETE" 0 "action item deleted"

cat > "$BAD_QUERY_FILE" << 'QUERIES'
query add_invalid_signal() {
    insert Signal {
        slug: "sig-invalid-enum"
        observedAt: datetime("2026-02-15T18:00:00Z")
        summary: "Invalid enum payload"
        urgency: "urgent"
        sourceType: "email"
        assertion: "fact"
        createdAt: datetime("2026-02-15T18:00:00Z")
    }
}
QUERIES

info "Validating enum safety on invalid query..."
if BAD_CHECK_OUT=$("$NG" check --db "$DB" --query "$BAD_QUERY_FILE" 2>&1); then
    fail "invalid enum mutation unexpectedly typechecked"
fi
assert_contains "$BAD_CHECK_OUT" "expects one of" "enum validation error surfaced"

info "Validating CDC windows..."
CHANGES_RANGE_JSON=$("$NG" changes "$DB" --from "$INSERT_DB_VERSION" --to "$DELETE_TASK_DB_VERSION" --format json)
assert_contains "$CHANGES_RANGE_JSON" "\"db_version\": $INSERT_DB_VERSION" "changes range includes insert version"
assert_contains "$CHANGES_RANGE_JSON" "\"db_version\": $UPDATE_OPP_DB_VERSION" "changes range includes first update version"
assert_contains "$CHANGES_RANGE_JSON" "\"db_version\": $UPDATE_TASK_DB_VERSION" "changes range includes second update version"
assert_contains "$CHANGES_RANGE_JSON" "\"db_version\": $DELETE_TASK_DB_VERSION" "changes range includes delete version"
assert_contains "$CHANGES_RANGE_JSON" '"type_name": "Signal"' "changes include Signal mutation events"
assert_contains "$CHANGES_RANGE_JSON" '"type_name": "Opportunity"' "changes include Opportunity mutation events"
assert_contains "$CHANGES_RANGE_JSON" '"type_name": "ActionItem"' "changes include ActionItem mutation events"
assert_contains "$CHANGES_RANGE_JSON" '"op": "insert"' "changes include insert operation"
assert_contains "$CHANGES_RANGE_JSON" '"op": "update"' "changes include update operation"
assert_contains "$CHANGES_RANGE_JSON" '"op": "delete"' "changes include delete operation"
assert_contains "$CHANGES_RANGE_JSON" '"urgency": "critical"' "changes payload preserves enum value"

CHANGES_SINCE_JSONL=$("$NG" changes "$DB" --since "$UPDATE_TASK_DB_VERSION" --format jsonl)
assert_contains "$CHANGES_SINCE_JSONL" "\"db_version\":$DELETE_TASK_DB_VERSION" "changes --since returns post-since version"
assert_contains "$CHANGES_SINCE_JSONL" '"op":"delete"' "changes --since includes delete operation"

info "Running introspection commands..."
DOCTOR_JSON=$("$NG" --json doctor "$DB")
assert_contains "$DOCTOR_JSON" '"status":"ok"' "doctor --json status"
assert_contains "$DOCTOR_JSON" '"healthy":true' "doctor reports healthy"

VERSION_JSON=$("$NG" --json version --db "$DB")
assert_contains "$VERSION_JSON" '"binary_version"' "version reports binary version"
assert_contains "$VERSION_JSON" '"db_version"' "version reports manifest db version"

DESCRIBE_JSON=$("$NG" describe --db "$DB" --format json)
assert_contains "$DESCRIBE_JSON" '"schema_ir_version"' "describe includes schema version"
assert_contains "$DESCRIBE_JSON" '"name": "Signal"' "describe includes Signal node type"

EXPORT_JSONL=$("$NG" export --db "$DB" --format jsonl)
assert_contains "$EXPORT_JSONL" '"type":"Signal"' "export emits Signal rows"
assert_contains "$EXPORT_JSONL" '"slug":"sig-test-renewal"' "export includes inserted signal"
if echo "$EXPORT_JSONL" | grep -F -q '"slug":"ai-draft-proposal"'; then
    fail "export still contains deleted action item"
fi
pass "export excludes deleted action item"

echo ""
pass "revops typed + cdc CLI e2e passed"
