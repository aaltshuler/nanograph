#!/usr/bin/env bash
set -euo pipefail

# Query Mutation CLI E2E
#
# Validates end-to-end query mutation flow through CLI on Star Wars fixtures:
#   1) global --json output for init/load/check/run/delete
#   2) insert Character
#   3) insert/delete Fought edge via mutation queries
#   4) update Character note
#   5) delete Character with edge cascade

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/starwars"
DB="/tmp/query_mutations_e2e.nanograph"
TMP_DIR="$(mktemp -d /tmp/query_mutations.XXXXXX)"
KEYED_SCHEMA="$TMP_DIR/starwars-keyed.pg"
QUERY_FILE="$TMP_DIR/mutations.gq"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_jsonl() {
    run_query_jsonl "$DB" "$QUERY_FILE" "$@"
}

run_count() {
    run_query_count "$DB" "$QUERY_FILE" "$@"
}

build_nanograph_binary "$ROOT"

create_character_name_keyed_schema "$EXAMPLES/starwars.pg" "$KEYED_SCHEMA"

cat > "$QUERY_FILE" << 'QUERIES'
query character_rows($name: String) {
    match { $c: Character { name: $name } }
    return { $c.name }
}

query character_note($name: String) {
    match { $c: Character { name: $name } }
    return { $c.note }
}

query all_duels() {
    match {
        $a: Character
        $a fought $b
    }
    return { $a.name, $b.name }
}

query duels_from($name: String) {
    match {
        $a: Character { name: $name }
        $a fought $b
    }
    return { $b.name }
}

query duels_to($name: String) {
    match {
        $a: Character
        $a fought $b
        $b.name = $name
    }
    return { $a.name }
}

query add_ezra_bridger() {
    insert Character {
        name: "Ezra Bridger"
        note: "Lothal rebel and Force-sensitive ally"
        species: "Human"
        gender: "Male"
        rank: "Padawan"
        era: "Rebels Era"
        alignment: "Hero"
    }
}

query update_luke_note($note: String) {
    update Character set {
        note: $note
    } where name = "Luke Skywalker"
}

query delete_vader() {
    delete Character where name = "Darth Vader"
}

query add_ezra_duel() {
    insert Fought {
        from: "Ezra Bridger"
        to: "Luke Skywalker"
        location: "Lothal"
    }
}

query delete_ezra_duel() {
    delete Fought where from = "Ezra Bridger"
}
QUERIES

rm -rf "$DB"
info "Initializing database..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$KEYED_SCHEMA")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"
pass "database initialized"

info "Loading baseline data..."
LOAD_JSON=$("$NG" --json load "$DB" --data "$EXAMPLES/starwars.jsonl" --mode overwrite)
assert_contains "$LOAD_JSON" '"status":"ok"' "load --json status"
pass "baseline loaded"

info "Typechecking queries..."
CHECK_JSON=$("$NG" --json check --db "$DB" --query "$QUERY_FILE")
assert_contains "$CHECK_JSON" '"status":"ok"' "check --json status"
pass "query check passed"

EZRA_BEFORE=$(run_count character_rows --param "name=Ezra Bridger")
assert_int_eq "$EZRA_BEFORE" 0 "baseline Ezra Bridger absent"

RUN_JSON=$("$NG" --json run --db "$DB" --query "$QUERY_FILE" --name character_rows --param "name=Luke Skywalker")
assert_contains "$RUN_JSON" '[' "run --json returns array"
assert_contains "$RUN_JSON" '"name"' "run --json includes field names"
assert_contains "$RUN_JSON" '"Luke Skywalker"' "run --json contains typed row"

DUELS_BEFORE=$(run_count all_duels)
VADER_FROM_BEFORE=$(run_count duels_from --param "name=Darth Vader")
VADER_TO_BEFORE=$(run_count duels_to --param "name=Darth Vader")
pass "captured baseline duel metrics"

info "Running insert mutation..."
INSERT_OUT=$(run_jsonl add_ezra_bridger)
INSERT_AFFECTED=$(json_field "$INSERT_OUT" "affected_nodes")
assert_str_eq "$INSERT_AFFECTED" "1" "insert affected_nodes"

EZRA_AFTER_INSERT=$(run_count character_rows --param "name=Ezra Bridger")
assert_int_eq "$EZRA_AFTER_INSERT" 1 "Ezra Bridger inserted"

info "Running edge insert mutation..."
EDGE_INSERT_OUT=$(run_jsonl add_ezra_duel)
EDGE_INSERT_AFFECTED_EDGES=$(json_field "$EDGE_INSERT_OUT" "affected_edges")
assert_str_eq "$EDGE_INSERT_AFFECTED_EDGES" "1" "edge insert affected_edges"

EZRA_DUELS_AFTER_EDGE_INSERT=$(run_count duels_from --param "name=Ezra Bridger")
assert_int_eq "$EZRA_DUELS_AFTER_EDGE_INSERT" 1 "Ezra duel inserted"

info "Running edge delete mutation..."
EDGE_DELETE_OUT=$(run_jsonl delete_ezra_duel)
EDGE_DELETE_AFFECTED_EDGES=$(json_field "$EDGE_DELETE_OUT" "affected_edges")
assert_str_eq "$EDGE_DELETE_AFFECTED_EDGES" "1" "edge delete affected_edges"

EZRA_DUELS_AFTER_EDGE_DELETE=$(run_count duels_from --param "name=Ezra Bridger")
assert_int_eq "$EZRA_DUELS_AFTER_EDGE_DELETE" 0 "Ezra duel removed"

info "Running update mutation..."
UPDATED_NOTE="UPDATED_NOTE_FROM_MUTATION_QUERY"
UPDATE_OUT=$(run_jsonl update_luke_note --param "note=$UPDATED_NOTE")
UPDATE_AFFECTED=$(json_field "$UPDATE_OUT" "affected_nodes")
assert_str_eq "$UPDATE_AFFECTED" "1" "update affected_nodes"

LUKE_NOTE_OUT=$(run_jsonl character_note --param "name=Luke Skywalker")
LUKE_NOTE=$(json_field "$LUKE_NOTE_OUT" "note")
assert_str_eq "$LUKE_NOTE" "$UPDATED_NOTE" "Luke note updated"

info "Running delete mutation..."
DELETE_OUT=$(run_jsonl delete_vader)
DELETE_AFFECTED_NODES=$(json_field "$DELETE_OUT" "affected_nodes")
DELETE_AFFECTED_EDGES=$(json_field "$DELETE_OUT" "affected_edges")
assert_str_eq "$DELETE_AFFECTED_NODES" "1" "delete affected_nodes"

MIN_EXPECTED_CASCADE=$((VADER_FROM_BEFORE + VADER_TO_BEFORE))
assert_int_ge "$DELETE_AFFECTED_EDGES" "$MIN_EXPECTED_CASCADE" "delete affected_edges cascaded"

VADER_AFTER=$(run_count character_rows --param "name=Darth Vader")
assert_int_eq "$VADER_AFTER" 0 "Darth Vader removed"

VADER_FROM_AFTER=$(run_count duels_from --param "name=Darth Vader")
VADER_TO_AFTER=$(run_count duels_to --param "name=Darth Vader")
assert_int_eq "$VADER_FROM_AFTER" 0 "outgoing duel edges from Vader cascaded"
assert_int_eq "$VADER_TO_AFTER" 0 "incoming duel edges to Vader cascaded"

DUELS_AFTER=$(run_count all_duels)
EXPECTED_DUELS_AFTER=$((DUELS_BEFORE - VADER_FROM_BEFORE - VADER_TO_BEFORE))
assert_int_eq "$DUELS_AFTER" "$EXPECTED_DUELS_AFTER" "duel count adjusted after cascade"

EZRA_AFTER_DELETE=$(run_count character_rows --param "name=Ezra Bridger")
assert_int_eq "$EZRA_AFTER_DELETE" 1 "Ezra Bridger remains after delete mutation"

info "Validating changes command..."
CHANGES_RANGE_JSON=$("$NG" changes "$DB" --from 2 --to 4 --format json)
assert_contains "$CHANGES_RANGE_JSON" '"db_version": 2' "changes range includes version 2"
assert_contains "$CHANGES_RANGE_JSON" '"db_version": 3' "changes range includes version 3"
assert_contains "$CHANGES_RANGE_JSON" '"db_version": 4' "changes range includes version 4"
assert_contains "$CHANGES_RANGE_JSON" '"type_name": "Character"' "changes range includes Character mutation"
assert_contains "$CHANGES_RANGE_JSON" '"type_name": "Fought"' "changes range includes Fought mutation"
assert_contains "$CHANGES_RANGE_JSON" '"op": "insert"' "changes range includes insert op"
assert_contains "$CHANGES_RANGE_JSON" '"op": "delete"' "changes range includes delete op"

CHANGES_SINCE_JSONL=$("$NG" changes "$DB" --since 5 --format jsonl)
assert_contains "$CHANGES_SINCE_JSONL" '"db_version":6' "changes --since returns latest mutation version"
assert_contains "$CHANGES_SINCE_JSONL" '"op":"delete"' "changes --since returns delete events"

info "Running --json delete command..."
DELETE_CMD_JSON=$("$NG" --json delete "$DB" --type Character --where "name=Ezra Bridger")
assert_contains "$DELETE_CMD_JSON" '"status":"ok"' "delete command --json status"
assert_contains "$DELETE_CMD_JSON" '"deleted_nodes":1' "delete command --json deleted_nodes"

EZRA_AFTER_DELETE_CMD=$(run_count character_rows --param "name=Ezra Bridger")
assert_int_eq "$EZRA_AFTER_DELETE_CMD" 0 "Ezra Bridger removed by delete command"

echo ""
pass "query mutation CLI e2e passed"
