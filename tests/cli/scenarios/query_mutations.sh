#!/usr/bin/env bash
set -euo pipefail

# Query Mutation CLI E2E
#
# Validates end-to-end query mutation flow through CLI on Star Wars fixtures:
#   1) insert Character
#   2) update Character note
#   3) delete Character with edge cascade

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
QUERIES

rm -rf "$DB"
info "Initializing database..."
"$NG" init "$DB" --schema "$KEYED_SCHEMA" >/dev/null
pass "database initialized"

info "Loading baseline data..."
"$NG" load "$DB" --data "$EXAMPLES/starwars.jsonl" --mode overwrite >/dev/null
pass "baseline loaded"

info "Typechecking queries..."
"$NG" check --db "$DB" --query "$QUERY_FILE" >/dev/null
pass "query check passed"

EZRA_BEFORE=$(run_count character_rows --param "name=Ezra Bridger")
assert_int_eq "$EZRA_BEFORE" 0 "baseline Ezra Bridger absent"

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

echo ""
pass "query mutation CLI e2e passed"
