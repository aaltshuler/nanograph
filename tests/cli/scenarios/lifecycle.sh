#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════════════
# Star Wars Write Lifecycle — End-to-End Test
#
# Exercises:
#   1. Init + baseline load (Star Wars fixture)
#   2. Keyed upsert on Character.slug (@key)
#   3. Edge dedup (no multigraph) on duplicate Fought edge input
#   4. Delete API + edge cascade (delete Character "Darth Vader")
#   5. Reopen/readback consistency after writes
# ═══════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/starwars"
TMP_DIR="$(mktemp -d /tmp/sw_lifecycle.XXXXXX)"
DB="$TMP_DIR/sw_lifecycle_test.nanograph"
QUERY_FILE="$TMP_DIR/lifecycle.gq"
UPSERT_DATA="$TMP_DIR/upsert.jsonl"

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

# ── 0. Build ───────────────────────────────────────────────────────────────
build_nanograph_binary "$ROOT"

# ── 1. Init + baseline load ────────────────────────────────────────────────
rm -rf "$DB"
info "Initializing database..."
"$NG" init "$DB" --schema "$EXAMPLES/starwars.pg"
pass "Database initialized"

info "Loading baseline Star Wars data..."
"$NG" load "$DB" --data "$EXAMPLES/starwars.jsonl" --mode overwrite
pass "Baseline data loaded"

# ── 2. Write lifecycle query set ───────────────────────────────────────────
cat > "$QUERY_FILE" << 'QUERIES'
query character_rows($name: String) {
    match { $c: Character { name: $name } }
    return { $c.name }
}

query luke_updated_marker() {
    match {
        $c: Character { name: "Luke Skywalker" }
        $c.note = "UPDATED_NOTE_LUKE_TEST"
    }
    return { $c.name }
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

query wielders() {
    match {
        $c: Character
        $c wields $w
    }
    return { $c.name, $w.name }
}

query wielders_for($name: String) {
    match {
        $c: Character { name: $name }
        $c wields $w
    }
    return { $w.name }
}
QUERIES
pass "Lifecycle query file written"

# ── 3. Baseline capture ────────────────────────────────────────────────────
info "Capturing baseline metrics..."
DUELS_BEFORE=$(run_count all_duels)
LUKE_DUELS_FROM_BEFORE=$(run_count duels_from --param "name=Luke Skywalker")
LUKE_DUELS_TO_BEFORE=$(run_count duels_to --param "name=Luke Skywalker")
LUKE_WIELDS_BEFORE=$(run_count wielders_for --param "name=Luke Skywalker")
pass "Captured baseline duel and Luke edge metrics"

# ── 4. Keyed upsert + dedup scenario ───────────────────────────────────────
cat > "$UPSERT_DATA" << 'JSONL'
{"type":"Character","data":{"slug":"luke-skywalker","name":"Luke Skywalker","note":"UPDATED_NOTE_LUKE_TEST","species":"Human","gender":"Male","rank":"Jedi Knight","era":"original","alignment":"hero","tags":["pilot","jedi"]}}
{"type":"Character","data":{"slug":"ahsoka-tano","name":"Ahsoka Tano","note":"Former Jedi and rebel ally","species":"Togruta","gender":"Female","rank":"Jedi Padawan","era":"clone_wars","alignment":"hero","tags":["jedi","rebel"]}}
{"edge":"Fought","from":"ahsoka-tano","to":"darth-vader","data":{"location":"Malachor"}}
{"edge":"Fought","from":"ahsoka-tano","to":"darth-vader","data":{"location":"Malachor"}}
JSONL

info "Applying keyed upsert load..."
"$NG" load "$DB" --data "$UPSERT_DATA" --mode merge
pass "Keyed upsert load applied"

LUKE_UPDATED=$(run_count luke_updated_marker)
assert_int_eq "$LUKE_UPDATED" 1 "Luke row updated via @key merge"

AHSOKA_COUNT=$(run_count character_rows --param "name=Ahsoka Tano")
assert_int_eq "$AHSOKA_COUNT" 1 "Ahsoka inserted via keyed upsert"

LUKE_DUELS_FROM_AFTER_UPSERT=$(run_count duels_from --param "name=Luke Skywalker")
LUKE_DUELS_TO_AFTER_UPSERT=$(run_count duels_to --param "name=Luke Skywalker")
LUKE_WIELDS_AFTER_UPSERT=$(run_count wielders_for --param "name=Luke Skywalker")
assert_int_eq "$LUKE_DUELS_FROM_AFTER_UPSERT" "$LUKE_DUELS_FROM_BEFORE" \
    "Luke outgoing duel edges preserved after keyed update"
assert_int_eq "$LUKE_DUELS_TO_AFTER_UPSERT" "$LUKE_DUELS_TO_BEFORE" \
    "Luke incoming duel edges preserved after keyed update"
assert_int_eq "$LUKE_WIELDS_AFTER_UPSERT" "$LUKE_WIELDS_BEFORE" \
    "Luke non-duel edges preserved after keyed update"

DUELS_AFTER_UPSERT=$(run_count all_duels)
EXPECTED_DUELS_AFTER_UPSERT=$((DUELS_BEFORE + 1))
assert_int_eq "$DUELS_AFTER_UPSERT" "$EXPECTED_DUELS_AFTER_UPSERT" \
    "Edge dedup applied (duplicate edge lines produce one duel)"

# ── 5. Delete + cascade checks ─────────────────────────────────────────────
info "Capturing pre-delete cascade metrics..."
DUELS_PRE_DELETE=$(run_count all_duels)
VADER_DUELS_FROM_PRE_DELETE=$(run_count duels_from --param "name=Darth Vader")
VADER_DUELS_TO_PRE_DELETE=$(run_count duels_to --param "name=Darth Vader")
WIELDERS_PRE_DELETE=$(run_count wielders)
VADER_WIELDS_PRE_DELETE=$(run_count wielders_for --param "name=Darth Vader")

info "Deleting Darth Vader with cascade..."
"$NG" delete "$DB" --type Character --where "slug=darth-vader"
pass "Delete command executed"

VADER_EXISTS_AFTER=$(run_count character_rows --param "name=Darth Vader")
assert_int_eq "$VADER_EXISTS_AFTER" 0 "Deleted Character row is gone"

VADER_DUELS_FROM_AFTER=$(run_count duels_from --param "name=Darth Vader")
VADER_DUELS_TO_AFTER=$(run_count duels_to --param "name=Darth Vader")
assert_int_eq "$VADER_DUELS_FROM_AFTER" 0 "Outgoing duel edges from deleted node were cascaded"
assert_int_eq "$VADER_DUELS_TO_AFTER" 0 "Incoming duel edges to deleted node were cascaded"

DUELS_AFTER_DELETE=$(run_count all_duels)
EXPECTED_DUELS_AFTER_DELETE=$((DUELS_PRE_DELETE - VADER_DUELS_FROM_PRE_DELETE - VADER_DUELS_TO_PRE_DELETE))
assert_int_eq "$DUELS_AFTER_DELETE" "$EXPECTED_DUELS_AFTER_DELETE" \
    "Total duel edges decreased by deleted node incident edges"

WIELDERS_AFTER_DELETE=$(run_count wielders)
EXPECTED_WIELDERS_AFTER_DELETE=$((WIELDERS_PRE_DELETE - VADER_WIELDS_PRE_DELETE))
assert_int_eq "$WIELDERS_AFTER_DELETE" "$EXPECTED_WIELDERS_AFTER_DELETE" \
    "Non-duel incident edges (Wields) also cascaded"

# ── 6. Reopen/readback sanity ──────────────────────────────────────────────
info "Verifying reopen/readback consistency..."
DUELS_REOPEN_CHECK=$(run_count all_duels)
assert_int_eq "$DUELS_REOPEN_CHECK" "$DUELS_AFTER_DELETE" \
    "Post-delete counts persist across reopen"

MANIFEST="$DB/graph.manifest.json"
[ -f "$MANIFEST" ] || fail "manifest not found at $MANIFEST"
NEXT_NODE_ID=$(rg -o '"next_node_id":[[:space:]]*[0-9]+' "$MANIFEST" | head -n1 | rg -o '[0-9]+')
[ -n "$NEXT_NODE_ID" ] || fail "failed to parse next_node_id from manifest"
[ "$NEXT_NODE_ID" -ge 1 ] || fail "next_node_id expected >= 1, got $NEXT_NODE_ID"
pass "Manifest is readable and counters are sane after lifecycle writes"

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Star Wars lifecycle test passed${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Database left at: $DB"
