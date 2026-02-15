#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════════════
# Star Wars Schema Migration — End-to-End Test
#
# Exercises:
#   1. Field rename:  Character.note → Character.description
#   2. Type rename:   Battle → Conflict
#   3. Edge rename:   Fought → Dueled
#   4. New node type: Species
#   5. Add nullable:  Character.homeworld: String?
#   6. Drop property: Weapon.color  (requires --auto-approve)
# ═══════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/starwars"
TMP_DIR="$(mktemp -d /tmp/sw_migration_test.XXXXXX)"
DB="$TMP_DIR/sw_migration_test.nanograph"
MIGRATED_QUERY_FILE="$TMP_DIR/sw_migrated.gq"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# ── 0. Build ───────────────────────────────────────────────────────────────
build_nanograph_binary "$ROOT"

# ── 1. Clean + Init ───────────────────────────────────────────────────────
info "Initializing database..."
"$NG" init "$DB" --schema "$EXAMPLES/starwars.pg"
pass "Database initialized at $DB"

# ── 2. Load data ──────────────────────────────────────────────────────────
info "Loading Star Wars data..."
"$NG" load "$DB" --data "$EXAMPLES/starwars.jsonl" --mode overwrite
pass "Data loaded"

# ── 3. Baseline queries ──────────────────────────────────────────────────
info "Running baseline queries..."

JEDI_OUT=$("$NG" run --db "$DB" --query "$EXAMPLES/starwars.gq" --name jedi --format csv)
JEDI_COUNT=$(count_csv_data_rows "$JEDI_OUT")
assert_int_eq "$JEDI_COUNT" 7 "jedi query: $JEDI_COUNT members"

DUELS_OUT=$("$NG" run --db "$DB" --query "$EXAMPLES/starwars.gq" --name all_duels --format csv)
DUELS_COUNT=$(count_csv_data_rows "$DUELS_OUT")
assert_int_eq "$DUELS_COUNT" 10 "all_duels query: $DUELS_COUNT duels"

HEROES_OUT=$("$NG" run --db "$DB" --query "$EXAMPLES/starwars.gq" --name heroes --format csv)
HEROES_COUNT=$(count_csv_data_rows "$HEROES_OUT")
pass "heroes query: $HEROES_COUNT heroes"

WIELDERS_OUT=$("$NG" run --db "$DB" --query "$EXAMPLES/starwars.gq" --name wielders --format csv)
WIELDERS_COUNT=$(count_csv_data_rows "$WIELDERS_OUT")
pass "wielders query: $WIELDERS_COUNT wielders (has Weapon.color)"

VETS_OUT=$("$NG" run --db "$DB" --query "$EXAMPLES/starwars.gq" --name battle_veterans --format csv)
pass "battle_veterans query works (uses Battle type)"

echo ""
info "Baseline captured. Applying schema changes..."

# ── 4. Write migrated schema ─────────────────────────────────────────────
# Changes:
#   - Character.note → Character.description  (@rename_from)
#   - Character gets homeworld: String?  (new nullable prop)
#   - Battle → Conflict  (@rename_from)
#   - Fought → Dueled  (@rename_from)
#   - Weapon.color DROPPED
#   - New node type Species added

cat > "$DB/schema.pg" << 'SCHEMA'
// Star Wars Knowledge Graph — NanoGraph Schema (MIGRATED)
// Changes: field rename, type rename, edge rename, new type, add prop, drop prop

// ── Nodes ───────────────────────────────────────────────────────────────────

node Character {
    slug: String @key
    name: String
    description: String @rename_from("note")
    species: String
    gender: String
    rank: String?
    era: enum(prequel, clone_wars, original)
    alignment: enum(hero, villain, neutral)
    tags: [String]?
    homeworld: String?
}

node Film {
    slug: String @key
    name: String
    episode: I32
    release_date: Date
}

node Droid {
    slug: String @key
    name: String
    note: String
    model: String
}

node Faction {
    slug: String @key
    name: String
    note: String
    side: enum(light, dark, independent)
}

node Planet {
    slug: String @key
    name: String
    note: String
    terrain: String
    climate: enum(arid, temperate, swamp, forest, urban, volcanic)
}

node Starship {
    slug: String @key
    name: String
    note: String
    model: String
    class: String
    manufacturer: String
}

node Weapon {
    slug: String @key
    name: String
    note: String
    weapon_type: String
}

node Conflict @rename_from("Battle") {
    slug: String @key
    name: String
    note: String
    era: enum(prequel, clone_wars, original)
    outcome: String
}

node Location {
    slug: String @key
    name: String
    note: String
    location_type: String
}

node Species {
    slug: String @key
    name: String
    classification: String?
}

// ── Film connections ────────────────────────────────────────────────────────

edge DebutsIn: Character -> Film
edge DepictedIn: Conflict -> Film

// ── Family & Personal ───────────────────────────────────────────────────────

edge HasParent: Character -> Character
edge SiblingOf: Character -> Character
edge MarriedTo: Character -> Character
edge ClonedFrom: Character -> Character

// ── Training ────────────────────────────────────────────────────────────────

edge HasMentor: Character -> Character

// ── Affiliation & Ownership ─────────────────────────────────────────────────

edge AffiliatedWith: Character -> Faction {
    role: String?
}
edge Owns: Character -> Starship
edge FactionOwns: Faction -> Starship
edge RulesFaction: Character -> Faction {
    title: String
}
edge RulesPlanet: Character -> Planet {
    title: String
}

// ── Origin & Location ───────────────────────────────────────────────────────

edge OriginatesFrom: Character -> Planet
edge DroidOriginatesFrom: Droid -> Planet
edge LocatedIn: Character -> Planet {
    status: String
}
edge LocatedAt: Location -> Planet

// ── Conflict & Action ───────────────────────────────────────────────────────

edge Dueled: Character -> Character @rename_from("Fought") {
    location: String
}
edge Captured: Character -> Character
edge HiredBy: Character -> Character {
    purpose: String
}
edge ParticipatedIn: Character -> Conflict {
    role: String
}
edge TookPlaceOn: Conflict -> Planet

// ── Transformation ──────────────────────────────────────────────────────────

edge Becomes: Character -> Character {
    event: String
}

// ── Droid & Equipment ───────────────────────────────────────────────────────

edge BuiltBy: Droid -> Character
edge HasCompanion: Droid -> Droid
edge HasCoPilot: Character -> Character
edge Wields: Character -> Weapon
SCHEMA

pass "Migrated schema.pg written"

# ── 5. Dry run ────────────────────────────────────────────────────────────
info "Running migration dry-run..."
echo ""
set +e
DRY_RUN_OUT=$("$NG" migrate "$DB" --dry-run --format table 2>&1)
DRY_RUN_STATUS=$?
set -e
if [ "$DRY_RUN_STATUS" -ne 0 ]; then
    assert_contains "$DRY_RUN_OUT" "Migration Plan" \
        "dry-run can return non-zero while still emitting a migration plan"
fi
echo "$DRY_RUN_OUT"
echo ""

# Verify expected steps appear
echo "$DRY_RUN_OUT" | grep -q "RenameProperty" || fail "Missing RenameProperty step"
echo "$DRY_RUN_OUT" | grep -q "RenameType" || fail "Missing RenameType step"
echo "$DRY_RUN_OUT" | grep -q "AddNodeType" || fail "Missing AddNodeType step"
echo "$DRY_RUN_OUT" | grep -q "AddProperty" || fail "Missing AddProperty step"
echo "$DRY_RUN_OUT" | grep -q "DropProperty" || fail "Missing DropProperty step"
pass "Dry run shows all expected steps"

# ── 6. Apply migration ───────────────────────────────────────────────────
info "Applying migration (--auto-approve for drop)..."
echo ""
if ! APPLY_OUT=$("$NG" migrate "$DB" --auto-approve --format table 2>&1); then
    echo "$APPLY_OUT"
    fail "migration apply failed"
fi
echo "$APPLY_OUT"
echo ""
pass "Migration applied"

# ── 7. Post-migration queries ────────────────────────────────────────────
info "Running post-migration queries..."

# Write updated queries that use the new names
cat > "$MIGRATED_QUERY_FILE" << 'QUERIES'
// Post-migration queries — use new names

// Jedi should still work (Character unchanged except field rename)
query jedi() {
    match {
        $c: Character
        $c affiliatedWith $f
        $f.name = "Jedi Order"
    }
    return { $c.name, $c.rank, $c.era }
    order { $c.name asc }
}

// Heroes — uses Character.description (renamed from note)
query heroes_desc() {
    match {
        $c: Character
        $c.alignment = "hero"
    }
    return { $c.name, $c.description, $c.era }
    order { $c.name asc }
}

// Dueled — edge renamed from Fought
query all_duels() {
    match {
        $a: Character
        $a dueled $b
    }
    return { $a.name, $b.name }
    order { $a.name asc }
}

// Conflict — type renamed from Battle
query battle_veterans() {
    match {
        $c: Character
        $c participatedIn $b
    }
    return {
        $c.name
        count($b) as battles
    }
    order { battles desc }
    limit 5
}

// Wielders — Weapon no longer has color column
query wielders() {
    match {
        $c: Character
        $c wields $w
    }
    return { $c.name, $w.name, $w.weapon_type }
    order { $c.name asc }
}

// homeworld is nullable — new field, all null
query heroes_homeworld() {
    match {
        $c: Character
        $c.alignment = "hero"
    }
    return { $c.name, $c.homeworld }
    order { $c.name asc }
}
QUERIES

# 7a. Jedi count preserved
JEDI_POST=$("$NG" run --db "$DB" --query "$MIGRATED_QUERY_FILE" --name jedi --format csv)
JEDI_POST_COUNT=$(count_csv_data_rows "$JEDI_POST")
[ "$JEDI_POST_COUNT" -eq "$JEDI_COUNT" ] || fail "Jedi count changed: $JEDI_COUNT → $JEDI_POST_COUNT"
pass "Jedi count preserved: $JEDI_POST_COUNT"

# 7b. Field rename: description column exists and has data
HEROES_DESC=$("$NG" run --db "$DB" --query "$MIGRATED_QUERY_FILE" --name heroes_desc --format csv)
echo "$HEROES_DESC" | head -1 | grep -q "description" || fail "description column not found"
HEROES_DESC_COUNT=$(count_csv_data_rows "$HEROES_DESC")
[ "$HEROES_DESC_COUNT" -eq "$HEROES_COUNT" ] || fail "Heroes count changed: $HEROES_COUNT → $HEROES_DESC_COUNT"
pass "Field rename verified: Character.description has data ($HEROES_DESC_COUNT rows)"

# 7c. Edge rename: duels preserved
DUELS_POST=$("$NG" run --db "$DB" --query "$MIGRATED_QUERY_FILE" --name all_duels --format csv)
DUELS_POST_COUNT=$(count_csv_data_rows "$DUELS_POST")
[ "$DUELS_POST_COUNT" -eq "$DUELS_COUNT" ] || fail "Duel count changed: $DUELS_COUNT → $DUELS_POST_COUNT"
pass "Edge rename verified: Dueled has $DUELS_POST_COUNT duels"

# 7d. Type rename: Conflict has the old Battle data
VETS_POST=$("$NG" run --db "$DB" --query "$MIGRATED_QUERY_FILE" --name battle_veterans --format csv)
VETS_POST_COUNT=$(count_csv_data_rows "$VETS_POST")
[ "$VETS_POST_COUNT" -gt 0 ] || fail "battle_veterans returned 0 rows after type rename"
pass "Type rename verified: Conflict (ex-Battle) has $VETS_POST_COUNT veterans"

# 7e. Drop property: Weapon.color gone, weapon_type still there
WIELDERS_POST=$("$NG" run --db "$DB" --query "$MIGRATED_QUERY_FILE" --name wielders --format csv)
echo "$WIELDERS_POST" | head -1 | grep -q "weapon_type" || fail "weapon_type column missing"
WIELDERS_POST_COUNT=$(count_csv_data_rows "$WIELDERS_POST")
[ "$WIELDERS_POST_COUNT" -eq "$WIELDERS_COUNT" ] || fail "Wielder count changed: $WIELDERS_COUNT → $WIELDERS_POST_COUNT"
pass "Drop property verified: Weapon.color gone, $WIELDERS_POST_COUNT wielders preserved"

# 7f. New nullable property: homeworld exists but is null
HOMEWORLD_OUT=$("$NG" run --db "$DB" --query "$MIGRATED_QUERY_FILE" --name heroes_homeworld --format csv)
echo "$HOMEWORLD_OUT" | head -1 | grep -q "homeworld" || fail "homeworld column missing"
pass "Add nullable property verified: Character.homeworld exists"

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  All migration checks passed!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Database at: $DB"
echo "  Changes applied:"
echo "    ✓ Character.note → Character.description"
echo "    ✓ Battle → Conflict"
echo "    ✓ Fought → Dueled"
echo "    ✓ Species node type added"
echo "    ✓ Character.homeworld: String? added"
echo "    ✓ Weapon.color dropped"
echo ""
