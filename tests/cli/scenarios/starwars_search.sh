#!/usr/bin/env bash
set -euo pipefail

# Star Wars search fixture CLI E2E
#
# Validates doc-style search workflows against search-test fixtures:
#   1) keyword / fuzzy / match_text filters
#   2) bm25 ranking
#   3) semantic nearest(Vector) ranking using a retrieved embedding vector
#   4) hybrid rrf(nearest,bm25)
#   5) traversal + search patterns (mentors, family, students, film traversal)
#   6) mutation edge insert for traversal behavior updates

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
FIXTURES="$ROOT/search-test"

TMP_DIR="$(mktemp -d /tmp/starwars_search.XXXXXX)"
DB="$TMP_DIR/starwars_search.nano"
QUERY_FILE="$TMP_DIR/queries.gq"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_jsonl() {
    run_query_jsonl "$DB" "$QUERY_FILE" "$@"
}

build_nanograph_binary "$ROOT"

cp "$FIXTURES/queries.gq" "$QUERY_FILE"
cat >> "$QUERY_FILE" << 'QUERIES'

query character_embedding($slug: String) {
    match { $c: Character { slug: $slug } }
    return { $c.slug as slug, $c.embedding as embedding }
    limit 1
}

query semantic_search_by_vector($vq: Vector(1536)) {
    match { $c: Character }
    return { $c.slug as slug, $c.name as name, nearest($c.embedding, $vq) as score }
    order { nearest($c.embedding, $vq) }
    limit 5
}
QUERIES

info "Using mock embeddings and chunking for deterministic, network-free search..."
export NANOGRAPH_EMBEDDINGS_MOCK=1
export NANOGRAPH_EMBED_CHUNK_CHARS=1500
export NANOGRAPH_EMBED_CHUNK_OVERLAP_CHARS=200
unset OPENAI_API_KEY || true

info "Initializing and loading Star Wars search fixtures..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$FIXTURES/schema.pg")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

LOAD_JSON=$("$NG" --json load "$DB" --data "$FIXTURES/data.jsonl" --mode overwrite)
assert_contains "$LOAD_JSON" '"status":"ok"' "load --json status"

CHECK_JSON=$("$NG" --json check --db "$DB" --query "$QUERY_FILE")
assert_contains "$CHECK_JSON" '"status":"ok"' "search fixture queries typecheck"

CACHE_FILE="$DB/_embedding_cache.jsonl"
[ -f "$CACHE_FILE" ] || fail "embedding cache file missing at $CACHE_FILE"
CACHE_SAMPLE="$(head -n 1 "$CACHE_FILE")"
assert_contains "$CACHE_SAMPLE" '"chunk_chars":1500' "embedding cache records chunk size"
assert_contains "$CACHE_SAMPLE" '"chunk_overlap_chars":200' "embedding cache records chunk overlap"

info "Validating keyword, fuzzy, and match_text filtering..."
KEYWORD_RESULTS=$(run_jsonl keyword_search --param "q=dark side")
KEYWORD_COUNT=$(run_query_count "$DB" "$QUERY_FILE" keyword_search --param "q=dark side")
assert_int_ge "$KEYWORD_COUNT" 2 "keyword search returns multiple dark-side matches"
assert_contains "$KEYWORD_RESULTS" '"slug":"anakin"' "keyword search includes Anakin"

FUZZY_RESULTS=$(run_jsonl fuzzy_search --param "q=Skywaker")
FUZZY_COUNT=$(run_query_count "$DB" "$QUERY_FILE" fuzzy_search --param "q=Skywaker")
assert_int_ge "$FUZZY_COUNT" 2 "fuzzy search tolerates typo"
assert_contains "$FUZZY_RESULTS" '"slug":"anakin"' "fuzzy search includes Anakin"
assert_contains "$FUZZY_RESULTS" '"slug":"luke"' "fuzzy search includes Luke"

MATCH_TEXT_RESULTS=$(run_jsonl match_text_search --param "q=Jedi Master")
MATCH_TEXT_COUNT=$(run_query_count "$DB" "$QUERY_FILE" match_text_search --param "q=Jedi Master")
assert_int_ge "$MATCH_TEXT_COUNT" 1 "match_text finds contiguous token phrase"
assert_contains "$MATCH_TEXT_RESULTS" '"slug":"obi-wan"' "match_text includes Obi-Wan"

info "Validating bm25 ranking..."
BM25_RESULTS=$(run_jsonl bm25_search --param "q=clone wars lightsaber")
BM25_COUNT=$(run_query_count "$DB" "$QUERY_FILE" bm25_search --param "q=clone wars lightsaber")
assert_int_eq "$BM25_COUNT" 5 "bm25 query returns limited top-k rows"
BM25_TOP=$(json_field "$BM25_RESULTS" "slug")
[ -n "$BM25_TOP" ] || fail "bm25 top row slug missing"
pass "bm25 top row present"
BM25_SCORE_1=$(json_field "$BM25_RESULTS" "score")
BM25_SCORE_2=$(json_field "$BM25_RESULTS" "score" 2)
awk -v a="$BM25_SCORE_1" -v b="$BM25_SCORE_2" 'BEGIN { exit (a+0 >= b+0 ? 0 : 1) }' \
    || fail "bm25 scores are not sorted desc (row1=$BM25_SCORE_1 row2=$BM25_SCORE_2)"
pass "bm25 scores sorted desc"

info "Validating semantic nearest(Vector) ranking..."
ANAKIN_ROW=$(run_jsonl character_embedding --param "slug=anakin")
ANAKIN_VECTOR=$(json_field "$ANAKIN_ROW" "embedding")
assert_contains "$ANAKIN_VECTOR" "[" "character embedding returned as JSON vector"

SEM_VECTOR_RESULTS=$(run_jsonl semantic_search_by_vector --param "vq=$ANAKIN_VECTOR")
SEM_VECTOR_TOP=$(json_field "$SEM_VECTOR_RESULTS" "slug")
assert_str_eq "$SEM_VECTOR_TOP" "anakin" "nearest(Vector) ranks identical vector first"

info "Validating hybrid fusion..."
HYBRID_RESULTS=$(run_jsonl hybrid_search --param "q=father and son conflict")
HYBRID_COUNT=$(run_query_count "$DB" "$QUERY_FILE" hybrid_search --param "q=father and son conflict")
assert_int_eq "$HYBRID_COUNT" 5 "hybrid query returns limited top-k rows"
assert_contains "$HYBRID_RESULTS" '"hybrid_score"' "hybrid output projects fused score"
assert_contains "$HYBRID_RESULTS" '"semantic_distance"' "hybrid output projects semantic distance"
assert_contains "$HYBRID_RESULTS" '"lexical_score"' "hybrid output projects lexical score"

info "Validating graph-constrained search patterns..."
MENTORS_RESULTS=$(run_jsonl mentors_of --param "slug=luke")
MENTORS_COUNT=$(run_query_count "$DB" "$QUERY_FILE" mentors_of --param "slug=luke")
assert_int_eq "$MENTORS_COUNT" 2 "mentors_of returns Luke's two mentors"
MENTOR_1=$(json_field "$MENTORS_RESULTS" "slug")
MENTOR_2=$(json_field "$MENTORS_RESULTS" "slug" 2)
assert_str_eq "$MENTOR_1" "obi-wan" "mentors_of ordering row1"
assert_str_eq "$MENTOR_2" "yoda" "mentors_of ordering row2"

FAMILY_RESULTS=$(run_jsonl family_semantic --param "slug=luke" --param "q=politician")
FAMILY_COUNT=$(run_query_count "$DB" "$QUERY_FILE" family_semantic --param "slug=luke" --param "q=politician")
assert_int_eq "$FAMILY_COUNT" 2 "family_semantic is constrained to Luke's parents"
assert_contains "$FAMILY_RESULTS" '"slug":"anakin"' "family_semantic includes Anakin"
assert_contains "$FAMILY_RESULTS" '"slug":"padme"' "family_semantic includes Padme"

STUDENTS_RESULTS=$(run_jsonl students_search --param "mentor=obi-wan" --param "q=dark side")
STUDENTS_COUNT=$(run_query_count "$DB" "$QUERY_FILE" students_search --param "mentor=obi-wan" --param "q=dark side")
assert_int_eq "$STUDENTS_COUNT" 2 "students_search constrained by mentor traversal"
assert_contains "$STUDENTS_RESULTS" '"slug":"anakin"' "students_search includes Anakin"
assert_contains "$STUDENTS_RESULTS" '"slug":"luke"' "students_search includes Luke"

DEBUT_RESULTS=$(run_jsonl debut_film --param "slug=anakin")
DEBUT_FILM=$(json_field "$DEBUT_RESULTS" "slug")
assert_str_eq "$DEBUT_FILM" "phantom-menace" "debut_film finds Anakin's debut"

SAME_DEBUT_RESULTS=$(run_jsonl same_debut --param "film=a-new-hope")
SAME_DEBUT_COUNT=$(run_query_count "$DB" "$QUERY_FILE" same_debut --param "film=a-new-hope")
assert_int_eq "$SAME_DEBUT_COUNT" 4 "same_debut returns all A New Hope characters"
assert_contains "$SAME_DEBUT_RESULTS" '"slug":"han"' "same_debut includes Han"
assert_contains "$SAME_DEBUT_RESULTS" '"slug":"leia"' "same_debut includes Leia"
assert_contains "$SAME_DEBUT_RESULTS" '"slug":"luke"' "same_debut includes Luke"
assert_contains "$SAME_DEBUT_RESULTS" '"slug":"obi-wan"' "same_debut includes Obi-Wan"

info "Validating edge mutation path updates traversal-constrained search..."
ADD_PARENT_OUT=$(run_jsonl add_edge_parent --param "from=luke" --param "to=yoda")
ADD_PARENT_EDGES=$(json_field "$ADD_PARENT_OUT" "affected_edges")
assert_str_eq "$ADD_PARENT_EDGES" "1" "insert edge mutation affected_edges"

LUKE_FAMILY_AFTER_MUTATION=$(run_jsonl family_semantic --param "slug=luke" --param "q=jedi")
LUKE_FAMILY_COUNT_AFTER_MUTATION=$(run_query_count "$DB" "$QUERY_FILE" family_semantic --param "slug=luke" --param "q=jedi")
assert_int_eq "$LUKE_FAMILY_COUNT_AFTER_MUTATION" 3 "family_semantic reflects newly inserted parent edge"
assert_contains "$LUKE_FAMILY_AFTER_MUTATION" '"slug":"yoda"' "new parent traversal includes Yoda"

echo ""
pass "starwars search fixture CLI e2e passed"
