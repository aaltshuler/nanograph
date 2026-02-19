#!/usr/bin/env bash
set -euo pipefail

# Traditional + fuzzy text search CLI E2E
#
# Validates:
#   1) keyword token search with search(field, query)
#   2) typo-tolerant matching with fuzzy(field, query)
#   3) strict fuzzy threshold via fuzzy(field, query, max_edits)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/revops"

TMP_DIR="$(mktemp -d /tmp/text_search.XXXXXX)"
DB="$TMP_DIR/text_search.nano"
SCHEMA="$EXAMPLES/revops.pg"
DATA="$TMP_DIR/revops_text_search.jsonl"
QUERY_FILE="$TMP_DIR/text_search.gq"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_jsonl() {
    run_query_jsonl "$DB" "$QUERY_FILE" "$@"
}

build_nanograph_binary "$ROOT"

info "Preparing RevOps data for text search checks..."
cp "$EXAMPLES/revops.jsonl" "$DATA"
cat >> "$DATA" << 'JSONL'
{"type":"Signal","data":{"slug":"sig-billing-delay","observedAt":"2026-02-16T09:00:00Z","summary":"billing reconciliation delay due to missing invoice data","urgency":"medium","sourceType":"email","assertion":"fact","createdAt":"2026-02-16T09:00:00Z"}}
{"type":"Signal","data":{"slug":"sig-referral-analytics","observedAt":"2026-02-16T10:00:00Z","summary":"warm referral for analytics migration project","urgency":"low","sourceType":"meeting","assertion":"fact","createdAt":"2026-02-16T10:00:00Z"}}
JSONL
pass "text-search fixture data prepared"

cat > "$QUERY_FILE" << 'QUERIES'
query keyword_signals($q: String) {
    match {
        $s: Signal
        search($s.summary, $q)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query match_text_signals($q: String) {
    match {
        $s: Signal
        match_text($s.summary, $q)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query fuzzy_signals($q: String) {
    match {
        $s: Signal
        fuzzy($s.summary, $q)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query fuzzy_signals_strict($q: String, $m: I64) {
    match {
        $s: Signal
        fuzzy($s.summary, $q, $m)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query bm25_signals($q: String) {
    match { $s: Signal }
    return { $s.slug as signal_slug, bm25($s.summary, $q) as score }
    order { bm25($s.summary, $q) desc }
    limit 3
}
QUERIES
pass "text search query file written"

info "Initializing and loading DB..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$SCHEMA")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

LOAD_JSON=$("$NG" --json load "$DB" --data "$DATA" --mode overwrite)
assert_contains "$LOAD_JSON" '"status":"ok"' "load --json status"

CHECK_JSON=$("$NG" --json check --db "$DB" --query "$QUERY_FILE")
assert_contains "$CHECK_JSON" '"status":"ok"' "text search query file typechecks"

info "Validating keyword token search..."
KEYWORD_RESULTS=$(run_jsonl keyword_signals --param "q=billing missing")
KEYWORD_TOP=$(json_field "$KEYWORD_RESULTS" "signal_slug")
assert_str_eq "$KEYWORD_TOP" "sig-billing-delay" "search(field, query) returns expected signal"

info "Validating match_text lexical search..."
MATCH_TEXT_RESULTS=$(run_jsonl match_text_signals --param "q=billing missing")
MATCH_TEXT_TOP=$(json_field "$MATCH_TEXT_RESULTS" "signal_slug")
assert_str_eq "$MATCH_TEXT_TOP" "sig-billing-delay" "match_text(field, query) returns expected signal"

info "Validating fuzzy typo-tolerant search..."
FUZZY_RESULTS=$(run_jsonl fuzzy_signals --param "q=reconciliaton delay")
FUZZY_TOP=$(json_field "$FUZZY_RESULTS" "signal_slug")
assert_str_eq "$FUZZY_TOP" "sig-billing-delay" "fuzzy(field, query) tolerates typo"

info "Validating bm25 relevance ranking..."
BM25_RESULTS=$(run_jsonl bm25_signals --param "q=billing missing invoice")
BM25_TOP=$(json_field "$BM25_RESULTS" "signal_slug")
assert_str_eq "$BM25_TOP" "sig-billing-delay" "bm25(field, query) ranks expected signal first"

info "Validating strict fuzzy threshold..."
STRICT_COUNT=$(run_query_count "$DB" "$QUERY_FILE" fuzzy_signals_strict --param "q=reconciliaton delay" --param "m=0")
assert_int_eq "$STRICT_COUNT" 0 "fuzzy(field, query, max_edits=0) rejects typo"

echo ""
pass "traditional/fuzzy text search CLI e2e passed"
