#!/usr/bin/env bash
set -euo pipefail

# Context Graph Semantic Search CLI E2E
#
# Uses the canonical RevOps context graph example and adds a temporary
# vector-embedding field for Signal summaries:
#   1) schema transform adds Signal.summaryEmbedding: Vector(8) @embed(summary) @index
#   2) load-time auto-embedding generation via mock embeddings (no OPENAI key required)
#   3) large-text chunking for @embed source text
#   4) lexical and fuzzy search constrained by graph traversal (SignalAffects -> Client)
#   5) bm25 ranking constrained by graph traversal
#   6) nearest(..., String) search over all signals and traversal-constrained subsets
#   7) nearest(..., Vector(8)) with CLI vector parameter syntax
#   8) combined traversal + semantic examples (vector semantic + lexical/fuzzy gates)
#   9) multi-hop traversal + semantic ranking over opportunities/projects
#   10) hybrid fusion via rrf(nearest, bm25) with explicit k and default k
#   11) nearest(..., String) search over a large-text chunked embedding

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/revops"
FIXTURES="$ROOT/crates/nanograph/tests/fixtures"

TMP_DIR="$(mktemp -d /tmp/context_graph_search.XXXXXX)"
DB="$TMP_DIR/context_graph_search.nano"
SCHEMA="$TMP_DIR/revops_search.pg"
DATA="$TMP_DIR/revops_search.jsonl"
QUERY_FILE="$TMP_DIR/context_search.gq"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_jsonl() {
    run_query_jsonl "$DB" "$QUERY_FILE" "$@"
}

build_nanograph_binary "$ROOT"

info "Preparing search-enabled RevOps schema (context graph)..."
awk '
    /^node[[:space:]]+Signal[[:space:]]*{/ { in_signal = 1; print; next }
    in_signal && /^[[:space:]]*summary:[[:space:]]*String[[:space:]]*$/ {
        print
        print "    summaryEmbedding: Vector(8) @embed(summary) @index"
        next
    }
    in_signal && /^}/ { in_signal = 0; print; next }
    { print }
' "$EXAMPLES/revops.pg" > "$SCHEMA"
grep -q 'summaryEmbedding: Vector(8) @embed(summary) @index' "$SCHEMA" \
    || fail "failed to inject Signal.summaryEmbedding into schema"
pass "search-enabled schema generated"

info "Preparing context graph data with extra signals..."
cp "$EXAMPLES/revops.jsonl" "$DATA"
LARGE_SUMMARY_MD="$FIXTURES/revops_large_signal.md"
[ -f "$LARGE_SUMMARY_MD" ] || fail "missing fixture file: $LARGE_SUMMARY_MD"
LARGE_SUMMARY_JSON="$(perl -MJSON::PP -e 'local $/; my $s = <>; print JSON::PP->new->encode($s);' "$LARGE_SUMMARY_MD")"
cat >> "$DATA" << 'JSONL'
{"type":"Signal","data":{"slug":"sig-billing-delay","observedAt":"2026-02-16T09:00:00Z","summary":"billing reconciliation delay due to missing invoice data","urgency":"medium","sourceType":"email","assertion":"fact","createdAt":"2026-02-16T09:00:00Z"}}
{"type":"Signal","data":{"slug":"sig-referral-analytics","observedAt":"2026-02-16T10:00:00Z","summary":"warm referral for analytics migration project","urgency":"low","sourceType":"meeting","assertion":"fact","createdAt":"2026-02-16T10:00:00Z"}}
JSONL
cat >> "$DATA" <<JSONL
{"type":"Signal","data":{"slug":"sig-enterprise-procurement","observedAt":"2026-02-16T11:00:00Z","summary":$LARGE_SUMMARY_JSON,"urgency":"high","sourceType":"observation","assertion":"fact","createdAt":"2026-02-16T11:00:00Z"}}
{"edge":"SignalAffects","from":"sig-billing-delay","to":"cli-priya-shah"}
{"edge":"SignalAffects","from":"sig-referral-analytics","to":"cli-jamie-lee"}
{"edge":"SignalAffects","from":"sig-enterprise-procurement","to":"cli-priya-shah"}
{"edge":"Surfaced","from":"sig-billing-delay","to":"opp-stripe-migration","data":{"confidence":"high"}}
{"edge":"Surfaced","from":"sig-referral-analytics","to":"opp-stripe-migration","data":{"confidence":"low"}}
{"edge":"Surfaced","from":"sig-enterprise-procurement","to":"opp-stripe-migration","data":{"confidence":"high"}}
JSONL
pass "context graph data prepared"

cat > "$QUERY_FILE" << 'QUERIES'
query semantic_signals($q: String) {
    match { $s: Signal }
    return { $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $q) }
    limit 3
}

query semantic_signals_by_vector($vq: Vector(8)) {
    match { $s: Signal }
    return { $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $vq) }
    limit 3
}

query client_row($client: String) {
    match { $c: Client { slug: $client } }
    return { $c.slug as client_slug, $c.name }
}

query signal_embedding($slug: String) {
    match { $s: Signal { slug: $slug } }
    return { $s.slug as signal_slug, $s.summaryEmbedding as embedding }
    limit 1
}

query semantic_signals_for_source($source: String, $q: String) {
    match {
        $s: Signal { sourceType: $source }
    }
    return { $s.sourceType as source_type, $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $q) }
    limit 3
}

query semantic_signals_for_client($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
    }
    return { $c.slug as client_slug, $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $q) }
    limit 3
}

query semantic_signals_for_client_by_vector($client: String, $vq: Vector(8)) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
    }
    return { $c.slug as client_slug, $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $vq) }
    limit 3
}

query semantic_keyword_signals_for_client($client: String, $q: String, $kw: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        search($s.summary, $kw)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $q) }
    limit 3
}

query semantic_fuzzy_signals_for_client($client: String, $q: String, $kw: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        fuzzy($s.summary, $kw)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { nearest($s.summaryEmbedding, $q) }
    limit 3
}

query semantic_opportunities_for_client($client: String, $vq: Vector(8)) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        $s surfaced $o
    }
    return { $o.slug as opportunity_slug, $s.slug as signal_slug, $o.title }
    order { nearest($s.summaryEmbedding, $vq) }
    limit 3
}

query semantic_projects_for_client($client: String, $vq: Vector(8)) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        $s surfaced $o
        $o contains $p
    }
    return { $p.slug as project_slug, $s.slug as signal_slug, $p.name }
    order { nearest($s.summaryEmbedding, $vq) }
    limit 3
}

query keyword_signals_for_client($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        search($s.summary, $q)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query match_text_signals_for_client($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        match_text($s.summary, $q)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query fuzzy_signals_for_client($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        fuzzy($s.summary, $q)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query fuzzy_signals_for_client_strict($client: String, $q: String, $m: I64) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
        fuzzy($s.summary, $q, $m)
    }
    return { $s.slug as signal_slug, $s.summary }
    order { $s.slug asc }
}

query bm25_signals_for_client($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
    }
    return { $s.slug as signal_slug, bm25($s.summary, $q) as lexical_score }
    order { bm25($s.summary, $q) desc }
    limit 3
}

query hybrid_signals_for_client($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
    }
    return { $s.slug as signal_slug, bm25($s.summary, $q) as lexical_score }
    order { rrf(nearest($s.summaryEmbedding, $q), bm25($s.summary, $q), 60) desc }
    limit 3
}

query hybrid_signals_for_client_default_k($client: String, $q: String) {
    match {
        $c: Client { slug: $client }
        $s: Signal
        $s signalAffects $c
    }
    return { $s.slug as signal_slug, bm25($s.summary, $q) as lexical_score }
    order { rrf(nearest($s.summaryEmbedding, $q), bm25($s.summary, $q)) desc }
    limit 3
}

QUERIES
pass "semantic search query file written"

info "Using deterministic mock embeddings and chunking for CI-safe large-text search..."
export NANOGRAPH_EMBEDDINGS_MOCK=1
export NANOGRAPH_EMBED_CHUNK_CHARS=1500
export NANOGRAPH_EMBED_CHUNK_OVERLAP_CHARS=200
unset OPENAI_API_KEY || true

info "Initializing and loading context graph..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$SCHEMA")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

LOAD_JSON=$("$NG" --json load "$DB" --data "$DATA" --mode overwrite)
assert_contains "$LOAD_JSON" '"status":"ok"' "load --json status"

CACHE_FILE="$DB/_embedding_cache.jsonl"
[ -f "$CACHE_FILE" ] || fail "embedding cache file missing at $CACHE_FILE"
CACHE_SAMPLE="$(head -n 1 "$CACHE_FILE")"
assert_contains "$CACHE_SAMPLE" '"chunk_chars":1500' "embedding cache records chunk size"
assert_contains "$CACHE_SAMPLE" '"chunk_overlap_chars":200' "embedding cache records chunk overlap"

CHECK_JSON=$("$NG" --json check --db "$DB" --query "$QUERY_FILE")
assert_contains "$CHECK_JSON" '"status":"ok"' "search query file typechecks"

info "Validating global semantic nearest search..."
QUERY_TEXT="billing reconciliation delay due to missing invoice data"
ALL_RESULTS=$(run_jsonl semantic_signals --param "q=$QUERY_TEXT")
TOP_SIGNAL=$(json_field "$ALL_RESULTS" "signal_slug")
assert_str_eq "$TOP_SIGNAL" "sig-billing-delay" "global nearest(String) ranks exact semantic match first"

info "Validating nearest(Vector) path with CLI vector parameter syntax..."
BILLING_VECTOR_ROW=$(run_jsonl signal_embedding --param "slug=sig-billing-delay")
BILLING_VECTOR=$(json_field "$BILLING_VECTOR_ROW" "embedding")
assert_contains "$BILLING_VECTOR" "[" "signal embedding was returned as vector JSON"
VECTOR_RESULTS=$(run_jsonl semantic_signals_by_vector --param "vq=$BILLING_VECTOR")
VECTOR_TOP=$(json_field "$VECTOR_RESULTS" "signal_slug")
assert_str_eq "$VECTOR_TOP" "sig-billing-delay" "nearest(Vector) ranks identical vector first"

info "Validating context-graph baseline and traversal-constrained semantic nearest search..."
CLIENT_ROW=$(run_jsonl client_row --param "client=cli-priya-shah")
CLIENT_SLUG=$(json_field "$CLIENT_ROW" "client_slug")
assert_str_eq "$CLIENT_SLUG" "cli-priya-shah" "context graph baseline query resolves expected client"

TRAVERSAL_RESULTS=$(run_jsonl semantic_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=$QUERY_TEXT")
TRAVERSAL_TOP=$(json_field "$TRAVERSAL_RESULTS" "signal_slug")
assert_str_eq "$TRAVERSAL_TOP" "sig-billing-delay" "traversal-constrained nearest(String) returns expected signal"

TRAVERSAL_CONSTRAINT_RESULTS=$(run_jsonl semantic_signals_for_client \
    --param "client=cli-jamie-lee" \
    --param "q=$QUERY_TEXT")
TRAVERSAL_CONSTRAINT_TOP=$(json_field "$TRAVERSAL_CONSTRAINT_RESULTS" "signal_slug")
assert_str_eq "$TRAVERSAL_CONSTRAINT_TOP" "sig-referral-analytics" "traversal constraint limits nearest results to connected client signals"

info "Validating several combined traversal + semantic query shapes..."
TRAVERSAL_VECTOR_RESULTS=$(run_jsonl semantic_signals_for_client_by_vector \
    --param "client=cli-priya-shah" \
    --param "vq=$BILLING_VECTOR")
TRAVERSAL_VECTOR_TOP=$(json_field "$TRAVERSAL_VECTOR_RESULTS" "signal_slug")
assert_str_eq "$TRAVERSAL_VECTOR_TOP" "sig-billing-delay" "combined traversal + nearest(Vector) ranks expected signal first"

COMBINED_KEYWORD_RESULTS=$(run_jsonl semantic_keyword_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=$QUERY_TEXT" \
    --param "kw=billing missing")
COMBINED_KEYWORD_TOP=$(json_field "$COMBINED_KEYWORD_RESULTS" "signal_slug")
assert_str_eq "$COMBINED_KEYWORD_TOP" "sig-billing-delay" "combined traversal + search + nearest(String) works"

COMBINED_FUZZY_RESULTS=$(run_jsonl semantic_fuzzy_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=$QUERY_TEXT" \
    --param "kw=reconciliaton delay")
COMBINED_FUZZY_TOP=$(json_field "$COMBINED_FUZZY_RESULTS" "signal_slug")
assert_str_eq "$COMBINED_FUZZY_TOP" "sig-billing-delay" "combined traversal + fuzzy + nearest(String) works"

OPPORTUNITY_RESULTS=$(run_jsonl semantic_opportunities_for_client \
    --param "client=cli-priya-shah" \
    --param "vq=$BILLING_VECTOR")
OPPORTUNITY_TOP=$(json_field "$OPPORTUNITY_RESULTS" "opportunity_slug")
OPPORTUNITY_SIGNAL_TOP=$(json_field "$OPPORTUNITY_RESULTS" "signal_slug")
assert_str_eq "$OPPORTUNITY_TOP" "opp-stripe-migration" "combined semantic + traversal ranks expected opportunity first"
assert_str_eq "$OPPORTUNITY_SIGNAL_TOP" "sig-billing-delay" "combined semantic + traversal ranks by the traversed signal"

PROJECT_RESULTS=$(run_jsonl semantic_projects_for_client \
    --param "client=cli-priya-shah" \
    --param "vq=$BILLING_VECTOR")
PROJECT_TOP=$(json_field "$PROJECT_RESULTS" "project_slug")
PROJECT_SIGNAL_TOP=$(json_field "$PROJECT_RESULTS" "signal_slug")
assert_str_eq "$PROJECT_TOP" "prj-data-pipeline" "combined semantic + multi-hop traversal ranks expected project first"
assert_str_eq "$PROJECT_SIGNAL_TOP" "sig-billing-delay" "combined semantic + multi-hop traversal ranks by the traversed signal"

info "Validating traversal-constrained lexical and fuzzy search..."
KEYWORD_RESULTS=$(run_jsonl keyword_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=billing missing")
KEYWORD_TOP=$(json_field "$KEYWORD_RESULTS" "signal_slug")
assert_str_eq "$KEYWORD_TOP" "sig-billing-delay" "search(field, query) works with graph traversal constraints"

MATCH_TEXT_RESULTS=$(run_jsonl match_text_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=billing missing")
MATCH_TEXT_TOP=$(json_field "$MATCH_TEXT_RESULTS" "signal_slug")
assert_str_eq "$MATCH_TEXT_TOP" "sig-billing-delay" "match_text(field, query) works with graph traversal constraints"

FUZZY_RESULTS=$(run_jsonl fuzzy_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=reconciliaton delay")
FUZZY_TOP=$(json_field "$FUZZY_RESULTS" "signal_slug")
assert_str_eq "$FUZZY_TOP" "sig-billing-delay" "fuzzy(field, query) works with graph traversal constraints"

STRICT_FUZZY_COUNT=$(run_query_count "$DB" "$QUERY_FILE" fuzzy_signals_for_client_strict \
    --param "client=cli-priya-shah" \
    --param "q=reconciliaton delay" \
    --param "m=0")
assert_int_eq "$STRICT_FUZZY_COUNT" 0 "fuzzy(field, query, max_edits=0) rejects typo under traversal constraints"

info "Validating traversal-constrained bm25 and hybrid fusion..."
BM25_RESULTS=$(run_jsonl bm25_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=billing missing invoice")
BM25_TOP=$(json_field "$BM25_RESULTS" "signal_slug")
assert_str_eq "$BM25_TOP" "sig-billing-delay" "bm25(field, query) ranks expected signal first under traversal constraints"

HYBRID_RESULTS=$(run_jsonl hybrid_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=$QUERY_TEXT")
HYBRID_TOP=$(json_field "$HYBRID_RESULTS" "signal_slug")
assert_str_eq "$HYBRID_TOP" "sig-billing-delay" "hybrid rrf(nearest,bm25,k) ranks expected signal first"

HYBRID_DEFAULT_RESULTS=$(run_jsonl hybrid_signals_for_client_default_k \
    --param "client=cli-priya-shah" \
    --param "q=$QUERY_TEXT")
HYBRID_DEFAULT_TOP=$(json_field "$HYBRID_DEFAULT_RESULTS" "signal_slug")
assert_str_eq "$HYBRID_DEFAULT_TOP" "sig-billing-delay" "hybrid rrf(nearest,bm25) default-k path works"

info "Validating scalar-filter constrained semantic nearest search..."
MEETING_QUERY="warm referral for analytics migration project"
SOURCE_RESULTS=$(run_jsonl semantic_signals_for_source --param "source=meeting" --param "q=$MEETING_QUERY")
TOP_SOURCE_SIGNAL=$(json_field "$SOURCE_RESULTS" "signal_slug")
assert_str_eq "$TOP_SOURCE_SIGNAL" "sig-referral-analytics" "filtered nearest(String) returns expected signal"

info "Validating large-text chunked embedding + nearest(String) search..."
LARGE_QUERY="security questionnaire backlog and mitigation owner tracking"
PROCUREMENT_SEARCH=$(run_jsonl semantic_signals_for_source \
    --param "source=observation" \
    --param "q=$LARGE_QUERY")
PROCUREMENT_TOP=$(json_field "$PROCUREMENT_SEARCH" "signal_slug")
assert_str_eq "$PROCUREMENT_TOP" "sig-enterprise-procurement" "nearest(String) works on the large-text chunked signal"

PROCUREMENT_TRAVERSAL=$(run_jsonl semantic_signals_for_client \
    --param "client=cli-priya-shah" \
    --param "q=$LARGE_QUERY")
assert_contains "$PROCUREMENT_TRAVERSAL" '"signal_slug":"sig-enterprise-procurement"' \
    "traversal-constrained nearest(String) includes large-text chunked signal in top-k"

echo ""
pass "context graph semantic search CLI e2e passed"
