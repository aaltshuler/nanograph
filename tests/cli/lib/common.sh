#!/usr/bin/env bash

# Shared helpers for CLI end-to-end shell scenarios.

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; exit 1; }
info() { echo -e "${YELLOW}→ $1${NC}"; }

repo_root_from_script_dir() {
    local script_dir="$1"
    cd "$script_dir/../../.." >/dev/null 2>&1 && pwd
}

build_nanograph_binary() {
    local root="$1"
    info "Building nanograph..."
    cargo build --manifest-path "$root/Cargo.toml" --quiet 2>/dev/null
    NG="$root/target/debug/nanograph"
    [ -x "$NG" ] || fail "binary not found at $NG"
    export NG
    pass "Binary built"
}

assert_int_eq() {
    local actual="$1"
    local expected="$2"
    local message="$3"
    if [ "$actual" -eq "$expected" ]; then
        pass "$message"
    else
        fail "$message (expected $expected, got $actual)"
    fi
}

assert_int_ge() {
    local actual="$1"
    local min="$2"
    local message="$3"
    if [ "$actual" -ge "$min" ]; then
        pass "$message"
    else
        fail "$message (expected >= $min, got $actual)"
    fi
}

assert_str_eq() {
    local actual="$1"
    local expected="$2"
    local message="$3"
    if [ "$actual" = "$expected" ]; then
        pass "$message"
    else
        fail "$message (expected '$expected', got '$actual')"
    fi
}

create_character_name_keyed_schema() {
    local source_schema="$1"
    local output_schema="$2"
    info "Preparing keyed Star Wars schema (Character.name @key)..."
    awk '
        /^node Character[[:space:]]*{/ { in_character = 1; print; next }
        in_character && /^[[:space:]]*name:[[:space:]]*String[[:space:]]*$/ {
            print "    name: String @key"
            next
        }
        in_character && /^}/ { in_character = 0; print; next }
        { print }
    ' "$source_schema" > "$output_schema"

    grep -q 'name: String @key' "$output_schema" \
        || fail "failed to inject @key into Character.name"
    pass "Keyed schema generated"
}

count_csv_data_rows() {
    local csv="$1"
    echo "$csv" | tail -n +2 | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' '
}

run_query_jsonl() {
    local db="$1"
    local query_file="$2"
    local query_name="$3"
    shift 3
    local out
    if ! out=$("$NG" run --db "$db" --query "$query_file" --name "$query_name" --format jsonl "$@" 2>/dev/null); then
        fail "query '$query_name' failed"
    fi
    echo "$out" | sed -n '/^{/p'
}

run_query_count() {
    local db="$1"
    local query_file="$2"
    local query_name="$3"
    shift 3
    local out
    out="$(run_query_jsonl "$db" "$query_file" "$query_name" "$@")"
    if [ -z "$out" ]; then
        echo 0
    else
        echo "$out" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' '
    fi
}

json_field() {
    local jsonl="$1"
    local field="$2"
    echo "$jsonl" | sed -n '/^{/p' | head -n 1 | sed -n "s/.*\"$field\":\"\\([^\"]*\\)\".*/\\1/p"
}
