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

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local message="$3"
    if echo "$haystack" | grep -F -q "$needle"; then
        pass "$message"
    else
        fail "$message (missing '$needle')"
    fi
}

create_character_name_keyed_schema() {
    local source_schema="$1"
    local output_schema="$2"
    info "Preparing keyed Star Wars schema (all node name fields @key)..."
    awk '
        /^node[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*{/ { in_node = 1; print; next }
        in_node && /^[[:space:]]*name:[[:space:]]*String[[:space:]]*$/ {
            print "    name: String @key"
            next
        }
        in_node && /^}/ { in_node = 0; print; next }
        { print }
    ' "$source_schema" > "$output_schema"

    grep -q 'name: String @key' "$output_schema" \
        || fail "failed to inject @key into node name fields"
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
    local err_file
    err_file="$(mktemp /tmp/nanograph_query_err.XXXXXX)"
    if ! out=$("$NG" run --db "$db" --query "$query_file" --name "$query_name" --format jsonl "$@" 2>"$err_file"); then
        echo -e "${RED}✗ query '$query_name' failed${NC}" >&2
        if [ -s "$err_file" ]; then
            cat "$err_file" >&2
        fi
        rm -f "$err_file"
        return 1
    fi
    rm -f "$err_file"
    echo "$out" | sed -n '/^{/p'
}

run_query_count() {
    local db="$1"
    local query_file="$2"
    local query_name="$3"
    shift 3
    local out
    if ! out="$(run_query_jsonl "$db" "$query_file" "$query_name" "$@")"; then
        return 1
    fi
    if [ -z "$out" ]; then
        echo 0
    else
        echo "$out" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' '
    fi
}

json_field() {
    local jsonl="$1"
    local field="$2"
    local row_index="${3:-1}"
    echo "$jsonl" | sed -n '/^{/p' | sed -n "${row_index}p" | perl -MJSON::PP -e '
use strict;
use warnings;

my $field = shift @ARGV;
local $/;
my $json = <STDIN>;
exit 0 if !defined($json) || $json eq "";

my $obj = eval { JSON::PP::decode_json($json) };
exit 0 if $@ || ref($obj) ne "HASH" || !exists $obj->{$field};

my $value = $obj->{$field};
if (!defined($value)) {
    print "null";
} elsif (ref($value)) {
    print JSON::PP::encode_json($value);
} else {
    print $value;
}
' "$field"
}
