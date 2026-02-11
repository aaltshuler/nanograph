#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

SCENARIO_DIR="$SCRIPT_DIR/scenarios"

usage() {
    cat << 'USAGE'
Usage:
  bash tests/cli/run-cli-e2e.sh                # run all scenarios
  bash tests/cli/run-cli-e2e.sh migration      # run one scenario
  bash tests/cli/run-cli-e2e.sh lifecycle query_mutations
  bash tests/cli/run-cli-e2e.sh --list
USAGE
}

list_scenarios() {
    find "$SCENARIO_DIR" -maxdepth 1 -type f -name '*.sh' -print0 \
        | xargs -0 -n1 basename \
        | sed 's/\.sh$//' \
        | sort
}

scenario_path() {
    local name="$1"
    local candidate="$SCENARIO_DIR/${name}.sh"
    if [ -f "$candidate" ]; then
        echo "$candidate"
        return 0
    fi
    candidate="$SCENARIO_DIR/$name"
    if [ -f "$candidate" ]; then
        echo "$candidate"
        return 0
    fi
    return 1
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    usage
    exit 0
fi

if [ "${1:-}" = "--list" ]; then
    list_scenarios
    exit 0
fi

if [ "$#" -eq 0 ]; then
    SELECTED=()
    while IFS= read -r scenario_name; do
        [ -n "$scenario_name" ] && SELECTED+=("$scenario_name")
    done < <(list_scenarios)
else
    SELECTED=("$@")
fi

if [ "${#SELECTED[@]}" -eq 0 ]; then
    fail "no scenarios found in $SCENARIO_DIR"
fi

for name in "${SELECTED[@]}"; do
    if ! script_path="$(scenario_path "$name")"; then
        fail "unknown scenario '$name' (run with --list)"
    fi
    info "Running $(basename "$script_path")..."
    bash "$script_path"
done

pass "CLI E2E scenarios completed"
