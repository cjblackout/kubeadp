#!/usr/bin/env bash
# Full local verification for agent-run: unit tests of the whole
# kubeadp-agent package, then the black-box suite (which builds both
# binaries and drives a private agent-run-api + scripted fake `agy`).
#
# Run with:  bash run-all.sh
#
# Environment knobs (passed through to run-tests.sh / cargo):
#   ONLY     space-separated list of black-box test ids (e.g. ONLY="T02 T07")
#   KEEP=1   keep the black-box temp dir for inspection
#   PORT     black-box server port (default 18002)
#   BIN_DIR  dir with prebuilt binaries (skips the suite's cargo rebuild)
#
# This script only BUILDS and TESTS: it never talks to a cluster, never
# pushes, and never kills processes by name pattern.

set -u -o pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CRATES_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

command -v cargo >/dev/null || { echo "FATAL: cargo not found in PATH" >&2; exit 1; }
command -v bash >/dev/null || { echo "FATAL: bash not found in PATH" >&2; exit 1; }

FAILED=0

# run_stage <name> <command...> -- prints one summary line, records the result.
run_stage() {
  local name="$1"
  shift
  local tmp
  tmp=$(mktemp 2>/dev/null) || tmp="/tmp/run-all-stage"
  if "$@" >"$tmp" 2>&1; then
    echo "OK   $name"
    rm -f "$tmp"
  else
    local rc=$?
    echo "FAIL $name (exit $rc)"
    echo "--- $name: last 40 lines ---" >&2
    tail -n 40 "$tmp" >&2
    rm -f "$tmp"
    FAILED=1
  fi
}

echo "== agent-run verification suite =="

run_stage "cargo test kubeadp-agent" \
  cargo test --manifest-path "$CRATES_DIR/Cargo.toml"
run_stage "black-box run-tests.sh" \
  bash "$SCRIPT_DIR/run-tests.sh"

echo
echo "=== run-all summary ==="
if [ "$FAILED" -ne 0 ]; then
  echo "RUN-ALL RESULT: FAILED" >&2
  exit 1
fi
echo "RUN-ALL RESULT: all stages passed"
