#!/usr/bin/env bash
# verify-decision-wires.sh — assert production symbols exist for every
# decision core that claims a production wire in specs/DECISION_CORES.md.
#
# Portable: uses find + grep (no ripgrep). Safe for GitHub Actions.
# Exit 1 if a documented wire is missing from the tree.
#
# Usage: scripts/verify-decision-wires.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# core_name | package path | required symbol
# Keep in sync with specs/DECISION_CORES.md production column.
wires=(
  "tui-reload|pkg/tui/results|logFetchResultFresh"
  "rate-limit|pkg/githubapi|rateLimitWaitNeeded"
  "timing-clamp|pkg/analyzer|timingclampspec"
  "sync-bounds|pkg/store|acceptJobsAttempt"
  "gha-lifecycle|pkg/analyzer|countsFailed"
  "gha-lifecycle|pkg/analyzer|countsQueue"
  "span-tree|pkg/analyzer|dropAPIForRunnerTwin"
  "log-groups|pkg/logparse|canOpenGroup"
  "log-groups|pkg/logparse|canCloseGroup"
)

gen_pkgs=(
  "pkg/tui/results/tuireloadspec/spec.go"
  "pkg/githubapi/ratelimitspec/spec.go"
  "pkg/analyzer/timingclampspec/spec.go"
  "pkg/store/syncboundsspec/spec.go"
  "pkg/analyzer/ghalifecyclespec/spec.go"
  "pkg/logparse/loggroupsspec/spec.go"
  "pkg/analyzer/spantreespec/spec.go"
)

# True if $sym appears in a non-test, non-*spec production .go under $dir.
# Uses find+grep so CI does not need ripgrep.
has_prod_symbol() {
  local dir="$1" sym="$2"
  local f
  while IFS= read -r -d '' f; do
    # Skip generated *spec packages (…/foospec/spec.go).
    case "$f" in
      *spec/*) continue ;;
    esac
    if grep -F -q -- "$sym" "$f" 2>/dev/null; then
      return 0
    fi
  done < <(find "$dir" -type f -name '*.go' ! -name '*_test.go' -print0 2>/dev/null)
  return 1
}

fail=0

echo "--- decision cores (Decision.tla) ---"
for d in specs/*/decision/Decision.tla; do
  [ -f "$d" ] || continue
  echo "  ok $d"
done

echo "--- generated packages ---"
for p in "${gen_pkgs[@]}"; do
  if [ -f "$p" ]; then
    echo "  ok $p"
  else
    echo "  FAIL missing $p"
    fail=1
  fi
done

echo "--- production wires ---"
for row in "${wires[@]}"; do
  IFS='|' read -r core dir sym <<<"$row"
  if has_prod_symbol "$dir" "$sym"; then
    echo "  ok $core → $sym"
  else
    echo "  FAIL $core: $sym not in $dir production sources"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "verify-decision-wires: FAILED"
  exit 1
fi
echo "verify-decision-wires: ok"
