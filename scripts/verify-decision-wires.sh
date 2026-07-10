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
# Keep in sync with specs/DECISION_CORES.md + specs/GATES.md.
wires=(
  "tui-reload|pkg/tui/results|logFetchResultFresh"
  "rate-limit|pkg/githubapi|rateLimitWaitNeeded"
  "timing-clamp|pkg/analyzer|timingclampspec"
  "sync-bounds|pkg/store|acceptJobsAttempt"
  "gha-lifecycle|pkg/analyzer|isJobPending"
  "gha-lifecycle|pkg/analyzer|countsFailed"
  "gha-lifecycle|pkg/analyzer|countsQueue"
  "span-tree|pkg/analyzer|dropAPIForRunnerTwin"
  "log-groups|pkg/logparse|canOpenGroup"
  "log-groups|pkg/logparse|canCloseGroup"
)

# Production package dir | gen package name that must appear in non-test imports.
# Ensures SSOT (prod calls *spec) rather than a re-inlined formula under the same symbol.
ssot_imports=(
  "pkg/tui/results|tuireloadspec"
  "pkg/githubapi|ratelimitspec"
  "pkg/analyzer|timingclampspec"
  "pkg/analyzer|ghalifecyclespec"
  "pkg/analyzer|spantreespec"
  "pkg/store|syncboundsspec"
  "pkg/logparse|loggroupsspec"
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

# Dual tests that pin production gates to generated Can*/actions.
dual_tests=(
  "pkg/tui/results/tuireload_decision_dual_test.go"
  "pkg/githubapi/rate_limit_decision_dual_test.go"
  "pkg/analyzer/clamp_decision_dual_test.go"
  "pkg/store/sync_bounds_decision_dual_test.go"
  "pkg/analyzer/gha_lifecycle_decision_dual_test.go"
  "pkg/logparse/loggroups_decision_dual_test.go"
  "pkg/analyzer/spantree_decision_dual_test.go"
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
    # PurePredicates registry is required for dual/CI enumeration.
    if ! grep -F -q 'func PurePredicates()' "$p" 2>/dev/null; then
      echo "  FAIL $p missing PurePredicates() registry"
      fail=1
    fi
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

echo "--- prod → gen SSOT imports ---"
for row in "${ssot_imports[@]}"; do
  IFS='|' read -r dir pkg <<<"$row"
  found=0
  while IFS= read -r -d '' f; do
    case "$f" in
      *spec/*) continue ;;
    esac
    if grep -F -q -- "$pkg" "$f" 2>/dev/null; then
      found=1
      break
    fi
  done < <(find "$dir" -type f -name '*.go' ! -name '*_test.go' -print0 2>/dev/null)
  if [ "$found" -eq 1 ]; then
    echo "  ok $dir imports $pkg"
  else
    echo "  FAIL $dir: missing import/use of $pkg (re-inlined formula?)"
    fail=1
  fi
done

echo "--- dual tests ---"
for t in "${dual_tests[@]}"; do
  if [ -f "$t" ]; then
    echo "  ok $t"
  else
    echo "  FAIL missing dual test $t"
    fail=1
  fi
done

# Optional: regen no-diff when SPECGEN_CHECK_REGEN=1 and specgen on PATH.
# Catches Decision.tla drift against committed *spec packages.
if [ "${SPECGEN_CHECK_REGEN:-}" = "1" ] && command -v specgen >/dev/null 2>&1; then
  echo "--- regen no-diff (SPECGEN_CHECK_REGEN=1) ---"
  if ! "$REPO_ROOT/scripts/regenerate-decision-cores.sh" >/tmp/regen-decision-cores.log 2>&1; then
    echo "  FAIL regenerate-decision-cores.sh"
    tail -20 /tmp/regen-decision-cores.log
    fail=1
  elif ! git -C "$REPO_ROOT" diff --quiet -- 'pkg/**/*spec/spec.go' 'pkg/**/*spec/spec_test.go' 2>/dev/null; then
    echo "  FAIL generated packages drift after regenerate:"
    git -C "$REPO_ROOT" diff --stat -- 'pkg/**/*spec/' || true
    fail=1
    # Restore tree so a failed check does not leave dirty sources.
    git -C "$REPO_ROOT" checkout -- 'pkg/**/*spec/' 2>/dev/null || true
  else
    echo "  ok no drift"
  fi
fi

if [ "$fail" -ne 0 ]; then
  echo "verify-decision-wires: FAILED"
  exit 1
fi
echo "verify-decision-wires: ok"
