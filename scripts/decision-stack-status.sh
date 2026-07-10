#!/usr/bin/env bash
# decision-stack-status.sh — human-readable inventory of the decision-core stack.
#
# One row per core: full TLC, decision core, Go gen, Rust gen, prod→gen SSOT,
# production symbols. No TLC/JVM required.
#
# Usage: scripts/decision-stack-status.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# core|gen_pkg|prod_symbols|ssot_gen_symbol
# Keep SSOT column in sync with specs/GATES.md.
rows=(
  "tui-reload|pkg/tui/results/tuireloadspec|logFetchResultFresh|CanFetchAccept"
  "rate-limit|pkg/githubapi/ratelimitspec|rateLimitWaitNeeded|WaitNeeded"
  "timing-clamp|pkg/analyzer/timingclampspec|clampSpanToParent|DoClamp"
  "sync-bounds|pkg/store/syncboundsspec|acceptJobsAttempt|AcceptAllowed"
  "gha-lifecycle|pkg/analyzer/ghalifecyclespec|isJobPending,counts*|CanClassify*"
  "log-groups|pkg/logparse/loggroupsspec|canOpen/Close|CanOpen/CanClose"
  "span-tree|pkg/analyzer/spantreespec|dropAPIForRunnerTwin|DedupChoose"
)

# prod package dir for import check (prefix of gen_pkg without *spec name)
prod_dir_for() {
  local gen="$1"
  # pkg/foo/barspec → pkg/foo ; pkg/a/b/cspec → pkg/a/b
  echo "${gen%/*}"
}

# core dir name → crates/decision_cores/src/<snake>.rs
rust_mod_for() {
  echo "${1//-/_}"
}

printf '%-14s %-4s %-4s %-4s %-4s %-6s %-18s %s\n' \
  "CORE" "FULL" "DEC" "GEN" "RS" "IMPORT" "SSOT" "PRODUCTION"
printf '%-14s %-4s %-4s %-4s %-4s %-6s %-18s %s\n' \
  "----" "----" "---" "---" "--" "------" "----" "----------"

for row in "${rows[@]}"; do
  core="${row%%|*}"
  rest="${row#*|}"
  gen="${rest%%|*}"
  rest="${rest#*|}"
  prod="${rest%%|*}"
  ssot="${rest#*|}"

  full="no"
  if find "specs/$core" -maxdepth 1 -name '*.tla' 2>/dev/null | grep -q .; then
    full="yes"
  fi

  decision="no"
  [ -f "specs/$core/decision/Decision.tla" ] && decision="yes"

  genok="no"
  [ -f "$gen/spec.go" ] && genok="yes"

  rs="no"
  rsmod="$(rust_mod_for "$core")"
  [ -f "crates/decision_cores/src/${rsmod}.rs" ] && rs="yes"

  # SSOT import: non-test production sources under parent package use *spec name
  import="no"
  pdir="$(prod_dir_for "$gen")"
  pkgbase="$(basename "$gen")"
  while IFS= read -r -d '' f; do
    case "$f" in
      *spec/*) continue ;;
    esac
    if grep -F -q -- "$pkgbase" "$f" 2>/dev/null; then
      import="yes"
      break
    fi
  done < <(find "$pdir" -type f -name '*.go' ! -name '*_test.go' -print0 2>/dev/null)

  printf '%-14s %-4s %-4s %-4s %-4s %-6s %-18s %s\n' \
    "$core" "$full" "$decision" "$genok" "$rs" "$import" "$ssot" "$prod"
done

echo
echo "Legend: FULL=full TLC  DEC=decision/  GEN=Go *spec  RS=Rust crate module"
echo "        IMPORT=Go prod package imports *spec (SSOT, not re-inlined)"
echo "        SSOT=generated symbol production should call"
echo "        Rust peer: crates/decision_cores (+ gates::* wrappers)"
echo "Verify: scripts/decision-check.sh · scripts/check-specs.sh"
