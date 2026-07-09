#!/usr/bin/env bash
# decision-stack-status.sh — human-readable inventory of the decision-core stack.
#
# Prints one row per subsystem: TLC full, decision core, generated package,
# pure preds, production gates. No TLC/JVM required.
#
# Usage: scripts/decision-stack-status.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# core|gen_pkg|prod_symbols (comma-separated)
rows=(
  "tui-reload|pkg/tui/results/tuireloadspec|logFetchResultFresh"
  "rate-limit|pkg/githubapi/ratelimitspec|rateLimitWaitNeeded"
  "timing-clamp|pkg/analyzer/timingclampspec|DoClamp(via timingclampspec)"
  "sync-bounds|pkg/store/syncboundsspec|acceptJobsAttempt"
  "gha-lifecycle|pkg/analyzer/ghalifecyclespec|countsFailed,countsQueue"
  "log-groups|pkg/logparse/loggroupsspec|canOpenGroup,canCloseGroup"
  "span-tree|pkg/analyzer/spantreespec|dropAPIForRunnerTwin"
)

printf '%-14s %-6s %-8s %-10s %-28s %s\n' \
  "CORE" "FULL" "DECISION" "GEN" "PURE_PREDS" "PRODUCTION"
printf '%-14s %-6s %-8s %-10s %-28s %s\n' \
  "----" "----" "--------" "---" "----------" "----------"

for row in "${rows[@]}"; do
  core="${row%%|*}"
  rest="${row#*|}"
  gen="${rest%%|*}"
  prod="${rest#*|}"

  full="no"
  if find "specs/$core" -maxdepth 1 -name '*.tla' 2>/dev/null | grep -q .; then
    full="yes"
  fi

  decision="no"
  [ -f "specs/$core/decision/Decision.tla" ] && decision="yes"

  genok="no"
  [ -f "$gen/spec.go" ] && genok="yes"

  preds="-"
  if [ -f "$gen/spec.go" ]; then
    preds=$(grep -oE 'Name: "[A-Za-z0-9_]+"' "$gen/spec.go" 2>/dev/null \
      | sed 's/Name: "//;s/"$//' \
      | tr '\n' ',' \
      | sed 's/,$//')
    [ -n "$preds" ] || preds="(none)"
  fi

  printf '%-14s %-6s %-8s %-10s %-28s %s\n' \
    "$core" "$full" "$decision" "$genok" "${preds:0:28}" "$prod"
done

echo
echo "Legend: FULL=specs/<core>/*.tla  DECISION=decision/Decision.tla  GEN=committed *spec"
echo "Verify: scripts/verify-decision-wires.sh · scripts/check-specs.sh"
