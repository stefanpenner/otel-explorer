#!/usr/bin/env bash
# End-to-end smoke matrix: exercises every ote input path against reality.
# Requires: a GitHub token (env or gh auth), network, and `ote sync rails/rails --days=7`
# having been run once (the store-backed checks need a synced repo).
#
# Usage: bazel build //:ote && ./scripts/e2e-smoke.sh
set -u

OTE=${OTE:-./bazel-bin/cmd/ote/ote_/ote}
pass=0; fail=0
check() {
	if eval "$2" >/dev/null 2>&1; then
		echo "PASS: $1"; pass=$((pass + 1))
	else
		echo "FAIL: $1"; fail=$((fail + 1))
	fi
}

check "trace file (chrome)"       "$OTE trace.json --no-tui 2>&1 | grep -q 'runs'"
check "convert round-trip"        "$OTE convert trace.json | head -c 100 | grep -q TraceID"
# --branch forces the API path (a synced store would otherwise serve it);
# 'runs sampled' asserts the sampling annotations rather than the 'obs
# targets' header, which only renders when job-detail sampling engages.
check "trends sampled (API)"      "$OTE trends nodejs/node --days=2 --branch=main 2>&1 | grep -q 'runs sampled'"
# 'Using local store' prints to stderr (stdout stays pipe-clean).
check "trends from store"         "$OTE trends rails/rails --days=7 2>&1 | grep -q 'Using local store'"
check "hourly patterns"           "$OTE trends rails/rails --days=7 2>/dev/null | grep -q 'Hourly Patterns'"
check "trends json parses"        "$OTE trends rails/rails --days=7 --format=json 2>/dev/null | python3 -c 'import json,sys; json.load(sys.stdin)'"
check "sync incremental"          "$OTE sync rails/rails --days=7 2>/dev/null | grep -q 'job detail fetched for 0'"
check "PR URL analysis"           "$OTE https://github.com/stefanpenner/otel-explorer/pull/49 --no-tui --no-artifacts 2>&1 | grep -q 'Run Summary'"
check "webhook stdin"             "echo '{\"workflow_run\":{\"head_sha\":\"d567422\"},\"repository\":{\"full_name\":\"stefanpenner/otel-explorer\"}}' | $OTE --no-tui --no-artifacts 2>&1 | grep -q 'Run Summary'"
check "help mentions sync"        "$OTE --help | grep -q 'ote sync'"

# OTLP receiver: start, post, terminate, expect a styled report.
$OTE --listen=:14319 --no-tui </dev/null >/tmp/ote-smoke-recv.txt 2>&1 &
RPID=$!
sleep 1
curl -s -o /dev/null -X POST -H "Content-Type: application/json" --data \
	'{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"Workflow: smoke","startTimeUnixNano":"1765000000000000000","endTimeUnixNano":"1765000600000000000","attributes":[{"key":"type","value":{"stringValue":"workflow"}},{"key":"github.conclusion","value":{"stringValue":"success"}}]}]}]}]}' \
	http://localhost:14319/v1/traces
kill -TERM $RPID 2>/dev/null
sleep 1
check "OTLP receiver report" "grep -q 'Trace Analyzer' /tmp/ote-smoke-recv.txt"

echo "=== $pass passed, $fail failed"
exit $fail
