#!/usr/bin/env bash
# Verifies that every OTel source sample in docs/samples/ renders with its
# documented markers — the screenshots in docs/otel-sources.md must match
# reality. Self-contained: no network or GitHub token required.
#
# Usage: go build -o /tmp/ote ./cmd/ote && OTE=/tmp/ote ./scripts/verify-sources.sh
#    or: bazel build //cmd/ote:ote && ./scripts/verify-sources.sh
set -u

OTE=${OTE:-./bazel-bin/cmd/ote/ote_/ote}
SAMPLES=${SAMPLES:-docs/samples}
pass=0
fail=0

# Render caches by file path, so clear it once up front (the rebuilt binary may
# otherwise serve a previous run's enrichment). --clear-cache exits without
# rendering, so it must be a separate invocation.
"$OTE" --clear-cache >/dev/null 2>&1 || true

# check NAME FILE PATTERN...: render FILE and assert every PATTERN is present.
check() {
	local name="$1" file="$2"
	shift 2
	local out
	out=$("$OTE" "$SAMPLES/$file" --no-tui 2>&1 | sed 's/\x1b\[[0-9;]*m//g')
	local missing=""
	local p
	for p in "$@"; do
		if ! grep -qF -- "$p" <<<"$out"; then
			missing="$missing [$p]"
		fi
	done
	if [ -z "$missing" ]; then
		echo "PASS: $name"
		pass=$((pass + 1))
	else
		echo "FAIL: $name — missing:$missing"
		fail=$((fail + 1))
	fi
}

check "rag-api (mixed sources)" rag-api.jsonl \
	"🤖" "🔢" "⛁" "✉" "4.2k→620 tok" "overloaded_error" "LLM Usage" "Resources"
check "cicd pipeline"           cicd-pipeline.jsonl "🚀" "🔨" "🧪" "❌"
check "llm agent loop"          llm-agent.jsonl "🧠" "🤖" "tok" "LLM Usage"
check "graphql + tools"         graphql-tools.jsonl "◆" "🔧" "🔎" "🧠"
check "exceptions"              exceptions.jsonl "❌" "PaymentDeclined"
check "feature flags"           feature-flags.jsonl "🚩" "new-dashboard=on"
check "microservices (otlp/json)" microservices.json "Resources" "k8s" "aws/us-east-1" "LLM Usage"
check "grpc status codes"       grpc.jsonl "⇌" "→ cart-svc" "[NOT_FOUND] ❌" "[UNAVAILABLE] ❌"
check "code origin + peer"      code-origin.jsonl "checkout.ProcessOrder (order.go:142)" "→ pricing-svc"

echo
echo "verify-sources: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
