package analyzer

// Tests derived from the specs/span-tree TLA+ campaign (Findings 1 and 3,
// FINDINGS.md #10): DedupeRunnerSpans must be idempotent and its survivor
// set must not depend on the arrival order of same-ID spans.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var dedupSpecTraceID = trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// dedupSpecSpan builds a span whose fingerprint (name + end time) is unique
// per input span, so survivor sets can be compared across permutations even
// when spans collide on the same span ID.
func dedupSpecSpan(id byte, runner bool, name string, endOffset time.Duration) tracetest.SpanStub {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	var sid trace.SpanID
	if id != 0 {
		sid = trace.SpanID{0, 0, 0, 0, 0, 0, 0, id}
	}
	stub := tracetest.SpanStub{
		Name: name,
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: dedupSpecTraceID, SpanID: sid, TraceFlags: trace.FlagsSampled,
		}),
		StartTime: base,
		EndTime:   base.Add(endOffset),
	}
	if runner {
		stub.InstrumentationScope = runnerScope
	}
	return stub
}

func snapshotAll(stubs []tracetest.SpanStub) []sdktrace.ReadOnlySpan {
	return tracetest.SpanStubs(stubs).Snapshots()
}

// spanFingerprint identifies one input span uniquely (tests assign each span
// a distinct name/end-time pair).
func spanFingerprint(s sdktrace.ReadOnlySpan) string {
	return fmt.Sprintf("%s|%s|%d|%v",
		s.SpanContext().SpanID(), s.Name(), s.EndTime().UnixNano(), spanIsRunner(s))
}

func fingerprints(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = spanFingerprint(s)
	}
	return out
}

func sortedFingerprints(spans []sdktrace.ReadOnlySpan) []string {
	out := fingerprints(spans)
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// specCollisionCorpus returns the collision cases from the span-tree spec
// README traces, plus surrounding shapes, as named stub sets.
func specCollisionCorpus() map[string][]tracetest.SpanStub {
	return map[string][]tracetest.SpanStub{
		// Finding 1 witness: [api, api', runner] all sharing one span ID.
		// First pass kept {runner, api'}; second pass dropped api'.
		"finding1_api_api_runner": {
			dedupSpecSpan(2, false, "step", 1*time.Second),
			dedupSpecSpan(2, false, "step-twin", 2*time.Second),
			dedupSpecSpan(2, true, "step", 3*time.Second),
		},
		// Finding 3 witness order: [api, runner, api'] kept only {runner}.
		"finding3_api_runner_api": {
			dedupSpecSpan(2, false, "step", 1*time.Second),
			dedupSpecSpan(2, true, "step", 3*time.Second),
			dedupSpecSpan(2, false, "step-twin", 2*time.Second),
		},
		// The one shape that must collapse: exactly {1 api, 1 runner}.
		"pair_collapses": {
			dedupSpecSpan(2, false, "step", 1*time.Second),
			dedupSpecSpan(2, true, "step", 2*time.Second),
		},
		// Two runners, one api — ambiguous, keep all.
		"api_runner_runner": {
			dedupSpecSpan(2, false, "step", 1*time.Second),
			dedupSpecSpan(2, true, "step", 2*time.Second),
			dedupSpecSpan(2, true, "step-b", 3*time.Second),
		},
		// Zero-ID spans always pass through untouched.
		"zero_ids": {
			dedupSpecSpan(0, false, "z1", 1*time.Second),
			dedupSpecSpan(0, false, "z2", 2*time.Second),
			dedupSpecSpan(2, false, "step", 3*time.Second),
			dedupSpecSpan(2, true, "step", 4*time.Second),
		},
		// Multiple groups: a collapsing pair next to a keep-all triple.
		"mixed_groups": {
			dedupSpecSpan(2, false, "a", 1*time.Second),
			dedupSpecSpan(2, false, "a-twin", 2*time.Second),
			dedupSpecSpan(2, true, "a", 3*time.Second),
			dedupSpecSpan(3, false, "b", 4*time.Second),
			dedupSpecSpan(3, true, "b", 5*time.Second),
			dedupSpecSpan(4, false, "c", 6*time.Second),
		},
	}
}

// TestDedupSpec_Idempotent pins the spec's DedupIdempotent invariant
// (span-tree Finding 1, refuting the old tree.go comment). Dedup really runs
// twice in production: main.go combine, then BuildTreeFromSpans.
func TestDedupSpec_Idempotent(t *testing.T) {
	for name, stubs := range specCollisionCorpus() {
		t.Run(name, func(t *testing.T) {
			once := DedupeRunnerSpans(snapshotAll(stubs))
			twice := DedupeRunnerSpans(once)
			if !equalStrings(fingerprints(once), fingerprints(twice)) {
				t.Errorf("dedup not idempotent:\n once: %v\ntwice: %v",
					fingerprints(once), fingerprints(twice))
			}
		})
	}

	// Generated corpus: random small span sets over a few colliding IDs.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		n := 1 + rng.Intn(8)
		stubs := make([]tracetest.SpanStub, n)
		for j := range stubs {
			id := byte(rng.Intn(4)) // 0 = zero span ID
			stubs[j] = dedupSpecSpan(id, rng.Intn(2) == 0,
				fmt.Sprintf("s%d", j), time.Duration(j+1)*time.Second)
		}
		once := DedupeRunnerSpans(snapshotAll(stubs))
		twice := DedupeRunnerSpans(once)
		if !equalStrings(fingerprints(once), fingerprints(twice)) {
			t.Fatalf("dedup not idempotent on generated case %d:\ninput: %v\n once: %v\ntwice: %v",
				i, fingerprints(snapshotAll(stubs)), fingerprints(once), fingerprints(twice))
		}
	}
}

func permutations(n int) [][]int {
	var out [][]int
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	var rec func(k int)
	rec = func(k int) {
		if k == n {
			out = append(out, append([]int(nil), perm...))
			return
		}
		for i := k; i < n; i++ {
			perm[k], perm[i] = perm[i], perm[k]
			rec(k + 1)
			perm[k], perm[i] = perm[i], perm[k]
		}
	}
	rec(0)
	return out
}

// TestDedupSpec_OrderInsensitive pins the spec's DedupOrderInsensitive
// invariant (span-tree Finding 3): which spans survive dedup must not depend
// on arrival order — combined-slice order is append-dependent in production.
func TestDedupSpec_OrderInsensitive(t *testing.T) {
	for name, stubs := range specCollisionCorpus() {
		t.Run(name, func(t *testing.T) {
			var want []string
			for _, perm := range permutations(len(stubs)) {
				ordered := make([]tracetest.SpanStub, len(stubs))
				for i, ix := range perm {
					ordered[i] = stubs[ix]
				}
				got := sortedFingerprints(DedupeRunnerSpans(snapshotAll(ordered)))
				if want == nil {
					want = got
					continue
				}
				if !equalStrings(want, got) {
					t.Fatalf("survivor set depends on order:\nperm %v: %v\nfirst perm: %v",
						perm, got, want)
				}
			}
		})
	}
}
