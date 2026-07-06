package analyzer

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// DedupeRunnerSpans collapses spans that share a trace+span ID — e.g. the same
// job or step emitted both by the GitHub API reconstruction and natively by the
// runner (which use the same deterministic ID contract). The runner-emitted
// span wins, because it carries sub-second precision timing.
//
// Resolution is per (traceID, spanID) GROUP, not per arrival: a group
// collapses only when it is exactly {one API span, one runner span} — the one
// unambiguous API/runner twin shape — and the runner survives. Any other group
// shape (singletons, or ≥2 spans on either side, e.g. two identically-named
// steps in one job colliding on a deterministic ID) is kept whole. Group-based
// resolution is deterministic, order-insensitive, and idempotent by
// construction (a collapsed group re-enters as a runner singleton). Pinned by
// TestDedupSpec_Idempotent / TestDedupSpec_OrderInsensitive (span-tree spec).
//
// Applied once to the combined span set so every output mode (TUI, stdout,
// markdown, perfetto, OTLP export) sees a single span per ID.
func DedupeRunnerSpans(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	type group struct{ api, runner int }
	groups := make(map[string]group)
	key := func(s sdktrace.ReadOnlySpan) string {
		sc := s.SpanContext()
		return sc.TraceID().String() + ":" + sc.SpanID().String()
	}
	for _, s := range spans {
		// Zero span IDs are not identities; those spans always pass through.
		if !s.SpanContext().SpanID().IsValid() {
			continue
		}
		g := groups[key(s)]
		if spanIsRunner(s) {
			g.runner++
		} else {
			g.api++
		}
		groups[key(s)] = g
	}
	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		if s.SpanContext().SpanID().IsValid() {
			if g := groups[key(s)]; g.api == 1 && g.runner == 1 && !spanIsRunner(s) {
				continue // the runner twin supersedes this API reconstruction
			}
		}
		out = append(out, s)
	}
	return out
}

// spanIsRunner reports whether a span was emitted natively by the GitHub Actions
// runner. It keys on standard OTel signals — the instrumentation scope name and
// the resource service.name — rather than a custom "source" attribute, so the
// distinction is the same one any OTel backend would make.
func spanIsRunner(s sdktrace.ReadOnlySpan) bool {
	if s.InstrumentationScope().Name == runnerScopeName {
		return true
	}
	if res := s.Resource(); res != nil {
		for _, a := range res.Attributes() {
			if string(a.Key) == "service.name" && a.Value.AsString() == runnerServiceName {
				return true
			}
		}
	}
	return false
}
