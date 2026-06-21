package analyzer

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// DedupeRunnerSpans collapses spans that share a trace+span ID — e.g. the same
// job or step emitted both by the GitHub API reconstruction and natively by the
// runner (which use the same deterministic ID contract). The runner-emitted span
// (attribute source=runner) wins, because it carries sub-second precision timing.
//
// Applied once to the combined span set so every output mode (TUI, stdout,
// markdown, perfetto, OTLP export) sees a single span per ID.
func DedupeRunnerSpans(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	seen := make(map[string]int)
	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		sc := s.SpanContext()
		if !sc.SpanID().IsValid() {
			out = append(out, s)
			continue
		}
		key := sc.TraceID().String() + ":" + sc.SpanID().String()
		if idx, ok := seen[key]; ok {
			// A prior span shares this trace+span ID. Only treat it as the SAME
			// logical span — the GitHub API reconstruction vs the native runner —
			// when exactly one side is the runner; then the runner wins (it has
			// sub-second timing). If neither (or both) is the runner, these are
			// legitimately distinct spans that happen to collide on a deterministic
			// ID (e.g. two identically-named steps in one job), so keep both.
			if spanIsRunner(s) != spanIsRunner(out[idx]) {
				if spanIsRunner(s) {
					out[idx] = s
				}
				continue
			}
			out = append(out, s)
			continue
		}
		seen[key] = len(out)
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
