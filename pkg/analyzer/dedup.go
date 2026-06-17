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
			// Prefer the runner span over a non-runner duplicate.
			if spanIsRunner(s) && !spanIsRunner(out[idx]) {
				out[idx] = s
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, s)
	}
	return out
}

func spanIsRunner(s sdktrace.ReadOnlySpan) bool {
	for _, a := range s.Attributes() {
		if string(a.Key) == "source" && a.Value.AsString() == "runner" {
			return true
		}
	}
	return false
}
