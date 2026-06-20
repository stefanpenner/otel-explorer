package analyzer

import (
	"bytes"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
)

// FuzzParseToAnalysis composes the two halves of the tool that were previously
// only fuzzed in isolation: it parses arbitrary bytes with the trace-file
// dispatcher and, on success, feeds the resulting spans through the entire
// downstream analysis pipeline (summary, concurrency, runs, combined metrics,
// wall/compute, tree build + flatten). A span can parse cleanly yet carry a
// pathological shape — negative duration, overflowed timestamps, nil resource,
// EndTime < StartTime — that the analysis/render code might not expect. None of
// those consumers may panic on parser output.
func FuzzParseToAnalysis(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"2","endTimeUnixNano":"1"}]}]}]}`), // end < start
		[]byte(`{"traceEvents":[{"ph":"X","name":"n","ts":-5,"dur":-9,"pid":1,"tid":1}]}`),                                                                                                             // negative chrome
		[]byte(`{"data":[{"spans":[{"traceID":"abc","spanID":"def","operationName":"op","startTime":9223372036854775807,"duration":9223372036854775807}]}]}`),                                          // jaeger overflow
		[]byte(`[{"traceId":"1","id":"2","name":"n","timestamp":1,"duration":-1,"localEndpoint":{"serviceName":"s"}}]`),                                                                                // zipkin negative
		[]byte(`{"SpanContext":{"TraceID":"00000000000000000000000000000001","SpanID":"0000000000000001"},"ParentSpanID":"0000000000000000","Name":"n"}`),
		[]byte(``),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	enricher := enrichment.DefaultEnricher()

	f.Fuzz(func(t *testing.T, data []byte) {
		spans, err := otlpfile.Parse(bytes.NewReader(data))
		if err != nil || len(spans) == 0 {
			return
		}

		// Time bounds for the tree builder, derived from the spans themselves.
		earliest, latest := spans[0].StartTime(), spans[0].EndTime()
		for _, s := range spans {
			if s.StartTime().Before(earliest) {
				earliest = s.StartTime()
			}
			if s.EndTime().After(latest) {
				latest = s.EndTime()
			}
		}

		// None of the real downstream consumers may panic on parser output.
		_ = CalculateSummary(spans, enricher)
		_ = CalculateConcurrency(spans, enricher)
		_ = RunsFromSpans(spans, enricher)
		_ = CombinedMetricsFromSpans(spans, enricher)
		_, _ = SpansWallCompute(spans, enricher)
		roots := BuildTreeFromSpans(spans, earliest, latest, enricher)
		_ = FlattenTree(roots)
	})
}
