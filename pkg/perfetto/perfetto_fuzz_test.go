package perfetto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
)

// FuzzWriteTrace fuzzes the emit side: it parses arbitrary bytes with the
// trace-file dispatcher and, on success, runs the resulting spans through the
// Perfetto protobuf writer (WriteTrace), which does per-span timestamp math,
// UUID/track derivation, and protobuf encoding. A span can parse cleanly yet
// carry a pathological shape (negative/overflowed timestamps, EndTime <
// StartTime, weird pid/tid in chrome traces) that the writer's math might not
// expect — it must never panic, only error or succeed. This is the previously
// unfuzzed half of the tool (the analysis half is covered by
// analyzer.FuzzParseToAnalysis).
func FuzzWriteTrace(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"traceEvents":[{"ph":"X","name":"n","ts":0,"dur":1,"pid":1,"tid":1}]}`),
		[]byte(`{"traceEvents":[{"ph":"X","name":"n","ts":-5,"dur":-9,"pid":-1,"tid":0}]}`),
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"2","endTimeUnixNano":"1"}]}]}]}`),
		[]byte(`{"data":[{"spans":[{"traceID":"abc","spanID":"def","operationName":"op","startTime":9223372036854775807,"duration":9223372036854775807}]}]}`),
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
		combined := analyzer.CombinedMetricsFromSpans(spans, enricher)
		// Emit to a discard writer; must not panic on pathological spans.
		_ = WriteTrace(io.Discard, nil, combined, nil, 0, "", false, spans)
	})
}
