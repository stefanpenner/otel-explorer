package otel

import (
	"bytes"
	"context"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
)

// FuzzRoundTrip is a property fuzzer (not just a panic check): it parses
// arbitrary bytes, re-emits the spans via the OTel stdout exporter (the format
// `ote --otel` produces and that otlpfile is designed to re-ingest), then
// re-parses that output. The invariant is fidelity — the span count must
// survive parse → emit → re-parse. A drop means the emit/re-ingest path silently
// loses spans, a correctness bug that panic-only fuzzing cannot see.
func FuzzRoundTrip(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`),
		[]byte(`{"SpanContext":{"TraceID":"00000000000000000000000000000001","SpanID":"0000000000000001"},"Name":"n","StartTime":"2024-01-01T00:00:00Z","EndTime":"2024-01-01T00:00:01Z"}`),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	ctx := context.Background()

	f.Fuzz(func(t *testing.T, data []byte) {
		spans1, err := otlpfile.Parse(bytes.NewReader(data))
		if err != nil || len(spans1) == 0 {
			return
		}

		var buf bytes.Buffer
		exp, err := NewStdoutExporter(&buf)
		if err != nil {
			t.Fatalf("NewStdoutExporter: %v", err)
		}
		if err := exp.Export(ctx, spans1); err != nil {
			return // emit failure is a separate concern; not the fidelity property
		}
		_ = exp.Finish(ctx)

		spans2, err := otlpfile.Parse(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("re-parsing emitted OTel output failed: %v\nemitted:\n%s", err, buf.String())
		}
		if len(spans2) != len(spans1) {
			t.Fatalf("round-trip span count drift: parsed %d, re-parsed %d after emit\nemitted:\n%s",
				len(spans1), len(spans2), buf.String())
		}
	})
}
