package otel

import (
	"bytes"
	"context"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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
		// Stronger invariant: the multiset of (traceID, spanID) identities must
		// survive the round-trip. Equal counts with different IDs would mean a
		// span's identity was corrupted or swapped through emit → re-parse.
		ids := func(spans []sdktrace.ReadOnlySpan) map[string]int {
			m := make(map[string]int, len(spans))
			for _, s := range spans {
				m[s.SpanContext().TraceID().String()+"/"+s.SpanContext().SpanID().String()]++
			}
			return m
		}
		before, after := ids(spans1), ids(spans2)
		for k, n := range before {
			if after[k] != n {
				t.Fatalf("round-trip identity drift for %s: before=%d after=%d\nemitted:\n%s",
					k, n, after[k], buf.String())
			}
		}
	})
}
