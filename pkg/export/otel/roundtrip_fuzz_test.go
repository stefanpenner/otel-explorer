package otel

import (
	"bytes"
	"context"
	"fmt"
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

// FuzzRoundTripStable checks a FIXED-POINT fidelity property: once spans have
// been through one parse → emit → re-parse, a *second* identical round-trip
// must not change their key semantic fields. Comparing the two post-round-trip
// snapshots (spans2 vs spans3) — rather than the original arbitrary parse —
// isolates true non-idempotency in the stdout emit/re-parse path from benign
// cross-parser differences. A drift means some field (name, times, status,
// attribute set) is silently mangled each time the tool re-ingests its own
// --otel output.
func FuzzRoundTripStable(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"build","startTimeUnixNano":"1","endTimeUnixNano":"2","attributes":[{"key":"k","value":{"stringValue":"v"}}]}]}]}]}`),
		[]byte(`{"SpanContext":{"TraceID":"00000000000000000000000000000001","SpanID":"0000000000000001"},"Name":"n","StartTime":"2024-01-01T00:00:00Z","EndTime":"2024-01-01T00:00:01Z"}`),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	ctx := context.Background()
	emit := func(spans []sdktrace.ReadOnlySpan) ([]sdktrace.ReadOnlySpan, string) {
		var buf bytes.Buffer
		exp, err := NewStdoutExporter(&buf)
		if err != nil {
			return nil, ""
		}
		if err := exp.Export(ctx, spans); err != nil {
			return nil, ""
		}
		_ = exp.Finish(ctx)
		out, err := otlpfile.Parse(bytes.NewReader(buf.Bytes()))
		if err != nil {
			return nil, buf.String()
		}
		return out, buf.String()
	}

	// fingerprint of the fields the tool semantically round-trips, keyed by ID.
	fp := func(spans []sdktrace.ReadOnlySpan) map[string]string {
		m := make(map[string]string, len(spans))
		for _, s := range spans {
			id := s.SpanContext().TraceID().String() + "/" + s.SpanContext().SpanID().String()
			m[id] = fmt.Sprintf("name=%q start=%d end=%d status=%s attrs=%d",
				s.Name(), s.StartTime().UnixNano(), s.EndTime().UnixNano(),
				s.Status().Code, len(s.Attributes()))
		}
		return m
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		spans1, err := otlpfile.Parse(bytes.NewReader(data))
		if err != nil || len(spans1) == 0 {
			return
		}
		spans2, _ := emit(spans1)
		if len(spans2) == 0 {
			return
		}
		spans3, buf2 := emit(spans2)

		a, b := fp(spans2), fp(spans3)
		if len(a) != len(b) {
			t.Fatalf("fixed-point count drift: round-trip 1 -> %d spans, round-trip 2 -> %d\n%s", len(a), len(b), buf2)
		}
		for id, v := range a {
			if b[id] != v {
				t.Fatalf("fixed-point field drift for %s:\n  rt1: %s\n  rt2: %s\nemitted:\n%s", id, v, b[id], buf2)
			}
		}
	})
}
