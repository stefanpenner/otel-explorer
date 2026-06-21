package filter

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// FuzzFilterApplyProperties checks the structural contract of Filter.Apply over
// fuzzer-controlled spans and a fuzzer-controlled filter expression:
//   - subset: the result is a subsequence of the input (same elements, order
//     preserved, never more) — Apply must not invent or reorder spans;
//   - idempotency: applying the same filter to its own output is a no-op.
//
// Violations would mean the filter drops the wrong spans, duplicates, reorders,
// or is unstable — correctness bugs a panic check can't see.
//
// (ReadOnlySpan's concrete type is uncomparable, so identity is compared via a
// per-span fingerprint string rather than ==.)
func FuzzFilterApplyProperties(f *testing.F) {
	f.Add([]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"build","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`), "otel.span_name=build")
	f.Add([]byte(`{"traceEvents":[{"ph":"X","name":"a","ts":0,"dur":5,"pid":1,"tid":1}]}`), "!name,x=*")
	f.Add([]byte(`{"traceEvents":[{"ph":"X","name":"a","ts":0,"dur":5,"pid":1,"tid":1}]}`), "")

	f.Fuzz(func(t *testing.T, data []byte, expr string) {
		spans, err := otlpfile.Parse(bytes.NewReader(data))
		if err != nil || len(spans) == 0 {
			return
		}
		flt, err := Parse(expr)
		if err != nil {
			return
		}

		r1 := flt.Apply(spans)
		if len(r1) > len(spans) {
			t.Fatalf("Apply grew the set: %d > %d", len(r1), len(spans))
		}
		in := fps(spans)
		// Subset + order: r1's fingerprints must be an order-preserving
		// subsequence of the input's.
		j := 0
		for _, fp := range fps(r1) {
			for j < len(in) && in[j] != fp {
				j++
			}
			if j >= len(in) {
				t.Fatalf("Apply result is not an order-preserving subset of the input")
			}
			j++
		}
		// Idempotency: filtering the filtered set changes nothing.
		r2 := flt.Apply(r1)
		if a, b := fps(r1), fps(r2); !eqStr(a, b) {
			t.Fatalf("Apply is not idempotent: %v vs %v", a, b)
		}
	})
}

func fps(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = fmt.Sprintf("%s/%s|%s|%d|%d", s.SpanContext().TraceID(), s.SpanContext().SpanID(),
			s.Name(), s.StartTime().UnixNano(), s.EndTime().UnixNano())
	}
	return out
}

func eqStr(a, b []string) bool {
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
