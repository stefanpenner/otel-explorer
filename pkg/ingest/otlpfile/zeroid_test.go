package otlpfile

import (
	"strings"
	"testing"
)

// TestZeroIDSpanContextConsistency pins a parse/re-parse fidelity fix found by
// the parse→emit→re-parse round-trip fuzzer (pkg/export/otel.FuzzRoundTrip).
//
// parseSpanContext treated empty-string IDs ("") as a valid zero context (span
// kept) but rejected the *equivalent* all-zero hex IDs ("00…00") via
// trace.TraceIDFromHex ("trace-id can't be all zero"). Since the stdouttrace
// exporter serializes a zero TraceID as "00…00", a degenerate span survived the
// first parse but vanished when its own emitted form was re-ingested — i.e.
// `ote --otel` was not idempotent. Both spellings of "no ID" must behave the
// same.
func TestZeroIDSpanContextConsistency(t *testing.T) {
	emptyIDs := `{"Name":"n"}`
	zeroHexIDs := `{"Name":"n","SpanContext":{"TraceID":"00000000000000000000000000000000","SpanID":"0000000000000000"}}`

	empty, err := Parse(strings.NewReader(emptyIDs))
	if err != nil {
		t.Fatalf("empty-ID parse: %v", err)
	}
	zero, err := Parse(strings.NewReader(zeroHexIDs))
	if err != nil {
		t.Fatalf("zero-hex-ID parse: %v", err)
	}
	if len(empty) != len(zero) {
		t.Fatalf("inconsistent: empty IDs -> %d spans, all-zero hex IDs -> %d spans (must match)",
			len(empty), len(zero))
	}
	if len(zero) != 1 {
		t.Fatalf("expected 1 span for an all-zero-ID span (zero context kept), got %d", len(zero))
	}
}
