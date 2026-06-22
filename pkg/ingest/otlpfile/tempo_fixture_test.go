package otlpfile

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// expectedTempoSpanNames are the 11 span names in the Tempo fixture trace
// (a github-actions-runner CI pipeline), in resource/scope order.
var expectedTempoSpanNames = []string{
	"Resolve actions/checkout@v4",
	"Set up job",
	"Checkout",
	"Install deps",
	"Build",
	"Unit tests",
	"Lint",
	"Done",
	"Post Checkout",
	"Complete job",
	"build-and-test",
}

// TestParseProtoTempoFixture exercises the real Tempo /api/traces response shape:
// top-level "resourceSpans" (after the batches->resourceSpans rename), base64
// trace/span IDs, protojson string enum kinds, and stringified unix-nano times.
// Regression test for the bug where every span silently failed convertProtoSpan
// and ParseProto returned "0 spans".
func TestParseProtoTempoFixture(t *testing.T) {
	path := filepath.Join("testdata", "tempo-resourceSpans.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	spans, err := ParseProto(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ParseProto: %v", err)
	}

	if len(spans) != 11 {
		t.Fatalf("expected 11 spans, got %d", len(spans))
	}

	var gotNames []string
	for _, s := range spans {
		gotNames = append(gotNames, s.Name())

		tid := s.SpanContext().TraceID().String()
		if _, err := hex.DecodeString(tid); err != nil || len(tid) != 32 {
			t.Errorf("span %q: trace ID %q is not 32-char hex", s.Name(), tid)
		}
		sid := s.SpanContext().SpanID().String()
		if _, err := hex.DecodeString(sid); err != nil || len(sid) != 16 {
			t.Errorf("span %q: span ID %q is not 16-char hex", s.Name(), sid)
		}
	}

	for _, want := range expectedTempoSpanNames {
		found := false
		for _, got := range gotNames {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected span %q; got %v", want, gotNames)
		}
	}

	// The root server span ("build-and-test") must carry kind=Server.
	var rootKind string
	for _, s := range spans {
		if s.Name() == "build-and-test" {
			rootKind = s.SpanKind().String()
		}
	}
	if rootKind != "server" {
		t.Errorf("root span kind = %q, want server", rootKind)
	}
}

// TestParseProtoSurfacesPerSpanError makes sure ParseProto no longer silently
// swallows every per-span decode error and returns 0 spans with nil error.
// When input contains spans but none convert, the first error must surface.
func TestParseProtoSurfacesPerSpanError(t *testing.T) {
	// One span with a malformed (non-hex, non-base64) trace ID.
	input := `{"resourceSpans":[{"scopeSpans":[{"spans":[
		{"traceId":"!!!not-valid!!!","spanId":"0bb519b87f8520fd","name":"bad",
		 "startTimeUnixNano":"1","endTimeUnixNano":"2"}
	]}]}]}`

	spans, err := ParseProto(strings.NewReader(input))
	if err == nil {
		t.Fatalf("expected error when all spans fail to convert, got nil (spans=%d)", len(spans))
	}
	if len(spans) != 0 {
		t.Fatalf("expected 0 spans, got %d", len(spans))
	}
}
