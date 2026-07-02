package otlpfile

import (
	"strings"
	"testing"
)

// A Zipkin trace whose tags contain a "ph" key must not be hijacked by the
// Chrome-trace detector (which used to byte-scan for `"ph":` anywhere).
func TestZipkinWithPhTagNotChrome(t *testing.T) {
	zipkin := `[{
		"traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
		"id": "00f067aa0ba902b7",
		"name": "render",
		"timestamp": 1718000000000000,
		"duration": 50000,
		"localEndpoint": {"serviceName": "web"},
		"tags": {"ph": "render-phase"}
	}]`
	spans, err := Parse(strings.NewReader(zipkin))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1 (zipkin span with ph tag was misrouted)", len(spans))
	}
	if spans[0].Name() != "render" {
		t.Errorf("span name = %q, want render", spans[0].Name())
	}
}

// A real bare-array Chrome trace (top-level "ph" keys) must still be detected.
func TestBareArrayChromeStillDetected(t *testing.T) {
	chrome := `[
		{"name": "compile", "ph": "B", "ts": 1000, "pid": 1, "tid": 1},
		{"name": "compile", "ph": "E", "ts": 5000, "pid": 1, "tid": 1}
	]`
	spans, err := Parse(strings.NewReader(chrome))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1 chrome span", len(spans))
	}
}
