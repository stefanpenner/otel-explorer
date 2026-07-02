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

const stdoutSpanA = `{"Name":"my-workflow","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:05:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"OK","Description":""}}`
const stdoutSpanB = `{"Name":"build","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"00f067aa0ba902b7","TraceFlags":"01"},"Parent":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331"},"SpanKind":1,"StartTime":"2024-01-15T10:00:30Z","EndTime":"2024-01-15T10:04:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

// A compact single-line JSON array of valid spans must parse, not silently
// vanish (the line-based heuristic skipped any line starting with '[').
func TestCompactArrayParses(t *testing.T) {
	input := "[" + stdoutSpanA + "," + stdoutSpanB + "]"
	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
}

// A pretty-printed single span object spanning multiple lines must parse.
func TestPrettyPrintedObjectParses(t *testing.T) {
	input := "{\n  \"Name\": \"my-workflow\",\n  \"SpanContext\": {\"TraceID\": \"0af7651916cd43dd8448eb211c80319c\", \"SpanID\": \"b7ad6b7169203331\"},\n  \"StartTime\": \"2024-01-15T10:00:00Z\",\n  \"EndTime\": \"2024-01-15T10:05:00Z\"\n}\n"
	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
}

// Unrecognized non-empty input must error, not return (0 spans, nil).
func TestGarbageInputErrors(t *testing.T) {
	for _, input := range []string{"hello world\nnot a trace\n", "<html><body>404</body></html>"} {
		spans, err := Parse(strings.NewReader(input))
		if err == nil && len(spans) == 0 {
			t.Errorf("Parse(%q) = (0 spans, nil err): silent data loss", input)
		}
	}
}

// Tolerance preserved: spans interleaved with app log lines still parse.
func TestMixedLogLinesStillParse(t *testing.T) {
	input := "INFO starting up\n" + stdoutSpanA + "\nWARN something odd\n" + stdoutSpanB + "\n"
	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
}

// Empty input stays a non-error empty result.
func TestEmptyInputNoError(t *testing.T) {
	spans, err := Parse(strings.NewReader(""))
	if err != nil || len(spans) != 0 {
		t.Fatalf("Parse(empty) = (%d, %v), want (0, nil)", len(spans), err)
	}
}

// OTLP/JSON with a NUMERIC intValue (hand-rolled emitters) must not reject
// the whole file — the spec form is a string, but numbers are common.
func TestProtoJSONNumericIntValue(t *testing.T) {
	input := `{"resourceSpans":[{"scopeSpans":[{"spans":[{
		"traceId":"0af7651916cd43dd8448eb211c80319c",
		"spanId":"b7ad6b7169203331",
		"name":"call",
		"startTimeUnixNano":"1718000000000000000",
		"endTimeUnixNano":"1718000001000000000",
		"attributes":[
			{"key":"http.status_code","value":{"intValue":500}},
			{"key":"retries","value":{"intValue":"3"}}
		]}]}]}]}`
	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	got := map[string]int64{}
	for _, a := range spans[0].Attributes() {
		got[string(a.Key)] = a.Value.AsInt64()
	}
	if got["http.status_code"] != 500 || got["retries"] != 3 {
		t.Errorf("attrs = %v, want status 500 and retries 3", got)
	}
}

// OTLP/JSON spans with zero IDs must be kept with a zero SpanContext (like
// the stdout path after fd4db38), not silently dropped.
func TestProtoJSONZeroIDSpanKept(t *testing.T) {
	input := `{"resourceSpans":[{"scopeSpans":[{"spans":[{
		"traceId":"00000000000000000000000000000000",
		"spanId":"0000000000000000",
		"name":"degenerate",
		"startTimeUnixNano":"1718000000000000000",
		"endTimeUnixNano":"1718000001000000000"}]}]}]}`
	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1 (zero-ID span silently dropped)", len(spans))
	}
	if spans[0].Name() != "degenerate" {
		t.Errorf("name = %q", spans[0].Name())
	}
}
