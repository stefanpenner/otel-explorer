package otlpfile

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1resource "go.opentelemetry.io/proto/otlp/resource/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestParseNewlineDelimited(t *testing.T) {
	input := `{"Name":"my-workflow","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:05:00Z","Attributes":[{"Key":"type","Value":{"Type":"STRING","Value":"workflow"}},{"Key":"github.conclusion","Value":{"Type":"STRING","Value":"success"}}],"Events":null,"Links":null,"Status":{"Code":"OK","Description":""}}
{"Name":"build","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"00f067aa0ba902b7","TraceFlags":"01"},"Parent":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331"},"SpanKind":1,"StartTime":"2024-01-15T10:00:30Z","EndTime":"2024-01-15T10:04:00Z","Attributes":[{"Key":"type","Value":{"Type":"STRING","Value":"job"}}],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// Check first span
	if spans[0].Name() != "my-workflow" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "my-workflow")
	}
	if spans[0].SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("span[0] traceID = %q", spans[0].SpanContext().TraceID().String())
	}
	if !spans[0].Parent().SpanID().IsValid() {
		// Parent has empty IDs, so it should be invalid
	} else {
		t.Errorf("span[0] should have invalid parent span ID")
	}

	// Check second span has parent
	if spans[1].Name() != "build" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "build")
	}
	if spans[1].Parent().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("span[1] parent spanID = %q, want %q", spans[1].Parent().SpanID().String(), "b7ad6b7169203331")
	}

	// Check attributes
	attrs := spans[0].Attributes()
	found := false
	for _, a := range attrs {
		if string(a.Key) == "type" && a.Value.AsString() == "workflow" {
			found = true
		}
	}
	if !found {
		t.Errorf("span[0] missing type=workflow attribute")
	}

	// Check timing
	expectedStart := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	if !spans[0].StartTime().Equal(expectedStart) {
		t.Errorf("span[0] start = %v, want %v", spans[0].StartTime(), expectedStart)
	}
}

func TestParseArrayFormat(t *testing.T) {
	input := `[
  {"Name":"step1","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}},
  {"Name":"step2","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"00f067aa0ba902b7","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:01:00Z","EndTime":"2024-01-15T10:02:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}
]`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[0].Name() != "step1" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "step1")
	}
	if spans[1].Name() != "step2" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "step2")
	}
}

func TestParseEmptyInput(t *testing.T) {
	spans, err := Parse(strings.NewReader(""))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 0 {
		t.Fatalf("expected 0 spans, got %d", len(spans))
	}
}

func TestParseSkipsMalformedLines(t *testing.T) {
	input := `not valid json
{"Name":"ok","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}
also not valid`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "ok" {
		t.Errorf("span name = %q, want %q", spans[0].Name(), "ok")
	}
}

func TestParseWithEvents(t *testing.T) {
	input := `{"Name":"with-events","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":[{"Name":"cache-hit","Attributes":[{"Key":"cache.type","Value":{"Type":"STRING","Value":"npm"}}],"Time":"2024-01-15T10:00:30Z"}],"Links":null,"Status":{"Code":"ERROR","Description":"build failed"}}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	events := spans[0].Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Name != "cache-hit" {
		t.Errorf("event name = %q, want %q", events[0].Name, "cache-hit")
	}
}

func TestParseInt64Attribute(t *testing.T) {
	input := `{"Name":"int-attr","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[{"Key":"http.status_code","Value":{"Type":"INT64","Value":200}}],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	attrs := spans[0].Attributes()
	found := false
	for _, a := range attrs {
		if string(a.Key) == "http.status_code" && a.Value.AsInt64() == 200 {
			found = true
		}
	}
	if !found {
		t.Errorf("missing http.status_code=200 attribute, got %v", attrs)
	}
}

func TestParseProtoBasic(t *testing.T) {
	input := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"traceId": "0af7651916cd43dd8448eb211c80319c",
					"spanId": "b7ad6b7169203331",
					"parentSpanId": "",
					"name": "HTTP GET",
					"kind": 2,
					"startTimeUnixNano": "1705312800000000000",
					"endTimeUnixNano":   "1705312801000000000",
					"attributes": [
						{"key": "http.method", "value": {"stringValue": "GET"}},
						{"key": "http.status_code", "value": {"intValue": "200"}}
					],
					"status": {"code": 1}
				}, {
					"traceId": "0af7651916cd43dd8448eb211c80319c",
					"spanId": "00f067aa0ba902b7",
					"parentSpanId": "b7ad6b7169203331",
					"name": "db.query",
					"kind": 3,
					"startTimeUnixNano": "1705312800100000000",
					"endTimeUnixNano":   "1705312800500000000",
					"attributes": [
						{"key": "db.system", "value": {"stringValue": "postgresql"}}
					],
					"status": {"code": 0}
				}]
			}]
		}]
	}`

	spans, err := ParseProto(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseProto failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// First span
	if spans[0].Name() != "HTTP GET" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "HTTP GET")
	}
	if spans[0].SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("span[0] traceID = %q", spans[0].SpanContext().TraceID().String())
	}
	if spans[0].Parent().SpanID().IsValid() {
		t.Errorf("span[0] should have no parent")
	}

	// Second span has parent
	if spans[1].Parent().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("span[1] parent spanID = %q, want %q", spans[1].Parent().SpanID().String(), "b7ad6b7169203331")
	}

	// Check attributes on first span
	attrs := spans[0].Attributes()
	var foundMethod, foundStatus bool
	for _, a := range attrs {
		if string(a.Key) == "http.method" && a.Value.AsString() == "GET" {
			foundMethod = true
		}
		if string(a.Key) == "http.status_code" && a.Value.AsInt64() == 200 {
			foundStatus = true
		}
	}
	if !foundMethod {
		t.Errorf("missing http.method=GET attribute")
	}
	if !foundStatus {
		t.Errorf("missing http.status_code=200 attribute")
	}

	// Check timing
	expectedStart := time.Unix(0, 1705312800000000000)
	if !spans[0].StartTime().Equal(expectedStart) {
		t.Errorf("span[0] start = %v, want %v", spans[0].StartTime(), expectedStart)
	}
}

func TestParseProtoNumericNanos(t *testing.T) {
	// Some backends emit timestamps as numbers instead of strings
	input := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"traceId": "0af7651916cd43dd8448eb211c80319c",
					"spanId": "b7ad6b7169203331",
					"name": "numeric-ts",
					"kind": 1,
					"startTimeUnixNano": 1705312800000000000,
					"endTimeUnixNano":   1705312801000000000,
					"status": {"code": 2, "message": "something broke"}
				}]
			}]
		}]
	}`

	spans, err := ParseProto(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseProto failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "numeric-ts" {
		t.Errorf("name = %q", spans[0].Name())
	}
	// Status should be Error
	if spans[0].Status().Code.String() != "Error" {
		t.Errorf("status = %v, want Error", spans[0].Status().Code)
	}
}

func TestParseProtoWithEvents(t *testing.T) {
	input := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"traceId": "0af7651916cd43dd8448eb211c80319c",
					"spanId": "b7ad6b7169203331",
					"name": "with-events",
					"kind": 1,
					"startTimeUnixNano": "1705312800000000000",
					"endTimeUnixNano":   "1705312801000000000",
					"events": [{
						"name": "exception",
						"timeUnixNano": "1705312800500000000",
						"attributes": [
							{"key": "exception.message", "value": {"stringValue": "null pointer"}}
						]
					}],
					"status": {"code": 0}
				}]
			}]
		}]
	}`

	spans, err := ParseProto(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseProto failed: %v", err)
	}
	events := spans[0].Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Name != "exception" {
		t.Errorf("event name = %q, want %q", events[0].Name, "exception")
	}
}

func TestParseAutoDetectsProtoFormat(t *testing.T) {
	// Parse() should auto-detect protobuf-JSON format
	input := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","name":"auto-detected","kind":1,"startTimeUnixNano":"1705312800000000000","endTimeUnixNano":"1705312801000000000","status":{"code":0}}]}]}]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "auto-detected" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "auto-detected")
	}
}

func TestParseProtoAttributeTypes(t *testing.T) {
	input := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"traceId": "0af7651916cd43dd8448eb211c80319c",
					"spanId": "b7ad6b7169203331",
					"name": "attr-types",
					"kind": 1,
					"startTimeUnixNano": "1705312800000000000",
					"endTimeUnixNano":   "1705312801000000000",
					"attributes": [
						{"key": "str", "value": {"stringValue": "hello"}},
						{"key": "num", "value": {"intValue": "42"}},
						{"key": "dbl", "value": {"doubleValue": 3.14}},
						{"key": "flag", "value": {"boolValue": true}}
					],
					"status": {"code": 0}
				}]
			}]
		}]
	}`

	spans, err := ParseProto(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseProto failed: %v", err)
	}
	attrs := spans[0].Attributes()
	if len(attrs) != 4 {
		t.Fatalf("expected 4 attrs, got %d", len(attrs))
	}

	checks := map[string]bool{"str": false, "num": false, "dbl": false, "flag": false}
	for _, a := range attrs {
		switch string(a.Key) {
		case "str":
			if a.Value.AsString() == "hello" {
				checks["str"] = true
			}
		case "num":
			if a.Value.AsInt64() == 42 {
				checks["num"] = true
			}
		case "dbl":
			if a.Value.AsFloat64() == 3.14 {
				checks["dbl"] = true
			}
		case "flag":
			if a.Value.AsBool() == true {
				checks["flag"] = true
			}
		}
	}
	for k, v := range checks {
		if !v {
			t.Errorf("attribute %q not found or wrong value", k)
		}
	}
}

func TestParseChromeTraceWrapped(t *testing.T) {
	input := `{
		"traceEvents": [
			{"name":"CompileC","ph":"X","ts":1000000,"dur":500000,"pid":1,"tid":1,"cat":"cc","args":{"file":"main.c"}},
			{"name":"Link","ph":"X","ts":1500000,"dur":200000,"pid":1,"tid":2,"cat":"link"}
		]
	}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	if spans[0].Name() != "CompileC" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "CompileC")
	}
	if spans[1].Name() != "Link" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "Link")
	}

	// Check timing: 1000000 microseconds = 1 second
	expectedStart := time.Unix(1, 0)
	if !spans[0].StartTime().Equal(expectedStart) {
		t.Errorf("span[0] start = %v, want %v", spans[0].StartTime(), expectedStart)
	}
	expectedEnd := time.Unix(1, 500000000) // 1.5 seconds
	if !spans[0].EndTime().Equal(expectedEnd) {
		t.Errorf("span[0] end = %v, want %v", spans[0].EndTime(), expectedEnd)
	}

	// Check attributes
	attrs := spans[0].Attributes()
	var foundCat, foundArg bool
	for _, a := range attrs {
		if string(a.Key) == "chrome.category" && a.Value.AsString() == "cc" {
			foundCat = true
		}
		if string(a.Key) == "chrome.args.file" && a.Value.AsString() == "main.c" {
			foundArg = true
		}
	}
	if !foundCat {
		t.Errorf("missing chrome.category=cc attribute")
	}
	if !foundArg {
		t.Errorf("missing chrome.args.file=main.c attribute")
	}
}

func TestParseChromeTraceBareArray(t *testing.T) {
	input := `[
		{"name":"Task","ph":"X","ts":0,"dur":100000,"pid":1,"tid":1},
		{"name":"Subtask","ph":"X","ts":10000,"dur":50000,"pid":1,"tid":1}
	]`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[0].Name() != "Task" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "Task")
	}
}

func TestParseChromeTraceBeginEnd(t *testing.T) {
	input := `{"traceEvents": [
		{"name":"LongOp","ph":"B","ts":1000000,"pid":1,"tid":1},
		{"name":"LongOp","ph":"E","ts":2000000,"pid":1,"tid":1,"args":{"result":"ok"}}
	]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "LongOp" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "LongOp")
	}

	// Duration should be 1 second
	dur := spans[0].EndTime().Sub(spans[0].StartTime())
	if dur != time.Second {
		t.Errorf("duration = %v, want 1s", dur)
	}

	// Check merged args from E event
	var foundResult bool
	for _, a := range spans[0].Attributes() {
		if string(a.Key) == "chrome.args.result" && a.Value.AsString() == "ok" {
			foundResult = true
		}
	}
	if !foundResult {
		t.Errorf("missing chrome.args.result=ok from end event")
	}
}

func TestParseChromeTraceInstant(t *testing.T) {
	input := `{"traceEvents": [
		{"name":"Marker","ph":"i","ts":5000000,"pid":1,"tid":1,"s":"g"}
	]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "Marker" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "Marker")
	}
	// Instant events should have zero duration
	if !spans[0].StartTime().Equal(spans[0].EndTime()) {
		t.Errorf("instant event should have start == end")
	}
}

func TestParseChromeTraceSkipsMetadata(t *testing.T) {
	input := `{"traceEvents": [
		{"name":"process_name","ph":"M","pid":1,"args":{"name":"Browser"}},
		{"name":"RealWork","ph":"X","ts":1000,"dur":5000,"pid":1,"tid":1}
	]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span (metadata skipped), got %d", len(spans))
	}
	if spans[0].Name() != "RealWork" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "RealWork")
	}

	// Verify process_name metadata is captured as attribute
	var foundProcessName bool
	for _, a := range spans[0].Attributes() {
		if string(a.Key) == "chrome.process_name" && a.Value.AsString() == "Browser" {
			foundProcessName = true
		}
	}
	if !foundProcessName {
		t.Errorf("missing chrome.process_name=Browser attribute from M event")
	}
}

func TestParseChromeTraceHierarchy(t *testing.T) {
	// Parent span on tid=1 fully contains child span on same tid
	input := `{"traceEvents": [
		{"name":"Parent","ph":"X","ts":0,"dur":100000,"pid":1,"tid":1},
		{"name":"Child","ph":"X","ts":10000,"dur":50000,"pid":1,"tid":1},
		{"name":"Sibling","ph":"X","ts":200000,"dur":30000,"pid":1,"tid":2}
	]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(spans))
	}

	// Find spans by name
	spanByName := make(map[string]sdktrace.ReadOnlySpan)
	for _, s := range spans {
		spanByName[s.Name()] = s
	}

	parent := spanByName["Parent"]
	child := spanByName["Child"]
	sibling := spanByName["Sibling"]

	// Parent should be a root (no valid parent)
	if parent.Parent().SpanID().IsValid() {
		t.Errorf("Parent span should be root, got parent SpanID %s", parent.Parent().SpanID())
	}

	// Child should have Parent as its parent
	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("Child parent SpanID = %s, want %s", child.Parent().SpanID(), parent.SpanContext().SpanID())
	}

	// Sibling on different thread should be a root
	if sibling.Parent().SpanID().IsValid() {
		t.Errorf("Sibling on different tid should be root, got parent SpanID %s", sibling.Parent().SpanID())
	}
}

func TestParseChromeTraceThreadNames(t *testing.T) {
	input := `{"traceEvents": [
		{"name":"thread_name","ph":"M","pid":1,"tid":42,"args":{"name":"Main Thread"}},
		{"name":"DoWork","ph":"X","ts":0,"dur":5000,"pid":1,"tid":42}
	]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	var foundThreadName bool
	for _, a := range spans[0].Attributes() {
		if string(a.Key) == "chrome.thread_name" && a.Value.AsString() == "Main Thread" {
			foundThreadName = true
		}
	}
	if !foundThreadName {
		t.Errorf("missing chrome.thread_name attribute")
	}
}

func TestParseChromeTraceOtherData(t *testing.T) {
	input := `{
		"otherData": {"bazel_version": "8.4.0", "build_id": "abc123"},
		"traceEvents": [
			{"name":"Build","ph":"X","ts":0,"dur":100000,"pid":1,"tid":1}
		]
	}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	attrs := make(map[string]string)
	for _, a := range spans[0].Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}

	if attrs["chrome.metadata.bazel_version"] != "8.4.0" {
		t.Errorf("missing or wrong bazel_version, got %q", attrs["chrome.metadata.bazel_version"])
	}
	if attrs["chrome.metadata.build_id"] != "abc123" {
		t.Errorf("missing or wrong build_id, got %q", attrs["chrome.metadata.build_id"])
	}
}

func TestParseChromeTraceProfileStartTs(t *testing.T) {
	t.Run("with profile_start_ts offsets timestamps", func(t *testing.T) {
		a := assert.New(t)
		input := `{
			"otherData": {"profile_start_ts": 1000000},
			"traceEvents": [
				{"name":"Build","ph":"X","ts":5000000,"dur":2000000,"pid":1,"tid":1}
			]
		}`

		spans, err := Parse(strings.NewReader(input))
		a.NoError(err)
		a.Len(spans, 1)

		// profile_start_ts=1000000 ms = 1,000 seconds from epoch
		// event ts=5000000 µs = 5s relative offset
		// event dur=2000000 µs = 2s
		// Absolute: start = 1000s + 5s = 1005s, end = 1005s + 2s = 1007s
		a.Equal(time.Unix(1005, 0), spans[0].StartTime())
		a.Equal(time.Unix(1007, 0), spans[0].EndTime())
	})

	t.Run("without profile_start_ts treats timestamps as absolute", func(t *testing.T) {
		a := assert.New(t)
		input := `{
			"traceEvents": [
				{"name":"Build","ph":"X","ts":5000000,"dur":2000000,"pid":1,"tid":1}
			]
		}`

		spans, err := Parse(strings.NewReader(input))
		a.NoError(err)
		a.Len(spans, 1)

		// No profile_start_ts: ts=5000000 µs = 5s absolute from epoch
		a.Equal(time.Unix(5, 0), spans[0].StartTime())
		a.Equal(time.Unix(7, 0), spans[0].EndTime())
	})
}

func TestParseChromeTraceFiltersShortEvents(t *testing.T) {
	input := `{"traceEvents": [
		{"name":"Long","ph":"X","ts":0,"dur":5000,"pid":1,"tid":1},
		{"name":"Tiny","ph":"X","ts":0,"dur":100,"pid":1,"tid":1},
		{"name":"Instant","ph":"i","ts":0,"pid":1,"tid":1}
	]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	names := make(map[string]bool)
	for _, s := range spans {
		names[s.Name()] = true
	}

	if !names["Long"] {
		t.Errorf("Long event (5ms) should be included")
	}
	if names["Tiny"] {
		t.Errorf("Tiny event (0.1ms) should be filtered out")
	}
	if !names["Instant"] {
		t.Errorf("Instant events should always be included")
	}
}

func TestParseFlatJSONSingleObject(t *testing.T) {
	input := `{
		"Name": "GET /user",
		"SpanContext": {
			"TraceID": "384719368474cf130bdd39cffbe0781f",
			"SpanID": "eb123f7615b18f36",
			"TraceFlags": "01"
		},
		"ParentSpanID": "0000000000000000",
		"SpanKind": 2,
		"StartTime": "2026-03-12T10:00:00.123456Z",
		"EndTime": "2026-03-12T10:00:00.125678Z",
		"Attributes": {
			"http.method": "GET",
			"http.route": "/user",
			"http.status_code": 200,
			"net.peer.ip": "192.168.1.10"
		},
		"Events": [
			{
				"Name": "database_query",
				"Attributes": {
					"db.statement": "SELECT * FROM users WHERE id = 1"
				},
				"Time": "2026-03-12T10:00:00.124000Z"
			}
		],
		"Resource": {
			"service.name": "user-service",
			"service.version": "1.2.0"
		},
		"Status": {
			"Code": "Ok"
		}
	}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Name() != "GET /user" {
		t.Errorf("name = %q, want %q", s.Name(), "GET /user")
	}
	if s.SpanContext().TraceID().String() != "384719368474cf130bdd39cffbe0781f" {
		t.Errorf("traceID = %q", s.SpanContext().TraceID().String())
	}
	// ParentSpanID all zeros → no parent
	if s.Parent().SpanID().IsValid() {
		t.Errorf("expected invalid parent for all-zero ParentSpanID")
	}

	// Check attributes
	attrMap := make(map[string]string)
	for _, a := range s.Attributes() {
		attrMap[string(a.Key)] = a.Value.Emit()
	}
	if attrMap["http.method"] != "GET" {
		t.Errorf("http.method = %q, want %q", attrMap["http.method"], "GET")
	}
	if attrMap["http.status_code"] != "200" {
		t.Errorf("http.status_code = %q, want %q", attrMap["http.status_code"], "200")
	}
	if attrMap["resource.service.name"] != "user-service" {
		t.Errorf("resource.service.name = %q, want %q", attrMap["resource.service.name"], "user-service")
	}

	// Check events
	events := s.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Name != "database_query" {
		t.Errorf("event name = %q", events[0].Name)
	}
	var foundStmt bool
	for _, a := range events[0].Attributes {
		if string(a.Key) == "db.statement" {
			foundStmt = true
		}
	}
	if !foundStmt {
		t.Errorf("missing db.statement event attribute")
	}

	// Check status
	if s.Status().Code.String() != "Ok" {
		t.Errorf("status = %v, want Ok", s.Status().Code)
	}
}

func TestParseFlatJSONWithParent(t *testing.T) {
	input := `[
		{
			"Name": "root",
			"SpanContext": {"TraceID": "384719368474cf130bdd39cffbe0781f", "SpanID": "eb123f7615b18f36", "TraceFlags": "01"},
			"ParentSpanID": "0000000000000000",
			"SpanKind": 1,
			"StartTime": "2026-03-12T10:00:00Z",
			"EndTime": "2026-03-12T10:00:01Z",
			"Attributes": {},
			"Status": {"Code": ""}
		},
		{
			"Name": "child",
			"SpanContext": {"TraceID": "384719368474cf130bdd39cffbe0781f", "SpanID": "aa00bb11cc22dd33", "TraceFlags": "01"},
			"ParentSpanID": "eb123f7615b18f36",
			"SpanKind": 1,
			"StartTime": "2026-03-12T10:00:00.1Z",
			"EndTime": "2026-03-12T10:00:00.5Z",
			"Attributes": {"custom.tag": "hello"},
			"Status": {"Code": ""}
		}
	]`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// Child should have parent pointing to root
	child := spans[1]
	if child.Name() != "child" {
		t.Errorf("span[1] name = %q, want %q", child.Name(), "child")
	}
	if child.Parent().SpanID().String() != "eb123f7615b18f36" {
		t.Errorf("child parent spanID = %q, want %q", child.Parent().SpanID().String(), "eb123f7615b18f36")
	}
}

func TestParseOTLPJsonLines(t *testing.T) {
	// Two separate TracesData objects, each with resourceSpans, one per line (JSONL).
	line1 := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","name":"span-from-line-1","kind":1,"startTimeUnixNano":"1705312800000000000","endTimeUnixNano":"1705312801000000000","status":{"code":0}}]}]}]}`
	line2 := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa0ba902b7","name":"span-from-line-2","kind":1,"startTimeUnixNano":"1705312801000000000","endTimeUnixNano":"1705312802000000000","status":{"code":0}}]}]}]}`
	input := line1 + "\n" + line2

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[0].Name() != "span-from-line-1" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "span-from-line-1")
	}
	if spans[1].Name() != "span-from-line-2" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "span-from-line-2")
	}
}

func TestParseOTLPJsonLinesSingleLine(t *testing.T) {
	// Single-line JSONL should work identically to a plain OTLP JSON object.
	input := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","name":"single-line","kind":1,"startTimeUnixNano":"1705312800000000000","endTimeUnixNano":"1705312801000000000","status":{"code":0}}]}]}]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "single-line" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "single-line")
	}
}

func TestParseOTLPJsonLinesWithBlankLines(t *testing.T) {
	// JSONL with blank lines interspersed should be handled gracefully.
	line1 := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","name":"line-a","kind":1,"startTimeUnixNano":"1705312800000000000","endTimeUnixNano":"1705312801000000000","status":{"code":0}}]}]}]}`
	line2 := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa0ba902b7","name":"line-b","kind":1,"startTimeUnixNano":"1705312801000000000","endTimeUnixNano":"1705312802000000000","status":{"code":0}}]}]}]}`
	input := "\n" + line1 + "\n\n" + line2 + "\n\n"

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[0].Name() != "line-a" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "line-a")
	}
	if spans[1].Name() != "line-b" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "line-b")
	}
}

func TestParseGzipCompressed(t *testing.T) {
	input := `{"Name":"gzipped-span","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte(input)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	spans, err := Parse(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "gzipped-span" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "gzipped-span")
	}
}

func TestParseZstdCompressed(t *testing.T) {
	input := `{"Name":"zstd-span","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd encoder: %v", err)
	}
	compressed := enc.EncodeAll([]byte(input), nil)

	spans, err := Parse(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "zstd-span" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "zstd-span")
	}
}

func TestParseGzipOTLPProtoJSON(t *testing.T) {
	input := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","name":"gzip-otlp","kind":1,"startTimeUnixNano":"1705312800000000000","endTimeUnixNano":"1705312801000000000","status":{"code":0}}]}]}]}`

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte(input)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	spans, err := Parse(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "gzip-otlp" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "gzip-otlp")
	}
}

func TestParseFileGzip(t *testing.T) {
	input := `{"Name":"file-gzip-span","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

	dir := t.TempDir()
	path := filepath.Join(dir, "trace.json.gz")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	gw := gzip.NewWriter(f)
	if _, err := gw.Write([]byte(input)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file close: %v", err)
	}

	spans, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "file-gzip-span" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "file-gzip-span")
	}
}

func TestStatusFromCode(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"OK", "Ok"},
		{"STATUS_CODE_OK", "Ok"},
		{"ERROR", "Error"},
		{"STATUS_CODE_ERROR", "Error"},
		{"UNSET", "Unset"},
		{"", "Unset"},
	}
	for _, tt := range tests {
		st := StatusFromCode(tt.code, "desc")
		if st.Code.String() != tt.want {
			t.Errorf("StatusFromCode(%q) = %v, want %v", tt.code, st.Code, tt.want)
		}
	}
}

// Helper to build a length-prefixed protobuf message from a TracesData.
func buildLengthPrefixedProtobuf(t *testing.T, td *v1.TracesData) []byte {
	t.Helper()
	data, err := proto.Marshal(td)
	if err != nil {
		t.Fatalf("failed to marshal TracesData: %v", err)
	}
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(data))); err != nil {
		t.Fatalf("failed to write length prefix: %v", err)
	}
	buf.Write(data)
	return buf.Bytes()
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex decode %q: %v", s, err)
	}
	return b
}

func TestParseProtobufBasic(t *testing.T) {
	traceIDBytes := mustDecodeHex(t, "0af7651916cd43dd8448eb211c80319c")
	spanIDBytes := mustDecodeHex(t, "b7ad6b7169203331")
	parentSpanIDBytes := mustDecodeHex(t, "00f067aa0ba902b7")

	td := &v1.TracesData{
		ResourceSpans: []*v1.ResourceSpans{{
			Resource: &v1resource.Resource{
				Attributes: []*v1common.KeyValue{{
					Key:   "service.name",
					Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "test-svc"}},
				}},
			},
			ScopeSpans: []*v1.ScopeSpans{{
				Spans: []*v1.Span{
					{
						TraceId:           traceIDBytes,
						SpanId:            spanIDBytes,
						Name:              "HTTP GET",
						Kind:              v1.Span_SPAN_KIND_SERVER,
						StartTimeUnixNano: 1705312800000000000,
						EndTimeUnixNano:   1705312801000000000,
						Attributes: []*v1common.KeyValue{
							{Key: "http.method", Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "GET"}}},
							{Key: "http.status_code", Value: &v1common.AnyValue{Value: &v1common.AnyValue_IntValue{IntValue: 200}}},
							{Key: "http.duration_ms", Value: &v1common.AnyValue{Value: &v1common.AnyValue_DoubleValue{DoubleValue: 3.14}}},
							{Key: "http.ok", Value: &v1common.AnyValue{Value: &v1common.AnyValue_BoolValue{BoolValue: true}}},
						},
						Status: &v1.Status{Code: v1.Status_STATUS_CODE_OK},
					},
					{
						TraceId:           traceIDBytes,
						SpanId:            parentSpanIDBytes,
						ParentSpanId:      spanIDBytes,
						Name:              "db.query",
						Kind:              v1.Span_SPAN_KIND_CLIENT,
						StartTimeUnixNano: 1705312800100000000,
						EndTimeUnixNano:   1705312800500000000,
						Attributes: []*v1common.KeyValue{
							{Key: "db.system", Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "postgresql"}}},
						},
						Status: &v1.Status{Code: v1.Status_STATUS_CODE_UNSET},
					},
				},
			}},
		}},
	}

	msg := buildLengthPrefixedProtobuf(t, td)
	spans, err := ParseProtobuf(bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("ParseProtobuf failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// First span
	if spans[0].Name() != "HTTP GET" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "HTTP GET")
	}
	if spans[0].SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("span[0] traceID = %q", spans[0].SpanContext().TraceID().String())
	}
	if spans[0].SpanContext().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("span[0] spanID = %q", spans[0].SpanContext().SpanID().String())
	}
	if spans[0].Parent().SpanID().IsValid() {
		t.Errorf("span[0] should have no parent")
	}

	// Check timing
	expectedStart := time.Unix(0, 1705312800000000000)
	if !spans[0].StartTime().Equal(expectedStart) {
		t.Errorf("span[0] start = %v, want %v", spans[0].StartTime(), expectedStart)
	}

	// Check attributes
	attrs := spans[0].Attributes()
	checks := map[string]bool{"http.method": false, "http.status_code": false, "http.duration_ms": false, "http.ok": false}
	for _, a := range attrs {
		switch string(a.Key) {
		case "http.method":
			if a.Value.AsString() == "GET" {
				checks["http.method"] = true
			}
		case "http.status_code":
			if a.Value.AsInt64() == 200 {
				checks["http.status_code"] = true
			}
		case "http.duration_ms":
			if a.Value.AsFloat64() == 3.14 {
				checks["http.duration_ms"] = true
			}
		case "http.ok":
			if a.Value.AsBool() {
				checks["http.ok"] = true
			}
		}
	}
	for k, v := range checks {
		if !v {
			t.Errorf("attribute %q not found or wrong value", k)
		}
	}

	// Check status
	if spans[0].Status().Code.String() != "Ok" {
		t.Errorf("span[0] status = %v, want Ok", spans[0].Status().Code)
	}

	// Second span has parent
	if spans[1].Name() != "db.query" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "db.query")
	}
	if spans[1].Parent().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("span[1] parent spanID = %q, want %q", spans[1].Parent().SpanID().String(), "b7ad6b7169203331")
	}
}

func TestParseProtobufMultipleMessages(t *testing.T) {
	traceIDBytes := mustDecodeHex(t, "0af7651916cd43dd8448eb211c80319c")

	td1 := &v1.TracesData{
		ResourceSpans: []*v1.ResourceSpans{{
			ScopeSpans: []*v1.ScopeSpans{{
				Spans: []*v1.Span{{
					TraceId:           traceIDBytes,
					SpanId:            mustDecodeHex(t, "b7ad6b7169203331"),
					Name:              "msg1-span",
					Kind:              v1.Span_SPAN_KIND_INTERNAL,
					StartTimeUnixNano: 1705312800000000000,
					EndTimeUnixNano:   1705312801000000000,
					Status:            &v1.Status{Code: v1.Status_STATUS_CODE_OK},
				}},
			}},
		}},
	}

	td2 := &v1.TracesData{
		ResourceSpans: []*v1.ResourceSpans{{
			ScopeSpans: []*v1.ScopeSpans{{
				Spans: []*v1.Span{{
					TraceId:           traceIDBytes,
					SpanId:            mustDecodeHex(t, "00f067aa0ba902b7"),
					Name:              "msg2-span",
					Kind:              v1.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: 1705312801000000000,
					EndTimeUnixNano:   1705312802000000000,
					Status:            &v1.Status{Code: v1.Status_STATUS_CODE_ERROR, Message: "timeout"},
				}},
			}},
		}},
	}

	// Concatenate two length-prefixed messages.
	var buf bytes.Buffer
	buf.Write(buildLengthPrefixedProtobuf(t, td1))
	buf.Write(buildLengthPrefixedProtobuf(t, td2))

	spans, err := ParseProtobuf(&buf)
	if err != nil {
		t.Fatalf("ParseProtobuf failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[0].Name() != "msg1-span" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "msg1-span")
	}
	if spans[1].Name() != "msg2-span" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "msg2-span")
	}
	if spans[1].Status().Code.String() != "Error" {
		t.Errorf("span[1] status = %v, want Error", spans[1].Status().Code)
	}
}

func TestParseProtobufAutoDetect(t *testing.T) {
	traceIDBytes := mustDecodeHex(t, "0af7651916cd43dd8448eb211c80319c")

	td := &v1.TracesData{
		ResourceSpans: []*v1.ResourceSpans{{
			ScopeSpans: []*v1.ScopeSpans{{
				Spans: []*v1.Span{{
					TraceId:           traceIDBytes,
					SpanId:            mustDecodeHex(t, "b7ad6b7169203331"),
					Name:              "auto-detected-pb",
					Kind:              v1.Span_SPAN_KIND_INTERNAL,
					StartTimeUnixNano: 1705312800000000000,
					EndTimeUnixNano:   1705312801000000000,
					Status:            &v1.Status{Code: v1.Status_STATUS_CODE_OK},
				}},
			}},
		}},
	}

	msg := buildLengthPrefixedProtobuf(t, td)

	// Use Parse() which should auto-detect binary protobuf.
	spans, err := Parse(bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("Parse (auto-detect protobuf) failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "auto-detected-pb" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "auto-detected-pb")
	}
}

func TestParseZipkinBasic(t *testing.T) {
	input := `[
		{
			"traceId": "0af7651916cd43dd8448eb211c80319c",
			"id": "b7ad6b7169203331",
			"name": "get /api",
			"kind": "SERVER",
			"timestamp": 1472470996199000,
			"duration": 207000,
			"localEndpoint": {"serviceName": "frontend"},
			"tags": {
				"http.method": "GET",
				"http.path": "/api"
			}
		},
		{
			"traceId": "0af7651916cd43dd8448eb211c80319c",
			"id": "00f067aa0ba902b7",
			"parentId": "b7ad6b7169203331",
			"name": "db.query",
			"kind": "CLIENT",
			"timestamp": 1472470996238000,
			"duration": 100000,
			"localEndpoint": {"serviceName": "frontend"},
			"tags": {
				"db.system": "postgresql"
			}
		}
	]`

	spans, err := ParseZipkin(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseZipkin failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// First span: root
	if spans[0].Name() != "get /api" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "get /api")
	}
	if spans[0].SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("span[0] traceID = %q", spans[0].SpanContext().TraceID().String())
	}
	if spans[0].SpanContext().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("span[0] spanID = %q", spans[0].SpanContext().SpanID().String())
	}
	if spans[0].Parent().SpanID().IsValid() {
		t.Errorf("span[0] should have no parent")
	}

	// Timing: 1472470996199000 microseconds since epoch
	expectedStart := time.Unix(0, 1472470996199000*1000)
	if !spans[0].StartTime().Equal(expectedStart) {
		t.Errorf("span[0] start = %v, want %v", spans[0].StartTime(), expectedStart)
	}
	expectedEnd := time.Unix(0, (1472470996199000+207000)*1000)
	if !spans[0].EndTime().Equal(expectedEnd) {
		t.Errorf("span[0] end = %v, want %v", spans[0].EndTime(), expectedEnd)
	}

	// Check tags
	attrMap := make(map[string]string)
	for _, a := range spans[0].Attributes() {
		attrMap[string(a.Key)] = a.Value.AsString()
	}
	if attrMap["http.method"] != "GET" {
		t.Errorf("http.method = %q, want %q", attrMap["http.method"], "GET")
	}
	if attrMap["http.path"] != "/api" {
		t.Errorf("http.path = %q, want %q", attrMap["http.path"], "/api")
	}

	// Second span: child with parent
	if spans[1].Name() != "db.query" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "db.query")
	}
	if spans[1].Parent().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("span[1] parent spanID = %q, want %q", spans[1].Parent().SpanID().String(), "b7ad6b7169203331")
	}
	if spans[1].Parent().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("span[1] parent traceID = %q", spans[1].Parent().TraceID().String())
	}
}

func TestParseZipkinAutoDetect(t *testing.T) {
	input := `[
		{
			"traceId": "0af7651916cd43dd8448eb211c80319c",
			"id": "b7ad6b7169203331",
			"name": "auto-detected-zipkin",
			"timestamp": 1472470996199000,
			"duration": 207000,
			"localEndpoint": {"serviceName": "test-svc"}
		}
	]`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "auto-detected-zipkin" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "auto-detected-zipkin")
	}
}

func TestParseZipkinKindMapping(t *testing.T) {
	tests := []struct {
		kind string
		want trace.SpanKind
	}{
		{"CLIENT", trace.SpanKindClient},
		{"SERVER", trace.SpanKindServer},
		{"PRODUCER", trace.SpanKindProducer},
		{"CONSUMER", trace.SpanKindConsumer},
		{"", trace.SpanKindUnspecified},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			kindField := ""
			if tt.kind != "" {
				kindField = `"kind": "` + tt.kind + `",`
			}
			input := `[{
				"traceId": "0af7651916cd43dd8448eb211c80319c",
				"id": "b7ad6b7169203331",
				"name": "kind-test",
				` + kindField + `
				"timestamp": 1472470996199000,
				"duration": 1000,
				"localEndpoint": {"serviceName": "svc"}
			}]`

			spans, err := ParseZipkin(strings.NewReader(input))
			if err != nil {
				t.Fatalf("ParseZipkin failed: %v", err)
			}
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}
			if spans[0].SpanKind() != tt.want {
				t.Errorf("kind = %v, want %v", spans[0].SpanKind(), tt.want)
			}
		})
	}
}

func TestParseZipkinShortTraceId(t *testing.T) {
	input := `[{
		"traceId": "5982fe77008310cc",
		"id": "b7ad6b7169203331",
		"name": "short-trace-id",
		"timestamp": 1472470996199000,
		"duration": 1000,
		"localEndpoint": {"serviceName": "svc"}
	}]`

	spans, err := ParseZipkin(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseZipkin failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].SpanContext().TraceID().String() != "00000000000000005982fe77008310cc" {
		t.Errorf("traceID = %q, want %q", spans[0].SpanContext().TraceID().String(), "00000000000000005982fe77008310cc")
	}
}

func TestParseZipkinWithEndpoint(t *testing.T) {
	input := `[{
		"traceId": "0af7651916cd43dd8448eb211c80319c",
		"id": "b7ad6b7169203331",
		"name": "endpoint-test",
		"timestamp": 1472470996199000,
		"duration": 1000,
		"localEndpoint": {
			"serviceName": "my-cool-service",
			"ipv4": "172.18.0.7",
			"port": 8080
		},
		"tags": {
			"custom.tag": "value"
		}
	}]`

	spans, err := ParseZipkin(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseZipkin failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	attrMap := make(map[string]string)
	for _, a := range spans[0].Attributes() {
		attrMap[string(a.Key)] = a.Value.AsString()
	}
	if attrMap["service.name"] != "my-cool-service" {
		t.Errorf("service.name = %q, want %q", attrMap["service.name"], "my-cool-service")
	}
	if attrMap["custom.tag"] != "value" {
		t.Errorf("custom.tag = %q, want %q", attrMap["custom.tag"], "value")
	}
}

func TestParsePrettyPrintedOTLPProtoJSON(t *testing.T) {
	input := `{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "pretty-svc"}}
        ]
      },
      "scopeSpans": [
        {
          "spans": [
            {
              "traceId": "0af7651916cd43dd8448eb211c80319c",
              "spanId": "b7ad6b7169203331",
              "name": "GET /api",
              "kind": 2,
              "startTimeUnixNano": "1700000000000000000",
              "endTimeUnixNano": "1700000001000000000"
            }
          ]
        }
      ]
    }
  ]
}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "GET /api" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "GET /api")
	}
}

func TestParseConcatenatedPrettyPrintedOTLPProtoJSON(t *testing.T) {
	doc := `{
  "resourceSpans": [{
    "scopeSpans": [{
      "spans": [{
        "traceId": "0af7651916cd43dd8448eb211c80319c",
        "spanId": "%s",
        "name": "%s",
        "startTimeUnixNano": "1700000000000000000",
        "endTimeUnixNano": "1700000001000000000"
      }]
    }]
  }]
}`
	input := fmt.Sprintf(doc, "b7ad6b7169203331", "span-one") + "\n" +
		fmt.Sprintf(doc, "00f067aa0ba902b7", "span-two")

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[0].Name() != "span-one" || spans[1].Name() != "span-two" {
		t.Errorf("names = %q, %q; want span-one, span-two", spans[0].Name(), spans[1].Name())
	}
}

func TestParseJaeger64BitTraceID(t *testing.T) {
	// Jaeger serializes trace IDs with only 16 hex chars when the high 64
	// bits are zero (e.g. Zipkin-originated traces). These must be
	// zero-padded, not dropped.
	input := `{"data":[{"traceID":"8448eb211c80319c","spans":[{"traceID":"8448eb211c80319c","spanID":"b7ad6b7169203331","operationName":"fetch","references":[],"startTime":1700000000000000,"duration":2000,"tags":[],"logs":[],"processID":"p1"}],"processes":{"p1":{"serviceName":"svc","tags":[]}}}]}`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	wantTraceID := "00000000000000008448eb211c80319c"
	if got := spans[0].SpanContext().TraceID().String(); got != wantTraceID {
		t.Errorf("traceID = %q, want %q", got, wantTraceID)
	}
	if spans[0].Name() != "fetch" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "fetch")
	}
}

func TestParseChromeTraceNestedSameNameBeginEnd(t *testing.T) {
	// Recursive/nested events with the same name must pair via per-thread
	// stack semantics: each E closes the most recent unclosed B.
	input := `[
		{"name":"f","ph":"B","ts":0,"pid":1,"tid":1},
		{"name":"f","ph":"B","ts":10000,"pid":1,"tid":1},
		{"name":"f","ph":"E","ts":20000,"pid":1,"tid":1},
		{"name":"f","ph":"E","ts":30000,"pid":1,"tid":1}
	]`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans (outer + inner), got %d", len(spans))
	}

	var durations []time.Duration
	for _, s := range spans {
		durations = append(durations, s.EndTime().Sub(s.StartTime()))
	}
	// Inner span: 10000µs..20000µs = 10ms; outer span: 0..30000µs = 30ms.
	wantInner, wantOuter := 10*time.Millisecond, 30*time.Millisecond
	foundInner, foundOuter := false, false
	for _, d := range durations {
		switch d {
		case wantInner:
			foundInner = true
		case wantOuter:
			foundOuter = true
		}
	}
	if !foundInner || !foundOuter {
		t.Errorf("durations = %v, want %v and %v", durations, wantInner, wantOuter)
	}
}

func TestParseChromeTraceEndEventWithoutName(t *testing.T) {
	// The Trace Event Format makes "name" optional on E events: an E closes
	// the most recent unclosed B on the same thread.
	input := `[
		{"name":"work","ph":"B","ts":0,"pid":1,"tid":1},
		{"ph":"E","ts":50000,"pid":1,"tid":1}
	]`

	spans, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "work" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "work")
	}
	if dur := spans[0].EndTime().Sub(spans[0].StartTime()); dur != 50*time.Millisecond {
		t.Errorf("duration = %v, want 50ms", dur)
	}
}

func TestParseProtobufRequest(t *testing.T) {
	// OTLP/HTTP bodies are bare ExportTraceServiceRequest messages with no
	// length prefix, unlike the fileexporter format ParseProtobuf handles.
	td := &v1.TracesData{
		ResourceSpans: []*v1.ResourceSpans{{
			ScopeSpans: []*v1.ScopeSpans{{
				Spans: []*v1.Span{{
					TraceId:           mustDecodeHex(t, "0af7651916cd43dd8448eb211c80319c"),
					SpanId:            mustDecodeHex(t, "b7ad6b7169203331"),
					Name:              "bare-proto",
					StartTimeUnixNano: 1705312800000000000,
					EndTimeUnixNano:   1705312801000000000,
				}},
			}},
		}},
	}
	raw, err := proto.Marshal(td)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	spans, err := ParseProtobufRequest(raw)
	if err != nil {
		t.Fatalf("ParseProtobufRequest failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "bare-proto" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "bare-proto")
	}
}
