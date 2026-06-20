package otlpfile

import (
	"bytes"
	"testing"
)

// FuzzParse drives the format-autodetecting Parse dispatcher with arbitrary
// bytes. Parse handles fully untrusted input (trace files from disk / stdin),
// so for any input it must return a value or an error — never panic, hang, or
// OOM. Seeds cover each detection branch so the fuzzer reaches every sub-parser
// (protobuf, OTLP-JSON, Jaeger, Chrome, Zipkin, flat JSON, stdouttrace) plus
// the gzip/zstd decompression front door.
func FuzzParse(f *testing.F) {
	seeds := [][]byte{
		[]byte(``),
		[]byte(`{}`),
		[]byte(`[]`),
		[]byte("\n\n\n"),
		// OTLP protobuf-JSON
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`),
		[]byte(`{"resourceSpans":[]}` + "\n" + `{"resourceSpans":[]}`),
		// Jaeger
		[]byte(`{"data":[{"traceID":"abc","spans":[{"traceID":"abc","spanID":"def","operationName":"op","startTime":1,"duration":2}]}]}`),
		// Chrome trace
		[]byte(`{"traceEvents":[{"ph":"X","name":"n","ts":0,"dur":1,"pid":1,"tid":1}]}`),
		[]byte(`[{"ph":"B","name":"n","ts":0,"pid":1,"tid":1}]`),
		// Zipkin
		[]byte(`[{"traceId":"1","id":"2","name":"n","timestamp":1,"duration":2,"localEndpoint":{"serviceName":"s"}}]`),
		// flat JSON / stdouttrace
		[]byte(`{"SpanContext":{"TraceID":"00000000000000000000000000000001","SpanID":"0000000000000001"},"ParentSpanID":"0000000000000000","Name":"n"}`),
		[]byte(`{"Name":"n","SpanContext":{"TraceID":"00000000000000000000000000000001","SpanID":"0000000000000001"}}`),
		// gzip magic prefix (exercises maybeDecompress)
		{0x1f, 0x8b, 0x08, 0x00},
		// protobuf-ish leading bytes
		{0x0a, 0x00},
		{0x0a, 0x05, 0x12, 0x03, 0x01, 0x02, 0x03},
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic. Result/err both acceptable; we only guard liveness.
		spans, err := Parse(bytes.NewReader(data))
		if err == nil {
			// On success every span must be non-nil (downstream derefs them).
			for i, s := range spans {
				if s == nil {
					t.Fatalf("Parse returned nil span at index %d for input %q", i, data)
				}
			}
		}
	})
}
