package receiver

import (
	"bytes"
	"compress/gzip"
	"net/http/httptest"
	"testing"
)

// FuzzHandleTraces drives the OTLP receiver's HTTP handler end-to-end with a
// fuzzer-controlled body, Content-Type, and optional gzip encoding. The
// receiver accepts unauthenticated POSTs from any sender, so no body — valid
// protobuf, JSON, truncated, or pure garbage, compressed or not — may panic the
// handler; it must always respond with a status code. Exercises the gzip path,
// the protobuf-vs-JSON content-type routing, and the concurrent span append
// that the direct parser fuzzers don't reach.
func FuzzHandleTraces(f *testing.F) {
	seeds := []struct {
		body        string
		contentType string
		gzipIt      bool
	}{
		{`{"resourceSpans":[]}`, "application/json", false},
		{`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`, "application/json", false},
		{"\x0a\x00", "application/x-protobuf", false},
		{"not protobuf at all", "application/x-protobuf", false},
		{`{"resourceSpans":[]}`, "application/json", true},
		{"", "", false},
		{"garbage", "text/plain", false},
		{"\x1f\x8b not really gzip", "application/json", false}, // gzip magic but bad stream, no encoding header
	}
	for _, s := range seeds {
		f.Add(s.body, s.contentType, s.gzipIt)
	}

	f.Fuzz(func(t *testing.T, body, contentType string, gzipIt bool) {
		payload := []byte(body)
		var encoding string
		if gzipIt {
			var buf bytes.Buffer
			gw := gzip.NewWriter(&buf)
			_, _ = gw.Write(payload)
			_ = gw.Close()
			payload = buf.Bytes()
			encoding = "gzip"
		}

		r := New(":0")
		req := httptest.NewRequest("POST", "/v1/traces", bytes.NewReader(payload))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if encoding != "" {
			req.Header.Set("Content-Encoding", encoding)
		}
		rec := httptest.NewRecorder()

		// Must not panic; must always write a status.
		r.handleTraces(rec, req)
		if rec.Code == 0 {
			t.Fatalf("handler wrote no status for body=%q ct=%q gzip=%v", body, contentType, gzipIt)
		}
	})
}
