package receiver

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// validOTLPSpanJSON is a single newline-delimited stdouttrace span
// that otlpfile.Parse can decode.
const validOTLPSpanJSON = `{"Name":"test-span","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:05:00Z","Attributes":[{"Key":"type","Value":{"Type":"STRING","Value":"workflow"}}],"Events":null,"Links":null,"Status":{"Code":"OK","Description":""}}`

// twoSpansJSON contains two newline-delimited spans.
const twoSpansJSON = `{"Name":"span-a","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331","TraceFlags":"01"},"Parent":{"TraceID":"","SpanID":""},"SpanKind":1,"StartTime":"2024-01-15T10:00:00Z","EndTime":"2024-01-15T10:01:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}
{"Name":"span-b","SpanContext":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"00f067aa0ba902b7","TraceFlags":"01"},"Parent":{"TraceID":"0af7651916cd43dd8448eb211c80319c","SpanID":"b7ad6b7169203331"},"SpanKind":1,"StartTime":"2024-01-15T10:01:00Z","EndTime":"2024-01-15T10:02:00Z","Attributes":[],"Events":null,"Links":null,"Status":{"Code":"","Description":""}}`

// mux returns the same HTTP handler that Start() would register,
// without actually starting a TCP listener.
func setupMux(r *Receiver) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", r.handleTraces)
	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func TestPostValidTracesAccumulatesSpans(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(twoSpansJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if r.SpanCount() != 2 {
		t.Fatalf("expected 2 spans, got %d", r.SpanCount())
	}

	spans := r.Spans()
	if spans[0].Name() != "span-a" {
		t.Errorf("span[0] name = %q, want %q", spans[0].Name(), "span-a")
	}
	if spans[1].Name() != "span-b" {
		t.Errorf("span[1] name = %q, want %q", spans[1].Name(), "span-b")
	}
}

func TestGetTracesReturns405(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	req := httptest.NewRequest(http.MethodGet, "/v1/traces", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rec.Code)
	}
}

func TestPostMalformedBodyRejected(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	// A body that decodes to zero spans is a parse error: the receiver must
	// reject it (400) rather than ACK a lost export with 200.
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("{{{not json at all"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for undecodable body, got %d: %s", rec.Code, rec.Body.String())
	}

	if r.SpanCount() != 0 {
		t.Errorf("expected 0 spans after malformed request, got %d", r.SpanCount())
	}
}

func TestPostCorruptGzipReturns400(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	// Bytes starting with gzip magic (0x1f 0x8b) but otherwise corrupt.
	// This causes maybeDecompress to fail, which surfaces as a parse error (400).
	corrupt := []byte{0x1f, 0x8b, 0x00, 0x00, 0xff, 0xff}
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(corrupt))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for corrupt gzip, got %d: %s", rec.Code, rec.Body.String())
	}

	if r.SpanCount() != 0 {
		t.Errorf("expected 0 spans, got %d", r.SpanCount())
	}
}

func TestHealthReturns200(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestSpanCountAndSpans(t *testing.T) {
	r := New(":0")

	if r.SpanCount() != 0 {
		t.Fatalf("expected 0 spans initially, got %d", r.SpanCount())
	}
	if len(r.Spans()) != 0 {
		t.Fatalf("expected empty Spans() initially, got %d", len(r.Spans()))
	}

	handler := setupMux(r)

	// Post one span.
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(validOTLPSpanJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if r.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", r.SpanCount())
	}

	spans := r.Spans()
	if len(spans) != 1 {
		t.Fatalf("expected Spans() length 1, got %d", len(spans))
	}
	if spans[0].Name() != "test-span" {
		t.Errorf("span name = %q, want %q", spans[0].Name(), "test-span")
	}

	// Spans() returns a copy, so mutating it should not affect the receiver.
	spans[0] = nil
	if r.SpanCount() != 1 {
		t.Errorf("mutating Spans() return value should not affect receiver, but SpanCount changed to %d", r.SpanCount())
	}
}

func TestConcurrentPosts(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(validOTLPSpanJSON))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Errorf("expected status 200, got %d", rec.Code)
			}
		}()
	}

	wg.Wait()

	if r.SpanCount() != goroutines {
		t.Errorf("expected %d spans, got %d", goroutines, r.SpanCount())
	}
}

// buildProtobufRequestBody builds a bare protobuf-encoded TracesData
// (wire-compatible with ExportTraceServiceRequest), as standard OTLP/HTTP
// protobuf exporters send it: no length prefix.
func buildProtobufRequestBody(t *testing.T) []byte {
	t.Helper()
	traceID, err := hex.DecodeString("0af7651916cd43dd8448eb211c80319c")
	if err != nil {
		t.Fatalf("decode trace ID: %v", err)
	}
	spanID, err := hex.DecodeString("b7ad6b7169203331")
	if err != nil {
		t.Fatalf("decode span ID: %v", err)
	}
	td := &v1.TracesData{
		ResourceSpans: []*v1.ResourceSpans{{
			ScopeSpans: []*v1.ScopeSpans{{
				Spans: []*v1.Span{{
					TraceId:           traceID,
					SpanId:            spanID,
					Name:              "proto-span",
					StartTimeUnixNano: 1705312800000000000,
					EndTimeUnixNano:   1705312801000000000,
				}},
			}},
		}},
	}
	body, err := proto.Marshal(td)
	if err != nil {
		t.Fatalf("marshal TracesData: %v", err)
	}
	return body
}

func TestPostProtobufBodyAccumulatesSpans(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	body := buildProtobufRequestBody(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if r.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", r.SpanCount())
	}
	if got := r.Spans()[0].Name(); got != "proto-span" {
		t.Errorf("span name = %q, want %q", got, "proto-span")
	}
}

func TestPostGzipProtobufBodyAccumulatesSpans(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(buildProtobufRequestBody(t)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", &buf)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if r.SpanCount() != 1 {
		t.Fatalf("expected 1 span, got %d", r.SpanCount())
	}
}

func TestPostMalformedProtobufReturns400(t *testing.T) {
	r := New(":0")
	handler := setupMux(r)

	// Garbage bytes must produce a 4xx, not a silent 200 with zero spans.
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("\x0a\xffnot a protobuf message at all\xff\xff"))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if r.SpanCount() != 0 {
		t.Fatalf("expected 0 spans, got %d", r.SpanCount())
	}
}
