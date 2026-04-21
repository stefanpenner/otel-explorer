package traceapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchTrace(t *testing.T) {
	t.Parallel()

	otlpJSON := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"traceId": "0af7651916cd43dd8448eb211c80319c",
					"spanId": "b7ad6b7169203331",
					"name": "HTTP GET /api",
					"kind": 2,
					"startTimeUnixNano": "1705312800000000000",
					"endTimeUnixNano":   "1705312801000000000",
					"attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}],
					"status": {"code": 1}
				}]
			}]
		}]
	}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/traces/0af7651916cd43dd8448eb211c80319c" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Accept") != "application/protobuf" {
			t.Errorf("expected Accept: application/protobuf, got %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(otlpJSON))
	}))
	defer srv.Close()

	client := New(srv.URL)
	spans, err := client.FetchTrace("0af7651916cd43dd8448eb211c80319c")
	if err != nil {
		t.Fatalf("FetchTrace failed: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "HTTP GET /api" {
		t.Errorf("name = %q, want %q", spans[0].Name(), "HTTP GET /api")
	}
}

func TestFetchTraceHTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("trace not found"))
	}))
	defer srv.Close()

	client := New(srv.URL)
	_, err := client.FetchTrace("nonexistent")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	t.Parallel()

	client := New("http://localhost:3200/")
	if client.baseURL != "http://localhost:3200" {
		t.Errorf("baseURL = %q, want no trailing slash", client.baseURL)
	}
}
