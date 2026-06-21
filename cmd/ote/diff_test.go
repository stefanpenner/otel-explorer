package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
)

// otlpTraceJSON builds a minimal one-span OTLP/JSON trace document that
// traceapi auto-detects as OTLP (it contains "resourceSpans").
func otlpTraceJSON(traceID, span string, startNano, endNano int64) string {
	return fmt.Sprintf(`{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"traceId": %q,
					"spanId": "b7ad6b7169203331",
					"name": %q,
					"kind": 2,
					"startTimeUnixNano": "%d",
					"endTimeUnixNano": "%d",
					"status": {"code": 1}
				}]
			}]
		}]
	}`, traceID, span, startNano, endNano)
}

// jaegerTraceJSON builds a minimal one-span Jaeger API response (top-level
// "data"), which traceapi auto-detects as Jaeger (no "resourceSpans").
func jaegerTraceJSON(traceID, op string, startMicros, durationMicros int64) string {
	return fmt.Sprintf(`{
		"data": [{
			"traceID": %q,
			"spans": [{
				"traceID": %q,
				"spanID": "b7ad6b7169203331",
				"operationName": %q,
				"references": [],
				"startTime": %d,
				"duration": %d,
				"tags": [],
				"processID": "p1"
			}],
			"processes": {"p1": {"serviceName": "ci", "tags": []}}
		}]
	}`, traceID, traceID, op, startMicros, durationMicros)
}

// backendServer serves /api/traces/{id} from a per-id body map, mimicking
// Tempo/Jaeger. Unknown ids return 404 so error paths can be exercised.
func backendServer(t *testing.T, bodies map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/traces/")
		body, ok := bodies[id]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestParseArgsDiffBackend: `diff --jaeger/--tempo=URL id1 id2` is accepted
// without --trace-id (the two positional inputs ARE the trace ids), even
// though the same backend flag without --trace-id is rejected outside diff.
func TestParseArgsDiffBackend(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantTempo  string
		wantJaeger string
	}{
		{
			name:       "jaeger backend with two trace ids",
			args:       []string{"diff", "--jaeger=http://localhost:16686", "id-before", "id-after"},
			wantJaeger: "http://localhost:16686",
		},
		{
			name:      "tempo backend with two trace ids",
			args:      []string{"diff", "--tempo=http://localhost:3200", "id-before", "id-after"},
			wantTempo: "http://localhost:3200",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseArgs(tt.args, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !cfg.diffMode {
				t.Errorf("diffMode = false, want true")
			}
			if cfg.tempoURL != tt.wantTempo {
				t.Errorf("tempoURL = %q, want %q", cfg.tempoURL, tt.wantTempo)
			}
			if cfg.jaegerURL != tt.wantJaeger {
				t.Errorf("jaegerURL = %q, want %q", cfg.jaegerURL, tt.wantJaeger)
			}
			want := []string{"id-before", "id-after"}
			if !slicesEqual(cfg.diffInputs, want) {
				t.Errorf("diffInputs = %v, want %v", cfg.diffInputs, want)
			}
		})
	}
}

// TestLoadDiffSideFromBackend: with a backend configured, a diff input is
// treated as a trace id and fetched from the backend, NOT as a file or URL.
func TestLoadDiffSideFromBackend(t *testing.T) {
	srv := backendServer(t, map[string]string{
		"trace-xyz": otlpTraceJSON("0af7651916cd43dd8448eb211c80319c", "HTTP GET", 1_000_000_000, 2_000_000_000),
	})
	cfg := config{jaegerURL: srv.URL}

	roots, label, err := loadDiffSide(context.Background(), "trace-xyz", "", enrichment.DefaultEnricher(), cfg)
	if err != nil {
		t.Fatalf("loadDiffSide: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("roots = %d, want 1", len(roots))
	}
	if roots[0].Name != "HTTP GET" {
		t.Errorf("root name = %q, want %q", roots[0].Name, "HTTP GET")
	}
	if label != "trace-xyz" {
		t.Errorf("label = %q, want %q", label, "trace-xyz")
	}
}

// TestLoadDiffSideBackendError: a backend fetch failure surfaces as an error
// rather than being misinterpreted as a (missing) local file or GitHub URL.
func TestLoadDiffSideBackendError(t *testing.T) {
	srv := backendServer(t, map[string]string{}) // every id → 404
	cfg := config{tempoURL: srv.URL}

	if _, _, err := loadDiffSide(context.Background(), "missing", "", enrichment.DefaultEnricher(), cfg); err == nil {
		t.Fatal("expected error for missing trace, got nil")
	}
}

// TestRunDiffFromBackend: end-to-end wiring — runDiff fetches both sides from
// the backend and renders a diff (no GitHub token required).
func TestRunDiffFromBackend(t *testing.T) {
	srv := backendServer(t, map[string]string{
		"before": otlpTraceJSON("0af7651916cd43dd8448eb211c80319c", "build", 0, 1_000_000_000),
		"after":  otlpTraceJSON("0af7651916cd43dd8448eb211c80319d", "build", 0, 5_000_000_000),
	})
	cfg := config{
		diffMode:   true,
		jaegerURL:  srv.URL,
		diffInputs: []string{"before", "after"},
		tuiMode:    false,
	}

	out := captureStdout(t, func() {
		if err := runDiff(context.Background(), cfg, enrichment.DefaultEnricher()); err != nil {
			t.Fatalf("runDiff: %v", err)
		}
	})

	if !strings.Contains(out, "Trace Diff") {
		t.Errorf("output missing diff header:\n%s", out)
	}
	if !strings.Contains(out, "build") {
		t.Errorf("output missing changed span:\n%s", out)
	}
}

func TestIsTraceBackendURL(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"http://localhost:16686/api/traces/abc", true},
		{"https://tempo.example.com:3200/api/traces/def", true},
		{"https://github.com/owner/repo/actions/runs/123", false},
		{"before.json", false},
		{"http://localhost:16686/search", false},
	}
	for _, tt := range tests {
		if got := isTraceBackendURL(tt.in); got != tt.want {
			t.Errorf("isTraceBackendURL(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// TestLoadDiffSidePerSideURL: a full /api/traces/<id> URL is fetched directly
// (no --tempo/--jaeger flag), so each side can name its own backend.
func TestLoadDiffSidePerSideURL(t *testing.T) {
	srv := backendServer(t, map[string]string{
		"trace-xyz": otlpTraceJSON("0af7651916cd43dd8448eb211c80319c", "HTTP GET", 0, 1_000_000_000),
	})
	cfg := config{} // no backend flag set

	url := srv.URL + "/api/traces/trace-xyz"
	roots, label, err := loadDiffSide(context.Background(), url, "", enrichment.DefaultEnricher(), cfg)
	if err != nil {
		t.Fatalf("loadDiffSide: %v", err)
	}
	if len(roots) != 1 || roots[0].Name != "HTTP GET" {
		t.Fatalf("roots = %+v, want one span named HTTP GET", roots)
	}
	if label != "trace-xyz" {
		t.Errorf("label = %q, want %q", label, "trace-xyz")
	}
}

// TestRunDiffCrossBackend: before from a Jaeger server, after from a Tempo
// (OTLP) server — different backends, different response formats, one diff.
func TestRunDiffCrossBackend(t *testing.T) {
	jaeger := backendServer(t, map[string]string{
		"j1": jaegerTraceJSON("0af7651916cd43dd8448eb211c80319c", "build", 0, 1_000_000), // 1s
	})
	tempo := backendServer(t, map[string]string{
		"t1": otlpTraceJSON("0af7651916cd43dd8448eb211c80319d", "build", 0, 5_000_000_000), // 5s
	})
	cfg := config{
		diffMode: true,
		diffInputs: []string{
			jaeger.URL + "/api/traces/j1",
			tempo.URL + "/api/traces/t1",
		},
		tuiMode: false,
	}

	out := captureStdout(t, func() {
		if err := runDiff(context.Background(), cfg, enrichment.DefaultEnricher()); err != nil {
			t.Fatalf("runDiff: %v", err)
		}
	})

	if !strings.Contains(out, "Trace Diff") {
		t.Errorf("output missing diff header:\n%s", out)
	}
	if !strings.Contains(out, "build") {
		t.Errorf("output missing changed span:\n%s", out)
	}
}

func TestShortTraceID(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"abc", "abc"},
		{"0af7651916cd", "0af7651916cd"}, // exactly 12, no truncation
		{"0af7651916cd43dd8448eb211c80319c", "0af7651916cd…"}, // 32 → 12+ellipsis
	}
	for _, tt := range tests {
		if got := shortTraceID(tt.in); got != tt.want {
			t.Errorf("shortTraceID(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// captureStdout redirects os.Stdout for the duration of fn and returns what
// was written.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stdout = orig
	return <-done
}
