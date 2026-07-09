package enrichment

import (
	"strings"
	"testing"
)

func TestLintSpans_DeprecatedHTTP(t *testing.T) {
	spans := []SpanData{
		{
			Name: "GET /api",
			Attrs: map[string]string{
				"http.method":      "GET",
				"http.status_code": "200",
				"http.url":         "http://example.com/api",
			},
			SpanKind: "CLIENT",
		},
	}

	results := LintSpans(spans)
	if len(results) == 0 {
		t.Fatal("expected lint results for deprecated HTTP attributes")
	}

	foundMethod := false
	foundStatusCode := false
	foundURL := false
	for _, r := range results {
		if strings.Contains(r.Message, "http.method") {
			foundMethod = true
		}
		if strings.Contains(r.Message, "http.status_code") {
			foundStatusCode = true
		}
		if strings.Contains(r.Message, "http.url") {
			foundURL = true
		}
	}
	if !foundMethod {
		t.Error("expected warning for deprecated http.method")
	}
	if !foundStatusCode {
		t.Error("expected warning for deprecated http.status_code")
	}
	if !foundURL {
		t.Error("expected warning for deprecated http.url")
	}
}

func TestLintSpans_CleanSpan(t *testing.T) {
	spans := []SpanData{
		{
			Name: "GET /api",
			Attrs: map[string]string{
				"http.request.method":       "GET",
				"http.response.status_code": "200",
				"server.address":            "api.example.com",
				"http.route":                "/api",
			},
			SpanKind: "SERVER",
		},
	}

	results := LintSpans(spans)
	// Should have no warnings (only info-level at most)
	for _, r := range results {
		if r.Level == "warning" || r.Level == "error" {
			t.Errorf("unexpected %s lint result: %s", r.Level, r.Message)
		}
	}
}

func TestLintSpans_MissingDBSystem(t *testing.T) {
	spans := []SpanData{
		{
			Name: "SELECT users",
			Attrs: map[string]string{
				"db.statement": "SELECT * FROM users",
			},
		},
	}

	results := LintSpans(spans)
	found := false
	for _, r := range results {
		if strings.Contains(r.Message, "db.system") {
			found = true
		}
	}
	if !found {
		t.Error("expected warning for missing db.system")
	}
}

func TestLintSpans_DeprecatedDB(t *testing.T) {
	spans := []SpanData{
		{
			Name: "SELECT users",
			Attrs: map[string]string{
				"db.system":    "postgresql",
				"db.statement": "SELECT * FROM users",
				"db.sql.table": "users",
			},
		},
	}

	results := LintSpans(spans)
	wantReplacements := map[string]bool{
		"db.system.name":     false,
		"db.query.text":      false,
		"db.collection.name": false,
	}
	for _, r := range results {
		for repl := range wantReplacements {
			if strings.Contains(r.Suggestion, repl) {
				wantReplacements[repl] = true
			}
		}
	}
	for repl, found := range wantReplacements {
		if !found {
			t.Errorf("expected a deprecation suggestion pointing to %q", repl)
		}
	}
}

func TestLintSpans_StableDBNamesClean(t *testing.T) {
	// A span using the current stable DB names must produce no warnings.
	spans := []SpanData{
		{
			Name: "SELECT users",
			Attrs: map[string]string{
				"db.system.name": "postgresql",
				"db.query.text":  "SELECT * FROM users",
			},
		},
	}
	for _, r := range LintSpans(spans) {
		if r.Level == "warning" || r.Level == "error" {
			t.Errorf("unexpected %s for stable DB span: %s", r.Level, r.Message)
		}
	}
}

func TestFormatLintResults_Empty(t *testing.T) {
	out := FormatLintResults(nil)
	if !strings.Contains(out, "No semconv issues") {
		t.Errorf("expected 'No semconv issues' message, got %q", out)
	}
}

func TestFormatLintResults_Dedup(t *testing.T) {
	results := []LintResult{
		{SpanName: "span1", Level: "warning", Message: "same message", Suggestion: "fix it"},
		{SpanName: "span2", Level: "warning", Message: "same message", Suggestion: "fix it"},
		{SpanName: "span3", Level: "warning", Message: "same message", Suggestion: "fix it"},
	}

	out := FormatLintResults(results)
	if !strings.Contains(out, "×3") {
		t.Errorf("expected deduplicated count (×3), got:\n%s", out)
	}
}

// TestLintSpans_DeprecationGolden locks the exact Message+Suggestion for every
// deprecated attribute. If lintSpan is refactored (e.g. table-driven), this
// catches any drift in user-facing text.
func TestLintSpans_DeprecationGolden(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		attr        string
		value       string // empty → no value
		wantMessage string
		wantSugg    string
	}{
		{"http.method with value", "http.method", "GET", "Deprecated attribute 'http.method' (value: GET)", "Use 'http.request.method' instead (semconv v1.20+)"},
		{"http.status_code with value", "http.status_code", "200", "Deprecated attribute 'http.status_code' (value: 200)", "Use 'http.response.status_code' instead (semconv v1.20+)"},
		{"http.url", "http.url", "x", "Deprecated attribute 'http.url'", "Use 'url.full' instead (semconv v1.20+)"},
		{"http.target", "http.target", "x", "Deprecated attribute 'http.target'", "Use 'url.path' and 'url.query' instead (semconv v1.20+)"},
		{"http.scheme", "http.scheme", "x", "Deprecated attribute 'http.scheme'", "Use 'url.scheme' instead (semconv v1.20+)"},
		{"http.host", "http.host", "x", "Deprecated attribute 'http.host'", "Use 'server.address' and 'server.port' instead (semconv v1.20+)"},
		{"db.system", "db.system", "pg", "Deprecated attribute 'db.system'", "Use 'db.system.name' instead"},
		{"db.statement", "db.statement", "x", "Deprecated attribute 'db.statement'", "Use 'db.query.text' instead"},
		{"db.operation", "db.operation", "x", "Deprecated attribute 'db.operation'", "Use 'db.operation.name' instead"},
		{"db.sql.table", "db.sql.table", "x", "Deprecated attribute 'db.sql.table'", "Use 'db.collection.name' instead"},
		{"db.name", "db.name", "x", "Deprecated attribute 'db.name'", "Use 'db.namespace' instead"},
		{"db.connection_string", "db.connection_string", "x", "Deprecated attribute 'db.connection_string'", "Use 'server.address and server.port' instead"},
		{"net.peer.name", "net.peer.name", "x", "Deprecated attribute 'net.peer.name'", "Use 'server.address' (client spans) or 'client.address' (server spans) instead"},
		{"net.peer.port", "net.peer.port", "x", "Deprecated attribute 'net.peer.port'", "Use 'server.port' (client spans) or 'client.port' (server spans) instead"},
		{"messaging.destination", "messaging.destination", "x", "Deprecated attribute 'messaging.destination'", "Use 'messaging.destination.name' instead (semconv v1.20+)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Provide ONLY the deprecated attr so other checks don't fire.
			// For http.method we must also suppress the "missing server.address"
			// info check by... well, info-level is fine; we filter to warnings.
			spans := []SpanData{{
				Name:    "span",
				Attrs:   map[string]string{tc.attr: tc.value},
				SpanKind: "INTERNAL",
			}}
			for _, r := range LintSpans(spans) {
				if r.Level != "warning" {
					continue
				}
				if !strings.Contains(r.Message, tc.attr) {
					continue
				}
				if r.Message != tc.wantMessage {
					t.Errorf("Message:\n got: %s\nwant: %s", r.Message, tc.wantMessage)
				}
				if r.Suggestion != tc.wantSugg {
					t.Errorf("Suggestion:\n got: %s\nwant: %s", r.Suggestion, tc.wantSugg)
				}
				return
			}
			t.Errorf("no warning-level result found for %s", tc.attr)
		})
	}
}
