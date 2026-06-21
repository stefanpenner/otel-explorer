package enrichment

import (
	"fmt"
	"strings"
)

// LintResult represents a single semconv lint finding.
type LintResult struct {
	SpanName   string
	Level      string // "warning", "error", "info"
	Message    string
	Suggestion string
}

// LintSpans analyzes span attributes for semantic convention compliance.
// It checks for deprecated attributes, missing required attributes, and
// common misconfigurations.
func LintSpans(spans []SpanData) []LintResult {
	var results []LintResult

	for _, span := range spans {
		results = append(results, lintSpan(span)...)
	}

	// Aggregate lint: check for missing instrumentation
	hasHTTP := false
	hasDB := false
	for _, span := range spans {
		if span.Attrs["http.request.method"] != "" || span.Attrs["http.method"] != "" {
			hasHTTP = true
		}
		if span.Attrs["db.system"] != "" || span.Attrs["db.system.name"] != "" {
			hasDB = true
		}
	}

	// Detect traces with HTTP but no DB (might indicate missing DB instrumentation)
	if hasHTTP && !hasDB && len(spans) > 10 {
		results = append(results, LintResult{
			Level:      "info",
			Message:    "Trace has HTTP spans but no database spans",
			Suggestion: "If your service uses a database, consider adding database instrumentation",
		})
	}

	return results
}

// SpanData is a simplified span representation for linting.
type SpanData struct {
	Name      string
	Attrs     map[string]string
	SpanKind  string
	ScopeName string
	HasEvents bool
}

// deprecatedAttr returns a warning when the span carries a deprecated
// attribute, suggesting its replacement, or nil if the attribute is absent.
func deprecatedAttr(span SpanData, old, replacement string) *LintResult {
	if span.Attrs[old] == "" {
		return nil
	}
	return &LintResult{
		SpanName:   span.Name,
		Level:      "warning",
		Message:    fmt.Sprintf("Deprecated attribute '%s'", old),
		Suggestion: fmt.Sprintf("Use '%s' instead", replacement),
	}
}

// lintSpan checks a single span for semconv issues.
func lintSpan(span SpanData) []LintResult {
	var results []LintResult

	// Check for deprecated HTTP attributes
	if span.Attrs["http.method"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    fmt.Sprintf("Deprecated attribute 'http.method' (value: %s)", span.Attrs["http.method"]),
			Suggestion: "Use 'http.request.method' instead (semconv v1.20+)",
		})
	}
	if span.Attrs["http.status_code"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    fmt.Sprintf("Deprecated attribute 'http.status_code' (value: %s)", span.Attrs["http.status_code"]),
			Suggestion: "Use 'http.response.status_code' instead (semconv v1.20+)",
		})
	}
	if span.Attrs["http.url"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'http.url'",
			Suggestion: "Use 'url.full' instead (semconv v1.20+)",
		})
	}
	if span.Attrs["http.target"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'http.target'",
			Suggestion: "Use 'url.path' and 'url.query' instead (semconv v1.20+)",
		})
	}
	if span.Attrs["http.scheme"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'http.scheme'",
			Suggestion: "Use 'url.scheme' instead (semconv v1.20+)",
		})
	}
	if span.Attrs["http.host"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'http.host'",
			Suggestion: "Use 'server.address' and 'server.port' instead (semconv v1.20+)",
		})
	}

	// Check for deprecated DB attributes (the stable database semconv renamed
	// them — db.system → db.system.name, db.statement → db.query.text, etc.).
	dbDeprecations := []struct{ old, replacement string }{
		{"db.system", "db.system.name"},
		{"db.statement", "db.query.text"},
		{"db.operation", "db.operation.name"},
		{"db.sql.table", "db.collection.name"},
		{"db.name", "db.namespace"},
		{"db.connection_string", "server.address and server.port"},
	}
	for _, d := range dbDeprecations {
		if r := deprecatedAttr(span, d.old, d.replacement); r != nil {
			results = append(results, *r)
		}
	}

	// A query without a database system is incomplete (accept old or new name).
	hasQuery := span.Attrs["db.query.text"] != "" || span.Attrs["db.statement"] != ""
	hasDBSystem := span.Attrs["db.system.name"] != "" || span.Attrs["db.system"] != ""
	if hasQuery && !hasDBSystem {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Has a DB query but missing required 'db.system.name'",
			Suggestion: "Add 'db.system.name' attribute (e.g., 'postgresql', 'mysql', 'redis')",
		})
	}

	// Check for deprecated net.* attributes
	if span.Attrs["net.peer.name"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'net.peer.name'",
			Suggestion: "Use 'server.address' (client spans) or 'client.address' (server spans) instead",
		})
	}
	if span.Attrs["net.peer.port"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'net.peer.port'",
			Suggestion: "Use 'server.port' (client spans) or 'client.port' (server spans) instead",
		})
	}

	// Check HTTP client spans for missing required attributes
	if span.Attrs["http.request.method"] != "" && span.SpanKind == "CLIENT" {
		if span.Attrs["server.address"] == "" && span.Attrs["net.peer.name"] == "" {
			results = append(results, LintResult{
				SpanName:   span.Name,
				Level:      "info",
				Message:    "HTTP client span missing 'server.address'",
				Suggestion: "Add 'server.address' for better service topology visibility",
			})
		}
	}

	// Check HTTP server spans for missing route
	if span.Attrs["http.request.method"] != "" && span.SpanKind == "SERVER" {
		if span.Attrs["http.route"] == "" {
			results = append(results, LintResult{
				SpanName:   span.Name,
				Level:      "info",
				Message:    "HTTP server span missing 'http.route'",
				Suggestion: "Add 'http.route' (e.g., '/api/users/:id') for meaningful span grouping",
			})
		}
	}

	// Check for deprecated messaging attributes
	if span.Attrs["messaging.destination"] != "" {
		results = append(results, LintResult{
			SpanName:   span.Name,
			Level:      "warning",
			Message:    "Deprecated attribute 'messaging.destination'",
			Suggestion: "Use 'messaging.destination.name' instead (semconv v1.20+)",
		})
	}

	// Check RPC spans for required attributes
	if span.Attrs["rpc.system"] != "" {
		if span.Attrs["rpc.service"] == "" {
			results = append(results, LintResult{
				SpanName:   span.Name,
				Level:      "info",
				Message:    "RPC span missing 'rpc.service'",
				Suggestion: "Add 'rpc.service' and 'rpc.method' for meaningful span identification",
			})
		}
	}

	return results
}

// FormatLintResults formats lint results for terminal output.
func FormatLintResults(results []LintResult) string {
	if len(results) == 0 {
		return "No semconv issues found."
	}

	var b strings.Builder

	// Group by level
	warnings := 0
	errors := 0
	infos := 0
	for _, r := range results {
		switch r.Level {
		case "warning":
			warnings++
		case "error":
			errors++
		case "info":
			infos++
		}
	}

	var parts []string
	if errors > 0 {
		parts = append(parts, fmt.Sprintf("%d errors", errors))
	}
	if warnings > 0 {
		parts = append(parts, fmt.Sprintf("%d warnings", warnings))
	}
	if infos > 0 {
		parts = append(parts, fmt.Sprintf("%d info", infos))
	}
	fmt.Fprintf(&b, "Semconv Lint: %d issues found (%s)\n\n", len(results), strings.Join(parts, ", "))

	// Deduplicate: group identical messages and count occurrences
	type lintKey struct {
		Level, Message, Suggestion string
	}
	counts := make(map[lintKey]int)
	var order []lintKey
	for _, r := range results {
		key := lintKey{r.Level, r.Message, r.Suggestion}
		if counts[key] == 0 {
			order = append(order, key)
		}
		counts[key]++
	}

	for _, key := range order {
		count := counts[key]
		icon := "ℹ"
		switch key.Level {
		case "warning":
			icon = "⚠"
		case "error":
			icon = "✗"
		}
		countStr := ""
		if count > 1 {
			countStr = fmt.Sprintf(" (×%d)", count)
		}
		fmt.Fprintf(&b, "  %s %s%s\n", icon, key.Message, countStr)
		if key.Suggestion != "" {
			fmt.Fprintf(&b, "    → %s\n", key.Suggestion)
		}
	}

	return b.String()
}
