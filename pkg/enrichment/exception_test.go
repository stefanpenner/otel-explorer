package enrichment

import "testing"

func TestApplyException_EscalatesUnsetOutcome(t *testing.T) {
	h := SpanHints{Category: "operation", Color: "blue"}
	ApplyException(&h, "ValueError")

	if h.Outcome != "failure" {
		t.Errorf("expected outcome escalated to failure, got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected color red, got %q", h.Color)
	}
	if h.Detail != "ValueError" {
		t.Errorf("expected Detail 'ValueError', got %q", h.Detail)
	}
}

func TestApplyException_RespectsExplicitSuccess(t *testing.T) {
	// A span that reported OK with a handled exception stays green, but the
	// exception type is still surfaced.
	h := SpanHints{Category: "operation", Outcome: "success", Color: "green"}
	ApplyException(&h, "RetryableError")

	if h.Outcome != "success" {
		t.Errorf("expected outcome to stay success, got %q", h.Outcome)
	}
	if h.Color != "green" {
		t.Errorf("expected color to stay green, got %q", h.Color)
	}
	if h.Detail != "RetryableError" {
		t.Errorf("expected Detail to surface the type, got %q", h.Detail)
	}
}

func TestApplyException_AppendsToExistingDetail(t *testing.T) {
	h := SpanHints{Detail: "GET /api [500]", Outcome: "failure", Color: "red"}
	ApplyException(&h, "TimeoutError")

	if h.Detail != "GET /api [500] · TimeoutError" {
		t.Errorf("expected appended Detail, got %q", h.Detail)
	}
}

func TestApplyException_Idempotent(t *testing.T) {
	h := SpanHints{Detail: "TimeoutError", Outcome: "failure", Color: "red"}
	ApplyException(&h, "TimeoutError")

	if h.Detail != "TimeoutError" {
		t.Errorf("expected no duplicate type in Detail, got %q", h.Detail)
	}
}

func TestApplyException_EmptyIsNoop(t *testing.T) {
	h := SpanHints{Category: "operation", Color: "blue"}
	ApplyException(&h, "")

	if h.Outcome != "" || h.Detail != "" {
		t.Errorf("expected no-op for empty type, got outcome=%q detail=%q", h.Outcome, h.Detail)
	}
}

func TestExceptionTypeFromEvent(t *testing.T) {
	cases := []struct {
		name  string
		attrs map[string]string
		want  string
	}{
		{"exception", map[string]string{"exception.type": "ValueError"}, "ValueError"},
		{"exception", map[string]string{"exception.message": "boom"}, "exception"},
		{"some-event", map[string]string{"exception.type": "IOError"}, "IOError"},
		{"log", map[string]string{"level": "info"}, ""},
		{"exception", nil, "exception"},
	}
	for _, c := range cases {
		got := ExceptionTypeFromEvent(c.name, c.attrs)
		if got != c.want {
			t.Errorf("ExceptionTypeFromEvent(%q, %v) = %q, want %q", c.name, c.attrs, got, c.want)
		}
	}
}
