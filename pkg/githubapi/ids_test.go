package githubapi

import (
	"testing"
)

func TestDeterministicIDs(t *testing.T) {
	t.Run("NewTraceID is deterministic", func(t *testing.T) {
		runID := int64(12345)
		attempt := int64(1)

		tid1 := NewTraceID(runID, attempt)
		tid2 := NewTraceID(runID, attempt)

		if tid1 != tid2 {
			t.Errorf("Expected same TraceID for same inputs, got %s and %s", tid1, tid2)
		}

		if !tid1.IsValid() {
			t.Error("Expected valid TraceID")
		}

		tid3 := NewTraceID(runID, 2)
		if tid1 == tid3 {
			t.Error("Expected different TraceID for different attempt")
		}
	})

	t.Run("NewSpanID is deterministic", func(t *testing.T) {
		jobID := int64(67890)

		sid1 := NewSpanID(jobID)
		sid2 := NewSpanID(jobID)

		if sid1 != sid2 {
			t.Errorf("Expected same SpanID for same inputs, got %s and %s", sid1, sid2)
		}

		if !sid1.IsValid() {
			t.Error("Expected valid SpanID")
		}

		sid3 := NewSpanID(67891)
		if sid1 == sid3 {
			t.Error("Expected different SpanID for different JobID")
		}
	})

	t.Run("NewSpanIDFromString is deterministic", func(t *testing.T) {
		name := "step-1"
		sid1 := NewSpanIDFromString(name)
		sid2 := NewSpanIDFromString(name)

		if sid1 != sid2 {
			t.Errorf("Expected same SpanID for same string, got %s and %s", sid1, sid2)
		}
	})
}

// TestCrossLanguageGoldenIDs locks the shared ID contract with the runner
// (runner/src/Test/L0/Worker/OTelTracerL0.cs uses the identical golden values).
func TestCrossLanguageGoldenIDs(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"trace", NewTraceID(99999, 1).String(), "37912fcf8909bcb43fd643580e6b5ee1"},
		{"workflow", NewSpanID(99999).String(), "000000000001869f"},
		{"job", NewJobSpanID(99999, 1, "build").String(), "224bc2674c838206"},
		{"step", NewStepSpanID(99999, 1, "build", "Run tests").String(), "8f4170e86c7435ac"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, c.got, c.want)
		}
	}

	// Attempt 0 normalizes to 1.
	if NewJobSpanID(99999, 0, "build") != NewJobSpanID(99999, 1, "build") {
		t.Error("attempt 0 should normalize to 1 for job span")
	}
}
