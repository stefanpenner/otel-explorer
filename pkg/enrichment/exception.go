package enrichment

import "strings"

// ApplyException folds an exception recorded on a span (via its "exception"
// span event, per OTel conventions) into already-computed SpanHints, so errors
// are visible in the timeline itself rather than only in the inspector.
//
// Policy:
//   - The exception type is surfaced in Detail (e.g. "ValueError"), once.
//   - The outcome is escalated to failure ONLY when no outcome was determined
//     from the span's status (Outcome==""). An explicit success/failure/skipped
//     status is respected — recording an exception does not, per the spec,
//     change span status, so a span that reported OK with a handled exception
//     stays green but still shows the type.
//
// exceptionType=="" is a no-op.
func ApplyException(h *SpanHints, exceptionType string) {
	if exceptionType == "" {
		return
	}
	if h.Outcome == "" {
		h.Outcome = "failure"
		h.Color = "red"
	}
	if !strings.Contains(h.Detail, exceptionType) {
		if h.Detail == "" {
			h.Detail = exceptionType
		} else {
			h.Detail += " · " + exceptionType
		}
	}
}

// ExceptionTypeFromEvent reports the exception type carried by a single span
// event, given its name and attributes. Returns "" if the event is not an
// exception event. An event is an exception event when it is named "exception"
// (the OTel convention) or carries an exception.type attribute.
func ExceptionTypeFromEvent(name string, attrs map[string]string) string {
	if t := attrs["exception.type"]; t != "" {
		return t
	}
	if name == "exception" {
		// Named exception event without a type — surface a generic marker so
		// the failure is still visible.
		return "exception"
	}
	return ""
}
