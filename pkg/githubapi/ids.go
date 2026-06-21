package githubapi

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/otel/trace"
)

// Deterministic IDs use SHA-256 truncated to the ID length (16 bytes for a trace
// ID, 8 for a span ID) — NOT MD5, which is disallowed under FIPS on the runner
// side. Both implementations must hash identical strings and take the same
// leading bytes so runner-emitted and API-reconstructed spans still merge.

// NewTraceID returns a deterministic TraceID based on GitHub Workflow Run ID and Attempt.
func NewTraceID(runID int64, runAttempt int64) trace.TraceID {
	if runAttempt == 0 {
		runAttempt = 1
	}
	id := fmt.Sprintf("%d-%d", runID, runAttempt)
	sum := sha256.Sum256([]byte(id))
	var tid trace.TraceID
	copy(tid[:], sum[:16])
	return tid
}

// NewSpanID returns a deterministic SpanID based on a GitHub ID (e.g., Job ID).
func NewSpanID(id int64) trace.SpanID {
	var sid trace.SpanID
	binary.BigEndian.PutUint64(sid[:], uint64(id))
	return sid
}

// NewSpanIDFromString returns a deterministic SpanID based on a string (e.g., Step Name).
func NewSpanIDFromString(s string) trace.SpanID {
	sum := sha256.Sum256([]byte(s))
	var sid trace.SpanID
	copy(sid[:], sum[:8])
	return sid
}

// NewJobSpanID returns a deterministic job SpanID derived only from identifiers
// the GitHub Actions runner also has (run id, run attempt, job display name), so
// runner-emitted OTLP job spans merge with API-reconstructed ones.
//
// Shared contract (mirrored in the runner's OTelTraceExporter.cs):
//
//	sha256("job-{runID}-{runAttempt}-{jobName}")[:8]
func NewJobSpanID(runID int64, runAttempt int64, jobName string) trace.SpanID {
	if runAttempt == 0 {
		runAttempt = 1
	}
	return NewSpanIDFromString(fmt.Sprintf("job-%d-%d-%s", runID, runAttempt, jobName))
}

// NewStepSpanID returns a deterministic step SpanID matching the runner contract:
//
//	sha256("step-{runID}-{runAttempt}-{jobName}-{stepNumber}-{stepName}")[:8]
//
// The 1-based step number disambiguates two steps that share a display name.
func NewStepSpanID(runID int64, runAttempt int64, jobName string, stepNumber int, stepName string) trace.SpanID {
	if runAttempt == 0 {
		runAttempt = 1
	}
	return NewSpanIDFromString(fmt.Sprintf("step-%d-%d-%s-%d-%s", runID, runAttempt, jobName, stepNumber, stepName))
}
