package githubapi

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/otel/trace"
)

// NewTraceID returns a deterministic TraceID based on GitHub Workflow Run ID and Attempt.
func NewTraceID(runID int64, runAttempt int64) trace.TraceID {
	if runAttempt == 0 {
		runAttempt = 1
	}
	id := fmt.Sprintf("%d-%d", runID, runAttempt)
	return trace.TraceID(md5.Sum([]byte(id)))
}

// NewSpanID returns a deterministic SpanID based on a GitHub ID (e.g., Job ID).
func NewSpanID(id int64) trace.SpanID {
	var sid trace.SpanID
	binary.BigEndian.PutUint64(sid[:], uint64(id))
	return sid
}

// NewSpanIDFromString returns a deterministic SpanID based on a string (e.g., Step Name).
func NewSpanIDFromString(s string) trace.SpanID {
	sum := md5.Sum([]byte(s))
	var sid trace.SpanID
	copy(sid[:], sum[:8])
	return sid
}

// NewJobSpanID returns a deterministic job SpanID derived only from identifiers
// the GitHub Actions runner also has (run id, run attempt, job display name), so
// runner-emitted OTLP job spans merge with API-reconstructed ones.
//
// Shared contract (mirrored in the runner's OTelTracer.cs):
//   md5("job-{runID}-{runAttempt}-{jobName}")[:8]
func NewJobSpanID(runID int64, runAttempt int64, jobName string) trace.SpanID {
	if runAttempt == 0 {
		runAttempt = 1
	}
	return NewSpanIDFromString(fmt.Sprintf("job-%d-%d-%s", runID, runAttempt, jobName))
}

// NewStepSpanID returns a deterministic step SpanID matching the runner contract:
//   md5("step-{runID}-{runAttempt}-{jobName}-{stepName}")[:8]
func NewStepSpanID(runID int64, runAttempt int64, jobName string, stepName string) trace.SpanID {
	if runAttempt == 0 {
		runAttempt = 1
	}
	return NewSpanIDFromString(fmt.Sprintf("step-%d-%d-%s-%s", runID, runAttempt, jobName, stepName))
}
