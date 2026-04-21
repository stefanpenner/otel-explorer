package arc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type captureExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *captureExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *captureExporter) Shutdown(_ context.Context) error { return nil }

func (e *captureExporter) Spans() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(e.spans))
	copy(out, e.spans)
	return out
}

func TestOTelRecorder_EmitsThreeSpans(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)

	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	msg := &JobCompleted{
		JobMessageBase: JobMessageBase{
			RunnerRequestID:    1,
			WorkflowRunID:      99999,
			JobID:              "42",
			JobDisplayName:     "build",
			OwnerName:          "acme",
			RepositoryName:     "widgets",
			JobWorkflowRef:     "acme/widgets/.github/workflows/ci.yml@refs/heads/main",
			QueueTime:          now,
			ScaleSetAssignTime: now.Add(10 * time.Second),
			RunnerAssignTime:   now.Add(40 * time.Second),
			FinishTime:         now.Add(5 * time.Minute),
		},
		Result:     "success",
		RunnerID:   7,
		RunnerName: "runner-abc-xyz",
	}

	rec.RecordJobCompleted(msg)

	spans := exp.Spans()
	require.Len(t, spans, 3)

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}

	// All spans share the same trace ID
	expectedTraceID := githubapi.NewTraceID(99999, 1)
	for _, s := range spans {
		assert.Equal(t, expectedTraceID, s.SpanContext().TraceID())
	}

	// All spans are children of the job span
	expectedParent := githubapi.NewSpanID(42)
	for _, s := range spans {
		assert.Equal(t, expectedParent, s.Parent().SpanID())
	}

	// runner.queue
	q := byName["runner.queue"]
	require.NotNil(t, q)
	assert.Equal(t, now, q.StartTime())
	assert.Equal(t, now.Add(10*time.Second), q.EndTime())
	assertAttr(t, q, "type", "runner.queue")

	// runner.startup
	s := byName["runner.startup"]
	require.NotNil(t, s)
	assert.Equal(t, now.Add(10*time.Second), s.StartTime())
	assert.Equal(t, now.Add(40*time.Second), s.EndTime())
	assertAttr(t, s, "type", "runner.startup")

	// runner.execution
	e := byName["runner.execution"]
	require.NotNil(t, e)
	assert.Equal(t, now.Add(40*time.Second), e.StartTime())
	assert.Equal(t, now.Add(5*time.Minute), e.EndTime())
	assertAttr(t, e, "type", "runner.execution")
	assertAttr(t, e, "github.conclusion", "success")
}

func TestOTelRecorder_SkipsMissingTimestamps(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)

	now := time.Now()
	msg := &JobCompleted{
		JobMessageBase: JobMessageBase{
			WorkflowRunID:    12345,
			JobID:            "1",
			RunnerAssignTime: now,
			FinishTime:       now.Add(time.Minute),
		},
		Result: "success",
	}

	rec.RecordJobCompleted(msg)

	spans := exp.Spans()
	require.Len(t, spans, 1, "only runner.execution should be emitted when queue/startup timestamps are zero")
	assert.Equal(t, "runner.execution", spans[0].Name())
}

func TestOTelRecorder_CommonAttributes(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)

	now := time.Now()
	msg := &JobCompleted{
		JobMessageBase: JobMessageBase{
			WorkflowRunID:    55555,
			JobID:            "100",
			JobDisplayName:   "test-suite",
			OwnerName:        "org",
			RepositoryName:   "repo",
			JobWorkflowRef:   "org/repo/.github/workflows/test.yml@main",
			RunnerAssignTime: now,
			FinishTime:       now.Add(2 * time.Minute),
		},
		Result:     "failure",
		RunnerID:   3,
		RunnerName: "runner-3",
	}

	rec.RecordJobCompleted(msg)

	spans := exp.Spans()
	require.Len(t, spans, 1)
	s := spans[0]

	assertAttr(t, s, "github.job_name", "test-suite")
	assertAttr(t, s, "github.repository", "org/repo")
	assertAttr(t, s, "github.runner_name", "runner-3")
	assertAttr(t, s, "github.conclusion", "failure")
}

func TestOTelRecorder_StringJobID(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)

	now := time.Now()
	msg := &JobCompleted{
		JobMessageBase: JobMessageBase{
			WorkflowRunID:    1,
			JobID:            "not-a-number",
			RunnerAssignTime: now,
			FinishTime:       now.Add(time.Second),
		},
		Result: "success",
	}

	rec.RecordJobCompleted(msg)

	spans := exp.Spans()
	require.Len(t, spans, 1)
	assert.True(t, spans[0].Parent().SpanID().IsValid())
}

func TestOTelRecorder_SetRunAttempt(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)
	rec.SetRunAttempt(3)

	now := time.Now()
	msg := &JobCompleted{
		JobMessageBase: JobMessageBase{
			WorkflowRunID:    77777,
			JobID:            "1",
			RunnerAssignTime: now,
			FinishTime:       now.Add(time.Second),
		},
		Result: "success",
	}

	rec.RecordJobCompleted(msg)

	spans := exp.Spans()
	require.Len(t, spans, 1)

	expectedTraceID := githubapi.NewTraceID(77777, 3)
	assert.Equal(t, expectedTraceID, spans[0].SpanContext().TraceID())
}

func TestOTelRecorder_JobStartedIsNoOp(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)

	rec.RecordJobStarted(&JobStarted{})

	assert.Empty(t, exp.Spans())
}

func TestOTelRecorder_StatisticsIsNoOp(t *testing.T) {
	exp := &captureExporter{}
	rec := NewOTelRecorder(exp)

	rec.RecordStatistics(&RunnerScaleSetStatistic{TotalRunningJobs: 5})

	assert.Empty(t, exp.Spans())
}

func assertAttr(t *testing.T, span sdktrace.ReadOnlySpan, key, expected string) {
	t.Helper()
	for _, a := range span.Attributes() {
		if string(a.Key) == key {
			assert.Equal(t, expected, a.Value.AsString(), "attribute %q", key)
			return
		}
	}
	t.Errorf("attribute %q not found on span %q", key, span.Name())
}
