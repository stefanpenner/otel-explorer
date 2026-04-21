package arc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// OTelRecorder implements MetricsRecorder by emitting OpenTelemetry
// spans for each job lifecycle. On completion, three child spans are
// created under the job span (using deterministic IDs that match
// otel-explorer's GitHub Actions trace):
//
//   - runner.queue:     QueueTime → ScaleSetAssignTime
//   - runner.startup:   ScaleSetAssignTime → RunnerAssignTime
//   - runner.execution: RunnerAssignTime → FinishTime
type OTelRecorder struct {
	mu       sync.Mutex
	exporter sdktrace.SpanExporter

	// runAttempt is used for trace ID generation. ARC messages don't
	// include run_attempt, so this defaults to 1. Set it if your
	// setup can determine the attempt number.
	runAttempt int64
}

// NewOTelRecorder creates a recorder that exports spans via the given exporter.
func NewOTelRecorder(exporter sdktrace.SpanExporter) *OTelRecorder {
	return &OTelRecorder{
		exporter:   exporter,
		runAttempt: 1,
	}
}

// SetRunAttempt overrides the default run attempt (1) used for trace
// ID generation. Call this if your setup can determine the attempt.
func (r *OTelRecorder) SetRunAttempt(attempt int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runAttempt = attempt
}

func (r *OTelRecorder) RecordJobStarted(_ *JobStarted) {
	// All timestamps needed for span emission are available on
	// JobCompleted, so we defer span creation until then.
}

func (r *OTelRecorder) RecordJobCompleted(msg *JobCompleted) {
	r.mu.Lock()
	attempt := r.runAttempt
	r.mu.Unlock()

	spans := r.buildSpans(msg, attempt)
	if len(spans) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.exporter.ExportSpans(ctx, spans)
}

func (r *OTelRecorder) RecordStatistics(_ *RunnerScaleSetStatistic) {}
func (r *OTelRecorder) RecordDesiredRunners(_ int)                  {}

// Shutdown flushes and shuts down the exporter.
func (r *OTelRecorder) Shutdown(ctx context.Context) error {
	return r.exporter.Shutdown(ctx)
}

func (r *OTelRecorder) buildSpans(msg *JobCompleted, attempt int64) []sdktrace.ReadOnlySpan {
	traceID := githubapi.NewTraceID(msg.WorkflowRunID, attempt)
	parentSpanID := jobSpanID(msg.JobID)
	parentSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     parentSpanID,
		TraceFlags: trace.FlagsSampled,
	})

	commonAttrs := []attribute.KeyValue{
		attribute.Int64("github.run_id", msg.WorkflowRunID),
		attribute.String("github.job_id", msg.JobID),
		attribute.String("github.job_name", msg.JobDisplayName),
		attribute.String("github.repository", msg.OwnerName+"/"+msg.RepositoryName),
		attribute.String("github.runner_name", msg.RunnerName),
		attribute.Int("github.runner_id", msg.RunnerID),
		attribute.String("github.workflow_ref", msg.JobWorkflowRef),
	}

	var stubs tracetest.SpanStubs

	if !msg.QueueTime.IsZero() && !msg.ScaleSetAssignTime.IsZero() {
		stubs = append(stubs, makeStub(
			traceID, parentSC, "runner.queue",
			msg.QueueTime, msg.ScaleSetAssignTime,
			append(commonAttrs, attribute.String("type", "runner.queue")),
		))
	}

	if !msg.ScaleSetAssignTime.IsZero() && !msg.RunnerAssignTime.IsZero() {
		stubs = append(stubs, makeStub(
			traceID, parentSC, "runner.startup",
			msg.ScaleSetAssignTime, msg.RunnerAssignTime,
			append(commonAttrs, attribute.String("type", "runner.startup")),
		))
	}

	if !msg.RunnerAssignTime.IsZero() && !msg.FinishTime.IsZero() {
		stubs = append(stubs, makeStub(
			traceID, parentSC, "runner.execution",
			msg.RunnerAssignTime, msg.FinishTime,
			append(commonAttrs,
				attribute.String("type", "runner.execution"),
				attribute.String("github.conclusion", msg.Result),
			),
		))
	}

	return stubs.Snapshots()
}

func makeStub(
	traceID trace.TraceID,
	parentSC trace.SpanContext,
	name string,
	start, end time.Time,
	attrs []attribute.KeyValue,
) tracetest.SpanStub {
	spanID := githubapi.NewSpanIDFromString(
		fmt.Sprintf("%s-%s", name, parentSC.SpanID()),
	)
	return tracetest.SpanStub{
		Name: name,
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		}),
		Parent:     parentSC,
		StartTime:  start,
		EndTime:    end,
		Attributes: attrs,
	}
}

func jobSpanID(jobID string) trace.SpanID {
	if id, err := strconv.ParseInt(jobID, 10, 64); err == nil {
		return githubapi.NewSpanID(id)
	}
	return githubapi.NewSpanIDFromString(jobID)
}
