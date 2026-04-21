// Package e2e tests the full ARC → OTLP → otel-explorer pipeline.
package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/arc"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/receiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

// TestLevel1_RecorderToReceiver verifies the full OTLP pipeline:
// OTelRecorder → OTLP/HTTP → otel-explorer receiver → enricher
func TestLevel1_RecorderToReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start OTLP receiver on a random port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	recv := receiver.New(addr)
	go recv.Start(ctx)

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", addr))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	}, 5*time.Second, 50*time.Millisecond, "receiver did not become ready")

	// Create OTLP exporter pointed at receiver
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(addr),
		otlptracehttp.WithInsecure(),
	)
	require.NoError(t, err)

	recorder := arc.NewOTelRecorder(exporter)

	runID := int64(99999)
	jobID := int64(42)
	attempt := int64(1)
	recorder.SetRunAttempt(attempt)

	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	recorder.RecordJobCompleted(&arc.JobCompleted{
		JobMessageBase: arc.JobMessageBase{
			RunnerRequestID:    1,
			WorkflowRunID:      runID,
			JobID:              strconv.FormatInt(jobID, 10),
			JobDisplayName:     "build",
			OwnerName:          "acme",
			RepositoryName:     "widgets",
			JobWorkflowRef:     "acme/widgets/.github/workflows/ci.yml@refs/heads/main",
			EventName:          "push",
			QueueTime:          now,
			ScaleSetAssignTime: now.Add(5 * time.Second),
			RunnerAssignTime:   now.Add(15 * time.Second),
			FinishTime:         now.Add(3 * time.Minute),
		},
		Result:     "succeeded",
		RunnerID:   7,
		RunnerName: "runner-abc",
	})

	// Flush exporter
	require.NoError(t, exporter.Shutdown(ctx))

	// Wait for receiver to accumulate spans (OTLP HTTP is async)
	var spans []sdktrace.ReadOnlySpan
	require.Eventually(t, func() bool {
		spans = recv.Spans()
		return len(spans) == 3
	}, 5*time.Second, 50*time.Millisecond, "expected 3 spans, got %d", recv.SpanCount())

	expectedTraceID := githubapi.NewTraceID(runID, attempt)
	expectedParentSpanID := githubapi.NewSpanID(jobID)

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}

	for name, s := range byName {
		assert.Equal(t, expectedTraceID, s.SpanContext().TraceID(), "trace ID mismatch on %s", name)
		assert.Equal(t, expectedParentSpanID, s.Parent().SpanID(), "parent span ID mismatch on %s", name)
	}

	// Protobuf roundtrip loses timezone (stores as unix nanos), so compare unix times
	q := byName["runner.queue"]
	require.NotNil(t, q)
	assert.Equal(t, now.UnixNano(), q.StartTime().UnixNano())
	assert.Equal(t, now.Add(5*time.Second).UnixNano(), q.EndTime().UnixNano())
	assertAttr(t, q, "type", "runner.queue")

	s := byName["runner.startup"]
	require.NotNil(t, s)
	assert.Equal(t, now.Add(5*time.Second).UnixNano(), s.StartTime().UnixNano())
	assert.Equal(t, now.Add(15*time.Second).UnixNano(), s.EndTime().UnixNano())
	assertAttr(t, s, "type", "runner.startup")

	e := byName["runner.execution"]
	require.NotNil(t, e)
	assert.Equal(t, now.Add(15*time.Second).UnixNano(), e.StartTime().UnixNano())
	assert.Equal(t, now.Add(3*time.Minute).UnixNano(), e.EndTime().UnixNano())
	assertAttr(t, e, "type", "runner.execution")
	assertAttr(t, e, "github.conclusion", "success")
	assertAttr(t, e, "cicd.pipeline.task.run.result", "success")
	assertAttr(t, e, "github.repository", "acme/widgets")
	assertAttr(t, e, "github.runner_name", "runner-abc")

	// Verify enricher recognizes all span types
	enricher := enrichment.DefaultEnricher()
	for _, span := range spans {
		attrs := map[string]string{}
		for _, a := range span.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}
		hints := enricher.Enrich(span.Name(), attrs, false)
		assert.NotEmpty(t, hints.Category,
			"enricher did not recognize span %q (type=%s)", span.Name(), attrs["type"])
		t.Logf("  %s → category=%s icon=%s color=%s", span.Name(), hints.Category, hints.Icon, hints.Color)
	}
}

// TestLevel1_IDCorrelation verifies that ARC's deterministic IDs match
// otel-explorer's, ensuring spans merge into the same trace.
func TestLevel1_IDCorrelation(t *testing.T) {
	runID := int64(24741863790) // real run from stefanpenner/otel-explorer
	jobID := int64(72383376636) // real job: "required-check"
	attempt := int64(1)

	expectedTraceID := githubapi.NewTraceID(runID, attempt)
	expectedJobSpanID := githubapi.NewSpanID(jobID)

	exp := &captureExporter{}
	recorder := arc.NewOTelRecorder(exp)
	recorder.SetRunAttempt(attempt)

	now := time.Now()
	recorder.RecordJobCompleted(&arc.JobCompleted{
		JobMessageBase: arc.JobMessageBase{
			WorkflowRunID:      runID,
			JobID:              strconv.FormatInt(jobID, 10),
			QueueTime:          now.Add(-10 * time.Second),
			ScaleSetAssignTime: now.Add(-5 * time.Second),
			RunnerAssignTime:   now,
			FinishTime:         now.Add(time.Minute),
		},
		Result: "Succeeded",
	})

	spans := exp.Spans()
	require.Len(t, spans, 3)

	for _, s := range spans {
		assert.Equal(t, expectedTraceID, s.SpanContext().TraceID(),
			"ARC span %q trace ID must match otel-explorer's for run %d", s.Name(), runID)
		assert.Equal(t, expectedJobSpanID, s.Parent().SpanID(),
			"ARC span %q parent must be otel-explorer's job span for job %d", s.Name(), jobID)
	}
}

// TestLevel1_MultipleJobs verifies that spans for multiple jobs in the
// same workflow all share the same trace ID but have distinct parent span IDs.
func TestLevel1_MultipleJobs(t *testing.T) {
	exp := &captureExporter{}
	recorder := arc.NewOTelRecorder(exp)

	runID := int64(12345)
	now := time.Now()
	jobs := []struct {
		id   int64
		name string
	}{
		{100, "build"},
		{200, "test"},
		{300, "deploy"},
	}

	for _, j := range jobs {
		recorder.RecordJobCompleted(&arc.JobCompleted{
			JobMessageBase: arc.JobMessageBase{
				WorkflowRunID:      runID,
				JobID:              strconv.FormatInt(j.id, 10),
				JobDisplayName:     j.name,
				QueueTime:          now,
				ScaleSetAssignTime: now.Add(time.Second),
				RunnerAssignTime:   now.Add(2 * time.Second),
				FinishTime:         now.Add(time.Minute),
			},
			Result: "Succeeded",
		})
	}

	spans := exp.Spans()
	require.Len(t, spans, 9, "3 spans per job × 3 jobs")

	expectedTraceID := githubapi.NewTraceID(runID, 1)
	parentIDs := map[string]bool{}

	for _, s := range spans {
		assert.Equal(t, expectedTraceID, s.SpanContext().TraceID(), "all spans share trace ID")
		parentIDs[s.Parent().SpanID().String()] = true
	}

	assert.Len(t, parentIDs, 3, "3 distinct parent span IDs (one per job)")
}

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

func assertAttr(t *testing.T, span sdktrace.ReadOnlySpan, key, expected string) {
	t.Helper()
	for _, a := range span.Attributes() {
		if string(a.Key) == key {
			assert.Equal(t, expected, a.Value.AsString(), "attribute %q on span %q", key, span.Name())
			return
		}
	}
	t.Errorf("attribute %q not found on span %q", key, span.Name())
}
