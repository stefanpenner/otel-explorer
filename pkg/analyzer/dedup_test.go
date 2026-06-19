package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestRunnerSpanDedup verifies that when the same step is present both from the
// GitHub API reconstruction and natively from the runner (identical trace+span
// IDs via the shared contract), only the runner span survives — it has
// sub-second precision timing.
func TestRunnerSpanDedup(t *testing.T) {
	var runID int64 = 99999
	var attempt int64 = 1
	jobName, stepName, stepNumber := "build", "Run tests", 3

	tid := githubapi.NewTraceID(runID, attempt)
	jobSID := githubapi.NewJobSpanID(runID, attempt, jobName)
	stepSID := githubapi.NewStepSpanID(runID, attempt, jobName, stepNumber, stepName)

	parentSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: jobSID, TraceFlags: trace.FlagsSampled,
	})
	stepSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: stepSID, TraceFlags: trace.FlagsSampled,
	})

	base := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

	builder := &SpanBuilder{}
	// API-reconstructed step: whole-second precision, no source attr.
	builder.Add(tracetest.SpanStub{
		Name:        stepName,
		SpanContext: stepSC,
		Parent:      parentSC,
		StartTime:   base,
		EndTime:     base.Add(13 * time.Second), // coarse
		Attributes: []attribute.KeyValue{
			attribute.String("type", "step"),
			attribute.String("cicd.pipeline.task.name", stepName),
		},
	})
	// Runner-native step: sub-second precision, source=runner.
	runnerEnd := base.Add(12*time.Second + 310*time.Millisecond)
	builder.Add(tracetest.SpanStub{
		Name:        stepName,
		SpanContext: stepSC,
		Parent:      parentSC,
		StartTime:   base,
		EndTime:     runnerEnd, // precise
		Attributes: []attribute.KeyValue{
			attribute.String("type", "step"),
			attribute.String("source", "runner"),
			attribute.String("cicd.pipeline.task.name", stepName),
		},
	})

	deduped := DedupeRunnerSpans(builder.Spans())

	var matches []sdktrace.ReadOnlySpan
	for _, s := range deduped {
		if s.SpanContext().SpanID() == stepSID {
			matches = append(matches, s)
		}
	}

	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 step span after dedup, got %d", len(matches))
	}
	if !spanIsRunner(matches[0]) {
		t.Errorf("expected the runner span to win dedup")
	}
	if !matches[0].EndTime().Equal(runnerEnd) {
		t.Errorf("expected runner's precise EndTime to survive, got %v", matches[0].EndTime())
	}
}

// BuildTreeFromSpans must collapse a job present from both the API and the runner
// into a single node (regression: the TUI showed "two build sections").
func TestBuildTreeDedupsDuplicateJob(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("37912fcf8909bcb43fd643580e6b5ee1")
	base := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	scOf := func(id string) trace.SpanContext {
		sid, _ := trace.SpanIDFromHex(id)
		return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	}
	b := &SpanBuilder{}
	job := func(src string) tracetest.SpanStub {
		a := []attribute.KeyValue{attribute.String("type", "job")}
		if src != "" {
			a = append(a, attribute.String("source", src))
		}
		return tracetest.SpanStub{Name: "build", SpanContext: scOf("00000000000000aa"), StartTime: base, EndTime: base.Add(5 * time.Second), Attributes: a}
	}
	b.Add(job(""))       // API-reconstructed
	b.Add(job("runner")) // runner-emitted, same span ID

	roots := BuildTreeFromSpans(b.Spans(), time.Time{}, time.Time{}, &enrichment.GHAEnricher{})
	count := 0
	var walk func(n *TreeNode)
	walk = func(n *TreeNode) {
		if n.Name == "build" {
			count++
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	for _, r := range roots {
		walk(r)
	}
	if count != 1 {
		t.Fatalf("expected 1 build node after dedup, got %d", count)
	}
}
