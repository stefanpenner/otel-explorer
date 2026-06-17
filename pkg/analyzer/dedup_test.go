package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"go.opentelemetry.io/otel/attribute"
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
	jobName, stepName := "build", "Run tests"

	tid := githubapi.NewTraceID(runID, attempt)
	jobSID := githubapi.NewJobSpanID(runID, attempt, jobName)
	stepSID := githubapi.NewStepSpanID(runID, attempt, jobName, stepName)

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

	roots := BuildTreeFromSpans(builder.Spans(), time.Time{}, time.Time{}, &enrichment.GHAEnricher{})

	// Collect every node in the tree keyed by span ID.
	var all []*TreeNode
	var walk func(n *TreeNode)
	walk = func(n *TreeNode) {
		all = append(all, n)
		for _, c := range n.Children {
			walk(c)
		}
	}
	for _, r := range roots {
		walk(r)
	}

	var matches []*TreeNode
	for _, n := range all {
		if n.SpanID == stepSID.String() {
			matches = append(matches, n)
		}
	}

	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 step node after dedup, got %d", len(matches))
	}
	if got := matches[0].Attrs["source"]; got != "runner" {
		t.Errorf("expected the runner span to win dedup, got source=%q", got)
	}
	if !matches[0].EndTime.Equal(runnerEnd) {
		t.Errorf("expected runner's precise EndTime to survive, got %v", matches[0].EndTime)
	}
}
