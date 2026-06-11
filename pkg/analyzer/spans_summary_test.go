package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// spanInputSpans builds a workflow → 2 jobs → 2 steps trace the way a trace
// file or OTLP receiver would deliver it: no GitHub URLResults exist.
func spanInputSpans() []tracetest.SpanStub {
	tid := githubapi.NewTraceID(7, 1)
	sc := func(id int64) trace.SpanContext {
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(id), TraceFlags: trace.FlagsSampled,
		})
	}
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	wf, jobA, jobB := sc(1), sc(2), sc(3)
	return []tracetest.SpanStub{
		{
			Name: "Workflow: CI", SpanContext: wf,
			StartTime: base, EndTime: base.Add(20 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "workflow"),
				attribute.String("github.conclusion", "success"),
			},
		},
		{
			Name: "build", SpanContext: jobA, Parent: wf,
			StartTime: base, EndTime: base.Add(10 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "job"),
				attribute.String("github.conclusion", "success"),
			},
		},
		{
			Name: "test", SpanContext: jobB, Parent: wf,
			StartTime: base.Add(10 * time.Minute), EndTime: base.Add(20 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "job"),
				attribute.String("github.conclusion", "failure"),
			},
		},
		{
			Name: "checkout", SpanContext: sc(4), Parent: jobA,
			StartTime: base, EndTime: base.Add(time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "step"),
				attribute.String("github.conclusion", "success"),
			},
		},
		{
			Name: "compile", SpanContext: sc(5), Parent: jobA,
			StartTime: base.Add(time.Minute), EndTime: base.Add(9 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "step"),
				attribute.String("github.conclusion", "success"),
			},
		},
	}
}

func TestCombinedMetricsFromSpans(t *testing.T) {
	t.Parallel()
	builder := &SpanBuilder{}
	for _, stub := range spanInputSpans() {
		builder.Add(stub)
	}

	cm := CombinedMetricsFromSpans(builder.Spans(), enrichment.DefaultEnricher())

	assert.Equal(t, 1, cm.TotalRuns, "one workflow root span = one run")
	assert.Equal(t, 2, cm.TotalJobs)
	assert.Equal(t, 2, cm.TotalSteps)
	assert.Equal(t, "100.0", cm.SuccessRate, "the workflow succeeded")
	assert.Equal(t, "50.0", cm.JobSuccessRate, "one of two jobs failed")
	assert.Equal(t, 1, cm.MaxConcurrency, "jobs are back-to-back")
}

func TestCombinedMetricsFromSpansEmpty(t *testing.T) {
	t.Parallel()
	cm := CombinedMetricsFromSpans(nil, enrichment.DefaultEnricher())
	assert.Equal(t, 0, cm.TotalRuns)
	assert.Equal(t, "0.0", cm.SuccessRate)
}

func TestSpansWallCompute(t *testing.T) {
	t.Parallel()
	builder := &SpanBuilder{}
	for _, stub := range spanInputSpans() {
		builder.Add(stub)
	}

	wallMs, computeMs := SpansWallCompute(builder.Spans(), enrichment.DefaultEnricher())

	assert.Equal(t, int64(20*60*1000), wallMs, "wall = earliest start to latest end")
	assert.Equal(t, int64(20*60*1000), computeMs, "compute = sum of job durations (10m + 10m)")
}

func TestSpansWallComputeRootsOnly(t *testing.T) {
	t.Parallel()
	// A flat trace with only root spans: compute falls back to root durations.
	tid := githubapi.NewTraceID(8, 1)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	builder := &SpanBuilder{}
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: solo",
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(1), TraceFlags: trace.FlagsSampled,
		}),
		StartTime:  base,
		EndTime:    base.Add(5 * time.Minute),
		Attributes: []attribute.KeyValue{attribute.String("type", "workflow")},
	})

	wallMs, computeMs := SpansWallCompute(builder.Spans(), enrichment.DefaultEnricher())
	assert.Equal(t, int64(5*60*1000), wallMs)
	assert.Equal(t, int64(5*60*1000), computeMs, "no job spans: fall back to root durations")
}

func TestCombinedMetricsFromSpansUntypedTrace(t *testing.T) {
	t.Parallel()
	// Chrome-converted traces carry no "type" attributes: parentless spans
	// must still count as runs, with their children as jobs.
	tid := githubapi.NewTraceID(9, 1)
	sc := func(id int64) trace.SpanContext {
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(id), TraceFlags: trace.FlagsSampled,
		})
	}
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	root1, root2 := sc(1), sc(2)
	builder := &SpanBuilder{}
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: Test Linux", SpanContext: root1,
		StartTime: base, EndTime: base.Add(7 * time.Minute),
	})
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: Test macOS", SpanContext: root2,
		StartTime: base, EndTime: base.Add(14 * time.Minute),
	})
	builder.Add(tracetest.SpanStub{
		Name: "compile", SpanContext: sc(3), Parent: root1,
		StartTime: base, EndTime: base.Add(6 * time.Minute),
	})

	cm := CombinedMetricsFromSpans(builder.Spans(), enrichment.DefaultEnricher())
	assert.Equal(t, 2, cm.TotalRuns, "parentless spans count as runs")
	assert.Equal(t, 1, cm.TotalJobs, "child spans count as jobs")

	wallMs, computeMs := SpansWallCompute(builder.Spans(), enrichment.DefaultEnricher())
	assert.Equal(t, int64(14*60*1000), wallMs)
	assert.Equal(t, int64(6*60*1000), computeMs, "compute = child span durations, not roots")
}
