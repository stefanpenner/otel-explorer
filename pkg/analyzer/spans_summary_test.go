package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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
	assert.Equal(t, "", cm.SuccessRate)
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

func TestCombinedMetricsFromSpansUnknownOutcomes(t *testing.T) {
	t.Parallel()
	// Untyped spans have no conclusion attributes: rates must be marked
	// unknown (empty string) rather than rendered as a misleading "0.0%".
	tid := githubapi.NewTraceID(10, 1)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	builder := &SpanBuilder{}
	root := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: githubapi.NewSpanID(1), TraceFlags: trace.FlagsSampled,
	})
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: untyped", SpanContext: root,
		StartTime: base, EndTime: base.Add(time.Minute),
	})
	builder.Add(tracetest.SpanStub{
		Name: "child",
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(2), TraceFlags: trace.FlagsSampled,
		}),
		Parent:    root,
		StartTime: base, EndTime: base.Add(30 * time.Second),
	})

	cm := CombinedMetricsFromSpans(builder.Spans(), enrichment.DefaultEnricher())
	assert.Equal(t, "", cm.SuccessRate, "unknown outcomes must not read as 0%")
	assert.Equal(t, "", cm.JobSuccessRate, "unknown job outcomes must not read as 0%")
}

func TestCombinedMetricsFromSpansMixedKnownUnknown(t *testing.T) {
	t.Parallel()
	// Rates must be computed over KNOWN outcomes only: an unknown-outcome
	// run must not count as a failure, and an unknown-outcome job must not
	// count as a success.
	tid := githubapi.NewTraceID(11, 1)
	sc := func(id int64) trace.SpanContext {
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(id), TraceFlags: trace.FlagsSampled,
		})
	}
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	known, untyped := sc(1), sc(2)
	builder := &SpanBuilder{}
	// Run 1: known success.
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: known", SpanContext: known,
		StartTime: base, EndTime: base.Add(time.Minute),
		Attributes: []attribute.KeyValue{
			attribute.String("type", "workflow"),
			attribute.String("github.conclusion", "success"),
		},
	})
	// Run 2: untyped root, unknown outcome.
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: untyped", SpanContext: untyped,
		StartTime: base, EndTime: base.Add(time.Minute),
	})
	// Job 1 (under run 1): known FAILURE.
	builder.Add(tracetest.SpanStub{
		Name: "build", SpanContext: sc(3), Parent: known,
		StartTime: base, EndTime: base.Add(30 * time.Second),
		Attributes: []attribute.KeyValue{
			attribute.String("type", "job"),
			attribute.String("github.conclusion", "failure"),
		},
	})
	// Job 2 (under run 2): unknown outcome.
	builder.Add(tracetest.SpanStub{
		Name: "mystery", SpanContext: sc(4), Parent: untyped,
		StartTime: base, EndTime: base.Add(30 * time.Second),
	})

	cm := CombinedMetricsFromSpans(builder.Spans(), enrichment.DefaultEnricher())
	assert.Equal(t, "100.0", cm.SuccessRate, "the only known run succeeded; unknown must not dilute")
	assert.Equal(t, "0.0", cm.JobSuccessRate, "the only known job failed; unknown must not inflate")
}

func TestCombinedMetricsFromSpansIntAttrOutcome(t *testing.T) {
	t.Parallel()
	// Int-typed attributes must reach the enricher: a gRPC span whose only
	// failure signal is the int attr rpc.grpc.status_code=5 is a failed job.
	// (AsString() returns "" for ints; Emit() is required.)
	tid := githubapi.NewTraceID(12, 1)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	root := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: githubapi.NewSpanID(1), TraceFlags: trace.FlagsSampled,
	})
	builder := &SpanBuilder{}
	builder.Add(tracetest.SpanStub{
		Name: "pipeline", SpanContext: root,
		StartTime: base, EndTime: base.Add(time.Minute),
	})
	builder.Add(tracetest.SpanStub{
		Name: "Cart/Get",
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(2), TraceFlags: trace.FlagsSampled,
		}),
		Parent:    root,
		StartTime: base, EndTime: base.Add(30 * time.Second),
		Attributes: []attribute.KeyValue{
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", "Cart"),
			attribute.Int64("rpc.grpc.status_code", 5),
		},
	})

	cm := CombinedMetricsFromSpans(builder.Spans(), enrichment.DefaultEnricher())
	assert.Equal(t, "0.0", cm.JobSuccessRate, "int-typed grpc status 5 = NOT_FOUND = failed job")
}

func TestStatusAndKindReachEnrichment(t *testing.T) {
	t.Parallel()
	// A span whose ONLY failure signal is OTel status=Error must enrich as a
	// failure, and span kind must drive the kind icons — neither was
	// reaching the enrichers (only Jaeger files carried them as tags).
	tid := githubapi.NewTraceID(13, 1)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	root := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: githubapi.NewSpanID(1), TraceFlags: trace.FlagsSampled,
	})
	builder := &SpanBuilder{}
	builder.Add(tracetest.SpanStub{
		Name: "pipeline", SpanContext: root,
		StartTime: base, EndTime: base.Add(time.Minute),
	})
	builder.Add(tracetest.SpanStub{
		Name: "charge-card",
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(2), TraceFlags: trace.FlagsSampled,
		}),
		Parent:    root,
		StartTime: base, EndTime: base.Add(30 * time.Second),
		SpanKind: trace.SpanKindClient,
		Status:   sdktrace.Status{Code: codes.Error, Description: "declined"},
	})

	spans := builder.Spans()
	cm := CombinedMetricsFromSpans(spans, enrichment.DefaultEnricher())
	assert.Equal(t, "0.0", cm.JobSuccessRate, "status=Error span is a failed job")

	roots := BuildTreeFromSpans(spans, time.Time{}, time.Time{}, enrichment.DefaultEnricher())
	if assert.Len(t, roots, 1) && assert.Len(t, roots[0].Children, 1) {
		child := roots[0].Children[0]
		assert.Equal(t, "failure", child.Hints.Outcome, "OTel ERROR status must set failure outcome")
		assert.Equal(t, "⇢ ", child.Hints.Icon, "CLIENT span kind must set the client icon")
	}
}
