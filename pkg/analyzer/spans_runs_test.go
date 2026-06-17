package analyzer

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sc(traceHex string, spanByte byte) trace.SpanContext {
	var tid trace.TraceID
	copy(tid[:], traceHex)
	sid := trace.SpanID{spanByte}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid})
}

func TestRunsFromSpans(t *testing.T) {
	base := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	runSC := sc("trace-0000000000001", 1)
	buildSC := sc("trace-0000000000001", 2)
	testSC := sc("trace-0000000000001", 3)
	stepSC := sc("trace-0000000000001", 4)

	stubs := tracetest.SpanStubs{
		{
			Name: "CI", SpanContext: runSC,
			StartTime: base, EndTime: base.Add(5 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "workflow"),
				attribute.String("github.conclusion", "success"),
				attribute.String("github.url", "https://run"),
				attribute.Int64("github.run_id", 999),
			},
		},
		{
			Name: "build (required)", SpanContext: buildSC, Parent: runSC,
			StartTime: base, EndTime: base.Add(3 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "job"),
				attribute.String("cicd.pipeline.task.name", "build"),
				attribute.String("github.conclusion", "success"),
				attribute.String("github.status", "completed"),
				attribute.String("github.url", "https://build"),
				attribute.Bool("github.is_required", true),
			},
		},
		{
			Name: "test", SpanContext: testSC, Parent: runSC,
			StartTime: base.Add(1 * time.Minute), EndTime: base.Add(2 * time.Minute),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "job"),
				attribute.String("cicd.pipeline.task.name", "test"),
				attribute.String("github.conclusion", "failure"),
			},
		},
		{
			Name: "checkout", SpanContext: stepSC, Parent: buildSC,
			StartTime: base, EndTime: base.Add(30 * time.Second),
			Attributes: []attribute.KeyValue{
				attribute.String("type", "step"),
				attribute.String("cicd.pipeline.task.name", "checkout"),
			},
		},
		{ // marker span — must be ignored
			Name: "merged", SpanContext: sc("trace-0000000000001", 5), Parent: runSC,
			StartTime: base, EndTime: base,
			Attributes: []attribute.KeyValue{
				attribute.String("type", "marker"),
				attribute.String("github.event_type", "merged"),
			},
		},
	}

	enricher := enrichment.NewChainEnricher(&enrichment.GHAEnricher{})
	runs := RunsFromSpans(stubs.Snapshots(), enricher)

	require.Len(t, runs, 1, "one workflow run")
	run := runs[0]
	assert.Equal(t, "999", run.Identifier)
	assert.Equal(t, "CI", run.Name)
	assert.Equal(t, "https://run", run.URL)
	assert.Equal(t, "success", run.Conclusion)

	require.Len(t, run.Jobs, 2, "marker excluded; two jobs")
	build := run.Jobs[0]
	assert.Equal(t, "build", build.Name, "prefers cicd.pipeline.task.name over span name")
	assert.True(t, build.Required)
	assert.Equal(t, "success", build.Conclusion)
	assert.Equal(t, int64(3*60*1000), build.EndMs-build.StartMs)
	require.Len(t, build.Steps, 1)
	assert.Equal(t, "checkout", build.Steps[0].Name)
	assert.Equal(t, int64(30*1000), build.Steps[0].DurationMs)

	assert.Equal(t, "failure", run.Jobs[1].Conclusion)
	assert.Empty(t, run.Jobs[1].Steps)
}

func TestRunsFromSpans_OrphanJobsGetFallbackRun(t *testing.T) {
	base := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	// A job span with a parent that isn't present → fallback run.
	stubs := tracetest.SpanStubs{{
		Name: "lint", SpanContext: sc("t", 2), Parent: sc("t", 99),
		StartTime: base, EndTime: base.Add(time.Minute),
		Attributes: []attribute.KeyValue{
			attribute.String("type", "job"),
			attribute.String("cicd.pipeline.task.name", "lint"),
			attribute.String("github.conclusion", "success"),
		},
	}}

	runs := RunsFromSpans(stubs.Snapshots(), enrichment.NewChainEnricher(&enrichment.GHAEnricher{}))
	require.Len(t, runs, 1)
	assert.Equal(t, "runs", runs[0].Name)
	require.Len(t, runs[0].Jobs, 1)
	assert.Equal(t, "lint", runs[0].Jobs[0].Name)
}
