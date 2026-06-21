package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// ghaTrace adds a workflow→job→step GHA trace for one run to the builder, all
// spans tagged with urlIndex so a combined set can be split back apart.
func ghaTrace(b *SpanBuilder, runID int64, urlIndex int, base time.Time, testJobDur time.Duration, testConclusion string) {
	tid := githubapi.NewTraceID(runID, 1)
	sc := func(id int64) oteltrace.SpanContext {
		return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(id), TraceFlags: oteltrace.FlagsSampled,
		})
	}
	idx := attribute.Int64("github.url_index", int64(urlIndex))
	wf := sc(1)
	b.Add(tracetest.SpanStub{
		Name: "CI", SpanContext: wf,
		StartTime: base, EndTime: base.Add(10 * time.Minute),
		Attributes: []attribute.KeyValue{attribute.String("type", "workflow"), attribute.String("github.conclusion", "success"), idx},
	})
	// build job: constant across runs
	build := sc(2)
	b.Add(tracetest.SpanStub{
		Name: "build", SpanContext: build, Parent: wf,
		StartTime: base, EndTime: base.Add(5 * time.Minute),
		Attributes: []attribute.KeyValue{attribute.String("type", "job"), attribute.String("github.conclusion", "success"), idx},
	})
	// test job: varies in duration and conclusion across runs
	test := sc(3)
	b.Add(tracetest.SpanStub{
		Name: "test", SpanContext: test, Parent: wf,
		StartTime: base, EndTime: base.Add(testJobDur),
		Attributes: []attribute.KeyValue{attribute.String("type", "job"), attribute.String("github.conclusion", testConclusion), idx},
	})
	step := sc(4)
	b.Add(tracetest.SpanStub{
		Name: "run specs", SpanContext: step, Parent: test,
		StartTime: base, EndTime: base.Add(testJobDur),
		Attributes: []attribute.KeyValue{attribute.String("type", "step"), attribute.String("github.conclusion", testConclusion), idx},
	})
}

// TestDiffEndToEndFromSpans exercises the real pipeline: GHA spans →
// BuildTreeFromSpans → split by URLIndex → DiffTraces. This is what the CLI
// `diff` subcommand does with two fetched runs.
func TestDiffEndToEndFromSpans(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	b := &SpanBuilder{}
	ghaTrace(b, 1001, 0, base, 5*time.Minute, "success") // before: test passes in 5m
	ghaTrace(b, 1002, 1, base, 8*time.Minute, "failure") // after: test fails after 8m

	roots := BuildTreeFromSpans(b.Spans(), time.Time{}, time.Time{}, enrichment.DefaultEnricher())
	before := RootsForURLIndex(roots, 0)
	after := RootsForURLIndex(roots, 1)
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("split roots: before=%d after=%d, want 1/1", len(before), len(after))
	}

	d := DiffTraces(before, after)

	test := find(d.Roots, "test")
	if test == nil {
		t.Fatal("test job missing from diff")
	}
	if !test.StatusChanged() || test.OutcomeAfter != "failure" {
		t.Errorf("test status = %s→%s, want success→failure", test.OutcomeBefore, test.OutcomeAfter)
	}
	step := find(d.Roots, "run specs")
	if step == nil || step.DurDelta != 3*time.Minute {
		t.Fatalf("run specs delta = %v, want 3m", durOf(step))
	}
	if build := find(d.Roots, "build"); build.Kind != Unchanged {
		t.Errorf("build kind = %v, want Unchanged", build.Kind)
	}
	flips := d.StatusFlips()
	if len(flips) != 2 { // test job + its run-specs step both flip
		t.Errorf("status flips = %d, want 2", len(flips))
	}
}

func durOf(d *DiffNode) time.Duration {
	if d == nil {
		return -1
	}
	return d.DurDelta
}
