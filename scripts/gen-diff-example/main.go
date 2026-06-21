// Command gen-diff-example writes two OTLP trace files representing two
// back-to-back commits' CI runs of a matrix build. Diffing them with
//
//	ote diff examples/diff/before.json examples/diff/after.json
//
// exercises every dimension of the semantic trace diff: a regressed step, a
// status flip, a matrix-dimension shift, an added job, and a removed job.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	otelexport "github.com/stefanpenner/otel-explorer/pkg/export/otel"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var base = time.Date(2026, 6, 20, 9, 0, 0, 0, time.UTC)

// builder accumulates spans for one trace, handing out unique span IDs.
type builder struct {
	b       *analyzer.SpanBuilder
	traceID oteltrace.TraceID
	nextID  int64
}

func newBuilder(runID int64) *builder {
	return &builder{b: &analyzer.SpanBuilder{}, traceID: githubapi.NewTraceID(runID, 1), nextID: 1}
}

func (g *builder) sc() oteltrace.SpanContext {
	g.nextID++
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: g.traceID, SpanID: githubapi.NewSpanID(g.nextID), TraceFlags: oteltrace.FlagsSampled,
	})
}

func (g *builder) add(name, typ, conclusion string, parent oteltrace.SpanContext, start, end time.Time) oteltrace.SpanContext {
	self := g.sc()
	g.b.Add(tracetest.SpanStub{
		Name:        name,
		SpanContext: self,
		Parent:      parent,
		StartTime:   start,
		EndTime:     end,
		Attributes: []attribute.KeyValue{
			attribute.String("type", typ),
			attribute.String("github.conclusion", conclusion),
		},
	})
	return self
}

// job adds a job span plus a sequence of back-to-back step spans, and returns
// the job's span context. steps is a list of {name, seconds, conclusion}.
func (g *builder) job(wf oteltrace.SpanContext, name, conclusion string, startOffset time.Duration, steps []step) {
	start := base.Add(startOffset)
	var total time.Duration
	for _, s := range steps {
		total += s.dur
	}
	jobSC := g.add(name, "job", conclusion, wf, start, start.Add(total))
	cursor := start
	for _, s := range steps {
		g.add(s.name, "step", s.conclusion, jobSC, cursor, cursor.Add(s.dur))
		cursor = cursor.Add(s.dur)
	}
}

type step struct {
	name       string
	dur        time.Duration
	conclusion string
}

func write(path string, g *builder) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	exp, err := otelexport.NewStdoutExporter(f)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err := exp.Export(ctx, g.b.Spans()); err != nil {
		return err
	}
	return exp.Finish(ctx)
}

func main() {
	beforePath, afterPath := "examples/diff/before.json", "examples/diff/after.json"
	if len(os.Args) == 3 {
		beforePath, afterPath = os.Args[1], os.Args[2]
	}

	// --- before: commit A, all green ---
	b := newBuilder(5001)
	wfB := b.add("CI", "workflow", "success", oteltrace.SpanContext{}, base, base.Add(8*time.Minute))
	b.job(wfB, "build", "success", 0, []step{
		{"Checkout", 10 * time.Second, "success"},
		{"Compile assets", 2*time.Minute + 40*time.Second, "success"},
		{"Upload", 10 * time.Second, "success"},
	})
	for _, ruby := range []string{"3.2", "3.3"} {
		b.job(wfB, "test (ruby "+ruby+")", "success", 0, []step{
			{"Checkout", 10 * time.Second, "success"},
			{"bundle install", 1 * time.Minute, "success"},
			{"Run specs", 6*time.Minute + 40*time.Second, "success"},
		})
	}
	b.job(wfB, "lint", "success", 0, []step{
		{"Checkout", 10 * time.Second, "success"},
		{"rubocop", 1*time.Minute + 50*time.Second, "success"},
	})

	// --- after: commit B, slower & failing ---
	a := newBuilder(5002)
	wfA := a.add("CI", "workflow", "failure", oteltrace.SpanContext{}, base, base.Add(11*time.Minute))
	a.job(wfA, "build", "success", 0, []step{ // unchanged
		{"Checkout", 10 * time.Second, "success"},
		{"Compile assets", 2*time.Minute + 40*time.Second, "success"},
		{"Upload", 10 * time.Second, "success"},
	})
	a.job(wfA, "test (ruby 3.3)", "failure", 0, []step{ // regressed + failed
		{"Checkout", 10 * time.Second, "success"},
		{"bundle install", 1 * time.Minute, "success"},
		{"Run specs", 9*time.Minute + 40*time.Second, "failure"},
	})
	a.job(wfA, "test (ruby 3.4)", "success", 0, []step{ // matrix shifted: 3.2 → 3.4
		{"Checkout", 10 * time.Second, "success"},
		{"bundle install", 1 * time.Minute, "success"},
		{"Run specs", 6*time.Minute + 40*time.Second, "success"},
	})
	a.job(wfA, "typecheck", "success", 0, []step{ // new job
		{"Checkout", 10 * time.Second, "success"},
		{"sorbet", 1*time.Minute + 20*time.Second, "success"},
	})
	// lint removed entirely.

	if err := write(beforePath, b); err != nil {
		fmt.Fprintln(os.Stderr, "write before:", err)
		os.Exit(1)
	}
	if err := write(afterPath, a); err != nil {
		fmt.Fprintln(os.Stderr, "write after:", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s and %s\n", beforePath, afterPath)
}
