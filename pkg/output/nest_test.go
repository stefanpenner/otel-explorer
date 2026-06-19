package output

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// A child span must render visually deeper than its parent — regression for the
// leaf-icon padding that made a tool span look like a sibling of its step.
func TestRenderOTelTimeline_ChildDeeperThanParent(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("37912fcf8909bcb43fd643580e6b5ee1")
	base := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	sc := func(id string) trace.SpanContext {
		sid, _ := trace.SpanIDFromHex(id)
		return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	}
	b := &analyzer.SpanBuilder{}
	b.Add(tracetest.SpanStub{Name: "build", SpanContext: sc("0000000000000001"), StartTime: base, EndTime: base.Add(9 * time.Second),
		Attributes: []attribute.KeyValue{attribute.String("type", "job")}})
	b.Add(tracetest.SpanStub{Name: "Run tests", SpanContext: sc("0000000000000002"), Parent: sc("0000000000000001"),
		StartTime: base, EndTime: base.Add(3 * time.Second), Attributes: []attribute.KeyValue{attribute.String("type", "step")}})
	b.Add(tracetest.SpanStub{Name: "jest tool span", SpanContext: sc("0000000000000003"), Parent: sc("0000000000000002"),
		StartTime: base, EndTime: base.Add(2 * time.Second), Attributes: []attribute.KeyValue{attribute.String("service.name", "jest")}})

	var buf bytes.Buffer
	RenderOTelTimeline(&buf, b.Spans(), time.Time{}, time.Time{}, enrichment.DefaultEnricher())

	ansi := regexp.MustCompile(`\x1b\[[0-9;]*m`)
	indentOf := func(name string) int {
		for _, l := range strings.Split(buf.String(), "\n") {
			clean := ansi.ReplaceAllString(l, "")
			if i := strings.LastIndex(clean, "│"); i >= 0 {
				lbl := clean[i+len("│"):]
				if strings.Contains(lbl, name) {
					return len(lbl) - len(strings.TrimLeft(lbl, " "))
				}
			}
		}
		return -1
	}
	step, tool := indentOf("Run tests"), indentOf("jest tool span")
	if step < 0 || tool < 0 {
		t.Fatalf("rows not found: step=%d tool=%d\n%s", step, tool, buf.String())
	}
	if tool <= step {
		t.Errorf("tool span indent (%d) must be greater than its step's (%d) — not nested", tool, step)
	}
}
