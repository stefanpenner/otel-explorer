package analyzer

// Tests derived from the specs/span-tree TLA+ campaign (Finding 2,
// FINDINGS.md #11): spans on a parent cycle are emitted but unreachable from
// the forest roots, so FlattenTree silently drops them from every renderer.

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// cycleSpan builds an enrichable job span with an explicit parent span ID.
func cycleSpan(name string, id, parent byte, start time.Time) tracetest.SpanStub {
	tid, _ := trace.TraceIDFromHex("37912fcf8909bcb43fd643580e6b5ee1")
	scOf := func(b byte) trace.SpanContext {
		var sid trace.SpanID
		if b != 0 {
			sid = trace.SpanID{0, 0, 0, 0, 0, 0, 0, b}
		}
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled,
		})
	}
	return tracetest.SpanStub{
		Name:        name,
		SpanContext: scOf(id),
		Parent:      scOf(parent),
		StartTime:   start,
		EndTime:     start.Add(5 * time.Second),
		Attributes:  []attribute.KeyValue{attribute.String("type", "job")},
	}
}

// TestTreeSpec_ParentCycleSpansReachable encodes the MCFinding2 witness:
// a mutual-parent pair (id5 <-> id6) links into a 2-cycle, neither is a
// root, and both silently vanish. After the fix, every emitted span must be
// reachable from the roots, exactly once.
func TestTreeSpec_ParentCycleSpansReachable(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		stubs    []tracetest.SpanStub
		wantRoot string // name of the deterministically promoted node
	}{
		{
			// The witness trace: spans 7,8 (id5 par6 / id6 par5) both survive.
			name: "mutual_parent_pair",
			stubs: []tracetest.SpanStub{
				cycleSpan("cycle-a", 5, 6, base),
				cycleSpan("cycle-b", 6, 5, base.Add(time.Second)),
			},
			wantRoot: "cycle-a", // smallest span ID (05 < 06)
		},
		{
			name: "three_cycle",
			stubs: []tracetest.SpanStub{
				cycleSpan("c1", 7, 9, base),
				cycleSpan("c2", 8, 7, base.Add(time.Second)),
				cycleSpan("c3", 9, 8, base.Add(2*time.Second)),
			},
			wantRoot: "c1", // smallest span ID (07)
		},
		{
			// A normal child hanging off a cycle member is unreachable too;
			// promoting the cycle must recover it.
			name: "cycle_with_tail",
			stubs: []tracetest.SpanStub{
				cycleSpan("cycle-a", 5, 6, base),
				cycleSpan("cycle-b", 6, 5, base.Add(time.Second)),
				cycleSpan("tail", 7, 5, base.Add(2*time.Second)),
			},
			wantRoot: "cycle-a",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &SpanBuilder{}
			// A normal rooted span alongside, so roots is nonempty either way.
			b.Add(cycleSpan("rooted", 1, 0, base))
			for _, s := range tc.stubs {
				b.Add(s)
			}

			roots := BuildTreeFromSpans(b.Spans(), time.Time{}, time.Time{}, &enrichment.GHAEnricher{})
			flat := FlattenTree(roots)

			counts := map[string]int{}
			for _, f := range flat {
				counts[f.Node.Name]++
			}

			for _, s := range tc.stubs {
				if counts[s.Name] != 1 {
					t.Errorf("span %q rendered %d times, want exactly 1 (flat: %v)",
						s.Name, counts[s.Name], counts)
				}
			}

			rootNames := map[string]bool{}
			for _, r := range roots {
				rootNames[r.Name] = true
			}
			if !rootNames[tc.wantRoot] {
				t.Errorf("expected %q (smallest span ID in the cycle) to be promoted to root, roots: %v",
					tc.wantRoot, rootNames)
			}
		})
	}
}
