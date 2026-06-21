package analyzer

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// FuzzAnalysisDeterministic asserts the analysis stage is a pure function of
// its span input: parse arbitrary bytes, then run the span-driven analysis
// (RunsFromSpans, CombinedMetricsFromSpans, BuildTreeFromSpans + FlattenTree)
// twice and require byte-identical fingerprints. The analysis groups spans into
// runs/jobs/tree-children via Go maps; if any output order or value leaked map
// iteration order, reports/timelines would be flaky run-to-run for the same
// input. Catches a class of bug invisible to single-run panic/property fuzzing.
func FuzzAnalysisDeterministic(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"CI","startTimeUnixNano":"1","endTimeUnixNano":"9"},{"traceId":"00000000000000000000000000000001","spanId":"0000000000000002","parentSpanId":"0000000000000001","name":"build","startTimeUnixNano":"2","endTimeUnixNano":"5"}]}]}]}`),
		[]byte(`{"traceEvents":[{"ph":"X","name":"a","ts":0,"dur":5,"pid":1,"tid":1},{"ph":"X","name":"b","ts":0,"dur":5,"pid":1,"tid":2}]}`),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	enricher := enrichment.DefaultEnricher()

	fingerprint := func(spans []sdktrace.ReadOnlySpan) string {
		var b bytes.Buffer
		for _, r := range RunsFromSpans(spans, enricher) {
			fmt.Fprintf(&b, "run=%q jobs=%d\n", r.Name, len(r.Jobs))
		}
		c := CombinedMetricsFromSpans(spans, enricher)
		fmt.Fprintf(&b, "combined runs=%d jobs=%d steps=%d succ=%s jsucc=%s conc=%d tl=%d\n",
			c.TotalRuns, c.TotalJobs, c.TotalSteps, c.SuccessRate, c.JobSuccessRate, c.MaxConcurrency, len(c.JobTimeline))
		var earliest, latest time.Time
		if len(spans) > 0 {
			earliest, latest = spans[0].StartTime(), spans[0].EndTime()
			for _, s := range spans {
				if s.StartTime().Before(earliest) {
					earliest = s.StartTime()
				}
				if s.EndTime().After(latest) {
					latest = s.EndTime()
				}
			}
		}
		for _, fn := range FlattenTree(BuildTreeFromSpans(spans, earliest, latest, enricher)) {
			fmt.Fprintf(&b, "node d=%d %q\n", fn.Depth, fn.Node.Name)
		}
		return b.String()
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		spans, err := otlpfile.Parse(bytes.NewReader(data))
		if err != nil || len(spans) == 0 {
			return
		}
		// Determinism is a structural property; verifying it needs variety, not
		// volume. Cap per-exec work so the doubled analysis + fingerprint can't
		// drive a worker OOM on huge mutated inputs (which only adds memory
		// pressure, not coverage).
		if len(spans) > 2000 {
			return
		}
		if a, b := fingerprint(spans), fingerprint(spans); a != b {
			t.Fatalf("non-deterministic analysis (output differs across identical calls):\n--- run 1 ---\n%s--- run 2 ---\n%s", a, b)
		}
	})
}
