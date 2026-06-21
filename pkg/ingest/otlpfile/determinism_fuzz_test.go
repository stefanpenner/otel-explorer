package otlpfile

import (
	"bytes"
	"fmt"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// FuzzParseDeterministic asserts that parsing the same bytes twice yields an
// identical ORDERED span sequence. Several sub-parsers (notably Chrome) build
// output by iterating Go maps (byThread, track registries), and Go randomizes
// map iteration order — so a parser that emits spans in map-iteration order
// would produce different output run-to-run for the same file, i.e. flaky
// output. The property: Parse is a pure function of its input.
func FuzzParseDeterministic(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"traceEvents":[{"ph":"X","name":"a","ts":0,"dur":5,"pid":1,"tid":1},{"ph":"X","name":"b","ts":0,"dur":5,"pid":1,"tid":2},{"ph":"X","name":"c","ts":0,"dur":5,"pid":2,"tid":1}]}`),
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`),
		[]byte(`{"data":[{"spans":[{"traceID":"a","spanID":"b","operationName":"op","startTime":1,"duration":2}]}]}`),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	seq := func(spans []sdktrace.ReadOnlySpan) string {
		var b bytes.Buffer
		for _, s := range spans {
			fmt.Fprintf(&b, "%s/%s|%s|%d|%d\n",
				s.SpanContext().TraceID(), s.SpanContext().SpanID(),
				s.Name(), s.StartTime().UnixNano(), s.EndTime().UnixNano())
		}
		return b.String()
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		a, errA := Parse(bytes.NewReader(data))
		b, errB := Parse(bytes.NewReader(data))
		if (errA == nil) != (errB == nil) {
			t.Fatalf("non-deterministic error: %v vs %v", errA, errB)
		}
		if errA != nil {
			return
		}
		if sa, sb := seq(a), seq(b); sa != sb {
			t.Fatalf("non-deterministic parse (span order/content differs across identical calls):\n--- run 1 ---\n%s--- run 2 ---\n%s", sa, sb)
		}
	})
}
