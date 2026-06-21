package export

import (
	"bytes"
	"io"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
)

// FuzzRenderReport fuzzes the export renderers — including the third-party
// binary writers (excelize for XLSX, go-docx for DOCX) and the HTML template —
// on a report derived from arbitrary parsed spans. It parses bytes, builds a
// run report from the resulting span-runs + combined metrics, then renders all
// five formats to io.Discard. A pathological span (odd unicode names, huge or
// negative durations, control chars) can produce a Report that trips a renderer
// even though earlier stages tolerated it; none may panic.
func FuzzRenderReport(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"build","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`),
		[]byte("{\"traceEvents\":[{\"ph\":\"X\",\"name\":\"weird\\u0000\\ufffd\",\"ts\":-5,\"dur\":-9,\"pid\":1,\"tid\":1}]}"),
		[]byte(`{"data":[{"spans":[{"traceID":"abc","spanID":"def","operationName":"op","startTime":9223372036854775807,"duration":9223372036854775807}]}]}`),
		[]byte(``),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	enricher := enrichment.DefaultEnricher()

	f.Fuzz(func(t *testing.T, data []byte) {
		spans, err := otlpfile.Parse(bytes.NewReader(data))
		if err != nil || len(spans) == 0 {
			return
		}
		spanRuns := analyzer.RunsFromSpans(spans, enricher)
		combined := analyzer.CombinedMetricsFromSpans(spans, enricher)
		rep := BuildRunReport(nil, spanRuns, combined, 0, 0, "2024-01-01T00:00:00Z")
		if rep == nil {
			return
		}
		// None of the five renderers may panic on a report from parsed spans.
		_ = RenderJSON(io.Discard, rep)
		_ = RenderHTML(io.Discard, rep)
		_ = RenderSlack(io.Discard, rep)
		_ = RenderXLSX(io.Discard, rep)
		_ = RenderDOCX(io.Discard, rep)
	})
}
