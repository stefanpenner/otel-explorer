package output

import (
	"bytes"
	"io"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
)

// FuzzTerminalRunReport drives the spans-based terminal renderers (the styled
// CLI run report, the markdown report, and the timeline buffer) over arbitrary
// parsed spans. These format durations, percentages, and lipgloss tables from
// span timings — a span with a negative/overflowed duration, EndTime<StartTime,
// zero counts (div-by-zero), or odd unicode names could panic the formatting.
// They render to io.Discard; the contract is simply: never panic.
func FuzzTerminalRunReport(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"build","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`),
		[]byte(`{"traceEvents":[{"ph":"X","name":"a","ts":-5,"dur":-9,"pid":1,"tid":1}]}`),
		[]byte(`{"data":[{"spans":[{"traceID":"a","spanID":"b","operationName":"op","startTime":9223372036854775807,"duration":9223372036854775807}]}]}`),
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
		// Bound work: rendering huge span sets only adds memory pressure, not
		// formatting-path coverage.
		if len(spans) > 2000 {
			return
		}

		earliest, latest := spans[0].StartTime().UnixNano(), spans[0].EndTime().UnixNano()
		for _, s := range spans {
			if n := s.StartTime().UnixNano(); n < earliest {
				earliest = n
			}
			if n := s.EndTime().UnixNano(); n > latest {
				latest = n
			}
		}
		combined := analyzer.CombinedMetricsFromSpans(spans, enricher)

		// None of the terminal renderers may panic on parsed spans.
		_ = OutputStyledResults(io.Discard, nil, combined, nil, earliest, latest, spans, enricher)
		_ = OutputCombinedResultsMarkdown(io.Discard, nil, combined, nil, earliest, latest, "", false, spans, enricher)
		_ = RenderTimelineToBuffer(spans, earliest, latest, enricher)
	})
}
