package output

import (
	"io"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

// FuzzTrendsRender builds a TrendAnalysis from fuzzer-derived runs (each with a
// job whose duration varies over time) and renders it via OutputTrends. This
// exercises the formatting-heavy trends path the export fuzzers don't reach:
// duration/success charts (min/max normalization — a classic div-by-zero spot),
// the summary, job trends, flaky detection, and the changepoint significance
// math (Mann-Whitney U). Edge shapes — one run, all-equal durations, zero/huge
// values, all-failure — must not panic the renderer.
func FuzzTrendsRender(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 250, 0, 7}, 7)
	f.Add([]byte{5}, 1)
	f.Add([]byte{0, 0, 0, 0}, 30)
	f.Add([]byte{}, 7)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	f.Fuzz(func(t *testing.T, data []byte, days int) {
		if days < 1 {
			days = 1
		}
		if days > 365 {
			days = 365
		}
		n := len(data)
		if n > 300 {
			n = 300
		}
		var runs []analyzer.RunData
		for i := 0; i < n; i++ {
			b := data[i]
			concl := "success"
			if b&1 == 1 {
				concl = "failure"
			}
			ts := base.Add(time.Duration(i) * time.Hour)
			dur := int64(b) * 1000 * 60 // up to ~4h
			runs = append(runs, analyzer.RunData{
				ID: int64(i + 1), WorkflowName: "CI", HeadSHA: string(rune('a' + (i % 26))),
				Status: "completed", Conclusion: concl,
				CreatedAt: ts, StartedAt: ts, UpdatedAt: ts.Add(time.Duration(dur) * time.Millisecond),
				Duration: dur,
				Jobs: []analyzer.JobData{{
					ID: int64(1000 + i), Name: "build", Status: "completed", Conclusion: concl,
					CreatedAt: ts, StartedAt: ts, CompletedAt: ts.Add(time.Duration(dur) * time.Millisecond),
					Duration: dur,
				}},
			})
		}
		if len(runs) == 0 {
			return
		}
		analysis := analyzer.AnalyzeTrendsFromRuns("o", "r", days, runs)
		// Must not panic for any shape of trend data.
		_ = OutputTrends(io.Discard, analysis, "terminal")
	})
}
