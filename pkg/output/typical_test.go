package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

func TestRenderTypicalRun(t *testing.T) {
	typical := &analyzer.TypicalRun{
		SampledRuns:    5,
		CommitsCovered: 4,
		TotalCommits:   20,
		Workflows: []analyzer.TypicalWorkflow{{
			Name:        "CI",
			SampledRuns: 5,
			TotalRuns:   12,
			RunDuration: analyzer.Quantiles{P50: 100, P95: 150},
			Jobs: []analyzer.TypicalJob{
				{
					Name:           "build",
					Samples:        5,
					PresenceRate:   100,
					SuccessRate:    100,
					StartOffset:    analyzer.Quantiles{P50: 10},
					Duration:       analyzer.Quantiles{P50: 60, P75: 70, P95: 90},
					TrendDirection: "stable",
				},
				{
					Name:           "a-very-long-job-name-that-needs-truncating-for-the-row",
					Samples:        2,
					PresenceRate:   40,
					SuccessRate:    50,
					StartOffset:    analyzer.Quantiles{P50: 70},
					Duration:       analyzer.Quantiles{P50: 30, P75: 35, P95: 45},
					TrendDirection: "degrading",
				},
			},
		}},
	}

	var buf bytes.Buffer
	renderTypicalRun(&buf, typical)
	out := buf.String()

	for _, want := range []string{"5 sampled runs", "4 of 20 commits", "▸ CI", "5/12 runs sampled", "build", "█", "in 40% of runs", "50% pass"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRenderTypicalRunLimitsWorkflows(t *testing.T) {
	typical := &analyzer.TypicalRun{SampledRuns: 20}
	for i := 0; i < 12; i++ {
		typical.Workflows = append(typical.Workflows, analyzer.TypicalWorkflow{
			Name:        strings.Repeat("w", i+1),
			SampledRuns: 12 - i,
			TotalRuns:   12 - i,
			RunDuration: analyzer.Quantiles{P50: 10, P95: 20},
			Jobs:        []analyzer.TypicalJob{{Name: "job", Samples: 1, PresenceRate: 100, SuccessRate: 100, Duration: analyzer.Quantiles{P50: 5, P75: 6, P95: 8}}},
		})
	}
	var buf bytes.Buffer
	renderTypicalRun(&buf, typical)
	out := buf.String()
	if !strings.Contains(out, "and 4 more workflows") {
		t.Errorf("expected workflow overflow note, got:\n%s", out)
	}
}

func TestRenderTypicalRunZeroDurations(t *testing.T) {
	// All-zero quantiles must not panic or divide by zero.
	typical := &analyzer.TypicalRun{
		SampledRuns: 1,
		Workflows: []analyzer.TypicalWorkflow{{
			Name:        "CI",
			SampledRuns: 1,
			TotalRuns:   1,
			Jobs:        []analyzer.TypicalJob{{Name: "noop", Samples: 1, PresenceRate: 100, SuccessRate: 100}},
		}},
	}
	var buf bytes.Buffer
	renderTypicalRun(&buf, typical)
	if buf.Len() == 0 {
		t.Error("expected output")
	}
}
