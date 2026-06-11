package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
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

func TestRenderTypicalRunExemplarLinks(t *testing.T) {
	utils.SetColorEnabled(true)
	defer utils.SetColorEnabled(false)

	typical := &analyzer.TypicalRun{
		SampledRuns: 3,
		Workflows: []analyzer.TypicalWorkflow{{
			Name: "CI", SampledRuns: 3, TotalRuns: 3,
			RunDuration: analyzer.Quantiles{P50: 60, P95: 90},
			Jobs: []analyzer.TypicalJob{{
				Name: "build", Samples: 3, PresenceRate: 100, SuccessRate: 100,
				Duration: analyzer.Quantiles{P50: 50, P75: 60, P95: 80},
				P50URL:   "https://github.com/o/r/actions/runs/1/job/50",
				P95URL:   "https://github.com/o/r/actions/runs/2/job/95",
			}},
		}},
	}
	var buf bytes.Buffer
	renderTypicalRun(&buf, typical)
	out := buf.String()
	for _, want := range []string{"https://github.com/o/r/actions/runs/1/job/50", "https://github.com/o/r/actions/runs/2/job/95"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing exemplar link %q", want)
		}
	}
}

func TestRenderHourlyPatterns(t *testing.T) {
	hp := &analyzer.HourlyPatterns{PeakVolumeHour: 9, PeakQueueHour: 15}
	hp.Hours[9] = analyzer.HourBucket{RunCount: 24, QueueP50: 10}
	hp.Hours[15] = analyzer.HourBucket{RunCount: 20, QueueP50: 60}

	var buf bytes.Buffer
	renderHourlyPatterns(&buf, hp)
	out := buf.String()
	for _, want := range []string{"(UTC)", "Runs", "Queue", "Busiest hour 09:00 (24 runs)", "Worst queue 15:00"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "runner contention") {
		t.Error("contention hint requires queue and volume peaks to coincide")
	}
}

func TestRenderTypicalRunQueuePrefix(t *testing.T) {
	typical := &analyzer.TypicalRun{
		SampledRuns: 10,
		Workflows: []analyzer.TypicalWorkflow{{
			Name: "CI", SampledRuns: 10, TotalRuns: 10,
			RunDuration: analyzer.Quantiles{P50: 100, P95: 100},
			Jobs: []analyzer.TypicalJob{
				{
					// Job starts at t=50s after a 30s queue: a visible ▒ prefix.
					Name: "queued-job", Samples: 10, PresenceRate: 100, SuccessRate: 100,
					StartOffset: analyzer.Quantiles{P50: 50},
					Duration:    analyzer.Quantiles{P50: 40, P75: 45, P95: 50},
					QueueTime:   analyzer.Quantiles{P50: 30},
				},
				{
					// No queue: no prefix.
					Name: "instant-job", Samples: 10, PresenceRate: 100, SuccessRate: 100,
					StartOffset: analyzer.Quantiles{P50: 0},
					Duration:    analyzer.Quantiles{P50: 40, P75: 45, P95: 50},
				},
			},
		}},
	}
	var buf bytes.Buffer
	renderTypicalRun(&buf, typical)
	out := buf.String()

	lines := strings.Split(out, "\n")
	var queuedLine, instantLine string
	for _, l := range lines {
		if strings.Contains(l, "queued-job") {
			queuedLine = l
		}
		if strings.Contains(l, "instant-job") {
			instantLine = l
		}
	}
	if !strings.Contains(queuedLine, "▒") {
		t.Errorf("queued-job row should show a ▒ queue prefix:\n%s", queuedLine)
	}
	if strings.Contains(instantLine, "▒") {
		t.Errorf("instant-job row should have no queue prefix:\n%s", instantLine)
	}
	if !strings.Contains(out, "▒ queue") {
		t.Error("legend should explain the queue prefix glyph")
	}
}
