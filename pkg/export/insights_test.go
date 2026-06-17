package export

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func titles(ins []Insight) []string {
	out := make([]string, len(ins))
	for i, in := range ins {
		out[i] = in.Title
	}
	return out
}

func TestHighlights_Trends_RanksBadFirstAndCovers(t *testing.T) {
	rep := &Report{Kind: KindTrends, Trends: &TrendReport{
		Days: 7,
		Summary: TrendSummary{
			TotalRuns: 100, AvgSuccessRatePct: 70, MedianDurationSec: 300, P95DurationSec: 600,
			TrendDirection: "degrading", PercentChange: 25, RerunRuns: 4, RerunComputeMs: 600000,
		},
		QueueStats:   QueueStats{QueueRatioPct: 35, MedianQueueSec: 20, P95QueueSec: 60},
		Regressions:  []JobChange{{Name: "lint", OldAvgSec: 10, NewAvgSec: 25, PercentChange: 150, DiffURL: "https://diff"}},
		FlakyJobs:    []FlakyJob{{Name: "test", FlakeRatePct: 12, FailureCount: 6, TotalRuns: 50, SameSHAFlakes: 2, SampleURL: "https://flake"}},
		Improvements: []JobChange{{Name: "build", OldAvgSec: 30, NewAvgSec: 20, PercentChange: -33}},
	}}

	ins := Highlights(rep)
	require.NotEmpty(t, ins)
	assert.LessOrEqual(t, len(ins), maxHighlights)

	// Bad-severity findings must come before good ones.
	assert.Equal(t, SeverityBad, ins[0].Severity)
	for i := 1; i < len(ins); i++ {
		assert.LessOrEqual(t, severityRank[ins[i-1].Severity], severityRank[ins[i].Severity], "insights stay severity-ordered")
	}

	joined := ""
	for _, in := range ins {
		joined += in.Title + "|"
	}
	assert.Contains(t, joined, "degrading")
	assert.Contains(t, joined, "lint") // regression
	assert.Contains(t, joined, "test") // flaky
	assert.Contains(t, joined, "70%")  // success rate

	// The flaky finding carries an actionable recommendation + URL.
	var flaky *Insight
	for i := range ins {
		if ins[i].Title == `"test" is flaky (12% failure rate)` {
			flaky = &ins[i]
		}
	}
	require.NotNil(t, flaky)
	assert.NotEmpty(t, flaky.Recommendation)
	assert.Equal(t, "https://flake", flaky.URL)
	assert.Contains(t, flaky.Detail, "identical commit")
}

func TestHighlights_Run_FailuresAndSlowest(t *testing.T) {
	rep := &Report{Kind: KindRunAnalysis, Run: &RunReport{
		Summary: RunSummary{TotalRuns: 1, TotalJobs: 3, FailedJobs: 1, WallClockMs: 10000},
		Runs: []Run{{
			MaxQueueMs: 90000,
			Jobs: []Job{
				{Name: "build", Conclusion: "success", DurationMs: 8000},
				{Name: "test", Conclusion: "failure", DurationMs: 2000},
				{Name: "lint", Conclusion: "success", DurationMs: 1000},
			},
		}},
	}}

	ins := Highlights(rep)
	require.NotEmpty(t, ins)
	assert.Equal(t, SeverityBad, ins[0].Severity, "failures lead")
	all := titles(ins)
	assert.Contains(t, all[0], "1 job(s) failed")

	joined := ""
	for _, in := range ins {
		joined += in.Title + "|"
	}
	assert.Contains(t, joined, "slowest job")
	assert.Contains(t, joined, "build") // slowest
	assert.Contains(t, joined, "queued")
}

func TestHighlights_Run_AllPassed(t *testing.T) {
	rep := &Report{Kind: KindRunAnalysis, Run: &RunReport{
		Summary: RunSummary{TotalRuns: 1, TotalJobs: 2, FailedJobs: 0, JobSuccessRatePct: f64(100), WallClockMs: 5000},
		Runs:    []Run{{Jobs: []Job{{Name: "a", Conclusion: "success", DurationMs: 5000}}}},
	}}
	ins := Highlights(rep)
	require.NotEmpty(t, ins)
	assert.Equal(t, SeverityGood, ins[0].Severity)
	assert.Contains(t, ins[0].Title, "All jobs passed")
}
