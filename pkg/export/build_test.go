package export

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRunReport(t *testing.T) {
	var m analyzer.FinalMetrics
	m.TotalJobs = 2
	m.FailedJobs = 1
	m.TotalSteps = 3
	m.SuccessfulRuns = 1
	m.AvgQueueTime = 1500
	m.MaxQueueTime = 4000
	m.JobTimeline = []analyzer.TimelineJob{
		{Name: "build", StartTime: 1000, EndTime: 4000, Conclusion: "success", Status: "completed", URL: "u1", IsRequired: true},
		{Name: "test", StartTime: 2000, EndTime: 2000, Conclusion: "failure", Status: "completed", URL: "u2"},
	}
	m.StepDurations = []analyzer.StepDuration{
		{Name: "checkout", Duration: 500, URL: "s1", JobName: "build"},
	}

	results := []analyzer.URLResult{{
		Owner: "o", Repo: "r", Identifier: "42", Type: "pr",
		BranchName: "main", HeadSHA: "deadbeef", DisplayName: "PR #42",
		DisplayURL: "https://github.com/o/r/pull/42", Metrics: m,
	}}
	combined := analyzer.CombinedMetrics{
		TotalRuns: 1, TotalJobs: 2, TotalSteps: 3,
		SuccessRate: "100.0", JobSuccessRate: "50.0", MaxConcurrency: 2,
	}

	rep := BuildRunReport(results, combined, 1000, 4000, "2026-06-17T00:00:00Z")

	assert.Equal(t, SchemaVersion, rep.SchemaVersion)
	assert.Equal(t, KindRunAnalysis, rep.Kind)
	assert.Equal(t, "o/r", rep.Meta.Repo)
	assert.Equal(t, "2026-06-17T00:00:00Z", rep.Meta.GeneratedAt)
	require.NotNil(t, rep.Run)

	s := rep.Run.Summary
	assert.Equal(t, 1, s.TotalRuns)
	assert.Equal(t, 1, s.SuccessfulRuns)
	assert.Equal(t, 2, s.TotalJobs)
	assert.Equal(t, 1, s.FailedJobs)
	assert.InDelta(t, 100.0, s.SuccessRatePct, 0.01)
	assert.InDelta(t, 50.0, s.JobSuccessRatePct, 0.01)
	assert.Equal(t, int64(3000), s.WallClockMs)

	require.Len(t, rep.Run.Runs, 1)
	run := rep.Run.Runs[0]
	assert.Equal(t, "o/r", run.Repo)
	assert.Equal(t, "pr", run.Type)
	assert.InDelta(t, 50.0, run.JobSuccessRatePct, 0.01)
	assert.Equal(t, int64(1500), run.AvgQueueMs)
	require.Len(t, run.Jobs, 2)
	assert.Equal(t, int64(3000), run.Jobs[0].DurationMs)
	assert.Equal(t, int64(0), run.Jobs[1].DurationMs, "negative/zero spans clamp to 0")
	require.Len(t, run.Steps, 1)
	assert.Equal(t, "build", run.Steps[0].Job)
	assert.Equal(t, int64(500), run.Steps[0].DurationMs)
}

func TestBuildTrendReport(t *testing.T) {
	a := &analyzer.TrendAnalysis{
		Owner: "o", Repo: "r",
		TimeRange: analyzer.TimeRange{Days: 7},
		Summary: analyzer.TrendSummary{
			TotalRuns: 100, AvgDuration: 300, MedianDuration: 280, P95Duration: 600,
			AvgSuccessRate: 92.5, TrendDirection: "stable", PercentChange: 1.2,
			RerunRuns: 5, RerunComputeMs: 123456,
		},
		QueueTimeStats: analyzer.QueueTimeStats{
			AvgQueueTime: 10, MedianQueueTime: 8, P95QueueTime: 30, QueueTimeRatio: 12.5,
		},
		Typical: &analyzer.TypicalRun{
			Workflows: []analyzer.TypicalWorkflow{{
				Name: "CI", SampledRuns: 20, TotalRuns: 50,
				RunDuration: analyzer.Quantiles{P50: 280, P95: 600},
				Jobs: []analyzer.TypicalJob{{
					Name: "build", Samples: 18, PresenceRate: 90, SuccessRate: 99,
					Duration: analyzer.Quantiles{P50: 120, P95: 200},
					P50URL:   "p50", P95URL: "p95", TrendDirection: "stable",
				}},
			}},
		},
		FlakyJobs: []analyzer.FlakyJob{{
			Name: "test", TotalRuns: 50, SuccessCount: 45, FailureCount: 5,
			FlakeRate: 10, SameSHAFlakes: 2, TransitionScore: 0.3, URLs: []string{"f1"},
		}},
		TopRegressions: []analyzer.JobRegression{{
			Name: "lint", OldAvgDuration: 10, NewAvgDuration: 20, PercentIncrease: 100, AbsoluteChange: 10,
			Changepoint: &analyzer.Changepoint{DiffURL: "diff1"},
		}},
		TopImprovements: []analyzer.JobImprovement{{
			Name: "build", OldAvgDuration: 30, NewAvgDuration: 20, PercentDecrease: 33.3, AbsoluteChange: 10,
		}},
		DurationTrend: []analyzer.DataPoint{
			{Timestamp: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC), Value: 290, Count: 10},
		},
	}

	rep := BuildTrendReport(a, "2026-06-17T00:00:00Z")

	assert.Equal(t, KindTrends, rep.Kind)
	assert.Equal(t, "o/r", rep.Meta.Repo)
	require.NotNil(t, rep.Trends)
	tr := rep.Trends
	assert.Equal(t, 7, tr.Days)
	assert.Equal(t, 100, tr.Summary.TotalRuns)
	assert.InDelta(t, 92.5, tr.Summary.AvgSuccessRatePct, 0.01)
	assert.InDelta(t, 12.5, tr.QueueStats.QueueRatioPct, 0.01)

	require.Len(t, tr.Typical, 1)
	require.Len(t, tr.Typical[0].Jobs, 1)
	assert.InDelta(t, 120, tr.Typical[0].Jobs[0].Duration.P50, 0.01)

	require.Len(t, tr.FlakyJobs, 1)
	assert.Equal(t, "f1", tr.FlakyJobs[0].SampleURL)

	require.Len(t, tr.Regressions, 1)
	assert.InDelta(t, 100, tr.Regressions[0].PercentChange, 0.01)
	assert.Equal(t, "diff1", tr.Regressions[0].DiffURL)

	require.Len(t, tr.Improvements, 1)
	assert.InDelta(t, -33.3, tr.Improvements[0].PercentChange, 0.01, "improvements are negative percent change")

	assert.Len(t, tr.Hourly, 0, "no hourly data → omitted")
	require.Len(t, tr.DailyDuration, 1)
	assert.Equal(t, "2026-06-01", tr.DailyDuration[0].Date)
}
