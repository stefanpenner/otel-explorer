package analyzer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateMaxConcurrency(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		starts []JobEvent
		ends   []JobEvent
		expect int
	}{
		{
			name:   "overlapping",
			starts: []JobEvent{{Ts: 1000, Type: "start"}, {Ts: 2000, Type: "start"}, {Ts: 3000, Type: "start"}},
			ends:   []JobEvent{{Ts: 4000, Type: "end"}, {Ts: 3500, Type: "end"}, {Ts: 5000, Type: "end"}},
			expect: 3,
		},
		{
			name:   "empty",
			starts: nil,
			ends:   nil,
			expect: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, CalculateMaxConcurrency(tc.starts, tc.ends))
		})
	}
}

func TestCalculateFinalMetrics(t *testing.T) {
	t.Parallel()

	metrics := Metrics{
		TotalRuns:      10,
		SuccessfulRuns: 8,
		FailedRuns:     2,
		TotalJobs:      20,
		FailedJobs:     3,
		TotalSteps:     100,
		FailedSteps:    5,
		JobDurations:   []float64{1000, 2000, 3000, 4000, 5000},
		StepDurations: []StepDuration{
			{Duration: 100},
			{Duration: 200},
			{Duration: 300},
		},
	}

	final := CalculateFinalMetrics(metrics, 10, nil, nil)
	assert.Equal(t, 3000.0, final.AvgJobDuration)
	assert.Equal(t, 200.0, final.AvgStepDuration)
	assert.Equal(t, "80.0", final.SuccessRate)
	assert.Equal(t, "85.0", final.JobSuccessRate)
}

func TestFindBottleneckJobs(t *testing.T) {
	t.Parallel()

	jobs := []TimelineJob{
		{Name: "FastJob", StartTime: 1000, EndTime: 2000},
		{Name: "SlowJob", StartTime: 1500, EndTime: 10000},
		{Name: "MediumJob", StartTime: 2000, EndTime: 5000},
	}
	bottlenecks := FindBottleneckJobs(jobs)
	assert.NotEmpty(t, bottlenecks)
	assert.Equal(t, "SlowJob", bottlenecks[0].Name)
}

func TestAnalyzeSlowJobsAndSteps(t *testing.T) {
	t.Parallel()

	metrics := Metrics{
		JobDurations: []float64{1000, 5000, 2000, 8000, 3000},
		JobNames:     []string{"Job1", "Job2", "Job3", "Job4", "Job5"},
		JobURLs:      []string{"url1", "url2", "url3", "url4", "url5"},
		StepDurations: []StepDuration{
			{Name: "Step1", Duration: 1000},
			{Name: "Step2", Duration: 5000},
			{Name: "Step3", Duration: 2000},
			{Name: "Step4", Duration: 8000},
		},
	}
	slowJobs := AnalyzeSlowJobs(metrics, 3)
	assert.Len(t, slowJobs, 3)
	assert.Equal(t, "Job4", slowJobs[0].Name)

	slowSteps := AnalyzeSlowSteps(metrics, 2)
	assert.Len(t, slowSteps, 2)
	assert.Equal(t, "Step4", slowSteps[0].Name)
}

func TestFindOverlappingJobs(t *testing.T) {
	t.Parallel()

	jobs := []TimelineJob{
		{Name: "Job1", StartTime: 1000, EndTime: 3000},
		{Name: "Job2", StartTime: 2000, EndTime: 4000},
		{Name: "Job3", StartTime: 5000, EndTime: 7000},
	}
	overlaps := FindOverlappingJobs(jobs)
	assert.Len(t, overlaps, 1)
	assert.Equal(t, "Job1", overlaps[0][0].Name)
	assert.Equal(t, "Job2", overlaps[0][1].Name)
}

func TestCalculateMaxConcurrencyEdgeCases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		starts []JobEvent
		ends   []JobEvent
		expect int
	}{
		{
			name:   "single job",
			starts: []JobEvent{{Ts: 1000, Type: "start"}},
			ends:   []JobEvent{{Ts: 2000, Type: "end"}},
			expect: 1,
		},
		{
			name:   "sequential jobs no overlap",
			starts: []JobEvent{{Ts: 1000, Type: "start"}, {Ts: 3000, Type: "start"}},
			ends:   []JobEvent{{Ts: 2000, Type: "end"}, {Ts: 4000, Type: "end"}},
			expect: 1,
		},
		{
			name:   "all jobs overlap completely",
			starts: []JobEvent{{Ts: 1000, Type: "start"}, {Ts: 1000, Type: "start"}, {Ts: 1000, Type: "start"}},
			ends:   []JobEvent{{Ts: 5000, Type: "end"}, {Ts: 5000, Type: "end"}, {Ts: 5000, Type: "end"}},
			expect: 3,
		},
		{
			// One job ends at the exact timestamp the next starts (common with
			// second-granularity GitHub timestamps and `needs:` chains): the
			// end event must be processed first, so they are not concurrent.
			name:   "back-to-back jobs sharing a boundary timestamp",
			starts: []JobEvent{{Ts: 0, Type: "start"}, {Ts: 1000, Type: "start"}},
			ends:   []JobEvent{{Ts: 1000, Type: "end"}, {Ts: 2000, Type: "end"}},
			expect: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, CalculateMaxConcurrency(tc.starts, tc.ends))
		})
	}
}

func TestMaxConcurrencyAtTimesBackToBack(t *testing.T) {
	t.Parallel()

	starts := []JobEvent{{Ts: 0, Type: "start"}, {Ts: 1000, Type: "start"}}
	ends := []JobEvent{{Ts: 1000, Type: "end"}, {Ts: 2000, Type: "end"}}
	assert.Equal(t, 1, maxConcurrencyAtTimes(starts, ends))
}

func TestCalculateCombinedSuccessRates(t *testing.T) {
	t.Parallel()

	t.Run("computed from raw counts, not formatted strings", func(t *testing.T) {
		results := []URLResult{
			{Metrics: FinalMetrics{
				Metrics:        Metrics{TotalRuns: 3, SuccessfulRuns: 1, TotalJobs: 3, FailedJobs: 2},
				SuccessRate:    "33.3",
				JobSuccessRate: "33.3",
			}},
			{Metrics: FinalMetrics{
				Metrics:        Metrics{TotalRuns: 30, SuccessfulRuns: 1, TotalJobs: 30, FailedJobs: 29},
				SuccessRate:    "3.3",
				JobSuccessRate: "3.3",
			}},
		}
		// True combined rate: 2/33 = 6.0606... -> "6.1".
		// Reconstructing from the 1-decimal strings would yield "6.0".
		assert.Equal(t, "6.1", CalculateCombinedSuccessRate(results))
		assert.Equal(t, "6.1", CalculateCombinedJobSuccessRate(results))
	})

	t.Run("empty input", func(t *testing.T) {
		assert.Equal(t, "0.0", CalculateCombinedSuccessRate(nil))
		assert.Equal(t, "0.0", CalculateCombinedJobSuccessRate(nil))
	})
}

func TestCalculateFinalMetricsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("zero runs returns zero success rate", func(t *testing.T) {
		metrics := Metrics{TotalRuns: 0}
		final := CalculateFinalMetrics(metrics, 0, nil, nil)
		assert.Equal(t, "0", final.SuccessRate)
	})

	t.Run("empty durations return zero averages", func(t *testing.T) {
		metrics := Metrics{
			TotalRuns:      10,
			SuccessfulRuns: 10,
			TotalJobs:      5,
			JobDurations:   []float64{},
			StepDurations:  []StepDuration{},
		}
		final := CalculateFinalMetrics(metrics, 10, nil, nil)
		assert.Equal(t, 0.0, final.AvgJobDuration)
		assert.Equal(t, 0.0, final.AvgStepDuration)
	})

	t.Run("all jobs failed gives 0% job success rate", func(t *testing.T) {
		metrics := Metrics{
			TotalJobs:  10,
			FailedJobs: 10,
		}
		final := CalculateFinalMetrics(metrics, 0, nil, nil)
		assert.Equal(t, "0.0", final.JobSuccessRate)
	})
}

func TestFindBottleneckJobsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty jobs returns empty", func(t *testing.T) {
		bottlenecks := FindBottleneckJobs(nil)
		assert.Empty(t, bottlenecks)
	})

	t.Run("single job is bottleneck", func(t *testing.T) {
		jobs := []TimelineJob{{Name: "only-job", StartTime: 1000, EndTime: 10000}}
		bottlenecks := FindBottleneckJobs(jobs)
		assert.Len(t, bottlenecks, 1)
		assert.Equal(t, "only-job", bottlenecks[0].Name)
	})

	t.Run("all equal duration jobs above threshold", func(t *testing.T) {
		// Jobs need duration > 1000ms to be considered "significant"
		jobs := []TimelineJob{
			{Name: "A", StartTime: 0, EndTime: 5000},
			{Name: "B", StartTime: 0, EndTime: 5000},
			{Name: "C", StartTime: 0, EndTime: 5000},
		}
		bottlenecks := FindBottleneckJobs(jobs)
		// All have equal duration so all could be considered bottlenecks
		assert.NotEmpty(t, bottlenecks)
	})
}

func TestAnalyzeSlowJobsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("returns fewer than requested when not enough jobs", func(t *testing.T) {
		metrics := Metrics{
			JobDurations: []float64{1000, 2000},
			JobNames:     []string{"Job1", "Job2"},
			JobURLs:      []string{"url1", "url2"},
		}
		slowJobs := AnalyzeSlowJobs(metrics, 10)
		assert.Len(t, slowJobs, 2)
	})

	t.Run("empty metrics returns empty", func(t *testing.T) {
		metrics := Metrics{}
		slowJobs := AnalyzeSlowJobs(metrics, 5)
		assert.Empty(t, slowJobs)
	})
}

func TestAnalyzeSlowStepsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("returns fewer than requested when not enough steps", func(t *testing.T) {
		metrics := Metrics{
			StepDurations: []StepDuration{
				{Name: "Step1", Duration: 1000},
			},
		}
		slowSteps := AnalyzeSlowSteps(metrics, 10)
		assert.Len(t, slowSteps, 1)
	})

	t.Run("empty steps returns empty", func(t *testing.T) {
		metrics := Metrics{}
		slowSteps := AnalyzeSlowSteps(metrics, 5)
		assert.Empty(t, slowSteps)
	})
}

func TestFindOverlappingJobsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty jobs returns empty", func(t *testing.T) {
		overlaps := FindOverlappingJobs(nil)
		assert.Empty(t, overlaps)
	})

	t.Run("single job returns empty", func(t *testing.T) {
		jobs := []TimelineJob{{Name: "only", StartTime: 1000, EndTime: 2000}}
		overlaps := FindOverlappingJobs(jobs)
		assert.Empty(t, overlaps)
	})

	t.Run("jobs touching but not overlapping", func(t *testing.T) {
		jobs := []TimelineJob{
			{Name: "A", StartTime: 1000, EndTime: 2000},
			{Name: "B", StartTime: 2000, EndTime: 3000}, // Starts exactly when A ends
		}
		overlaps := FindOverlappingJobs(jobs)
		assert.Empty(t, overlaps)
	})

	t.Run("multiple overlapping pairs", func(t *testing.T) {
		jobs := []TimelineJob{
			{Name: "A", StartTime: 1000, EndTime: 3000},
			{Name: "B", StartTime: 2000, EndTime: 4000}, // Overlaps with A
			{Name: "C", StartTime: 5000, EndTime: 7000},
			{Name: "D", StartTime: 6000, EndTime: 8000}, // Overlaps with C
		}
		overlaps := FindOverlappingJobs(jobs)
		assert.Len(t, overlaps, 2)
	})
}

func TestSortJobEventsEndBeforeStartAtSameTimestamp(t *testing.T) {
	t.Parallel()

	events := []JobEvent{
		{Ts: 2000, Type: "start"},
		{Ts: 1000, Type: "start"},
		{Ts: 2000, Type: "end"},
		{Ts: 3000, Type: "end"},
	}
	SortJobEvents(events)
	assert.Equal(t, []JobEvent{
		{Ts: 1000, Type: "start"},
		{Ts: 2000, Type: "end"},
		{Ts: 2000, Type: "start"},
		{Ts: 3000, Type: "end"},
	}, events, "end must sort before start at the same timestamp so back-to-back jobs are not concurrent")
}
