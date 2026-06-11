package analyzer

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func typicalTestRuns() []RunData {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	runs := make([]RunData, 0, 12)
	for i := 0; i < 10; i++ {
		created := base.Add(time.Duration(i) * 24 * time.Hour)
		run := RunData{
			ID:           int64(i + 1),
			WorkflowName: "CI",
			HeadSHA:      fmt.Sprintf("sha%d", i),
			CreatedAt:    created,
			StartedAt:    created,
			Duration:     int64(100+i) * 1000,
		}
		// Sample only even runs (odd runs have no job detail)
		if i%2 == 0 {
			buildConclusion := "success"
			if i == 4 {
				buildConclusion = "failure"
			}
			run.Jobs = []JobData{
				{
					Name:        "build",
					Conclusion:  buildConclusion,
					StartedAt:   created.Add(10 * time.Second),
					CompletedAt: created.Add(70 * time.Second),
					Duration:    60_000,
					QueueTime:   10_000,
				},
				{
					Name:        "test",
					Conclusion:  "success",
					StartedAt:   created.Add(70 * time.Second),
					CompletedAt: created.Add(70*time.Second + time.Duration(30+10*i)*time.Second),
					Duration:    int64(30+10*i) * 1000,
					QueueTime:   5_000,
				},
				// Skipped on every run: must not appear in the typical view
				{
					Name:       "release",
					Conclusion: "skipped",
					Duration:   0,
				},
			}
			// "deploy" only present in the final sampled run
			if i == 8 {
				run.Jobs = append(run.Jobs, JobData{
					Name:        "deploy",
					Conclusion:  "success",
					StartedAt:   created.Add(120 * time.Second),
					CompletedAt: created.Add(150 * time.Second),
					Duration:    30_000,
					QueueTime:   2_000,
				})
			}
		}
		runs = append(runs, run)
	}
	// A second, busier-named workflow with one sampled run; should sort after
	// CI (fewer sampled runs).
	nightly := RunData{
		ID:           100,
		WorkflowName: "Nightly",
		HeadSHA:      "sha-nightly",
		CreatedAt:    base,
		StartedAt:    base,
		Duration:     500_000,
		Jobs: []JobData{{
			Name:        "soak",
			Conclusion:  "success",
			StartedAt:   base.Add(5 * time.Second),
			CompletedAt: base.Add(505 * time.Second),
			Duration:    500_000,
		}},
	}
	runs = append(runs, nightly)
	return runs
}

func TestComputeTypicalRun(t *testing.T) {
	typical := computeTypicalRun(typicalTestRuns())
	if typical == nil {
		t.Fatal("expected typical run, got nil")
	}

	if typical.SampledRuns != 6 {
		t.Errorf("SampledRuns = %d, want 6 (5 CI + 1 Nightly)", typical.SampledRuns)
	}
	if typical.TotalCommits != 11 {
		t.Errorf("TotalCommits = %d, want 11", typical.TotalCommits)
	}
	if typical.CommitsCovered != 6 {
		t.Errorf("CommitsCovered = %d, want 6", typical.CommitsCovered)
	}

	if len(typical.Workflows) != 2 {
		t.Fatalf("len(Workflows) = %d, want 2", len(typical.Workflows))
	}
	// Sorted by sampled-run count: CI (5) before Nightly (1)
	ci := typical.Workflows[0]
	if ci.Name != "CI" {
		t.Fatalf("Workflows[0].Name = %q, want CI", ci.Name)
	}
	if ci.SampledRuns != 5 || ci.TotalRuns != 10 {
		t.Errorf("CI sampled/total = %d/%d, want 5/10", ci.SampledRuns, ci.TotalRuns)
	}
	// Run-level duration quantiles span all 10 CI runs: 100s..109s
	if ci.RunDuration.P50 < 100 || ci.RunDuration.P50 > 109 {
		t.Errorf("CI RunDuration.P50 = %v, want within [100, 109]", ci.RunDuration.P50)
	}

	// "release" is always skipped, so CI has exactly build, test, deploy.
	if len(ci.Jobs) != 3 {
		t.Fatalf("len(CI.Jobs) = %d, want 3 (skipped job excluded)", len(ci.Jobs))
	}
	wantOrder := []string{"build", "test", "deploy"}
	for i, want := range wantOrder {
		if ci.Jobs[i].Name != want {
			t.Errorf("CI.Jobs[%d].Name = %q, want %q", i, ci.Jobs[i].Name, want)
		}
	}

	build := ci.Jobs[0]
	if math.Abs(build.Duration.P50-60) > 0.01 {
		t.Errorf("build Duration.P50 = %v, want 60", build.Duration.P50)
	}
	if math.Abs(build.StartOffset.P50-10) > 0.01 {
		t.Errorf("build StartOffset.P50 = %v, want 10", build.StartOffset.P50)
	}
	if math.Abs(build.PresenceRate-100) > 0.01 {
		t.Errorf("build PresenceRate = %v, want 100", build.PresenceRate)
	}
	if math.Abs(build.SuccessRate-80) > 0.01 {
		t.Errorf("build SuccessRate = %v, want 80 (4/5)", build.SuccessRate)
	}

	test := ci.Jobs[1]
	// test durations: 30, 50, 70, 90, 110 → p50 = 70, growing → degrading
	if math.Abs(test.Duration.P50-70) > 0.01 {
		t.Errorf("test Duration.P50 = %v, want 70", test.Duration.P50)
	}
	if test.TrendDirection != "degrading" {
		t.Errorf("test TrendDirection = %q, want degrading", test.TrendDirection)
	}
	if test.Duration.P95 < test.Duration.P50 {
		t.Errorf("test P95 (%v) < P50 (%v)", test.Duration.P95, test.Duration.P50)
	}

	deploy := ci.Jobs[2]
	if math.Abs(deploy.PresenceRate-20) > 0.01 {
		t.Errorf("deploy PresenceRate = %v, want 20 (1/5 sampled runs)", deploy.PresenceRate)
	}

	nightly := typical.Workflows[1]
	if nightly.Name != "Nightly" || len(nightly.Jobs) != 1 || nightly.Jobs[0].Name != "soak" {
		t.Errorf("Nightly workflow = %+v, want one 'soak' job", nightly)
	}
}

func TestComputeTypicalRunEmpty(t *testing.T) {
	if got := computeTypicalRun(nil); got != nil {
		t.Errorf("computeTypicalRun(nil) = %+v, want nil", got)
	}
	// Runs without any job detail → nil
	runs := []RunData{{ID: 1, WorkflowName: "CI", CreatedAt: time.Now(), Duration: 1000}}
	if got := computeTypicalRun(runs); got != nil {
		t.Errorf("computeTypicalRun(no jobs) = %+v, want nil", got)
	}
}

func TestComputeTypicalRunNegativeOffsetExcluded(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	runs := []RunData{{
		ID:           1,
		WorkflowName: "CI",
		HeadSHA:      "abc",
		CreatedAt:    created,
		StartedAt:    created,
		Duration:     1000,
		Jobs: []JobData{{
			Name:        "weird",
			Conclusion:  "success",
			StartedAt:   created.Add(-1 * time.Hour), // clock skew: before run start
			CompletedAt: created.Add(1 * time.Minute),
			Duration:    60_000,
		}},
	}}
	typical := computeTypicalRun(runs)
	if typical == nil {
		t.Fatal("expected typical run")
	}
	job := typical.Workflows[0].Jobs[0]
	if job.StartOffset.P50 != 0 {
		t.Errorf("StartOffset.P50 = %v, want 0 (negative offsets excluded)", job.StartOffset.P50)
	}
}

func TestComputeTypicalRunRetryOffsets(t *testing.T) {
	// For a retried run, offsets must be relative to the attempt's effective
	// start (StartedAt), not the original CreatedAt days earlier.
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	retryStart := created.Add(48 * time.Hour)
	runs := []RunData{{
		ID:           1,
		WorkflowName: "CI",
		HeadSHA:      "abc",
		CreatedAt:    created,
		StartedAt:    retryStart,
		Duration:     60_000,
		Jobs: []JobData{{
			Name:        "build",
			Conclusion:  "success",
			StartedAt:   retryStart.Add(10 * time.Second),
			CompletedAt: retryStart.Add(70 * time.Second),
			Duration:    60_000,
		}},
	}}
	typical := computeTypicalRun(runs)
	if typical == nil {
		t.Fatal("expected typical run")
	}
	job := typical.Workflows[0].Jobs[0]
	if math.Abs(job.StartOffset.P50-10) > 0.01 {
		t.Errorf("StartOffset.P50 = %v, want 10 (relative to attempt start)", job.StartOffset.P50)
	}
}

func TestComputeTypicalRunExemplars(t *testing.T) {
	t.Parallel()
	// Each percentile should link to the real job observation nearest it.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var runs []RunData
	durations := []int64{10, 20, 30, 40, 100} // seconds; p50=30, p95=100
	for i, d := range durations {
		created := base.Add(time.Duration(i) * time.Hour)
		runs = append(runs, RunData{
			ID: int64(i + 1), WorkflowName: "CI", HeadSHA: fmt.Sprintf("s%d", i),
			CreatedAt: created, StartedAt: created, Duration: d * 1000,
			Jobs: []JobData{{
				Name: "build", Conclusion: "success",
				URL:         fmt.Sprintf("https://example.com/job/%d", i),
				StartedAt:   created,
				CompletedAt: created.Add(time.Duration(d) * time.Second),
				Duration:    d * 1000,
			}},
		})
	}

	typical := computeTypicalRun(runs)
	if typical == nil {
		t.Fatal("expected typical run")
	}
	job := typical.Workflows[0].Jobs[0]
	assert.Equal(t, "https://example.com/job/2", job.P50URL, "30s observation is the median exemplar")
	assert.Equal(t, "https://example.com/job/4", job.P95URL, "100s observation is the p95 exemplar")
}
