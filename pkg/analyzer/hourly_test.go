package analyzer

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func hourlyTestRuns() []RunData {
	// 24 runs at 09:00 UTC with 10s queue, 24 runs at 15:00 UTC with 60s
	// queue, across several days.
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	var runs []RunData
	id := int64(0)
	for day := 0; day < 12; day++ {
		for i, spec := range []struct {
			hour    int
			queueMs int64
		}{{9, 10_000}, {9, 10_000}, {15, 60_000}, {15, 60_000}} {
			id++
			created := base.Add(time.Duration(day)*24*time.Hour + time.Duration(spec.hour)*time.Hour + time.Duration(i)*time.Minute)
			runs = append(runs, RunData{
				ID: id, WorkflowName: "CI", HeadSHA: fmt.Sprintf("s%d", id),
				Status: "completed", Conclusion: "success",
				CreatedAt: created, StartedAt: created, Duration: 300_000,
				Jobs: []JobData{{
					ID: id * 10, Name: "build", Conclusion: "success",
					CreatedAt: created, StartedAt: created.Add(time.Duration(spec.queueMs) * time.Millisecond),
					CompletedAt: created.Add(5 * time.Minute),
					Duration:    280_000, QueueTime: spec.queueMs,
				}},
			})
		}
	}
	return runs
}

func TestComputeHourlyPatterns(t *testing.T) {
	t.Parallel()
	hp := computeHourlyPatterns(hourlyTestRuns())
	if hp == nil {
		t.Fatal("expected hourly patterns with 48 runs across 2 hours")
	}

	if hp.Hours[9].RunCount != 24 || hp.Hours[15].RunCount != 24 {
		t.Errorf("run counts: 09=%d 15=%d, want 24/24", hp.Hours[9].RunCount, hp.Hours[15].RunCount)
	}
	if hp.Hours[3].RunCount != 0 {
		t.Errorf("hour 03 should be empty, got %d", hp.Hours[3].RunCount)
	}
	if math.Abs(hp.Hours[9].QueueP50-10) > 0.01 {
		t.Errorf("09:00 queue p50 = %v, want 10s", hp.Hours[9].QueueP50)
	}
	if math.Abs(hp.Hours[15].QueueP50-60) > 0.01 {
		t.Errorf("15:00 queue p50 = %v, want 60s", hp.Hours[15].QueueP50)
	}
	if hp.PeakQueueHour != 15 {
		t.Errorf("PeakQueueHour = %d, want 15", hp.PeakQueueHour)
	}
	if hp.PeakVolumeHour != 9 && hp.PeakVolumeHour != 15 {
		t.Errorf("PeakVolumeHour = %d, want 9 or 15 (tied)", hp.PeakVolumeHour)
	}
}

func TestComputeHourlyPatternsInsufficientData(t *testing.T) {
	t.Parallel()
	if hp := computeHourlyPatterns(nil); hp != nil {
		t.Errorf("nil runs should yield nil, got %+v", hp)
	}
	// A handful of runs is not enough to claim an hourly pattern.
	few := hourlyTestRuns()[:4]
	if hp := computeHourlyPatterns(few); hp != nil {
		t.Errorf("4 runs should yield nil, got %+v", hp)
	}
}

func TestComputeHourlyPatterns_NoQueueData(t *testing.T) {
	t.Parallel()
	// 48 runs with durations but no queue times — PeakQueueHour
	// must not falsely report hour 0 as the peak.
	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	var runs []RunData
	for i := 0; i < 48; i++ {
		runs = append(runs, RunData{
			ID: int64(i + 1), WorkflowName: "CI", HeadSHA: fmt.Sprintf("s%d", i),
			Status: "completed", Conclusion: "success",
			CreatedAt: base.Add(time.Duration(i/2) * 24 * time.Hour),
			Duration:  300_000,
		})
	}
	hp := computeHourlyPatterns(runs)
	if hp == nil {
		t.Fatal("expected non-nil hourly patterns")
	}
	for _, h := range hp.Hours {
		if h.QueueP50 > 0 {
			t.Fatalf("expected no queue data, found QueueP50=%v at some hour", h.QueueP50)
		}
	}
	if hp.PeakQueueHour == 0 {
		t.Error("PeakQueueHour should not be 0 when no queue data exists — it falsely suggests midnight is the peak")
	}
}
