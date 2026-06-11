package analyzer

import (
	"math"
	"sort"
)

// notableDeltaPct is the deviation from the typical p50 below which a job is
// considered normal and omitted from the run-vs-typical comparison.
const notableDeltaPct = 25

// JobObservation is one job's duration from a single run under analysis.
type JobObservation struct {
	Name        string
	DurationSec float64
}

// RunDelta describes how one job in a single run deviated from the
// repository's typical baseline.
type RunDelta struct {
	JobName      string
	WorkflowName string
	ActualSec    float64
	TypicalP50   float64
	TypicalP95   float64
	DeltaSec     float64 // actual - p50
	DeltaPct     float64
	AboveP95     bool
}

// CompareJobsToTypical compares a single run's job durations against the
// typical baseline, returning only notable deviations (>=25% from p50),
// largest absolute time delta first. Jobs without a baseline are skipped —
// a brand-new job has no "typical". When the same job name exists in
// several workflows, the baseline with the most samples wins.
func CompareJobsToTypical(observed []JobObservation, typical *TypicalRun) []RunDelta {
	if typical == nil {
		return nil
	}

	type baseline struct {
		workflow string
		job      TypicalJob
	}
	byName := make(map[string]baseline)
	for _, wf := range typical.Workflows {
		for _, job := range wf.Jobs {
			if cur, ok := byName[job.Name]; ok && cur.job.Samples >= job.Samples {
				continue
			}
			byName[job.Name] = baseline{workflow: wf.Name, job: job}
		}
	}

	var deltas []RunDelta
	for _, obs := range observed {
		base, ok := byName[obs.Name]
		if !ok || base.job.Duration.P50 <= 0 || obs.DurationSec <= 0 {
			continue
		}
		deltaSec := obs.DurationSec - base.job.Duration.P50
		deltaPct := deltaSec / base.job.Duration.P50 * 100
		if math.Abs(deltaPct) < notableDeltaPct {
			continue
		}
		deltas = append(deltas, RunDelta{
			JobName:      obs.Name,
			WorkflowName: base.workflow,
			ActualSec:    obs.DurationSec,
			TypicalP50:   base.job.Duration.P50,
			TypicalP95:   base.job.Duration.P95,
			DeltaSec:     deltaSec,
			DeltaPct:     deltaPct,
			AboveP95:     base.job.Duration.P95 > 0 && obs.DurationSec > base.job.Duration.P95,
		})
	}

	sort.Slice(deltas, func(i, j int) bool {
		return math.Abs(deltas[i].DeltaSec) > math.Abs(deltas[j].DeltaSec)
	})
	return deltas
}
