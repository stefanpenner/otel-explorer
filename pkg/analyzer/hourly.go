package analyzer

import "sort"

// minHourlyRuns is the smallest dataset worth bucketing into 24 hours: below
// this most buckets hold so few observations that the "pattern" is noise.
// Reliably reached by store-backed (exact) analyses, rarely by sampled ones.
const minHourlyRuns = 48

// HourBucket aggregates one UTC hour-of-day across the analysis window.
type HourBucket struct {
	RunCount    int
	QueueP50    float64 // seconds; 0 when no queue observations
	DurationP50 float64 // seconds; run-level
}

// HourlyPatterns summarizes when CI runs and when it waits, by UTC hour.
// Queue-time peaks that track volume peaks mean runner contention ("buy
// runners"); duration changes that don't track volume mean the code changed.
type HourlyPatterns struct {
	Hours          [24]HourBucket
	PeakQueueHour  int // hour with the highest queue p50 (among hours with data)
	PeakVolumeHour int // hour with the most runs
}

// computeHourlyPatterns buckets runs by the UTC hour they were created.
// Returns nil when the dataset is too small for hourly claims.
func computeHourlyPatterns(runs []RunData) *HourlyPatterns {
	if len(runs) < minHourlyRuns {
		return nil
	}

	var durations [24][]float64
	var queues [24][]float64
	hp := &HourlyPatterns{}

	for _, run := range runs {
		if run.CreatedAt.IsZero() {
			continue
		}
		h := run.CreatedAt.UTC().Hour()
		hp.Hours[h].RunCount++
		if run.Duration > 0 {
			durations[h] = append(durations[h], float64(run.Duration)/1000.0)
		}
		for _, job := range run.Jobs {
			if job.QueueTime > 0 {
				queues[h] = append(queues[h], float64(job.QueueTime)/1000.0)
			}
		}
	}

	for h := 0; h < 24; h++ {
		if len(durations[h]) > 0 {
			sort.Float64s(durations[h])
			hp.Hours[h].DurationP50 = calculatePercentile(durations[h], 50)
		}
		if len(queues[h]) >= 3 { // too few queue observations is noise
			sort.Float64s(queues[h])
			hp.Hours[h].QueueP50 = calculatePercentile(queues[h], 50)
		}
		if hp.Hours[h].RunCount > hp.Hours[hp.PeakVolumeHour].RunCount {
			hp.PeakVolumeHour = h
		}
		if hp.Hours[h].QueueP50 > hp.Hours[hp.PeakQueueHour].QueueP50 {
			hp.PeakQueueHour = h
		}
	}
	return hp
}
