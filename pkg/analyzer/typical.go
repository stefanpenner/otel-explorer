package analyzer

import (
	"sort"
	"time"
)

// Quantiles summarizes a distribution at the percentiles used by the
// typical-run view. All values are seconds.
type Quantiles struct {
	P5  float64
	P25 float64
	P50 float64
	P75 float64
	P95 float64
}

// TypicalJob is the statistical aggregate of one job (a "segment" of the
// pipeline) across the sampled runs of one workflow. Skipped and cancelled
// observations are excluded from all statistics.
type TypicalJob struct {
	Name           string
	URLs           []string  // sample recent job URLs (newest first)
	P50URL         string    // exemplar: the real observation nearest the median duration
	P95URL         string    // exemplar: the real observation nearest the p95 duration
	Samples        int       // number of counted (non-skipped) observations
	PresenceRate   float64   // % of the workflow's sampled runs with a counted observation
	SuccessRate    float64   // % of counted observations that succeeded
	StartOffset    Quantiles // seconds after run start that the job began
	Duration       Quantiles
	QueueTime      Quantiles
	TrendDirection string // "improving", "stable", "degrading"
}

// TypicalWorkflow is the statistically typical pipeline of one workflow:
// per-job medians and ranges aggregated across that workflow's sampled runs.
type TypicalWorkflow struct {
	Name        string
	SampledRuns int       // runs of this workflow with job-level detail
	TotalRuns   int       // all completed runs of this workflow in the window
	RunDuration Quantiles // run-level duration quantiles (all completed runs)
	Jobs        []TypicalJob
}

// TypicalRun is the "statistically typical" view of a repo's CI: each
// workflow aggregated across sampled runs, rather than any single real run.
type TypicalRun struct {
	SampledRuns    int // runs with job-level detail, across all workflows
	CommitsCovered int // unique head SHAs among sampled runs
	TotalCommits   int // unique head SHAs among all runs in the window
	Workflows      []TypicalWorkflow
}

// quantilesOf computes the Quantiles of values (seconds). Returns the zero
// value when values is empty.
func quantilesOf(values []float64) Quantiles {
	if len(values) == 0 {
		return Quantiles{}
	}
	return Quantiles{
		P5:  calculatePercentile(values, 5),
		P25: calculatePercentile(values, 25),
		P50: calculatePercentile(values, 50),
		P75: calculatePercentile(values, 75),
		P95: calculatePercentile(values, 95),
	}
}

// countedObservation reports whether a job observation should contribute to
// typical-run statistics. Skipped and cancelled jobs would otherwise pollute
// duration quantiles with zeros and pass rates with non-failures.
func countedObservation(job JobData) bool {
	return job.Conclusion != "skipped" && job.Conclusion != "cancelled" && job.Conclusion != "neutral"
}

type typicalJobAgg struct {
	offsets    []float64
	durations  []float64
	queueTimes []float64
	successes  int
	total      int
	runsSeen   int
	urls       []string      // chronological; reversed to newest-first later
	obs        []observation // duration+URL pairs for exemplar selection
}

// observation pairs a duration with the URL of the real job it came from.
type observation struct {
	durationSec float64
	url         string
}

// exemplarURL returns the URL of the observation nearest the target value.
func exemplarURL(obs []observation, target float64) string {
	best := ""
	bestDist := -1.0
	for _, o := range obs {
		if o.url == "" {
			continue
		}
		dist := o.durationSec - target
		if dist < 0 {
			dist = -dist
		}
		if bestDist < 0 || dist < bestDist {
			bestDist = dist
			best = o.url
		}
	}
	return best
}

type typicalWorkflowAgg struct {
	sampledRuns  int
	totalRuns    int
	runDurations []float64
	jobs         map[string]*typicalJobAgg
}

// computeTypicalRun aggregates sampled job data into a TypicalRun, grouped by
// workflow so that jobs from unrelated pipelines are never mixed into one
// timeline. runs must be sorted chronologically (oldest first) so per-job
// trend direction compares early observations against recent ones. Returns
// nil when no sampled run has job data.
func computeTypicalRun(runs []RunData) *TypicalRun {
	allCommits := make(map[string]struct{})
	sampledCommits := make(map[string]struct{})
	workflows := make(map[string]*typicalWorkflowAgg)
	sampledRuns := 0

	for _, run := range runs {
		if run.HeadSHA != "" {
			allCommits[run.HeadSHA] = struct{}{}
		}
		wf := workflows[run.WorkflowName]
		if wf == nil {
			wf = &typicalWorkflowAgg{jobs: make(map[string]*typicalJobAgg)}
			workflows[run.WorkflowName] = wf
		}
		wf.totalRuns++
		if run.Duration > 0 {
			wf.runDurations = append(wf.runDurations, float64(run.Duration)/1000.0)
		}
		if len(run.Jobs) == 0 {
			continue
		}
		sampledRuns++
		wf.sampledRuns++
		if run.HeadSHA != "" {
			sampledCommits[run.HeadSHA] = struct{}{}
		}

		runStart := run.StartedAt
		if runStart.IsZero() {
			runStart = run.CreatedAt
		}

		seenThisRun := make(map[string]bool)
		for _, job := range run.Jobs {
			if !countedObservation(job) {
				continue
			}
			agg := wf.jobs[job.Name]
			if agg == nil {
				agg = &typicalJobAgg{}
				wf.jobs[job.Name] = agg
			}
			agg.total++
			if !seenThisRun[job.Name] {
				seenThisRun[job.Name] = true
				agg.runsSeen++
			}
			if job.Conclusion == "success" {
				agg.successes++
			}
			if job.Duration > 0 {
				agg.durations = append(agg.durations, float64(job.Duration)/1000.0)
				agg.obs = append(agg.obs, observation{durationSec: float64(job.Duration) / 1000.0, url: job.URL})
			}
			if job.QueueTime > 0 {
				agg.queueTimes = append(agg.queueTimes, float64(job.QueueTime)/1000.0)
			}
			if !job.StartedAt.IsZero() && !runStart.IsZero() {
				offset := job.StartedAt.Sub(runStart)
				if offset >= 0 && offset < 24*time.Hour {
					agg.offsets = append(agg.offsets, offset.Seconds())
				}
			}
			if job.URL != "" {
				agg.urls = append(agg.urls, job.URL)
			}
		}
	}

	if sampledRuns == 0 {
		return nil
	}

	typical := &TypicalRun{
		SampledRuns:    sampledRuns,
		CommitsCovered: len(sampledCommits),
		TotalCommits:   len(allCommits),
	}

	for name, wf := range workflows {
		if wf.sampledRuns == 0 || len(wf.jobs) == 0 {
			continue
		}
		tw := TypicalWorkflow{
			Name:        name,
			SampledRuns: wf.sampledRuns,
			TotalRuns:   wf.totalRuns,
			RunDuration: quantilesOf(wf.runDurations),
		}
		for jobName, agg := range wf.jobs {
			if agg.total == 0 {
				continue
			}
			duration := quantilesOf(agg.durations)
			tw.Jobs = append(tw.Jobs, TypicalJob{
				Name:           jobName,
				URLs:           lastURLs(agg.urls, 5),
				P50URL:         exemplarURL(agg.obs, duration.P50),
				P95URL:         exemplarURL(agg.obs, duration.P95),
				Samples:        agg.total,
				PresenceRate:   float64(agg.runsSeen) / float64(wf.sampledRuns) * 100,
				SuccessRate:    float64(agg.successes) / float64(agg.total) * 100,
				StartOffset:    quantilesOf(agg.offsets),
				Duration:       duration,
				QueueTime:      quantilesOf(agg.queueTimes),
				TrendDirection: trendOf(agg.durations),
			})
		}
		// Order like a timeline: by median start offset, then by name for
		// deterministic output when offsets tie.
		sort.Slice(tw.Jobs, func(i, j int) bool {
			if tw.Jobs[i].StartOffset.P50 != tw.Jobs[j].StartOffset.P50 {
				return tw.Jobs[i].StartOffset.P50 < tw.Jobs[j].StartOffset.P50
			}
			return tw.Jobs[i].Name < tw.Jobs[j].Name
		})
		typical.Workflows = append(typical.Workflows, tw)
	}

	// Busiest workflows first; name breaks ties deterministically.
	sort.Slice(typical.Workflows, func(i, j int) bool {
		if typical.Workflows[i].SampledRuns != typical.Workflows[j].SampledRuns {
			return typical.Workflows[i].SampledRuns > typical.Workflows[j].SampledRuns
		}
		return typical.Workflows[i].Name < typical.Workflows[j].Name
	})

	return typical
}

// trendOf classifies the first-half vs second-half change of chronological
// duration observations, matching the thresholds used elsewhere in trends.
func trendOf(durations []float64) string {
	if len(durations) < 4 {
		return "stable"
	}
	mid := len(durations) / 2
	first := average(durations[:mid])
	second := average(durations[mid:])
	if first <= 0 {
		return "stable"
	}
	change := (second - first) / first * 100
	switch {
	case change < -5:
		return "improving"
	case change > 5:
		return "degrading"
	}
	return "stable"
}

// lastURLs returns up to limit URLs from the end of urls, newest first.
func lastURLs(urls []string, limit int) []string {
	var out []string
	for i := len(urls) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, urls[i])
	}
	return out
}
