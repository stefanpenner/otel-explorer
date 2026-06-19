package export

import "sort"

// Severity ranks an insight for ordering and color-coding across formats.
type Severity string

const (
	SeverityBad  Severity = "bad"  // needs attention (failures, regressions, high flake)
	SeverityWarn Severity = "warn" // worth watching (queue pressure, retries, slow)
	SeverityGood Severity = "good" // positive signal (improving, high success)
	SeverityInfo Severity = "info" // neutral context
)

// severityRank orders insights bad → warn → good → info.
var severityRank = map[Severity]int{SeverityBad: 0, SeverityWarn: 1, SeverityGood: 2, SeverityInfo: 3}

// Insight is one executive-summary finding: a headline, supporting detail, and
// — where actionable — a recommendation. This is the shared "key findings"
// backbone every format renders (callout in HTML, exec summary in DOCX,
// highlights array in JSON, dashboard notes in XLSX).
type Insight struct {
	Severity       Severity `json:"severity"`
	Title          string   `json:"title"`
	Detail         string   `json:"detail,omitempty"`
	Recommendation string   `json:"recommendation,omitempty"`
	URL            string   `json:"url,omitempty"`
}

const maxHighlights = 6

// reportHighlights returns the report's pre-computed highlights, or derives
// them on the fly when absent (e.g. a Report assembled directly in a test).
func reportHighlights(rep *Report) []Insight {
	if len(rep.Highlights) > 0 {
		return rep.Highlights
	}
	return Highlights(rep)
}

// Highlights derives the ranked key findings for a report. Returns at most
// maxHighlights, ordered by severity then source order.
func Highlights(rep *Report) []Insight {
	var ins []Insight
	switch rep.Kind {
	case KindTrends:
		ins = trendInsights(rep.Trends)
	case KindRunAnalysis:
		ins = runInsights(rep.Run)
	}
	sort.SliceStable(ins, func(i, j int) bool {
		return severityRank[ins[i].Severity] < severityRank[ins[j].Severity]
	})
	if len(ins) > maxHighlights {
		ins = ins[:maxHighlights]
	}
	return ins
}

func trendInsights(t *TrendReport) []Insight {
	if t == nil {
		return nil
	}
	var ins []Insight

	// Overall trend direction.
	switch t.Summary.TrendDirection {
	case "degrading":
		ins = append(ins, Insight{
			Severity: SeverityBad,
			Title:    sprintf("Pipeline duration is degrading (%+.0f%%)", t.Summary.PercentChange),
			Detail:   sprintf("Median run %s; p95 %s over the last %dd.", humanSec(t.Summary.MedianDurationSec), humanSec(t.Summary.P95DurationSec), t.Days),
		})
	case "improving":
		ins = append(ins, Insight{
			Severity: SeverityGood,
			Title:    sprintf("Pipeline duration is improving (%+.0f%%)", t.Summary.PercentChange),
			Detail:   sprintf("Median run %s over the last %dd.", humanSec(t.Summary.MedianDurationSec), t.Days),
		})
	}

	// Success rate.
	if t.Summary.TotalRuns > 0 {
		sev := SeverityGood
		if t.Summary.AvgSuccessRatePct < 80 {
			sev = SeverityBad
		} else if t.Summary.AvgSuccessRatePct < 95 {
			sev = SeverityWarn
		}
		in := Insight{Severity: sev, Title: sprintf("%.0f%% average success rate", t.Summary.AvgSuccessRatePct),
			Detail: sprintf("Across %d runs.", t.Summary.TotalRuns)}
		if sev != SeverityGood {
			in.Recommendation = "Investigate the flakiest and most-failed jobs below."
		}
		ins = append(ins, in)
	}

	// Worst regression.
	if len(t.Regressions) > 0 {
		r := t.Regressions[0]
		detail := sprintf("%s → %s on average.", humanSec(r.OldAvgSec), humanSec(r.NewAvgSec))
		rec := "Review the changes at the regression's changepoint."
		if r.NarrowedCommits > 0 {
			noun := "commit"
			if r.NarrowedCommits != 1 {
				noun = "commits"
			}
			detail += sprintf(" Narrowed to %d %s (%s confidence).", r.NarrowedCommits, noun, r.Confidence)
			rec = sprintf("Inspect the %d-%s window where it shifted (code, or infra such as caches/runners).", r.NarrowedCommits, noun)
		}
		ins = append(ins, Insight{
			Severity: SeverityBad, Title: sprintf("%q is %+.0f%% slower", r.Name, r.PercentChange),
			Detail: detail, Recommendation: rec, URL: r.DiffURL,
		})
	}

	// Flakiest job.
	if len(t.FlakyJobs) > 0 {
		f := mostFlaky(t.FlakyJobs)
		if f.FlakeRatePct > 0 {
			detail := sprintf("%d/%d runs failed.", f.FailureCount, f.TotalRuns)
			if f.SameSHAFlakes > 0 {
				detail = sprintf("%s passed and failed on %d identical commit(s) — definitively flaky.", detail, f.SameSHAFlakes)
			}
			ins = append(ins, Insight{
				Severity:       SeverityBad,
				Title:          sprintf("%q is flaky (%.0f%% failure rate)", f.Name, f.FlakeRatePct),
				Detail:         detail,
				Recommendation: "Quarantine or fix this job to stop blocking unrelated changes.",
				URL:            f.SampleURL,
			})
		}
	}

	// Queue pressure.
	if t.QueueStats.QueueRatioPct >= 20 {
		ins = append(ins, Insight{
			Severity:       SeverityWarn,
			Title:          sprintf("Jobs spend %.0f%% of their time queued", t.QueueStats.QueueRatioPct),
			Detail:         sprintf("Median queue %s, p95 %s.", humanSec(t.QueueStats.MedianQueueSec), humanSec(t.QueueStats.P95QueueSec)),
			Recommendation: "Add runner capacity or rebalance concurrency to cut wait time.",
		})
	}

	// Retry burn.
	if t.Summary.RerunRuns > 0 {
		ins = append(ins, Insight{
			Severity: SeverityWarn,
			Title:    sprintf("%s wasted on retries", humanSec(float64(t.Summary.RerunComputeMs)/1000)),
			Detail:   sprintf("%d run(s) were re-run.", t.Summary.RerunRuns),
		})
	}

	// Biggest improvement (positive note).
	if len(t.Improvements) > 0 {
		im := t.Improvements[0]
		ins = append(ins, Insight{
			Severity: SeverityGood,
			Title:    sprintf("%q is %.0f%% faster", im.Name, -im.PercentChange),
			Detail:   sprintf("%s → %s on average.", humanSec(im.OldAvgSec), humanSec(im.NewAvgSec)),
		})
	}

	return ins
}

func runInsights(r *RunReport) []Insight {
	if r == nil {
		return nil
	}
	var ins []Insight

	// Failures.
	if r.Summary.FailedJobs > 0 {
		names := failedJobNames(r, 3)
		ins = append(ins, Insight{
			Severity:       SeverityBad,
			Title:          sprintf("%d job(s) failed", r.Summary.FailedJobs),
			Detail:         names,
			Recommendation: "Inspect the failed jobs' logs and steps.",
		})
	} else if r.Summary.TotalJobs > 0 && r.Summary.JobSuccessRatePct != nil && *r.Summary.JobSuccessRatePct >= 100 {
		ins = append(ins, Insight{Severity: SeverityGood, Title: "All jobs passed",
			Detail: sprintf("%d jobs across %d run(s).", r.Summary.TotalJobs, r.Summary.TotalRuns)})
	}

	// Slowest job and its share of wall clock.
	if j, run := slowestJob(r); j != nil {
		title := sprintf("%q is the slowest job (%s)", j.Name, humanSec(float64(j.DurationMs)/1000))
		detail := ""
		if r.Summary.WallClockMs > 0 {
			detail = sprintf("%.0f%% of the %s wall clock.", float64(j.DurationMs)/float64(r.Summary.WallClockMs)*100, humanSec(float64(r.Summary.WallClockMs)/1000))
		}
		ins = append(ins, Insight{Severity: SeverityInfo, Title: title, Detail: detail, URL: j.URL})
		_ = run
	}

	// Queue pressure.
	if maxQ := maxQueueMs(r); maxQ >= 60_000 {
		ins = append(ins, Insight{
			Severity:       SeverityWarn,
			Title:          sprintf("Up to %s spent queued", humanSec(float64(maxQ)/1000)),
			Recommendation: "Consider more runner capacity if this recurs.",
		})
	}

	return ins
}

func mostFlaky(fs []FlakyJob) FlakyJob {
	best := fs[0]
	for _, f := range fs[1:] {
		if f.SameSHAFlakes > best.SameSHAFlakes || (f.SameSHAFlakes == best.SameSHAFlakes && f.FlakeRatePct > best.FlakeRatePct) {
			best = f
		}
	}
	return best
}

func failedJobNames(r *RunReport, n int) string {
	var names []string
	for _, run := range r.Runs {
		for _, j := range run.Jobs {
			if j.Conclusion == "failure" || j.Conclusion == "timed_out" {
				names = append(names, j.Name)
				if len(names) >= n {
					return joinTruncated(names, r.Summary.FailedJobs)
				}
			}
		}
	}
	return joinTruncated(names, r.Summary.FailedJobs)
}

func joinTruncated(names []string, total int) string {
	if len(names) == 0 {
		return ""
	}
	s := names[0]
	for _, n := range names[1:] {
		s += ", " + n
	}
	if total > len(names) {
		s += sprintf(" (+%d more)", total-len(names))
	}
	return s
}

func slowestJob(r *RunReport) (*Job, *Run) {
	var best *Job
	var bestRun *Run
	for ri := range r.Runs {
		for ji := range r.Runs[ri].Jobs {
			j := &r.Runs[ri].Jobs[ji]
			if best == nil || j.DurationMs > best.DurationMs {
				best, bestRun = j, &r.Runs[ri]
			}
		}
	}
	return best, bestRun
}

func maxQueueMs(r *RunReport) int64 {
	var m int64
	for _, run := range r.Runs {
		if run.MaxQueueMs > m {
			m = run.MaxQueueMs
		}
	}
	return m
}
