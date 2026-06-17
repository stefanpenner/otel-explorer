package export

import (
	"math"
	"strconv"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

// parseRatePtr parses analyzer's formatted rate strings (e.g. "92.3", "92.3%")
// into a percent pointer. An empty/unparseable string ("–" or "" for untyped
// traces) yields nil — unknown, distinct from a real 0%.
func parseRatePtr(s string) *float64 {
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "%"))
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}

// ratePtr returns num/den as a percent pointer, or nil when den is zero
// (no denominator → unknown rather than 0%).
func ratePtr(num, den int) *float64 {
	if den <= 0 {
		return nil
	}
	v := float64(num) / float64(den) * 100
	return &v
}

// BuildRunReport projects single-run/URL analysis into a Report. It prefers
// the GitHub URL results; when those are empty (trace files, the OTLP receiver,
// trace backends) it falls back to runs reconstructed from spans, so those
// inputs still produce a fully populated report. generatedAt should be an
// RFC3339 timestamp supplied by the caller (kept out of this function so it
// stays deterministic and testable).
func BuildRunReport(results []analyzer.URLResult, spanRuns []analyzer.SpanRun, combined analyzer.CombinedMetrics, globalEarliestMs, globalLatestMs int64, generatedAt string) *Report {
	rr := &RunReport{Runs: []Run{}} // never nil, so `.run.runs[]` is jq-safe
	var repo string

	if len(results) > 0 {
		for _, res := range results {
			if repo == "" && res.Owner != "" {
				repo = res.Owner + "/" + res.Repo
			}
			rr.Runs = append(rr.Runs, runFromURLResult(res))
		}
	} else {
		for _, sr := range spanRuns {
			rr.Runs = append(rr.Runs, runFromSpanRun(sr))
		}
	}

	rr.Summary = RunSummary{
		TotalRuns:         combined.TotalRuns,
		TotalJobs:         combined.TotalJobs,
		TotalSteps:        combined.TotalSteps,
		MaxConcurrency:    combined.MaxConcurrency,
		SuccessRatePct:    parseRatePtr(combined.SuccessRate),
		JobSuccessRatePct: parseRatePtr(combined.JobSuccessRate),
		WallClockMs:       maxInt64(globalLatestMs-globalEarliestMs, 0),
	}
	// Derive run/job pass/fail counts from the per-run metrics.
	for _, res := range results {
		m := res.Metrics
		rr.Summary.SuccessfulRuns += m.SuccessfulRuns
		rr.Summary.FailedRuns += m.FailedRuns
		rr.Summary.FailedJobs += m.FailedJobs
	}
	// Span-derived path: combined lacks per-run pass/fail, so count from runs.
	if len(results) == 0 {
		for _, run := range rr.Runs {
			switch run.conclusion {
			case "success":
				rr.Summary.SuccessfulRuns++
			case "failure":
				rr.Summary.FailedRuns++
			}
			rr.Summary.FailedJobs += run.FailedJobs
		}
	}

	return &Report{
		SchemaVersion: SchemaVersion,
		Kind:          KindRunAnalysis,
		Meta:          Meta{Tool: "ote", GeneratedAt: generatedAt, Repo: repo},
		Run:           rr,
	}
}

func runFromURLResult(res analyzer.URLResult) Run {
	m := res.Metrics
	run := Run{
		Repo:              res.Owner + "/" + res.Repo,
		Identifier:        res.Identifier,
		Type:              res.Type,
		Branch:            res.BranchName,
		HeadSHA:           res.HeadSHA,
		DisplayName:       res.DisplayName,
		URL:               res.DisplayURL,
		TotalJobs:         m.TotalJobs,
		FailedJobs:        m.FailedJobs,
		TotalSteps:        m.TotalSteps,
		JobSuccessRatePct: ratePtr(m.TotalJobs-m.FailedJobs, m.TotalJobs),
		AvgQueueMs:        int64(m.AvgQueueTime),
		MaxQueueMs:        int64(m.MaxQueueTime),
	}
	for _, j := range m.JobTimeline {
		dur := j.EndTime - j.StartTime
		if dur < 0 {
			dur = 0
		}
		run.Jobs = append(run.Jobs, Job{
			Name: j.Name, Status: j.Status, Conclusion: j.Conclusion,
			StartMs: j.StartTime, EndMs: j.EndTime, DurationMs: dur,
			Required: j.IsRequired, URL: j.URL,
		})
	}
	for _, s := range m.StepDurations {
		run.Steps = append(run.Steps, Step{Job: s.JobName, Name: s.Name, DurationMs: int64(s.Duration), URL: s.URL})
	}
	return run
}

// runFromSpanRun maps a span-reconstructed run into the report model, computing
// the per-run totals the URLResult path gets from its metrics.
func runFromSpanRun(sr analyzer.SpanRun) Run {
	run := Run{
		Identifier:  sr.Identifier,
		Type:        "run",
		Branch:      sr.Branch,
		DisplayName: sr.Name,
		URL:         sr.URL,
		conclusion:  sr.Conclusion,
	}
	var failed, known int
	for _, j := range sr.Jobs {
		dur := j.EndMs - j.StartMs
		if dur < 0 {
			dur = 0
		}
		if j.Conclusion != "" {
			known++
		}
		if j.Conclusion == "failure" || j.Conclusion == "timed_out" {
			failed++
		}
		run.Jobs = append(run.Jobs, Job{
			Name: j.Name, Status: j.Status, Conclusion: j.Conclusion,
			StartMs: j.StartMs, EndMs: j.EndMs, DurationMs: dur,
			Required: j.Required, URL: j.URL,
		})
		for _, s := range j.Steps {
			run.Steps = append(run.Steps, Step{Job: j.Name, Name: s.Name, DurationMs: s.DurationMs, URL: s.URL})
		}
	}
	run.TotalJobs = len(sr.Jobs)
	run.FailedJobs = failed
	run.TotalSteps = len(run.Steps)
	// Rate over jobs with a known outcome; nil when none are known (untyped).
	run.JobSuccessRatePct = ratePtr(known-failed, known)
	return run
}

// BuildTrendReport projects a trend analysis into a Report.
func BuildTrendReport(a *analyzer.TrendAnalysis, generatedAt string) *Report {
	tr := &TrendReport{
		Days: a.TimeRange.Days,
		Summary: TrendSummary{
			TotalRuns:         a.Summary.TotalRuns,
			AvgDurationSec:    a.Summary.AvgDuration,
			MedianDurationSec: a.Summary.MedianDuration,
			P95DurationSec:    a.Summary.P95Duration,
			AvgSuccessRatePct: a.Summary.AvgSuccessRate,
			TrendDirection:    a.Summary.TrendDirection,
			TrendDescription:  a.Summary.TrendDescription,
			PercentChange:     a.Summary.PercentChange,
			RerunRuns:         a.Summary.RerunRuns,
			RerunComputeMs:    a.Summary.RerunComputeMs,
		},
		QueueStats: QueueStats{
			AvgQueueSec:    a.QueueTimeStats.AvgQueueTime,
			MedianQueueSec: a.QueueTimeStats.MedianQueueTime,
			P95QueueSec:    a.QueueTimeStats.P95QueueTime,
			QueueRatioPct:  a.QueueTimeStats.QueueTimeRatio,
		},
	}

	if a.Typical != nil {
		for _, w := range a.Typical.Workflows {
			tw := TypicalWorkflow{
				Name:        w.Name,
				SampledRuns: w.SampledRuns,
				TotalRuns:   w.TotalRuns,
				RunDuration: quant(w.RunDuration),
			}
			for _, j := range w.Jobs {
				tw.Jobs = append(tw.Jobs, TypicalJob{
					Name:            j.Name,
					Samples:         j.Samples,
					PresenceRatePct: j.PresenceRate,
					SuccessRatePct:  j.SuccessRate,
					StartOffset:     quant(j.StartOffset),
					Duration:        quant(j.Duration),
					QueueTime:       quant(j.QueueTime),
					TrendDirection:  j.TrendDirection,
					P50URL:          j.P50URL,
					P95URL:          j.P95URL,
				})
			}
			tr.Typical = append(tr.Typical, tw)
		}
	}

	for _, f := range a.FlakyJobs {
		tr.FlakyJobs = append(tr.FlakyJobs, FlakyJob{
			Name:            f.Name,
			TotalRuns:       f.TotalRuns,
			SuccessCount:    f.SuccessCount,
			FailureCount:    f.FailureCount,
			FlakeRatePct:    f.FlakeRate,
			RecentFailures:  f.RecentFailures,
			SameSHAFlakes:   f.SameSHAFlakes,
			TransitionScore: f.TransitionScore,
			SampleURL:       first(f.URLs),
		})
	}
	for _, r := range a.TopRegressions {
		tr.Regressions = append(tr.Regressions, JobChange{
			Name:          r.Name,
			OldAvgSec:     r.OldAvgDuration,
			NewAvgSec:     r.NewAvgDuration,
			PercentChange: r.PercentIncrease,
			AbsoluteSec:   r.AbsoluteChange,
			DiffURL:       changepointDiff(r.Changepoint),
		})
	}
	for _, im := range a.TopImprovements {
		tr.Improvements = append(tr.Improvements, JobChange{
			Name:          im.Name,
			OldAvgSec:     im.OldAvgDuration,
			NewAvgSec:     im.NewAvgDuration,
			PercentChange: -im.PercentDecrease,
			AbsoluteSec:   im.AbsoluteChange,
			DiffURL:       changepointDiff(im.Changepoint),
		})
	}
	if a.Hourly != nil {
		for h, b := range a.Hourly.Hours {
			tr.Hourly = append(tr.Hourly, HourBucket{
				Hour:           h,
				RunCount:       b.RunCount,
				QueueP50Sec:    b.QueueP50,
				DurationP50Sec: b.DurationP50,
			})
		}
	}
	for _, p := range a.DurationTrend {
		tr.DailyDuration = append(tr.DailyDuration, DailyPoint{
			Date: p.Timestamp.UTC().Format("2006-01-02"), Value: p.Value, Count: p.Count,
		})
	}
	for _, p := range a.SuccessRateTrend {
		tr.DailySuccess = append(tr.DailySuccess, DailyPoint{
			Date: p.Timestamp.UTC().Format("2006-01-02"), Value: p.Value, Count: p.Count,
		})
	}

	return &Report{
		SchemaVersion: SchemaVersion,
		Kind:          KindTrends,
		Meta:          Meta{Tool: "ote", GeneratedAt: generatedAt, Repo: a.Owner + "/" + a.Repo},
		Trends:        tr,
	}
}

func quant(q analyzer.Quantiles) Quantiles {
	return Quantiles{P5: q.P5, P25: q.P25, P50: q.P50, P75: q.P75, P95: q.P95}
}

func changepointDiff(c *analyzer.Changepoint) string {
	if c == nil {
		return ""
	}
	return c.DiffURL
}

func first(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// pctText renders a nullable percent for human formats: "—" when unknown.
func pctText(p *float64) string {
	if p == nil {
		return "—"
	}
	return strconv.FormatFloat(round1(*p), 'f', -1, 64) + "%"
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }
