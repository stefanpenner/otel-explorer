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
	runs, repo := pickRuns(results, spanRuns)
	summary := runSummary(combined, globalEarliestMs, globalLatestMs)
	countOutcomes(&summary, results, runs)

	rep := &Report{
		SchemaVersion: SchemaVersion,
		Kind:          KindRunAnalysis,
		Meta:          Meta{Tool: "ote", GeneratedAt: generatedAt, Repo: repo},
		Run:           &RunReport{Runs: runs, Summary: summary},
	}
	rep.Highlights = Highlights(rep)
	return rep
}

// pickRuns prefers URL results. Span runs fill the report only when results
// are empty. The slice is never nil, so `.run.runs[]` is jq-safe.
func pickRuns(results []analyzer.URLResult, spanRuns []analyzer.SpanRun) (runs []Run, repo string) {
	runs = []Run{}
	if len(results) > 0 {
		for _, res := range results {
			if repo == "" && res.Owner != "" {
				repo = res.Owner + "/" + res.Repo
			}
			runs = append(runs, runFromURLResult(res))
		}
		return runs, repo
	}
	for _, sr := range spanRuns {
		runs = append(runs, runFromSpanRun(sr))
	}
	return runs, ""
}

func runSummary(combined analyzer.CombinedMetrics, earliestMs, latestMs int64) RunSummary {
	return RunSummary{
		TotalRuns:         combined.TotalRuns,
		TotalJobs:         combined.TotalJobs,
		TotalSteps:        combined.TotalSteps,
		MaxConcurrency:    combined.MaxConcurrency,
		SuccessRatePct:    parseRatePtr(combined.SuccessRate),
		JobSuccessRatePct: parseRatePtr(combined.JobSuccessRate),
		WallClockMs:       max(latestMs-earliestMs, int64(0)),
	}
}

// countOutcomes fills pass/fail. URL metrics carry the counts; the span path
// has none on combined, so it counts run conclusions and failed jobs instead.
func countOutcomes(summary *RunSummary, results []analyzer.URLResult, runs []Run) {
	if len(results) > 0 {
		for _, res := range results {
			m := res.Metrics
			summary.SuccessfulRuns += m.SuccessfulRuns
			summary.FailedRuns += m.FailedRuns
			summary.FailedJobs += m.FailedJobs
		}
		return
	}
	for _, run := range runs {
		switch run.conclusion {
		case "success":
			summary.SuccessfulRuns++
		case "failure":
			summary.FailedRuns++
		}
		summary.FailedJobs += run.FailedJobs
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
		Days:          a.TimeRange.Days,
		Summary:       trendSummary(a.Summary),
		QueueStats:    queueStats(a.QueueTimeStats),
		Typical:       typicalWorkflows(a.Typical),
		FlakyJobs:     flakyJobs(a.FlakyJobs),
		Regressions:   regressions(a.TopRegressions),
		Improvements:  improvements(a.TopImprovements),
		Hourly:        hourlyBuckets(a.Hourly),
		DailyDuration: dailyPoints(a.DurationTrend),
		DailySuccess:  dailyPoints(a.SuccessRateTrend),
	}
	rep := &Report{
		SchemaVersion: SchemaVersion,
		Kind:          KindTrends,
		Meta:          Meta{Tool: "ote", GeneratedAt: generatedAt, Repo: a.Owner + "/" + a.Repo},
		Trends:        tr,
	}
	rep.Highlights = Highlights(rep)
	return rep
}

func trendSummary(s analyzer.TrendSummary) TrendSummary {
	return TrendSummary{
		TotalRuns:         s.TotalRuns,
		AvgDurationSec:    s.AvgDuration,
		MedianDurationSec: s.MedianDuration,
		P95DurationSec:    s.P95Duration,
		AvgSuccessRatePct: s.AvgSuccessRate,
		TrendDirection:    s.TrendDirection,
		TrendDescription:  s.TrendDescription,
		PercentChange:     s.PercentChange,
		RerunRuns:         s.RerunRuns,
		RerunComputeMs:    s.RerunComputeMs,
	}
}

func queueStats(q analyzer.QueueTimeStats) QueueStats {
	return QueueStats{
		AvgQueueSec:    q.AvgQueueTime,
		MedianQueueSec: q.MedianQueueTime,
		P95QueueSec:    q.P95QueueTime,
		QueueRatioPct:  q.QueueTimeRatio,
	}
}

func typicalWorkflows(t *analyzer.TypicalRun) []TypicalWorkflow {
	if t == nil {
		return nil
	}
	var out []TypicalWorkflow
	for _, w := range t.Workflows {
		tw := TypicalWorkflow{
			Name:        w.Name,
			SampledRuns: w.SampledRuns,
			TotalRuns:   w.TotalRuns,
			RunDuration: quant(w.RunDuration),
		}
		for _, j := range w.Jobs {
			tw.Jobs = append(tw.Jobs, typicalJob(j))
		}
		out = append(out, tw)
	}
	return out
}

func typicalJob(j analyzer.TypicalJob) TypicalJob {
	return TypicalJob{
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
	}
}

func flakyJobs(in []analyzer.FlakyJob) []FlakyJob {
	var out []FlakyJob
	for _, f := range in {
		out = append(out, FlakyJob{
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
	return out
}

func regressions(in []analyzer.JobRegression) []JobChange {
	var out []JobChange
	for _, r := range in {
		out = append(out, jobChange(r.Name, r.OldAvgDuration, r.NewAvgDuration, r.PercentIncrease, r.AbsoluteChange, r.Changepoint))
	}
	return out
}

func improvements(in []analyzer.JobImprovement) []JobChange {
	var out []JobChange
	for _, im := range in {
		out = append(out, jobChange(im.Name, im.OldAvgDuration, im.NewAvgDuration, -im.PercentDecrease, im.AbsoluteChange, im.Changepoint))
	}
	return out
}

func jobChange(name string, oldSec, newSec, percent, absolute float64, c *analyzer.Changepoint) JobChange {
	jc := JobChange{
		Name:          name,
		OldAvgSec:     oldSec,
		NewAvgSec:     newSec,
		PercentChange: percent,
		AbsoluteSec:   absolute,
	}
	applyChangepoint(&jc, c)
	return jc
}

func hourlyBuckets(h *analyzer.HourlyPatterns) []HourBucket {
	if h == nil {
		return nil
	}
	var out []HourBucket
	for hour, b := range h.Hours {
		out = append(out, HourBucket{
			Hour:           hour,
			RunCount:       b.RunCount,
			QueueP50Sec:    b.QueueP50,
			DurationP50Sec: b.DurationP50,
		})
	}
	return out
}

func dailyPoints(in []analyzer.DataPoint) []DailyPoint {
	var out []DailyPoint
	for _, p := range in {
		out = append(out, DailyPoint{
			Date: p.Timestamp.UTC().Format("2006-01-02"), Value: p.Value, Count: p.Count,
		})
	}
	return out
}

func quant(q analyzer.Quantiles) Quantiles {
	return Quantiles{P5: q.P5, P25: q.P25, P50: q.P50, P75: q.P75, P95: q.P95}
}

// applyChangepoint copies a significant changepoint's localization onto a
// JobChange: the confidence label, the narrowed commit count, and the compare
// URL for the tight window (falling back to the point boundary).
func applyChangepoint(jc *JobChange, c *analyzer.Changepoint) {
	if c == nil {
		return
	}
	jc.Confidence = c.Confidence
	jc.NarrowedCommits = c.RangeCommits
	if c.RangeDiffURL != "" {
		jc.DiffURL = c.RangeDiffURL
	} else {
		jc.DiffURL = c.DiffURL
	}
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

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }
