package export

// Shared presentation helpers used by the HTML and XLSX renderers: KPI cards
// and the tone (good/warn/bad/neutral) color language.

// kpi is a single headline metric card.
type kpi struct {
	Label string
	Value string
	Sub   string
	Tone  string // good | warn | bad | neutral ("" treated as neutral)
}

func runKPIs(r *RunReport) []kpi {
	failTone := "good"
	if r.Summary.FailedJobs > 0 {
		failTone = "bad"
	}
	return []kpi{
		{Label: "Job success", Value: pctText(r.Summary.JobSuccessRatePct), Tone: rateTone(r.Summary.JobSuccessRatePct)},
		{Label: "Jobs", Value: itoa(r.Summary.TotalJobs), Sub: sprintf("%d run(s)", r.Summary.TotalRuns)},
		{Label: "Failed jobs", Value: itoa(r.Summary.FailedJobs), Tone: failTone},
		{Label: "Max concurrency", Value: itoa(r.Summary.MaxConcurrency)},
		{Label: "Wall clock", Value: humanSec(float64(r.Summary.WallClockMs) / 1000)},
	}
}

func trendKPIs(t *TrendReport) []kpi {
	return []kpi{
		{Label: "Avg success", Value: sprintf("%.0f%%", t.Summary.AvgSuccessRatePct), Tone: rateToneF(t.Summary.AvgSuccessRatePct)},
		{Label: "Median run", Value: humanSec(t.Summary.MedianDurationSec), Sub: "p95 " + humanSec(t.Summary.P95DurationSec)},
		{Label: "Flaky jobs", Value: itoa(len(t.FlakyJobs)), Tone: countTone(len(t.FlakyJobs))},
		{Label: "Queued", Value: sprintf("%.0f%%", t.QueueStats.QueueRatioPct), Sub: "of job time", Tone: queueTone(t.QueueStats.QueueRatioPct)},
		{Label: "Retry burn", Value: humanSec(float64(t.Summary.RerunComputeMs) / 1000), Sub: sprintf("%d reruns", t.Summary.RerunRuns)},
	}
}

// --- tone helpers ---

func toneFor(s Severity) string {
	switch s {
	case SeverityBad:
		return "bad"
	case SeverityWarn:
		return "warn"
	case SeverityGood:
		return "good"
	default:
		return "neutral"
	}
}

func conclusionTone(c string) string {
	switch c {
	case "failure", "timed_out":
		return "bad"
	case "cancelled", "skipped":
		return "warn"
	default:
		return ""
	}
}

func rateTone(p *float64) string {
	if p == nil {
		return "neutral"
	}
	return rateToneF(*p)
}

func rateToneF(p float64) string {
	switch {
	case p >= 95:
		return "good"
	case p >= 80:
		return "warn"
	default:
		return "bad"
	}
}

func flakeTone(p float64) string {
	switch {
	case p >= 10:
		return "bad"
	case p > 0:
		return "warn"
	default:
		return ""
	}
}

func countTone(n int) string {
	if n > 0 {
		return "warn"
	}
	return "good"
}

func queueTone(p float64) string {
	switch {
	case p >= 30:
		return "bad"
	case p >= 15:
		return "warn"
	default:
		return "good"
	}
}
