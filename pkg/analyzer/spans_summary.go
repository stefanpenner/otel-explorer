package analyzer

import (
	"fmt"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"go.opentelemetry.io/otel/sdk/trace"
)

// CombinedMetricsFromSpans derives header metrics directly from spans, for
// inputs with no GitHub URL results — trace files, the OTLP receiver, and
// trace backends. Without this fallback those inputs render an all-zero
// header even though the timeline below is fully populated.
func CombinedMetricsFromSpans(spans []trace.ReadOnlySpan, enricher enrichment.Enricher) CombinedMetrics {
	var runs, knownRuns, successRuns, jobs, knownJobs, failedJobs, steps int
	countJob := func(outcome string) {
		jobs++
		if outcome != "" {
			knownJobs++
		}
		if outcome == "failure" {
			failedJobs++
		}
	}
	for _, it := range classifySpans(spans, enricher) {
		switch it.kind {
		case "":
			continue
		case "run":
			runs++
			if it.hints.Outcome != "" {
				knownRuns++
			}
			if it.hints.Outcome == "success" {
				successRuns++
			}
			// Native runner collapses the job into the run-root: count it as a
			// job too so the job total isn't zero for runner-emitted traces.
			if it.runIsJob {
				countJob(it.hints.Outcome)
			}
		case "step":
			steps++
		default:
			countJob(it.hints.Outcome)
		}
	}

	// Empty rate strings mean "unknown" — untyped traces carry no outcome
	// attributes, and 0.0% would misread as everything failing.
	successRate := ""
	if knownRuns > 0 {
		successRate = fmt.Sprintf("%.1f", float64(successRuns)/float64(runs)*100)
	}
	jobSuccessRate := ""
	if knownJobs > 0 {
		jobSuccessRate = fmt.Sprintf("%.1f", float64(jobs-failedJobs)/float64(jobs)*100)
	}

	return CombinedMetrics{
		TotalRuns:      runs,
		TotalJobs:      jobs,
		TotalSteps:     steps,
		SuccessRate:    successRate,
		JobSuccessRate: jobSuccessRate,
		MaxConcurrency: CalculateSummary(spans, enricher).MaxConcurrency,
	}
}

// SpansWallCompute returns wall time (earliest start to latest end) and
// compute time (sum of job-level span durations; root durations when the
// trace has no job spans) in milliseconds, over categorized spans.
func SpansWallCompute(spans []trace.ReadOnlySpan, enricher enrichment.Enricher) (wallMs, computeMs int64) {
	var earliest, latest int64
	var jobMs, rootMs int64
	for _, span := range spans {
		hints := enrichSpan(span, enricher)
		if hints.Category == "" || hints.IsMarker {
			continue
		}
		start := span.StartTime().UnixMilli()
		end := span.EndTime().UnixMilli()
		if end < start {
			continue
		}
		if earliest == 0 || start < earliest {
			earliest = start
		}
		if end > latest {
			latest = end
		}
		switch {
		case isRootSpan(span, hints):
			rootMs += end - start
		case !hints.IsLeaf:
			jobMs += end - start
		}
	}
	if latest > earliest {
		wallMs = latest - earliest
	}
	computeMs = jobMs
	if computeMs == 0 {
		computeMs = rootMs
	}
	return wallMs, computeMs
}

// isRootSpan treats a span as a pipeline root when enrichment says so, or —
// for untyped traces (e.g. Chrome conversions, generic OTLP) — when it has
// no parent.
func isRootSpan(span trace.ReadOnlySpan, hints enrichment.SpanHints) bool {
	return hints.IsRoot || !span.Parent().SpanID().IsValid()
}

// enrichSpan runs the enricher over a span's attributes the same way the
// summary and tree builders do.
func enrichSpan(span trace.ReadOnlySpan, enricher enrichment.Enricher) enrichment.SpanHints {
	attrs := make(map[string]string, len(span.Attributes()))
	for _, a := range span.Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}
	isZeroDuration := span.EndTime().Before(span.StartTime()) || span.EndTime().Equal(span.StartTime())
	return enricher.Enrich(span.Name(), attrs, isZeroDuration)
}
