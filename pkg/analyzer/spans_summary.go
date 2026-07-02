package analyzer

import (
	"fmt"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	apitrace "go.opentelemetry.io/otel/trace"
)

// CombinedMetricsFromSpans derives header metrics directly from spans, for
// inputs with no GitHub URL results — trace files, the OTLP receiver, and
// trace backends. Without this fallback those inputs render an all-zero
// header even though the timeline below is fully populated.
func CombinedMetricsFromSpans(spans []trace.ReadOnlySpan, enricher enrichment.Enricher) CombinedMetrics {
	var runs, knownRuns, successRuns, jobs, knownJobs, failedJobs, steps int
	for _, span := range spans {
		hints := enrichSpan(span, enricher)
		if hints.Category == "" || hints.IsMarker {
			continue
		}
		switch {
		case isRootSpan(span, hints):
			runs++
			if hints.Outcome != "" {
				knownRuns++
			}
			if hints.Outcome == "success" {
				successRuns++
			}
		case hints.IsLeaf:
			steps++
		default:
			jobs++
			if hints.Outcome != "" {
				knownJobs++
			}
			if hints.Outcome == "failure" {
				failedJobs++
			}
		}
	}

	// Empty rate strings mean "unknown" — untyped traces carry no outcome
	// attributes, and 0.0% would misread as everything failing. Rates are
	// computed over known outcomes only: dividing by the full count would
	// treat unknown runs as failures and unknown jobs as successes.
	successRate := ""
	if knownRuns > 0 {
		successRate = fmt.Sprintf("%.1f", float64(successRuns)/float64(knownRuns)*100)
	}
	jobSuccessRate := ""
	if knownJobs > 0 {
		jobSuccessRate = fmt.Sprintf("%.1f", float64(knownJobs-failedJobs)/float64(knownJobs)*100)
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

// SpanEnrichmentAttrs flattens a span's attributes for enrichment. It uses
// Emit() (AsString returns "" for int/bool/double values, which would hide
// e.g. rpc.grpc.status_code from enrichers) and injects the span's OTel
// status and kind as the synthetic attributes the enrichers read
// (otel.status_code / otel.span_kind, uppercase per OTel convention) —
// without this only Jaeger files, which persist them as literal tags,
// carried them, and the documented status→Outcome mapping was dead code.
// Real attributes with those names win over the injected values.
func SpanEnrichmentAttrs(span trace.ReadOnlySpan) map[string]string {
	attrs := make(map[string]string, len(span.Attributes())+2)
	for _, a := range span.Attributes() {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	if _, ok := attrs["otel.status_code"]; !ok && span.Status().Code != codes.Unset {
		attrs["otel.status_code"] = strings.ToUpper(span.Status().Code.String())
	}
	if _, ok := attrs["otel.span_kind"]; !ok && span.SpanKind() != apitrace.SpanKindUnspecified {
		attrs["otel.span_kind"] = strings.ToUpper(span.SpanKind().String())
	}
	return attrs
}

// enrichSpan runs the enricher over a span's attributes the same way the
// summary and tree builders do.
func enrichSpan(span trace.ReadOnlySpan, enricher enrichment.Enricher) enrichment.SpanHints {
	isZeroDuration := span.EndTime().Before(span.StartTime()) || span.EndTime().Equal(span.StartTime())
	return enricher.Enrich(span.Name(), SpanEnrichmentAttrs(span), isZeroDuration)
}
