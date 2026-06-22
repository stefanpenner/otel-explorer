package analyzer

import (
	"sort"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Summary represents the aggregated metrics from a set of spans.
type Summary struct {
	TotalRuns      int
	SuccessfulRuns int
	TotalJobs      int
	FailedJobs     int
	MaxConcurrency int
	// Enrichment metrics extracted from span attributes
	AvgQueueTimeMs float64
	MaxQueueTimeMs float64
	QueueCount     int
	RetriedRuns    int
	BillableMs     map[string]int64 // OS name → total ms (e.g. "ubuntu", "macos", "windows")
}

// isSyntheticQueueSpan reports whether the span is a synthetic "queued"
// filler span ("⏳ Queued" / "⏳ Workflow Queued"). These are visual-only:
// the real job span already carries queue_time_ms, so counting them as jobs
// would double queue stats and inflate TotalJobs/MaxConcurrency.
func isSyntheticQueueSpan(attrs map[string]string) bool {
	t := attrs["type"]
	return t == "queued" || t == "workflow_queued"
}

// CalculateSummary analyzes OTel spans to produce a high-level summary.
// It uses the provided enricher to classify spans as root/child and determine outcome.
func CalculateSummary(spans []trace.ReadOnlySpan, enricher enrichment.Enricher) Summary {
	s := Summary{
		BillableMs: make(map[string]int64),
	}

	var totalQueueMs float64

	// Per-run aggregation so retry attempts (one root span per attempt,
	// sharing the same github.run_id) count as a single logical run —
	// matching the Metrics-path semantics.
	type runInfo struct {
		maxAttempt int64
		success    bool
		retried    bool
	}
	runsByID := make(map[int64]*runInfo)

	// classifySpans aligns run/job/step detection with the markdown summary and
	// timeline, so the native GitHub Actions runner's job-as-root (no separate
	// workflow-run span) is recognized as a run here too.
	classes := classifySpans(spans, enricher)
	classByID := make(map[string]spanClass, len(classes))
	for _, c := range classes {
		classByID[c.span.SpanContext().SpanID().String()] = c
	}

	for _, span := range spans {
		attrs := make(map[string]string)
		var attrInts map[string]int64
		for _, a := range span.Attributes() {
			key := string(a.Key)
			attrs[key] = a.Value.AsString()
			// Capture int64 attributes for billable/queue/retry/run identity
			if strings.HasPrefix(key, "billable.") || key == "queue_time_ms" || key == "github.run_attempt" || key == "github.run_id" {
				if attrInts == nil {
					attrInts = make(map[string]int64)
				}
				attrInts[key] = a.Value.AsInt64()
			}
		}

		if isSyntheticQueueSpan(attrs) {
			continue
		}

		isZeroDuration := span.EndTime().Before(span.StartTime()) || span.EndTime().Equal(span.StartTime())
		hints := enricher.Enrich(span.Name(), attrs, isZeroDuration)

		if hints.Category == "" {
			continue
		}

		cls := classByID[span.SpanContext().SpanID().String()]

		// Native runner job-as-root is a run AND a job: count the job here, then
		// fall through to the run-counting branch below.
		if cls.runIsJob {
			s.TotalJobs++
			if hints.Outcome == "failure" {
				s.FailedJobs++
			}
		}

		if cls.kind == "run" {
			attempt := attrInts["github.run_attempt"]
			if attempt == 0 {
				attempt = 1
			}
			if runID, ok := attrInts["github.run_id"]; ok {
				info := runsByID[runID]
				if info == nil {
					info = &runInfo{}
					runsByID[runID] = info
				}
				if attempt > info.maxAttempt {
					info.maxAttempt = attempt
					info.success = hints.Outcome == "success"
				}
				if attempt > 1 {
					info.retried = true
				}
			} else {
				// Root span without a run ID (e.g. external OTLP trace)
				s.TotalRuns++
				if hints.Outcome == "success" {
					s.SuccessfulRuns++
				}
				if attempt > 1 {
					s.RetriedRuns++
				}
			}
			// Extract billable data from workflow spans
			for key, ms := range attrInts {
				if strings.HasPrefix(key, "billable.") && strings.HasSuffix(key, "_ms") {
					// "billable.ubuntu_ms" → "ubuntu"
					osName := strings.TrimSuffix(strings.TrimPrefix(key, "billable."), "_ms")
					s.BillableMs[osName] += ms
				}
			}
		} else if cls.kind == "job" {
			// "job"-level span (excludes native runner step spans, which are
			// classified as "step" even though they carry cicd.pipeline.task.*).
			s.TotalJobs++
			if hints.Outcome == "failure" {
				s.FailedJobs++
			}
			// Extract queue time from job spans
			if qMs, ok := attrInts["queue_time_ms"]; ok && qMs > 0 {
				s.QueueCount++
				totalQueueMs += float64(qMs)
				if float64(qMs) > s.MaxQueueTimeMs {
					s.MaxQueueTimeMs = float64(qMs)
				}
			}
		}
	}

	for _, info := range runsByID {
		s.TotalRuns++
		if info.success {
			s.SuccessfulRuns++
		}
		if info.retried {
			s.RetriedRuns++
		}
	}

	if s.QueueCount > 0 {
		s.AvgQueueTimeMs = totalQueueMs / float64(s.QueueCount)
	}

	s.MaxConcurrency = CalculateConcurrency(spans, enricher)
	return s
}

// CalculateConcurrency calculates the maximum number of overlapping non-root,
// non-marker, non-leaf spans (i.e. "job"-level concurrency).
func CalculateConcurrency(spans []trace.ReadOnlySpan, enricher enrichment.Enricher) int {
	type event struct {
		ts    time.Time
		delta int // +1 for start, -1 for end
	}

	var events []event
	for _, s := range spans {
		attrs := make(map[string]string)
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}

		// Synthetic queue spans overlap other running jobs and would
		// inflate the concurrency count.
		if isSyntheticQueueSpan(attrs) {
			continue
		}

		isZeroDuration := s.EndTime().Before(s.StartTime()) || s.EndTime().Equal(s.StartTime())
		hints := enricher.Enrich(s.Name(), attrs, isZeroDuration)

		if hints.Category == "" || hints.IsRoot || hints.IsMarker || hints.IsLeaf {
			continue
		}

		events = append(events, event{s.StartTime(), 1})
		events = append(events, event{s.EndTime(), -1})
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].ts.Equal(events[j].ts) {
			return events[i].delta < events[j].delta // End before start if same time
		}
		return events[i].ts.Before(events[j].ts)
	})

	maxConcurrency := 0
	curr := 0
	for _, e := range events {
		curr += e.delta
		if curr > maxConcurrency {
			maxConcurrency = curr
		}
	}
	return maxConcurrency
}
