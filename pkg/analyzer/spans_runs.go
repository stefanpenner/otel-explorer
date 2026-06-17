package analyzer

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
)

// SpanRun, SpanJob, and SpanStep are workflow runs reconstructed from OTel
// spans, for inputs that have no GitHub URL results (trace files, the OTLP
// receiver, trace backends). They mirror the structure the GitHub analysis
// path produces so a single report builder can consume either source.
type SpanRun struct {
	Identifier string
	Name       string
	URL        string
	Branch     string
	Conclusion string
	Jobs       []SpanJob
}

// SpanJob is a job-level span within a run.
type SpanJob struct {
	Name       string
	Status     string
	Conclusion string
	StartMs    int64
	EndMs      int64
	Required   bool
	URL        string
	Steps      []SpanStep
}

// SpanStep is a leaf-level span within a job.
type SpanStep struct {
	Name       string
	DurationMs int64
	URL        string
}

// RunsFromSpans reconstructs runs/jobs/steps from spans using the same
// enricher-aware classification as the summary and timeline (root → run,
// leaf → step, else → job). Spans the enricher can't categorize (untyped
// traces) and marker spans are skipped. Jobs with no resolvable parent run
// are grouped under a single fallback run so nothing is dropped. Input order
// is preserved for deterministic output.
func RunsFromSpans(spans []sdktrace.ReadOnlySpan, enricher enrichment.Enricher) []SpanRun {
	type classified struct {
		span  sdktrace.ReadOnlySpan
		hints enrichment.SpanHints
		attrs map[string]string
		kind  string // "run", "job", "step", ""
	}

	items := make([]classified, 0, len(spans))
	byID := make(map[trace.SpanID]int, len(spans)) // spanID → index in items
	for _, s := range spans {
		attrs := spanAttrs(s)
		hints := enricher.Enrich(s.Name(), attrs, isZeroDur(s))
		kind := ""
		switch {
		case hints.Category == "" || hints.IsMarker:
			kind = ""
		case isRootSpan(s, hints):
			kind = "run"
		case hints.IsLeaf:
			kind = "step"
		default:
			kind = "job"
		}
		byID[s.SpanContext().SpanID()] = len(items)
		items = append(items, classified{span: s, hints: hints, attrs: attrs, kind: kind})
	}

	var runs []SpanRun
	runIndex := make(map[trace.SpanID]int)  // run spanID → index in runs
	jobLoc := make(map[trace.SpanID][2]int) // job spanID → (runIdx, jobIdx)
	fallbackRun := -1

	ensureFallback := func() int {
		if fallbackRun < 0 {
			runs = append(runs, SpanRun{Name: "runs"})
			fallbackRun = len(runs) - 1
		}
		return fallbackRun
	}
	// findRunIdx resolves the run a non-run span belongs to by walking up the
	// parent chain to the nearest run span, falling back to the catch-all run.
	findRunIdx := func(s sdktrace.ReadOnlySpan) int {
		cur := s.Parent().SpanID()
		for cur.IsValid() {
			idx, ok := byID[cur]
			if !ok {
				break
			}
			if items[idx].kind == "run" {
				if ri, ok := runIndex[cur]; ok {
					return ri
				}
				break
			}
			cur = items[idx].span.Parent().SpanID()
		}
		return ensureFallback()
	}

	// Pass 1: runs (so child lookups can resolve to them).
	for _, it := range items {
		if it.kind != "run" {
			continue
		}
		runs = append(runs, SpanRun{
			Identifier: firstNonEmpty(it.hints.RunID, it.attrs["github.run_id"]),
			Name:       firstNonEmpty(it.attrs["cicd.pipeline.name"], it.span.Name()),
			URL:        firstNonEmpty(it.hints.URL, it.attrs["github.url"]),
			Branch:     it.hints.VCSBranch,
			Conclusion: it.hints.Outcome,
		})
		runIndex[it.span.SpanContext().SpanID()] = len(runs) - 1
	}

	// Pass 2: jobs.
	for _, it := range items {
		if it.kind != "job" {
			continue
		}
		ri := findRunIdx(it.span)
		job := SpanJob{
			Name:       firstNonEmpty(it.attrs["cicd.pipeline.task.name"], it.span.Name()),
			Status:     it.attrs["github.status"],
			Conclusion: it.hints.Outcome,
			StartMs:    it.span.StartTime().UnixMilli(),
			EndMs:      it.span.EndTime().UnixMilli(),
			Required:   it.hints.IsRequired,
			URL:        firstNonEmpty(it.hints.URL, it.attrs["github.url"]),
		}
		runs[ri].Jobs = append(runs[ri].Jobs, job)
		jobLoc[it.span.SpanContext().SpanID()] = [2]int{ri, len(runs[ri].Jobs) - 1}
	}

	// Pass 3: steps, attached to their parent job when resolvable.
	for _, it := range items {
		if it.kind != "step" {
			continue
		}
		loc, ok := jobLoc[it.span.Parent().SpanID()]
		if !ok {
			continue // orphan step (no job parent); counted elsewhere, skip here
		}
		dur := it.span.EndTime().UnixMilli() - it.span.StartTime().UnixMilli()
		if dur < 0 {
			dur = 0
		}
		step := SpanStep{
			Name:       firstNonEmpty(it.attrs["cicd.pipeline.task.name"], it.span.Name()),
			DurationMs: dur,
			URL:        firstNonEmpty(it.hints.URL, it.attrs["github.url"]),
		}
		runs[loc[0]].Jobs[loc[1]].Steps = append(runs[loc[0]].Jobs[loc[1]].Steps, step)
	}

	return runs
}

func spanAttrs(s sdktrace.ReadOnlySpan) map[string]string {
	attrs := make(map[string]string, len(s.Attributes()))
	for _, a := range s.Attributes() {
		// Emit (not AsString) so non-string attributes like github.run_id
		// (int) and github.is_required (bool) stringify instead of vanishing.
		attrs[string(a.Key)] = a.Value.Emit()
	}
	return attrs
}

func isZeroDur(s sdktrace.ReadOnlySpan) bool {
	return s.EndTime().Before(s.StartTime()) || s.EndTime().Equal(s.StartTime())
}
