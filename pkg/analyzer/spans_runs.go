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

// spanClass is the run/job/step classification of a single span.
type spanClass struct {
	span  sdktrace.ReadOnlySpan
	hints enrichment.SpanHints
	attrs map[string]string
	kind  string // "run", "job", "step", ""
	// runIsJob marks the collapsed job-as-root emitted by the native GitHub
	// Actions runner: a trace-root span that carries run-level attributes
	// (cicd.pipeline.run.id) AND task-level attributes (cicd.pipeline.task.*).
	// The runner emits no separate workflow-run span above the job, so this
	// single span represents BOTH the run and the job for counting.
	runIsJob bool
}

// presentSpanIDs returns the set of span IDs present in the batch, so root
// detection can treat a span whose parent is absent (a "dangling" parent ref)
// as a trace root. The native runner emits the job as the trace root with its
// parent pointing at a workflow-run span it never exports, so that ref dangles.
func presentSpanIDs(spans []sdktrace.ReadOnlySpan) map[trace.SpanID]bool {
	ids := make(map[trace.SpanID]bool, len(spans))
	for _, s := range spans {
		ids[s.SpanContext().SpanID()] = true
	}
	return ids
}

// isTraceRoot reports whether a span is a root of the ingested batch: it has no
// parent, or its parent is not present among the spans (dangling ref).
func isTraceRoot(span sdktrace.ReadOnlySpan, present map[trace.SpanID]bool) bool {
	parent := span.Parent().SpanID()
	if !parent.IsValid() {
		return true
	}
	return !present[parent]
}

// classifySpans assigns each span a run/job/step kind using enricher hints plus
// trace topology. It recognizes two CI shapes:
//
//   - ote's GitHub-API model: a workflow-run root (run) → jobs → steps, keyed
//     off the enricher's IsRoot/IsLeaf hints.
//   - the native GitHub Actions runner contract: a job span as the trace root
//     carrying cicd.pipeline.run.id (the run) with cicd.pipeline.task.* step
//     children. There is no separate workflow-run span, so the job-root counts
//     as both the run and the job, and its cicd task children count as steps.
func classifySpans(spans []sdktrace.ReadOnlySpan, enricher enrichment.Enricher) []spanClass {
	present := presentSpanIDs(spans)
	items := make([]spanClass, 0, len(spans))

	// Pass A: enrich and identify run-roots (incl. the native runner's
	// job-as-root). runnerRunRoots holds the span IDs of native runner roots so
	// their cicd task children can be classified as steps in pass B.
	runnerRunRoots := make(map[trace.SpanID]bool)
	for _, s := range spans {
		attrs := spanAttrs(s)
		hints := enricher.Enrich(s.Name(), attrs, isZeroDur(s))
		root := isTraceRoot(s, present)
		kind := ""
		runIsJob := false
		switch {
		case hints.Category == "" || hints.IsMarker:
			kind = ""
		case root && attrs["cicd.pipeline.run.id"] != "":
			// Native runner job-as-root: the run. If it also carries task
			// attributes it is the collapsed job too.
			kind = "run"
			runIsJob = attrs["cicd.pipeline.task.name"] != ""
			if runIsJob {
				runnerRunRoots[s.SpanContext().SpanID()] = true
			}
		case hints.IsRoot || !s.Parent().SpanID().IsValid():
			// Enricher-declared roots, or truly parentless spans (untyped /
			// Chrome-converted traces). A merely dangling parent does NOT
			// promote a span to a run — that is handled above, only for the
			// native runner contract (cicd.pipeline.run.id present).
			kind = "run"
		case hints.IsLeaf:
			kind = "step"
		default:
			kind = "job"
		}
		items = append(items, spanClass{span: s, hints: hints, attrs: attrs, kind: kind, runIsJob: runIsJob})
	}

	// Pass B: native runner task spans are a flat run→task tree. A cicd task
	// span whose parent is the job-as-root is a step, not a job. (ote's own
	// GitHub-API model and multi-level pipelines are untouched: their job spans
	// don't parent directly to a runner run-root.)
	if len(runnerRunRoots) > 0 {
		for i := range items {
			it := &items[i]
			if it.kind != "job" || it.attrs["cicd.pipeline.task.name"] == "" {
				continue
			}
			if runnerRunRoots[it.span.Parent().SpanID()] {
				it.kind = "step"
			}
		}
	}
	return items
}

// RunsFromSpans reconstructs runs/jobs/steps from spans using the same
// enricher-aware classification as the summary and timeline (root → run,
// leaf → step, else → job). Spans the enricher can't categorize (untyped
// traces) and marker spans are skipped. Jobs with no resolvable parent run
// are grouped under a single fallback run so nothing is dropped. Input order
// is preserved for deterministic output.
func RunsFromSpans(spans []sdktrace.ReadOnlySpan, enricher enrichment.Enricher) []SpanRun {
	type classified = spanClass

	classes := classifySpans(spans, enricher)
	items := make([]classified, 0, len(spans))
	byID := make(map[trace.SpanID]int, len(spans)) // spanID → index in items
	for _, it := range classes {
		byID[it.span.SpanContext().SpanID()] = len(items)
		items = append(items, it)
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
			// Prefer the run-level id; cicd.pipeline.run.id is authoritative on
			// the native runner's job-as-root, where hints.RunID would hold the
			// task run id instead.
			Identifier: firstNonEmpty(it.attrs["cicd.pipeline.run.id"], it.hints.RunID, it.attrs["github.run_id"]),
			Name:       firstNonEmpty(it.attrs["cicd.pipeline.name"], it.span.Name()),
			URL:        firstNonEmpty(it.hints.URL, it.attrs["github.url"]),
			Branch:     it.hints.VCSBranch,
			Conclusion: it.hints.Outcome,
		})
		ri := len(runs) - 1
		runIndex[it.span.SpanContext().SpanID()] = ri

		// Native runner job-as-root: the run span is also the job. Materialize
		// the implicit job so step children (whose parent is this span) nest
		// under it, and so job counts are non-zero.
		if it.runIsJob {
			runs[ri].Jobs = append(runs[ri].Jobs, SpanJob{
				Name:       firstNonEmpty(it.attrs["cicd.pipeline.task.name"], it.span.Name()),
				Status:     it.attrs["github.status"],
				Conclusion: it.hints.Outcome,
				StartMs:    it.span.StartTime().UnixMilli(),
				EndMs:      it.span.EndTime().UnixMilli(),
				Required:   it.hints.IsRequired,
				URL:        firstNonEmpty(it.hints.URL, it.attrs["cicd.pipeline.task.run.url.full"], it.attrs["github.url"]),
			})
			jobLoc[it.span.SpanContext().SpanID()] = [2]int{ri, len(runs[ri].Jobs) - 1}
		}
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
