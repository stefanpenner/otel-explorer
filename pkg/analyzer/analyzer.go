package analyzer

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Instrumentation identity for all GHA-generated spans.
const instrumentationName = "github.com/stefanpenner/otel-explorer/pkg/analyzer"

// SpanBuilder accumulates tracetest.SpanStubs and converts them to ReadOnlySpans.
// Thread-safe for concurrent use by processWorkflowRun goroutines.
type SpanBuilder struct {
	mu    sync.Mutex
	stubs tracetest.SpanStubs
}

func (b *SpanBuilder) Add(stub tracetest.SpanStub) {
	// Stamp every span with our instrumentation scope.
	if stub.InstrumentationScope.Name == "" {
		stub.InstrumentationScope.Name = instrumentationName
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stubs = append(b.stubs, stub)
}

func (b *SpanBuilder) Spans() []sdktrace.ReadOnlySpan {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stubs.Snapshots()
}

type ProgressReporter interface {
	StartURL(urlIndex int, url string)
	SetURLRuns(runCount int)
	SetPhase(phase string)
	SetDetail(detail string)
	ProcessRun()
	Finish()
}

type URLError struct {
	URL string
	Err error
}

func (e URLError) Error() string {
	return fmt.Sprintf("Error processing URL %s: %s", e.URL, e.Err.Error())
}

type AnalyzeOptions struct {
	Window      time.Duration
	NoArtifacts bool
	FetchLogs   bool
}

func AnalyzeURLs(ctx context.Context, urls []string, client githubapi.GitHubProvider, reporter ProgressReporter, opts AnalyzeOptions) ([]URLResult, []TraceEvent, int64, int64, []sdktrace.ReadOnlySpan, []URLError) {
	allTraceEvents := []TraceEvent{}
	allJobStartTimes := []JobEvent{}
	allJobEndTimes := []JobEvent{}
	urlResults := []URLResult{}
	globalEarliestTime := int64(1<<63 - 1)
	globalLatestTime := int64(0)
	urlErrors := []URLError{}

	builder := &SpanBuilder{}
	provider := NewDataProvider(client)
	emitter := NewTraceEmitter(builder)

	for urlIndex, githubURL := range urls {
		if reporter != nil {
			reporter.StartURL(urlIndex, githubURL)
		}
		
		rawData, err := provider.Fetch(ctx, githubURL, urlIndex, reporter, opts)
		if err != nil {
			urlErrors = append(urlErrors, URLError{URL: githubURL, Err: err})
			continue
		}
		if rawData == nil {
			continue
		}

		// Emit marker spans for review/merge events
		emitter.EmitMarkers(rawData, urlIndex)

		// Calculate urlEarliestTime here to ensure it's consistent
		urlEarliestTime := FindEarliestTimestamp(rawData.Runs)
		if rawData.CommitTimeMs != nil && *rawData.CommitTimeMs < urlEarliestTime {
			urlEarliestTime = *rawData.CommitTimeMs
		}
		if rawData.CommitPushedAtMs != nil && *rawData.CommitPushedAtMs < urlEarliestTime {
			urlEarliestTime = *rawData.CommitPushedAtMs
		}
		for _, event := range rawData.ReviewEvents {
			ms := event.TimeMillis()
			if ms < urlEarliestTime {
				urlEarliestTime = ms
			}
		}

		result, err := buildURLResult(ctx, rawData.Parsed, urlIndex, rawData.HeadSHA, rawData.BranchName, rawData.DisplayName, rawData.DisplayURL, rawData.ReviewEvents, rawData.MergedAtMs, rawData.CommitTimeMs, rawData.CommitPushedAtMs, rawData.AllCommitRunsCount, rawData.AllCommitRunsComputeMs, rawData.Runs, rawData.RequiredContexts, rawData.ChangedFilesCount, rawData.ChangedAdditions, rawData.ChangedDeletions, client, reporter, urlEarliestTime, builder, emitter, opts)
		if err != nil {
			urlErrors = append(urlErrors, URLError{URL: githubURL, Err: err})
			continue
		}
		if result == nil {
			continue
		}
		urlResults = append(urlResults, *result)
		allTraceEvents = append(allTraceEvents, result.TraceEvents...)
		allJobStartTimes = append(allJobStartTimes, result.JobStartTimes...)
		allJobEndTimes = append(allJobEndTimes, result.JobEndTimes...)

		if result.EarliestTime < globalEarliestTime {
			globalEarliestTime = result.EarliestTime
		}
		// Calculate the actual latest time from all events in this result
		urlLatest := result.EarliestTime
		for _, job := range result.Metrics.JobTimeline {
			if job.EndTime > urlLatest {
				urlLatest = job.EndTime
			}
		}
		for _, event := range result.ReviewEvents {
			ms := event.TimeMillis()
			if ms > urlLatest {
				urlLatest = ms
			}
		}
		if urlLatest > globalLatestTime {
			globalLatestTime = urlLatest
		}
	}

	if reporter != nil {
		reporter.Finish()
	}

	if len(urlResults) == 0 {
		return nil, nil, 0, 0, nil, urlErrors
	}

	GenerateConcurrencyCounters(allJobStartTimes, allJobEndTimes, &allTraceEvents, globalEarliestTime)
	addReviewMarkersToTrace(urlResults, &allTraceEvents)

	combinedTrace := append([]TraceEvent{}, allTraceEvents...)
	allTraceEvents = combinedTrace
	return urlResults, allTraceEvents, globalEarliestTime, globalLatestTime, builder.Spans(), urlErrors
}

func buildURLResult(ctx context.Context, parsed utils.ParsedGitHubURL, urlIndex int, headSHA, branchName, displayName, displayURL string, reviewEvents []ReviewEvent, mergedAtMs, commitTimeMs, commitPushedAtMs *int64, allCommitRunsCount int, allCommitRunsComputeMs int64, runs []githubapi.WorkflowRun, requiredContexts []string, changedFilesCount, changedAdditions, changedDeletions int, client githubapi.GitHubProvider, reporter ProgressReporter, urlEarliestTime int64, builder *SpanBuilder, emitter *TraceEmitter, opts AnalyzeOptions) (*URLResult, error) {
	if reporter != nil {
		reporter.SetURLRuns(len(runs))
		reporter.SetPhase("Processing workflow runs")
		reporter.SetDetail(fmt.Sprintf("%d runs", len(runs)))
	}
	metrics := InitializeMetrics()
	traceEvents := []TraceEvent{}
	jobStartTimes := []JobEvent{}
	jobEndTimes := []JobEvent{}
	
	type runResult struct {
		metrics     Metrics
		traceEvents []TraceEvent
		jobStarts   []JobEvent
		jobEnds     []JobEvent
		err         error
	}

	workerCount := minInt(runtime.GOMAXPROCS(0), len(runs))
	if workerCount == 0 {
		workerCount = 1
	}

	jobsCh := make(chan struct {
		index int
		run   githubapi.WorkflowRun
	})
	resultsCh := make(chan runResult, len(runs))
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobsCh {
				processID := (urlIndex+1)*1000 + job.index + 1
				runMetrics, runTrace, runStarts, runEnds, err := processWorkflowRun(ctx, job.run, job.index, processID, urlEarliestTime, parsed.Owner, parsed.Repo, parsed.Identifier, urlIndex, displayURL, parsed.Type, requiredContexts, changedFilesCount, changedAdditions, changedDeletions, client, reporter, builder, emitter, opts)
				resultsCh <- runResult{
					metrics:     runMetrics,
					traceEvents: runTrace,
					jobStarts:   runStarts,
					jobEnds:     runEnds,
					err:         err,
				}
			}
		}()
	}

	for runIndex, run := range runs {
		jobsCh <- struct {
			index int
			run   githubapi.WorkflowRun
		}{index: runIndex, run: run}
	}
	close(jobsCh)
	wg.Wait()
	close(resultsCh)

	for result := range resultsCh {
		if result.err != nil {
			return nil, result.err
		}
		mergeMetrics(&metrics, result.metrics)
		traceEvents = append(traceEvents, result.traceEvents...)
		jobStartTimes = append(jobStartTimes, result.jobStarts...)
		jobEndTimes = append(jobEndTimes, result.jobEnds...)
		if reporter != nil {
			reporter.ProcessRun()
		}
	}

	finalMetrics := CalculateFinalMetrics(metrics, len(runs), jobStartTimes, jobEndTimes)
	result := URLResult{
		Owner:                  parsed.Owner,
		Repo:                   parsed.Repo,
		Identifier:             parsed.Identifier,
		BranchName:             branchName,
		HeadSHA:                headSHA,
		Metrics:                finalMetrics,
		TraceEvents:            traceEvents,
		Type:                   parsed.Type,
		DisplayName:            displayName,
		DisplayURL:             displayURL,
		URLIndex:               urlIndex,
		JobStartTimes:          jobStartTimes,
		JobEndTimes:            jobEndTimes,
		EarliestTime:           urlEarliestTime,
		ReviewEvents:           reviewEvents,
		MergedAtMs:             mergedAtMs,
		CommitTimeMs:           commitTimeMs,
		CommitPushedAtMs:       commitPushedAtMs,
		AllCommitRunsCount:     allCommitRunsCount,
		AllCommitRunsComputeMs: allCommitRunsComputeMs,
	}
	return &result, nil
}

func processWorkflowRun(ctx context.Context, run githubapi.WorkflowRun, runIndex, processID int, earliestTime int64, owner, repo, identifier string, urlIndex int, displayURL, sourceType string, requiredContexts []string, changedFilesCount, changedAdditions, changedDeletions int, client githubapi.GitHubProvider, reporter ProgressReporter, builder *SpanBuilder, emitter *TraceEmitter, opts AnalyzeOptions) (Metrics, []TraceEvent, []JobEvent, []JobEvent, error) {
	metrics := InitializeMetrics()
	traceEvents := []TraceEvent{}
	jobStartTimes := []JobEvent{}
	jobEndTimes := []JobEvent{}

	metrics.TotalRuns = 1
	if run.Status == "completed" && run.Conclusion == "success" {
		metrics.SuccessfulRuns = 1
	} else {
		metrics.FailedRuns = 1
	}
	if run.RunAttempt > 1 {
		metrics.RetriedRuns = 1
	}

	baseURL := fmt.Sprintf("https://api.github.com/repos/%s/%s", run.Repository.Owner.Login, run.Repository.Name)
	jobsURL := fmt.Sprintf("%s/actions/runs/%d/jobs?per_page=100", baseURL, run.ID)
	if reporter != nil {
		reporter.SetPhase("Fetching jobs")
		reporter.SetDetail(defaultRunName(run))
	}
	jobs, err := client.FetchJobsPaginated(ctx, jobsURL)
	if err != nil {
		return metrics, traceEvents, jobStartTimes, jobEndTimes, err
	}

	// Fetch and process previous retry attempts.
	// The default /jobs endpoint only returns the latest attempt's jobs,
	// so we must fetch each previous attempt explicitly.
	if run.RunAttempt > 1 {
		for attempt := int64(1); attempt < run.RunAttempt; attempt++ {
			attemptJobsURL := fmt.Sprintf("%s/actions/runs/%d/attempts/%d/jobs?per_page=100", baseURL, run.ID, attempt)
			attemptJobs, err := client.FetchJobsPaginated(ctx, attemptJobsURL)
			if err != nil {
				continue // best-effort: skip attempts we can't fetch
			}
			processPreviousAttempt(attempt, attemptJobs, run, processID, earliestTime, owner, repo, identifier, urlIndex, displayURL, sourceType, requiredContexts, builder, &traceEvents, &metrics, &jobStartTimes, &jobEndTimes)
		}
	}

	runStart, ok := utils.ParseTime(run.CreatedAt)
	if !ok {
		return metrics, traceEvents, jobStartTimes, jobEndTimes, nil
	}
	// For retried runs, CreatedAt is from the original attempt. Use RunStartedAt
	// as the effective start of this attempt so timing reflects this attempt only.
	if run.RunAttempt > 1 && run.RunStartedAt != "" {
		if retryStart, ok := utils.ParseTime(run.RunStartedAt); ok {
			runStart = retryStart
		}
	}
	runEnd, ok := utils.ParseTime(run.UpdatedAt)
	if !ok {
		runEnd = runStart.Add(time.Millisecond)
	}

	workflowURL := fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d", run.Repository.Owner.Login, run.Repository.Name, run.ID)
	if run.RunAttempt > 1 {
		workflowURL = fmt.Sprintf("%s/attempts/%d", workflowURL, run.RunAttempt)
	}

	tid := githubapi.NewTraceID(run.ID, run.RunAttempt)
	wfSID := githubapi.NewSpanID(run.ID)
	wfSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     wfSID,
		TraceFlags: trace.FlagsSampled,
	})

	runEndTs := runEnd.UnixMilli()
	runStartTs := runStart.UnixMilli()
	for _, job := range jobs {
		if job.Status != "completed" {
			continue
		}
		if t, ok := utils.ParseTime(job.CompletedAt); ok {
			if t.UnixMilli() > runEndTs {
				runEndTs = t.UnixMilli()
			}
		}
	}
	// Clamp runEndTs to not be too far in the future if the run is completed
	if run.Status == "completed" && runEndTs > runStartTs+24*3600*1000 {
		// If a run claims to take more than 24h but jobs are short, something is wrong.
		// We'll use the max job end time instead.
		maxJobEnd := runStartTs
		for _, job := range jobs {
			if t, ok := utils.ParseTime(job.CompletedAt); ok {
				if t.UnixMilli() > maxJobEnd {
					maxJobEnd = t.UnixMilli()
				}
			}
		}
		if maxJobEnd > runStartTs {
			runEndTs = maxJobEnd
		}
	}
	runDurationMs := runEndTs - runStartTs
	metrics.TotalDuration += float64(runDurationMs)

	sourceInfo := sourceType
	if sourceType == "pr" {
		sourceInfo = fmt.Sprintf("PR #%s", identifier)
	} else {
		sourceInfo = fmt.Sprintf("commit %s", truncateString(identifier, 8))
	}

	processName := fmt.Sprintf("[%d] %s - %s (%s)", urlIndex+1, sourceInfo, defaultRunName(run), run.Status)
	colors := []string{"#4285f4", "#ea4335", "#fbbc04", "#34a853", "#ff6d01", "#46bdc6", "#7b1fa2", "#d81b60"}
	colorIndex := urlIndex % len(colors)

	traceEvents = append(traceEvents, TraceEvent{
		Name: "process_name",
		Ph:   "M",
		Pid:  processID,
		Args: map[string]interface{}{
			"name":              processName,
			"source_url":        displayURL,
			"source_type":       sourceType,
			"source_identifier": identifier,
			"repository":        fmt.Sprintf("%s/%s", owner, repo),
		},
	})
	traceEvents = append(traceEvents, TraceEvent{
		Name: "process_color",
		Ph:   "M",
		Pid:  processID,
		Args: map[string]interface{}{
			"color":      colors[colorIndex],
			"color_name": fmt.Sprintf("url_%d_color", urlIndex+1),
		},
	})

	workflowThreadID := 1
	AddThreadMetadata(&traceEvents, processID, workflowThreadID, "📋 Workflow Overview", intPtr(0))

	prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%s", owner, repo, identifier)

	normalizedRunStart := (runStartTs - earliestTime) * 1000
	normalizedRunEnd := (runEndTs - earliestTime) * 1000
	traceEvents = append(traceEvents, TraceEvent{
		Name: fmt.Sprintf("Workflow: %s [%d]", defaultRunName(run), urlIndex+1),
		Ph:   "X",
		Ts:   normalizedRunStart,
		Dur:  normalizedRunEnd - normalizedRunStart,
		Pid:  processID,
		Tid:  workflowThreadID,
		Cat:  "workflow",
		Args: map[string]interface{}{
			"status":            run.Status,
			"conclusion":        run.Conclusion,
			"run_id":            run.ID,
			"duration_ms":       runDurationMs,
			"job_count":         len(jobs),
			"url":               workflowURL,
			"github_url":        workflowURL,
			"pr_url":            prURL,
			"pr_number":         identifier,
			"repository":        fmt.Sprintf("%s/%s", owner, repo),
			"source_url":        displayURL,
			"source_type":       sourceType,
			"source_identifier": identifier,
			"url_index":         urlIndex + 1,
			"run_attempt":       run.RunAttempt,
		},
	})

	// Emit workflow-level queue span (CreatedAt → RunStartedAt)
	if run.RunStartedAt != "" {
		if runStartedAt, ok := utils.ParseTime(run.RunStartedAt); ok && runStartedAt.After(runStart) {
			queueDurMs := runStartedAt.UnixMilli() - runStartTs
			normalizedQueueStart := (runStartTs - earliestTime) * 1000
			normalizedQueueEnd := (runStartedAt.UnixMilli() - earliestTime) * 1000
			traceEvents = append(traceEvents, TraceEvent{
				Name: fmt.Sprintf("⏳ Workflow Queued [%d]", urlIndex+1),
				Ph:   "X",
				Ts:   normalizedQueueStart,
				Dur:  normalizedQueueEnd - normalizedQueueStart,
				Pid:  processID,
				Tid:  workflowThreadID,
				Cat:  "queued",
				Args: map[string]interface{}{
					"type":          "workflow_queued",
					"queue_time_ms": queueDurMs,
				},
			})

			queueSID := githubapi.NewSpanIDFromString(fmt.Sprintf("wf-queued-%d", run.ID))
			builder.Add(tracetest.SpanStub{
				Name: "⏳ Workflow Queued",
				SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    tid,
					SpanID:     queueSID,
					TraceFlags: trace.FlagsSampled,
				}),
				Parent:    wfSC,
				StartTime: runStart,
				EndTime:   runStartedAt,
				Attributes: []attribute.KeyValue{
					attribute.String("type", "workflow_queued"),
					attribute.Int64("queue_time_ms", queueDurMs),
				},
			})
		}
	}

	// Fetch annotations for failed check runs (best-effort)
	jobAnnotations := map[string][]githubapi.Annotation{}
	checkRuns, err := client.FetchCheckRunsForCommit(ctx, run.Repository.Owner.Login, run.Repository.Name, run.HeadSHA)
	if err == nil {
		for _, cr := range checkRuns {
			if cr.Conclusion == "failure" {
				annotations, err := client.FetchAnnotations(ctx, run.Repository.Owner.Login, run.Repository.Name, cr.ID)
				if err == nil && len(annotations) > 0 {
					jobAnnotations[cr.Name] = append(jobAnnotations[cr.Name], annotations...)
				}
			}
		}
	}

	for jobIndex, job := range jobs {
		jobThreadID := jobIndex + 10
		processJob(job, jobIndex, run, jobThreadID, processID, earliestTime, &metrics, &traceEvents, &jobStartTimes, &jobEndTimes, prURL, urlIndex, displayURL, sourceType, identifier, requiredContexts, builder, tid, wfSC, jobAnnotations[job.Name])
	}

	// Fetch billable timing (best-effort, don't fail on error)
	if run.Status == "completed" {
		timing, err := client.FetchRunTiming(ctx, run.Repository.Owner.Login, run.Repository.Name, run.ID)
		if err == nil && timing != nil {
			for osName, billable := range timing.Billable {
				if billable.TotalMs > 0 {
					metrics.BillableMs[osName] += billable.TotalMs
				}
			}
		}
	}

	// Ingest trace artifacts (best-effort) and capture full artifact list
	var runArtifacts []githubapi.Artifact
	if !opts.NoArtifacts {
		runArtifacts, _ = IngestTraceArtifacts(ctx, client, run.Repository.Owner.Login, run.Repository.Name, run.ID, builder, urlIndex, wfSC)
	}

	// Ingest step logs (best-effort) for sub-step span extraction
	if opts.FetchLogs {
		_ = IngestStepLogs(ctx, client, owner, repo, jobs, builder, urlIndex, tid, nil)
	}

	// Build workflow span stub (after processing jobs so runEnd may be adjusted)
	wfAttrs := []attribute.KeyValue{
		// OTel CI/CD semconv
		attribute.String("cicd.pipeline.name", defaultRunName(run)),
		attribute.String("cicd.pipeline.run.id", fmt.Sprintf("%d", run.ID)),
		attribute.String("cicd.pipeline.run.url.full", workflowURL),
		attribute.String("vcs.repository.url.full", fmt.Sprintf("https://github.com/%s/%s", owner, repo)),
		// GitHub-specific (preserved for compatibility + no semconv equivalent)
		attribute.String("type", "workflow"),
		attribute.Int64("github.run_id", run.ID),
		attribute.String("github.status", run.Status),
		attribute.String("github.conclusion", run.Conclusion),
		attribute.String("github.repo", fmt.Sprintf("%s/%s", owner, repo)),
		attribute.String("github.url", workflowURL),
		attribute.Int("github.url_index", urlIndex),
		attribute.String("cicd.pipeline.definition", run.Path),
	}
	if run.HeadSHA != "" {
		wfAttrs = append(wfAttrs, attribute.String("vcs.revision", run.HeadSHA))
	}
	if run.HeadBranch != "" {
		wfAttrs = append(wfAttrs, attribute.String("vcs.ref.head.name", run.HeadBranch))
	}
	// Map conclusion to cicd.pipeline.run.result
	wfAttrs = append(wfAttrs, attribute.String("cicd.pipeline.run.result", ghConclusionToResult(run.Conclusion)))
	if run.RunAttempt > 1 {
		wfAttrs = append(wfAttrs, attribute.Int64("github.run_attempt", run.RunAttempt))
	}
	// Add billable timing attributes
	for osName, ms := range metrics.BillableMs {
		wfAttrs = append(wfAttrs, attribute.Int64(fmt.Sprintf("billable.%s_ms", strings.ToLower(osName)), ms))
	}

	// Add changed file stats as VCS attributes (from PR/commit metadata — no extra API call)
	if changedFilesCount > 0 {
		wfAttrs = append(wfAttrs,
			attribute.String("vcs.changes.count", fmt.Sprintf("%d", changedFilesCount)),
			attribute.String("vcs.changes.additions", fmt.Sprintf("%d", changedAdditions)),
			attribute.String("vcs.changes.deletions", fmt.Sprintf("%d", changedDeletions)),
		)
	}

	// Add uploaded artifact metadata (from ListArtifacts — already called, no extra API call)
	if len(runArtifacts) > 0 {
		var names []string
		var totalSize int64
		idx := 0
		for _, a := range runArtifacts {
			if !a.Expired {
				names = append(names, a.Name)
				totalSize += a.SizeInBytes
				artifactURL := fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d/artifacts/%d",
					run.Repository.Owner.Login, run.Repository.Name, run.ID, a.ID)
				wfAttrs = append(wfAttrs,
					attribute.String(fmt.Sprintf("cicd.pipeline.artifact.%d.name", idx), a.Name),
					attribute.String(fmt.Sprintf("cicd.pipeline.artifact.%d.size", idx), formatBytes(a.SizeInBytes)),
					attribute.String(fmt.Sprintf("cicd.pipeline.artifact.%d.url", idx), artifactURL),
				)
				idx++
			}
		}
		if len(names) > 0 {
			wfAttrs = append(wfAttrs,
				attribute.String("cicd.pipeline.artifacts.count", fmt.Sprintf("%d", len(names))),
				attribute.String("cicd.pipeline.artifacts.names", strings.Join(names, ", ")),
				attribute.String("cicd.pipeline.artifacts.size", formatBytes(totalSize)),
			)
		}
	}

	// Collect review/merge events as span events on the workflow root span
	wfEvents := emitter.CollectEvents(urlIndex, tid)

	// Build span links for retry attempts
	var wfLinks []sdktrace.Link
	if run.RunAttempt > 1 {
		prevTID := githubapi.NewTraceID(run.ID, run.RunAttempt-1)
		prevWfSID := previousAttemptSpanID(run.ID, run.RunAttempt-1)
		wfLinks = append(wfLinks, sdktrace.Link{
			SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    prevTID,
				SpanID:     prevWfSID,
				TraceFlags: trace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("link.type", "retry"),
				attribute.Int64("github.previous_attempt", run.RunAttempt-1),
			},
		})
	}

	wfName := defaultRunName(run)
	if run.RunAttempt > 1 {
		wfName = fmt.Sprintf("#%d %s", run.RunAttempt, wfName)
	}

	builder.Add(tracetest.SpanStub{
		Name:        wfName,
		SpanContext: wfSC,
		StartTime:   runStart,
		EndTime:     runEnd,
		Attributes:  wfAttrs,
		Events:      wfEvents,
		Links:       wfLinks,
		Status:      ghConclusionToStatus(run.Conclusion),
	})

	return metrics, traceEvents, jobStartTimes, jobEndTimes, nil
}

// processPreviousAttempt creates a synthetic workflow span and job spans for a previous
// retry attempt. This surfaces the full retry history in the trace tree.
func processPreviousAttempt(attempt int64, jobs []githubapi.Job, run githubapi.WorkflowRun, processID int, earliestTime int64, owner, repo, identifier string, urlIndex int, displayURL, sourceType string, requiredContexts []string, builder *SpanBuilder, traceEvents *[]TraceEvent, metrics *Metrics, jobStartTimes, jobEndTimes *[]JobEvent) {
	if len(jobs) == 0 {
		return
	}

	tid := githubapi.NewTraceID(run.ID, attempt)
	wfSID := previousAttemptSpanID(run.ID, attempt)
	wfSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     wfSID,
		TraceFlags: trace.FlagsSampled,
	})

	// Derive workflow timing and conclusion from jobs
	var wfStart, wfEnd time.Time
	conclusion := "success"
	for _, job := range jobs {
		if t, ok := utils.ParseTime(job.CreatedAt); ok && (wfStart.IsZero() || t.Before(wfStart)) {
			wfStart = t
		}
		if t, ok := utils.ParseTime(job.CompletedAt); ok && (wfEnd.IsZero() || t.After(wfEnd)) {
			wfEnd = t
		}
		if job.Conclusion == "failure" {
			conclusion = "failure"
		} else if job.Conclusion == "cancelled" && conclusion != "failure" {
			conclusion = "cancelled"
		}
	}
	if wfStart.IsZero() {
		return
	}
	if wfEnd.IsZero() {
		wfEnd = wfStart.Add(time.Millisecond)
	}

	workflowURL := fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d/attempts/%d", run.Repository.Owner.Login, run.Repository.Name, run.ID, attempt)
	attemptName := fmt.Sprintf("#%d %s", attempt, defaultRunName(run))

	wfAttrs := []attribute.KeyValue{
		attribute.String("cicd.pipeline.name", attemptName),
		attribute.String("cicd.pipeline.run.id", fmt.Sprintf("%d", run.ID)),
		attribute.String("cicd.pipeline.run.url.full", workflowURL),
		attribute.String("vcs.repository.url.full", fmt.Sprintf("https://github.com/%s/%s", owner, repo)),
		attribute.String("type", "workflow"),
		attribute.Int64("github.run_id", run.ID),
		attribute.String("github.status", "completed"),
		attribute.String("github.conclusion", conclusion),
		attribute.String("github.repo", fmt.Sprintf("%s/%s", owner, repo)),
		attribute.String("github.url", workflowURL),
		attribute.Int("github.url_index", urlIndex),
		attribute.Int64("github.run_attempt", attempt),
		attribute.String("cicd.pipeline.run.result", ghConclusionToResult(conclusion)),
		attribute.String("cicd.pipeline.definition", run.Path),
	}

	// Span link to previous attempt
	var wfLinks []sdktrace.Link
	if attempt > 1 {
		prevTID := githubapi.NewTraceID(run.ID, attempt-1)
		prevWfSID := previousAttemptSpanID(run.ID, attempt-1)
		wfLinks = append(wfLinks, sdktrace.Link{
			SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    prevTID,
				SpanID:     prevWfSID,
				TraceFlags: trace.FlagsSampled,
			}),
			Attributes: []attribute.KeyValue{
				attribute.String("link.type", "retry"),
				attribute.Int64("github.previous_attempt", attempt-1),
			},
		})
	}

	builder.Add(tracetest.SpanStub{
		Name:        attemptName,
		SpanContext: wfSC,
		StartTime:   wfStart,
		EndTime:     wfEnd,
		Attributes:  wfAttrs,
		Links:       wfLinks,
		Status:      ghConclusionToStatus(conclusion),
	})

	// Process each job under this attempt's workflow span
	prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%s", owner, repo, identifier)
	for jobIndex, job := range jobs {
		jobThreadID := jobIndex + 10
		processJob(job, jobIndex, run, jobThreadID, processID, earliestTime, metrics, traceEvents, jobStartTimes, jobEndTimes, prURL, urlIndex, displayURL, sourceType, identifier, requiredContexts, builder, tid, wfSC, nil)
	}
}

func processJob(job githubapi.Job, jobIndex int, run githubapi.WorkflowRun, jobThreadID, processID int, earliestTime int64, metrics *Metrics, traceEvents *[]TraceEvent, jobStartTimes, jobEndTimes *[]JobEvent, prURL string, urlIndex int, displayURL, sourceType, identifier string, requiredContexts []string, builder *SpanBuilder, traceID trace.TraceID, parentSC trace.SpanContext, annotations []githubapi.Annotation) {
	if job.StartedAt == "" {
		return
	}

	jobStart, _ := utils.ParseTime(job.StartedAt)
	jobEnd, _ := utils.ParseTime(job.CompletedAt)

	jobURL := job.HTMLURL
	if jobURL == "" {
		jobURL = fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d/job/%d", run.Repository.Owner.Login, run.Repository.Name, run.ID, job.ID)
	}

	jobSID := githubapi.NewSpanID(job.ID)
	jobSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     jobSID,
		TraceFlags: trace.FlagsSampled,
	})

	// Determine if this job is a required status check
	isRequired := isJobRequired(job.Name, run.Name, requiredContexts)
	requiredSuffix := ""
	if isRequired {
		requiredSuffix = " 🔒"
	}

	isPending := job.Status != "completed" || job.CompletedAt == ""
	if isPending {
		metrics.PendingJobs = append(metrics.PendingJobs, PendingJob{
			Name:       job.Name,
			Status:     job.Status,
			StartedAt:  job.StartedAt,
			URL:        jobURL,
			IsRequired: isRequired,
		})
	}

	absoluteJobStart, ok := utils.ParseTime(job.StartedAt)
	if !ok {
		return
	}
	absoluteJobEnd := time.Now()
	if !isPending {
		if t, ok := utils.ParseTime(job.CompletedAt); ok {
			absoluteJobEnd = t
		}
	}

	metrics.TotalJobs++
	if !isPending && (job.Status != "completed" || job.Conclusion != "success") {
		metrics.FailedJobs++
	}

	jobStartTs := absoluteJobStart.UnixMilli()
	jobEndTs := maxInt64(jobStartTs+1, absoluteJobEnd.UnixMilli())
	jobDuration := jobEndTs - jobStartTs
	if jobStartTs >= jobEndTs || jobDuration <= 0 {
		return
	}

	metrics.JobDurations = append(metrics.JobDurations, float64(jobDuration))
	metrics.JobNames = append(metrics.JobNames, job.Name)
	metrics.JobURLs = append(metrics.JobURLs, jobURL)
	if jobDuration > int64(metrics.LongestJob.Duration) {
		metrics.LongestJob = JobDuration{Name: job.Name, Duration: float64(jobDuration)}
	}
	if float64(jobDuration) < metrics.ShortestJob.Duration {
		metrics.ShortestJob = JobDuration{Name: job.Name, Duration: float64(jobDuration)}
	}
	if job.RunnerName != "" {
		metrics.RunnerJobCounts[job.RunnerName]++
		metrics.RunnerDurations[job.RunnerName] += float64(jobDuration)
	}

	// Queue time: CreatedAt → StartedAt (only for jobs that actually ran)
	if job.CreatedAt != "" && job.Conclusion != "skipped" && job.Conclusion != "cancelled" {
		if createdAt, ok := utils.ParseTime(job.CreatedAt); ok {
			queueMs := float64(absoluteJobStart.UnixMilli() - createdAt.UnixMilli())
			if queueMs > 0 {
				metrics.QueueTimes = append(metrics.QueueTimes, queueMs)
			}
		}
	}

	*jobStartTimes = append(*jobStartTimes, JobEvent{Ts: jobStartTs, Type: "start"})
	*jobEndTimes = append(*jobEndTimes, JobEvent{Ts: jobEndTs, Type: "end"})

	jobIcon := "❓"
	if isPending {
		jobIcon = "⏳"
	} else if job.Conclusion == "failure" {
		// No icon here, handled in output rendering
	} else if job.Conclusion == "skipped" || job.Conclusion == "cancelled" {
		jobIcon = "⏸️"
	}

	metrics.JobTimeline = append(metrics.JobTimeline, TimelineJob{
		Name:       job.Name,
		StartTime:  jobStartTs,
		EndTime:    jobEndTs,
		Conclusion: job.Conclusion,
		Status:     job.Status,
		URL:        jobURL,
		IsRequired: isRequired,
	})

	jobLabel := fmt.Sprintf("%s %s%s", jobIcon, job.Name, requiredSuffix)
	if job.Conclusion == "failure" {
		jobLabel = fmt.Sprintf("%s%s ❌", job.Name, requiredSuffix)
	}
	AddThreadMetadata(traceEvents, processID, jobThreadID, jobLabel, intPtr(jobIndex+10))

	// Emit queued span (CreatedAt → StartedAt) — only for jobs that actually ran
	if job.CreatedAt != "" && job.Conclusion != "skipped" && job.Conclusion != "cancelled" {
		if createdAt, ok := utils.ParseTime(job.CreatedAt); ok {
			queueStartTs := createdAt.UnixMilli()
			if queueStartTs < jobStartTs {
				normalizedQueueStart := (queueStartTs - earliestTime) * 1000
				normalizedQueueEnd := (jobStartTs - earliestTime) * 1000
				queueDurMs := jobStartTs - queueStartTs
				*traceEvents = append(*traceEvents, TraceEvent{
					Name: fmt.Sprintf("⏳ Queued [%d]", urlIndex+1),
					Ph:   "X",
					Ts:   normalizedQueueStart,
					Dur:  normalizedQueueEnd - normalizedQueueStart,
					Pid:  processID,
					Tid:  jobThreadID,
					Cat:  "queued",
					Args: map[string]interface{}{
						"type":            "queued",
						"github.job_name": job.Name,
						"queue_time_ms":   queueDurMs,
					},
				})

				// Emit OTel span for queued period (child of job, not workflow)
				queueSID := githubapi.NewSpanIDFromString(fmt.Sprintf("queued-%d", job.ID))
				builder.Add(tracetest.SpanStub{
					Name: "⏳ Queued",
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID:    traceID,
						SpanID:     queueSID,
						TraceFlags: trace.FlagsSampled,
					}),
					Parent:    jobSC,
					StartTime: createdAt,
					EndTime:   absoluteJobStart,
					Attributes: []attribute.KeyValue{
						attribute.String("type", "queued"),
						attribute.String("github.job_name", job.Name),
						attribute.Int64("queue_time_ms", queueDurMs),
					},
				})
			}
		}
	}

	normalizedJobStart := (jobStartTs - earliestTime) * 1000
	normalizedJobEnd := (jobEndTs - earliestTime) * 1000
	*traceEvents = append(*traceEvents, TraceEvent{
		Name: fmt.Sprintf("Job: %s%s [%d]", job.Name, requiredSuffix, urlIndex+1),
		Ph:   "X",
		Ts:   normalizedJobStart,
		Dur:  normalizedJobEnd - normalizedJobStart,
		Pid:  processID,
		Tid:  jobThreadID,
		Cat:  "job",
		Args: map[string]interface{}{
			"status":            job.Status,
			"conclusion":        job.Conclusion,
			"duration_ms":       jobDuration,
			"runner_name":       defaultString(job.RunnerName, "unknown"),
			"step_count":        len(job.Steps),
			"url":               jobURL,
			"github_url":        jobURL,
			"pr_url":            prURL,
			"pr_number":         lastPathSegment(prURL),
			"repository":        repoFromURL(prURL),
			"job_id":            job.ID,
			"source_url":        displayURL,
			"source_type":       sourceType,
			"source_identifier": identifier,
			"url_index":         urlIndex + 1,
			"is_required":       isRequired,
		},
	})

	for _, step := range job.Steps {
		processStep(step, job, run, jobThreadID, processID, earliestTime, jobEndTs, metrics, traceEvents, prURL, urlIndex, displayURL, sourceType, identifier, builder, traceID, jobSC)
	}

	jobAttrs := []attribute.KeyValue{
		// OTel CI/CD semconv
		attribute.String("cicd.pipeline.task.name", job.Name),
		attribute.String("cicd.pipeline.task.type", "build"),
		attribute.String("cicd.pipeline.task.run.id", fmt.Sprintf("%d", job.ID)),
		attribute.String("cicd.pipeline.task.run.url.full", jobURL),
		attribute.String("cicd.pipeline.task.run.result", ghConclusionToResult(job.Conclusion)),
		// GitHub-specific (preserved)
		attribute.String("type", "job"),
		attribute.Int64("github.job_id", job.ID),
		attribute.String("github.status", job.Status),
		attribute.String("github.conclusion", job.Conclusion),
		attribute.String("github.runner_name", job.RunnerName),
		attribute.String("github.url", jobURL),
		attribute.Bool("github.is_required", isRequired),
	}
	if job.RunnerName != "" {
		jobAttrs = append(jobAttrs, attribute.String("k8s.pod.name", job.RunnerName))
	}
	// Add queue_time_ms if we have CreatedAt
	if job.CreatedAt != "" {
		if createdAt, ok := utils.ParseTime(job.CreatedAt); ok {
			queueMs := absoluteJobStart.UnixMilli() - createdAt.UnixMilli()
			if queueMs > 0 {
				jobAttrs = append(jobAttrs, attribute.Int64("queue_time_ms", queueMs))
			}
		}
	}
	// Build annotation events for the job span
	var spanEvents []sdktrace.Event
	for _, ann := range annotations {
		title := ann.Title
		if title == "" {
			title = ann.Message
		}
		spanEvents = append(spanEvents, sdktrace.Event{
			Name: title,
			Attributes: []attribute.KeyValue{
				attribute.String("annotation.path", ann.Path),
				attribute.Int("annotation.line", ann.StartLine),
				attribute.String("annotation.level", ann.Level),
				attribute.String("annotation.message", ann.Message),
			},
		})
	}
	builder.Add(tracetest.SpanStub{
		Name:        job.Name + requiredSuffix,
		SpanContext: jobSC,
		Parent:      parentSC,
		StartTime:   jobStart,
		EndTime:     jobEnd,
		Attributes:  jobAttrs,
		Events:      spanEvents,
		Status:      ghConclusionToStatus(job.Conclusion),
	})
}

func processStep(step githubapi.Step, job githubapi.Job, run githubapi.WorkflowRun, jobThreadID, processID int, earliestTime, jobEndTs int64, metrics *Metrics, traceEvents *[]TraceEvent, prURL string, urlIndex int, displayURL, sourceType, identifier string, builder *SpanBuilder, traceID trace.TraceID, parentSC trace.SpanContext) {
	if step.StartedAt == "" || step.CompletedAt == "" {
		return
	}

	jobURL := job.HTMLURL
	if jobURL == "" {
		jobURL = fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d/job/%d", run.Repository.Owner.Login, run.Repository.Name, run.ID, job.ID)
	}

	start, ok := utils.ParseTime(step.StartedAt)
	if !ok {
		return
	}
	end, ok := utils.ParseTime(step.CompletedAt)
	if !ok {
		return
	}

	stepURL := fmt.Sprintf("%s#step:%d:1", jobURL, step.Number)

	stepSID := githubapi.NewSpanIDFromString(fmt.Sprintf("%d-%s", job.ID, step.Name))
	stepSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     stepSID,
		TraceFlags: trace.FlagsSampled,
	})

	builder.Add(tracetest.SpanStub{
		Name:        step.Name,
		SpanContext: stepSC,
		Parent:      parentSC,
		StartTime:   start,
		EndTime:     end,
		Attributes: []attribute.KeyValue{
			// OTel CI/CD semconv
			attribute.String("cicd.pipeline.task.name", step.Name),
			attribute.String("cicd.pipeline.task.type", "build"),
			attribute.String("cicd.pipeline.task.run.result", ghConclusionToResult(step.Conclusion)),
			attribute.String("cicd.pipeline.task.run.url.full", stepURL),
			// GitHub-specific
			attribute.String("type", "step"),
			attribute.Int("github.step_number", step.Number),
			attribute.String("github.status", step.Status),
			attribute.String("github.conclusion", step.Conclusion),
			attribute.String("github.url", stepURL),
		},
		Status: ghConclusionToStatus(step.Conclusion),
	})

	metrics.TotalSteps++
	if step.Conclusion == "failure" {
		metrics.FailedSteps++
	}

	stepStart := start.UnixMilli()
	stepEnd := maxInt64(stepStart+1, end.UnixMilli())
	if stepEnd > jobEndTs {
		stepEnd = maxInt64(stepStart+1, jobEndTs)
	}
	duration := stepEnd - stepStart
	if stepStart >= stepEnd || duration <= 0 {
		return
	}

	stepIcon := utils.GetStepIcon(step.Name, step.Conclusion)
	stepCategory := utils.CategorizeStep(step.Name)

	metrics.StepDurations = append(metrics.StepDurations, StepDuration{
		Name:     fmt.Sprintf("%s %s", stepIcon, step.Name),
		Duration: float64(duration),
		URL:      stepURL,
		JobName:  job.Name,
	})

	normalizedStepStart := (stepStart - earliestTime) * 1000
	normalizedStepEnd := (stepEnd - earliestTime) * 1000
	*traceEvents = append(*traceEvents, TraceEvent{
		Name: fmt.Sprintf("%s %s [%d]", stepIcon, step.Name, urlIndex+1),
		Ph:   "X",
		Ts:   normalizedStepStart,
		Dur:  normalizedStepEnd - normalizedStepStart,
		Pid:  processID,
		Tid:  jobThreadID,
		Cat:  stepCategory,
		Args: map[string]interface{}{
			"status":            step.Status,
			"conclusion":        step.Conclusion,
			"duration_ms":       duration,
			"job_name":          job.Name,
			"url":               stepURL,
			"github_url":        stepURL,
			"pr_url":            prURL,
			"pr_number":         lastPathSegment(prURL),
			"repository":        repoFromURL(prURL),
			"step_number":       step.Number,
			"source_url":        displayURL,
			"source_type":       sourceType,
			"source_identifier": identifier,
			"url_index":         urlIndex + 1,
		},
	})
}

func addReviewMarkersToTrace(urlResults []URLResult, events *[]TraceEvent) {
	metricsProcessID := 999
	markersThreadID := 2
	AddThreadMetadata(events, metricsProcessID, markersThreadID, "GitHub PR Events", intPtr(1))

	for i := range urlResults {
		result := &urlResults[i]
		if len(result.ReviewEvents) == 0 {
			continue
		}
		
		for _, event := range result.ReviewEvents {
			originalEventTime := event.TimeMillis()
			ts := (originalEventTime - result.EarliestTime) * 1000
			name := "Approved"
			user := event.Reviewer
			label := utils.YellowText("▲ approved")
			if event.Type == "merged" {
				name = "Merged"
				user = event.MergedBy
				label = utils.GreenText("◆ merged")
			}
			if user != "" {
				if event.Type == "merged" {
					label = utils.GreenText(fmt.Sprintf("◆ merged by %s", user))
				} else {
					label = utils.YellowText(fmt.Sprintf("▲ approved by %s", user))
				}
			}

			userURL := ""
			if user != "" {
				userURL = fmt.Sprintf("https://github.com/%s", user)
			}
			marker := TraceEvent{
				Name: name,
				Ph:   "i",
				S:    "p",
				Ts:   ts,
				Pid:  metricsProcessID,
				Tid:  markersThreadID,
				Args: map[string]interface{}{
					"url_index":              result.URLIndex + 1,
					"source_url":             firstNonEmpty(event.URL, result.DisplayURL),
					"github_url":             firstNonEmpty(event.URL, result.DisplayURL),
					"url":                    firstNonEmpty(event.URL, result.DisplayURL),
					"source_type":            result.Type,
					"source_identifier":      result.Identifier,
					"user":                   user,
					"user_url":               userURL,
					"label":                  label,
					"original_event_time_ms": originalEventTime,
					"clamped":                false,
				},
			}
			*events = append(*events, marker)
			// Also add to the individual result so ingestors see it
			result.TraceEvents = append(result.TraceEvents, marker)
		}
	}
}

func shipItMatch(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "ship it") || strings.Contains(lower, "shipit")
}

func truncateString(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

// previousAttemptSpanID returns a deterministic span ID for a previous retry attempt's
// workflow span. Must differ from NewSpanID(runID) so tree.go's spanID-keyed map
// doesn't collide with the current attempt.
func previousAttemptSpanID(runID, attempt int64) trace.SpanID {
	return githubapi.NewSpanIDFromString(fmt.Sprintf("wf-attempt-%d-%d", runID, attempt))
}

func defaultRunName(run githubapi.WorkflowRun) string {
	if run.Name != "" {
		return run.Name
	}
	return fmt.Sprintf("Run %d", run.ID)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func lastPathSegment(urlValue string) string {
	parts := strings.Split(strings.TrimRight(urlValue, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func repoFromURL(urlValue string) string {
	parts := strings.Split(strings.TrimRight(urlValue, "/"), "/")
	if len(parts) < 4 {
		return ""
	}
	return strings.Join(parts[len(parts)-4:len(parts)-2], "/")
}

func maxJobEnd(events []JobEvent) int64 {
	max := int64(0)
	for _, event := range events {
		if event.Type == "end" && event.Ts > max {
			max = event.Ts
		}
	}
	return max
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func isJobRequired(jobName, workflowName string, requiredContexts []string) bool {
	if len(requiredContexts) == 0 {
		return true // No branch protection = treat all as required (preserve current behavior)
	}

	// GitHub status checks can match: "workflow / job", "job", or "workflow"
	fullName := fmt.Sprintf("%s / %s", workflowName, jobName)

	for _, ctx := range requiredContexts {
		if ctx == fullName || ctx == jobName || ctx == workflowName {
			return true
		}
		// Handle matrix jobs: "test (ubuntu, 18)" matches "test"
		if strings.HasPrefix(fullName, ctx) || strings.HasPrefix(jobName, ctx) {
			return true
		}
	}
	return false
}

// ghConclusionToResult maps a GitHub conclusion to a cicd.pipeline.run.result value.
func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func ghConclusionToResult(conclusion string) string {
	switch conclusion {
	case "success":
		return "success"
	case "failure":
		return "failure"
	case "cancelled":
		return "cancelled"
	case "skipped":
		return "skipped"
	case "timed_out":
		return "error"
	default:
		return conclusion
	}
}

// ghConclusionToStatus maps a GitHub conclusion to an OTel span status.
func ghConclusionToStatus(conclusion string) sdktrace.Status {
	switch conclusion {
	case "success":
		return sdktrace.Status{Code: codes.Ok}
	case "failure", "timed_out":
		return sdktrace.Status{Code: codes.Error, Description: conclusion}
	case "cancelled":
		return sdktrace.Status{Code: codes.Ok, Description: "Cancelled"}
	case "skipped":
		return sdktrace.Status{Code: codes.Ok, Description: "Skipped"}
	default:
		return sdktrace.Status{Code: codes.Unset}
	}
}

func mergeMetrics(target *Metrics, source Metrics) {
	target.TotalRuns += source.TotalRuns
	target.SuccessfulRuns += source.SuccessfulRuns
	target.FailedRuns += source.FailedRuns
	target.TotalJobs += source.TotalJobs
	target.FailedJobs += source.FailedJobs
	target.TotalSteps += source.TotalSteps
	target.FailedSteps += source.FailedSteps
	target.TotalDuration += source.TotalDuration

	target.JobDurations = append(target.JobDurations, source.JobDurations...)
	target.JobNames = append(target.JobNames, source.JobNames...)
	target.JobURLs = append(target.JobURLs, source.JobURLs...)
	target.StepDurations = append(target.StepDurations, source.StepDurations...)
	target.JobTimeline = append(target.JobTimeline, source.JobTimeline...)
	target.PendingJobs = append(target.PendingJobs, source.PendingJobs...)

	for runner, count := range source.RunnerJobCounts {
		target.RunnerJobCounts[runner] += count
	}
	for runner, dur := range source.RunnerDurations {
		target.RunnerDurations[runner] += dur
	}
	target.QueueTimes = append(target.QueueTimes, source.QueueTimes...)
	target.RetriedRuns += source.RetriedRuns
	for os, ms := range source.BillableMs {
		target.BillableMs[os] += ms
	}

	if source.LongestJob.Duration > target.LongestJob.Duration {
		target.LongestJob = source.LongestJob
	}
	if source.ShortestJob.Duration < target.ShortestJob.Duration {
		target.ShortestJob = source.ShortestJob
	}
}
