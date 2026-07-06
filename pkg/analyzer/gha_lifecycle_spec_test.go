package analyzer

// Tests derived from the specs/gha-lifecycle TLA+ campaign witness traces
// (see specs/gha-lifecycle/README.md, findings 1-4 + discovery notes).
// Each test encodes one TLC counterexample as a concrete analyzer scenario.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// specAttrStr returns the string value of a span attribute (or "" if absent).
func specAttrStr(s sdktrace.ReadOnlySpan, key string) string {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

// specWorkflowSpans returns all spans with type=workflow.
func specWorkflowSpans(builder *SpanBuilder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range builder.Spans() {
		if specAttrStr(s, "type") == "workflow" {
			out = append(out, s)
		}
	}
	return out
}

func specRun(status, conclusion string, attempt int64) githubapi.WorkflowRun {
	return githubapi.WorkflowRun{
		ID:           100,
		RunAttempt:   attempt,
		Name:         "CI",
		Status:       status,
		Conclusion:   conclusion,
		CreatedAt:    "2026-03-18T17:00:00Z",
		RunStartedAt: "2026-03-18T17:00:00Z",
		UpdatedAt:    "2026-03-18T17:30:00Z",
		HeadSHA:      "abc123",
		Repository: githubapi.RepoRef{
			Owner: githubapi.RepoOwner{Login: "owner"},
			Name:  "repo",
		},
	}
}

func specJob(id int64, status, conclusion string) githubapi.Job {
	j := githubapi.Job{
		ID:         id,
		Name:       fmt.Sprintf("job-%d", id),
		Status:     status,
		Conclusion: conclusion,
		CreatedAt:  "2026-03-18T17:00:00Z",
		StartedAt:  "2026-03-18T17:05:00Z",
		RunnerName: "runner-1",
	}
	if status == "completed" {
		j.CompletedAt = "2026-03-18T17:20:00Z"
	}
	return j
}

func specProcessRun(t *testing.T, run githubapi.WorkflowRun, mockClient *mockGitHubProvider, builder *SpanBuilder) Metrics {
	t.Helper()
	if run.Status == "completed" {
		mockClient.On("FetchRunTiming", mock.Anything, "owner", "repo", run.ID).
			Return((*githubapi.RunTiming)(nil), nil).Maybe()
	}
	createdAt, _ := utils.ParseTime(run.CreatedAt)
	metrics, _, _, _, err := processWorkflowRun(
		context.Background(), run, 0, 1001, createdAt.UnixMilli(),
		"owner", "repo", "1", 0, "https://github.com/owner/repo/pull/1", "pr",
		nil, 0, 0, 0, mockClient, nil, builder, NewTraceEmitter(builder),
		AnalyzeOptions{NoArtifacts: true}, nil,
	)
	assert.NoError(t, err)
	return metrics
}

// Finding 2 (MCFinding2 witness, 2 states): the analyzer fetches a run that
// is still queued/in_progress and counts it as FAILED (analyzer.go:296-300).
// Spec invariant InvPendingRunNotFailed: a run counted failed must be
// completed. Fixed code classifies three ways: pending / success / failed.
func TestGhaLifecycleSpec_PendingRunNotCountedFailed(t *testing.T) {
	cases := []struct {
		name        string
		status      string
		conclusion  string
		wantPending int
		wantSuccess int
		wantFailed  int
	}{
		// The witness: run still queued at AFetchRun -> runCounted = "failed".
		{"queued run is pending, not failed", "queued", "", 1, 0, 0},
		{"in_progress run is pending, not failed", "in_progress", "", 1, 0, 0},
		{"completed success", "completed", "success", 0, 1, 0},
		{"completed failure", "completed", "failure", 0, 0, 1},
		// Empty conclusion on completed is a GitHub fault; still a failure.
		{"completed empty conclusion is failed", "completed", "", 0, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := new(mockGitHubProvider)
			builder := &SpanBuilder{}
			run := specRun(tc.status, tc.conclusion, 1)
			jobsURL := "https://api.github.com/repos/owner/repo/actions/runs/100/jobs?per_page=100"
			mockClient.On("FetchJobsPaginated", mock.Anything, jobsURL).
				Return([]githubapi.Job{specJob(200, tc.status, tc.conclusion)}, nil)

			metrics := specProcessRun(t, run, mockClient, builder)

			assert.Equal(t, 1, metrics.TotalRuns)
			assert.Equal(t, tc.wantPending, metrics.PendingRuns, "PendingRuns")
			assert.Equal(t, tc.wantSuccess, metrics.SuccessfulRuns, "SuccessfulRuns")
			assert.Equal(t, tc.wantFailed, metrics.FailedRuns, "FailedRuns")
		})
	}
}

// Finding 1 (MCFinding1 witness): RunAttempt=2, the attempt-1 jobs fetch
// FAILS (analyzer.go:324 continue) so no attempt-1 span is emitted — but the
// latest attempt still emits a retry link to the never-emitted span
// (analyzer.go:609-623). Spec invariant InvLinkTargetsEmitted: every retry
// link must target an emitted span.
func TestGhaLifecycleSpec_LinkTargetsEmitted(t *testing.T) {
	baseURL := "https://api.github.com/repos/owner/repo/actions/runs/100"

	t.Run("attempt fetch error: latest span has no dangling link", func(t *testing.T) {
		mockClient := new(mockGitHubProvider)
		builder := &SpanBuilder{}
		run := specRun("completed", "success", 2)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/jobs?per_page=100").
			Return([]githubapi.Job{specJob(200, "completed", "success")}, nil)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/attempts/1/jobs?per_page=100").
			Return([]githubapi.Job{}, errors.New("boom"))

		specProcessRun(t, run, mockClient, builder)

		wf := specWorkflowSpans(builder)
		assert.Len(t, wf, 1, "only the latest attempt span is emitted")
		assert.Empty(t, wf[0].Links(),
			"retry link must not target a span that was never emitted")
	})

	t.Run("attempt returns no jobs: no span, no dangling link", func(t *testing.T) {
		mockClient := new(mockGitHubProvider)
		builder := &SpanBuilder{}
		run := specRun("completed", "success", 2)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/jobs?per_page=100").
			Return([]githubapi.Job{specJob(200, "completed", "success")}, nil)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/attempts/1/jobs?per_page=100").
			Return([]githubapi.Job{}, nil)

		specProcessRun(t, run, mockClient, builder)

		wf := specWorkflowSpans(builder)
		assert.Len(t, wf, 1)
		assert.Empty(t, wf[0].Links())
	})

	t.Run("middle attempt fetch fails at RunAttempt=3", func(t *testing.T) {
		mockClient := new(mockGitHubProvider)
		builder := &SpanBuilder{}
		run := specRun("completed", "success", 3)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/jobs?per_page=100").
			Return([]githubapi.Job{specJob(200, "completed", "success")}, nil)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/attempts/1/jobs?per_page=100").
			Return([]githubapi.Job{specJob(201, "completed", "failure")}, nil)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/attempts/2/jobs?per_page=100").
			Return([]githubapi.Job{}, errors.New("boom"))

		specProcessRun(t, run, mockClient, builder)

		emitted := map[trace.SpanID]bool{}
		for _, s := range specWorkflowSpans(builder) {
			emitted[s.SpanContext().SpanID()] = true
		}
		for _, s := range specWorkflowSpans(builder) {
			for _, l := range s.Links() {
				assert.True(t, emitted[l.SpanContext.SpanID()],
					"span %q links to a span that was never emitted", s.Name())
			}
		}
	})

	t.Run("happy path keeps the retry link", func(t *testing.T) {
		mockClient := new(mockGitHubProvider)
		builder := &SpanBuilder{}
		run := specRun("completed", "success", 2)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/jobs?per_page=100").
			Return([]githubapi.Job{specJob(200, "completed", "success")}, nil)
		mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/attempts/1/jobs?per_page=100").
			Return([]githubapi.Job{specJob(201, "completed", "failure")}, nil)

		specProcessRun(t, run, mockClient, builder)

		wf := specWorkflowSpans(builder)
		assert.Len(t, wf, 2, "attempt-1 and latest spans")
		prevSID := previousAttemptSpanID(100, 1)
		var latestLinks []sdktrace.Link
		for _, s := range wf {
			if s.SpanContext().SpanID() != prevSID {
				latestLinks = s.Links()
			}
		}
		if assert.Len(t, latestLinks, 1, "latest attempt links to attempt 1") {
			assert.Equal(t, prevSID, latestLinks[0].SpanContext.SpanID())
		}
	})
}

// Finding 3 (MCFinding3 witness): a job that started but has NOT completed
// (in_progress, conclusion "") gets its queue time counted and a Queued span
// emitted — analyzer.go:852/890 gate on conclusion but never on completion,
// contradicting the comment at :851 and the isPending discipline at :826.
// Spec invariant InvQueueOnlyCompletedJobs.
func TestGhaLifecycleSpec_QueueTimeOnlyCompletedJobs(t *testing.T) {
	cases := []struct {
		name      string
		job       githubapi.Job
		wantQueue int
	}{
		{
			// The witness: GhJobStart -> analyzer processes an in_progress job.
			name:      "in_progress job: queue time not counted",
			job:       specJob(200, "in_progress", ""),
			wantQueue: 0,
		},
		{
			// completed_at missing on a completed job (fault) — still pending.
			name: "completed without completed_at: queue time not counted",
			job: githubapi.Job{
				ID: 200, Name: "job-200", Status: "completed", Conclusion: "failure",
				CreatedAt: "2026-03-18T17:00:00Z", StartedAt: "2026-03-18T17:05:00Z",
			},
			wantQueue: 0,
		},
		{
			name:      "completed job: queue time counted",
			job:       specJob(200, "completed", "success"),
			wantQueue: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := &SpanBuilder{}
			run := specRun("in_progress", "", 1)
			tid := githubapi.NewTraceID(run.ID, run.RunAttempt)
			wfSC := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tid,
				SpanID:     githubapi.NewSpanID(run.ID),
				TraceFlags: trace.FlagsSampled,
			})
			wfStart, _ := utils.ParseTime(run.CreatedAt)
			wfEnd, _ := utils.ParseTime(run.UpdatedAt)

			metrics := InitializeMetrics()
			var traceEvents []TraceEvent
			var jobStartTimes, jobEndTimes []JobEvent
			processJob(tc.job, 0, run, 10, 1001, wfEnd.UnixMilli(), wfStart.UnixMilli(), wfEnd.UnixMilli(),
				&metrics, &traceEvents, &jobStartTimes, &jobEndTimes,
				"", 0, "", "", "", nil, builder, tid, wfSC, nil)

			assert.Len(t, metrics.QueueTimes, tc.wantQueue, "QueueTimes")

			queuedSpans := 0
			for _, s := range builder.Spans() {
				if specAttrStr(s, "type") == "queued" {
					queuedSpans++
				}
			}
			assert.Equal(t, tc.wantQueue, queuedSpans, "Queued OTel spans")

			queuedEvents := 0
			for _, ev := range traceEvents {
				if ev.Cat == "queued" {
					queuedEvents++
				}
			}
			assert.Equal(t, tc.wantQueue, queuedEvents, "Queued trace events")
		})
	}
}

// Discovery note (spec README "Spec-derived simplification notes"): jobs that
// never started (StartedAt == "") return at analyzer.go:767 before the
// pending classification — a queued-but-unstarted job is invisible to
// PendingJobs.
func TestGhaLifecycleSpec_UnstartedQueuedJobVisibleAsPending(t *testing.T) {
	builder := &SpanBuilder{}
	run := specRun("in_progress", "", 1)
	tid := githubapi.NewTraceID(run.ID, run.RunAttempt)
	wfSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     githubapi.NewSpanID(run.ID),
		TraceFlags: trace.FlagsSampled,
	})
	wfStart, _ := utils.ParseTime(run.CreatedAt)
	wfEnd, _ := utils.ParseTime(run.UpdatedAt)

	job := githubapi.Job{
		ID: 200, Name: "queued-job", Status: "queued",
		CreatedAt: "2026-03-18T17:00:00Z", // never started
	}

	metrics := InitializeMetrics()
	var traceEvents []TraceEvent
	var jobStartTimes, jobEndTimes []JobEvent
	processJob(job, 0, run, 10, 1001, wfEnd.UnixMilli(), wfStart.UnixMilli(), wfEnd.UnixMilli(),
		&metrics, &traceEvents, &jobStartTimes, &jobEndTimes,
		"", 0, "", "", "", nil, builder, tid, wfSC, nil)

	if assert.Len(t, metrics.PendingJobs, 1, "unstarted queued job must be visible as pending") {
		assert.Equal(t, "queued-job", metrics.PendingJobs[0].Name)
		assert.Equal(t, "queued", metrics.PendingJobs[0].Status)
	}
	// No timing to process: still no job span / durations.
	assert.Empty(t, metrics.JobDurations)
}

// Finding 4 (MCFinding4 witness): attempt-1's only job completed timed_out;
// processPreviousAttempt derives the attempt conclusion handling only
// failure/cancelled (analyzer.go:697-701) so the attempt span says success,
// while FailedJobs (:826) counts the same job as failed.
// Spec invariant InvTimedOutAttemptNotSuccess.
func TestGhaLifecycleSpec_TimedOutAttemptNotSuccess(t *testing.T) {
	baseURL := "https://api.github.com/repos/owner/repo/actions/runs/100"
	mockClient := new(mockGitHubProvider)
	builder := &SpanBuilder{}
	run := specRun("completed", "success", 2)
	mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/jobs?per_page=100").
		Return([]githubapi.Job{specJob(200, "completed", "success")}, nil)
	mockClient.On("FetchJobsPaginated", mock.Anything, baseURL+"/attempts/1/jobs?per_page=100").
		Return([]githubapi.Job{specJob(201, "completed", "timed_out")}, nil)

	metrics := specProcessRun(t, run, mockClient, builder)

	prevSID := previousAttemptSpanID(100, 1)
	var found bool
	for _, s := range specWorkflowSpans(builder) {
		if s.SpanContext().SpanID() == prevSID {
			found = true
			concl := specAttrStr(s, "github.conclusion")
			assert.NotEqual(t, "success", concl,
				"timed-out attempt must not carry a success conclusion")
			assert.Equal(t, "timed_out", concl)
		}
	}
	assert.True(t, found, "attempt-1 span emitted")
	// Consistency with FailedJobs: the timed-out job counts as failed.
	assert.Equal(t, 1, metrics.FailedJobs)
}

// Shared conclusion derivation, latest-attempt path (analyzer.go:638):
// a COMPLETED run with an empty conclusion (API fault) gets its workflow-span
// conclusion derived from jobs — same helper as previous attempts.
func TestGhaLifecycleSpec_CompletedRunEmptyConclusionDerivedFromJobs(t *testing.T) {
	cases := []struct {
		name      string
		runStatus string
		jobConcl  string
		wantConcl string
	}{
		{"completed empty conclusion, failed job", "completed", "failure", "failure"},
		{"completed empty conclusion, timed_out job", "completed", "timed_out", "timed_out"},
		{"completed empty conclusion, success job", "completed", "success", "success"},
		// A run still in progress must NOT be given a derived conclusion —
		// its jobs haven't finished; empty stays empty.
		{"in_progress run keeps empty conclusion", "in_progress", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := new(mockGitHubProvider)
			builder := &SpanBuilder{}
			run := specRun(tc.runStatus, "", 1)
			jobsURL := "https://api.github.com/repos/owner/repo/actions/runs/100/jobs?per_page=100"
			jobStatus := "completed"
			if tc.jobConcl == "" {
				jobStatus = "in_progress"
			}
			mockClient.On("FetchJobsPaginated", mock.Anything, jobsURL).
				Return([]githubapi.Job{specJob(200, jobStatus, tc.jobConcl)}, nil)

			specProcessRun(t, run, mockClient, builder)

			wf := specWorkflowSpans(builder)
			if assert.Len(t, wf, 1) {
				assert.Equal(t, tc.wantConcl, specAttrStr(wf[0], "github.conclusion"))
				assert.Equal(t, ghConclusionToResult(tc.wantConcl), specAttrStr(wf[0], "cicd.pipeline.run.result"))
			}
		})
	}
}
