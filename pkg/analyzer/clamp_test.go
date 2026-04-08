package analyzer

import (
	"context"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/trace"
)

// findSpansByType returns all spans with the given type attribute value.
func findSpansByType(builder *SpanBuilder, typ string) []struct {
	name            string
	startMs, endMs int64
} {
	var results []struct {
		name            string
		startMs, endMs int64
	}
	for _, s := range builder.Spans() {
		for _, a := range s.Attributes() {
			if string(a.Key) == "type" && a.Value.AsString() == typ {
				results = append(results, struct {
					name            string
					startMs, endMs int64
				}{s.Name(), s.StartTime().UnixMilli(), s.EndTime().UnixMilli()})
			}
		}
	}
	return results
}

func TestStepSpanClampedToJobEnd(t *testing.T) {
	builder := &SpanBuilder{}
	tid := githubapi.NewTraceID(100, 1)
	jobSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     githubapi.NewSpanID(200),
		TraceFlags: trace.FlagsSampled,
	})

	job := githubapi.Job{
		ID:          200,
		Name:        "Build",
		Status:      "completed",
		Conclusion:  "success",
		StartedAt:   "2026-03-18T17:00:00Z",
		CompletedAt: "2026-03-18T17:10:00Z",
	}

	// Step ends AFTER the job's end time
	step := githubapi.Step{
		Name:        "Run tests",
		Number:      1,
		Status:      "completed",
		Conclusion:  "success",
		StartedAt:   "2026-03-18T17:05:00Z",
		CompletedAt: "2026-03-18T17:15:00Z", // 5 minutes past job end
	}

	run := githubapi.WorkflowRun{
		ID:         100,
		RunAttempt: 1,
		Repository: githubapi.RepoRef{
			Owner: githubapi.RepoOwner{Login: "owner"},
			Name:  "repo",
		},
	}

	jobEnd, _ := utils.ParseTime(job.CompletedAt)
	jobEndTs := jobEnd.UnixMilli()

	metrics := InitializeMetrics()
	var traceEvents []TraceEvent

	processStep(step, job, run, 10, 1001, jobEnd.UnixMilli(), jobEndTs,
		&metrics, &traceEvents, "", 0, "", "", "",
		builder, tid, jobSC)

	steps := findSpansByType(builder, "step")
	assert.Len(t, steps, 1, "expected 1 step span")

	stepSpan := steps[0]
	assert.Equal(t, "Run tests", stepSpan.name)
	assert.LessOrEqual(t, stepSpan.endMs, jobEndTs,
		"step span end time should be clamped to job end time")
}

func TestStepSpanStartClampedToJobEnd(t *testing.T) {
	builder := &SpanBuilder{}
	tid := githubapi.NewTraceID(100, 1)
	jobSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     githubapi.NewSpanID(200),
		TraceFlags: trace.FlagsSampled,
	})

	job := githubapi.Job{
		ID:          200,
		Name:        "Build",
		Status:      "completed",
		Conclusion:  "success",
		StartedAt:   "2026-03-18T17:00:00Z",
		CompletedAt: "2026-03-18T17:10:00Z",
	}

	// Step starts AND ends after the job
	step := githubapi.Step{
		Name:        "Post cleanup",
		Number:      5,
		Status:      "completed",
		Conclusion:  "success",
		StartedAt:   "2026-03-18T17:12:00Z",
		CompletedAt: "2026-03-18T17:15:00Z",
	}

	run := githubapi.WorkflowRun{
		ID:         100,
		RunAttempt: 1,
		Repository: githubapi.RepoRef{
			Owner: githubapi.RepoOwner{Login: "owner"},
			Name:  "repo",
		},
	}

	jobEnd, _ := utils.ParseTime(job.CompletedAt)
	jobEndTs := jobEnd.UnixMilli()

	metrics := InitializeMetrics()
	var traceEvents []TraceEvent

	processStep(step, job, run, 10, 1001, jobEnd.UnixMilli(), jobEndTs,
		&metrics, &traceEvents, "", 0, "", "", "",
		builder, tid, jobSC)

	steps := findSpansByType(builder, "step")
	assert.Len(t, steps, 1, "expected 1 step span")

	stepSpan := steps[0]
	assert.LessOrEqual(t, stepSpan.startMs, jobEndTs,
		"step span start time should be clamped to job end time")
	assert.LessOrEqual(t, stepSpan.endMs, jobEndTs,
		"step span end time should be clamped to job end time")
}

func TestJobSpanClampedToWorkflowEnd(t *testing.T) {
	builder := &SpanBuilder{}
	tid := githubapi.NewTraceID(100, 1)
	wfSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     githubapi.NewSpanID(100),
		TraceFlags: trace.FlagsSampled,
	})

	run := githubapi.WorkflowRun{
		ID:         100,
		RunAttempt: 1,
		Name:       "CI",
		Status:     "completed",
		Conclusion: "success",
		CreatedAt:  "2026-03-18T17:00:00Z",
		UpdatedAt:  "2026-03-18T17:30:00Z",
		Repository: githubapi.RepoRef{
			Owner: githubapi.RepoOwner{Login: "owner"},
			Name:  "repo",
		},
	}

	// Job ends AFTER the workflow's end time
	job := githubapi.Job{
		ID:          200,
		Name:        "Post-merge",
		Status:      "completed",
		Conclusion:  "success",
		CreatedAt:   "2026-03-18T17:00:00Z",
		StartedAt:   "2026-03-18T17:05:00Z",
		CompletedAt: "2026-03-18T17:45:00Z", // 15 minutes past workflow end
		RunnerName:  "runner-1",
	}

	wfEnd, _ := utils.ParseTime(run.UpdatedAt)
	runEndTs := wfEnd.UnixMilli()
	earliestTime := wfEnd.UnixMilli() // doesn't matter for span times

	metrics := InitializeMetrics()
	var traceEvents []TraceEvent
	var jobStartTimes, jobEndTimes []JobEvent

	processJob(job, 0, run, 10, 1001, earliestTime, runEndTs,
		&metrics, &traceEvents, &jobStartTimes, &jobEndTimes,
		"", 0, "", "", "", nil, builder, tid, wfSC, nil)

	jobs := findSpansByType(builder, "job")
	assert.Len(t, jobs, 1, "expected 1 job span")

	jobSpan := jobs[0]
	assert.LessOrEqual(t, jobSpan.endMs, runEndTs,
		"job span end time should be clamped to workflow end time")
}

func TestPendingJobSpanUsesWorkflowEnd(t *testing.T) {
	builder := &SpanBuilder{}
	tid := githubapi.NewTraceID(100, 1)
	wfSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     githubapi.NewSpanID(100),
		TraceFlags: trace.FlagsSampled,
	})

	run := githubapi.WorkflowRun{
		ID:         100,
		RunAttempt: 1,
		Name:       "CI",
		Status:     "in_progress",
		Conclusion: "",
		CreatedAt:  "2026-03-18T17:00:00Z",
		UpdatedAt:  "2026-03-18T17:30:00Z",
		Repository: githubapi.RepoRef{
			Owner: githubapi.RepoOwner{Login: "owner"},
			Name:  "repo",
		},
	}

	// Pending job: no CompletedAt
	job := githubapi.Job{
		ID:         200,
		Name:       "Post-merge",
		Status:     "in_progress",
		Conclusion: "",
		CreatedAt:  "2026-03-18T17:00:00Z",
		StartedAt:  "2026-03-18T17:05:00Z",
		RunnerName: "runner-1",
	}

	wfEnd, _ := utils.ParseTime(run.UpdatedAt)
	runEndTs := wfEnd.UnixMilli()
	earliestTime := wfEnd.UnixMilli()

	metrics := InitializeMetrics()
	var traceEvents []TraceEvent
	var jobStartTimes, jobEndTimes []JobEvent

	processJob(job, 0, run, 10, 1001, earliestTime, runEndTs,
		&metrics, &traceEvents, &jobStartTimes, &jobEndTimes,
		"", 0, "", "", "", nil, builder, tid, wfSC, nil)

	jobs := findSpansByType(builder, "job")
	assert.Len(t, jobs, 1, "expected 1 job span")

	jobSpan := jobs[0]
	// Pending job with no CompletedAt should use time.Now(), but clamped to runEndTs
	assert.LessOrEqual(t, jobSpan.endMs, runEndTs,
		"pending job span should be clamped to workflow end time")
	assert.Greater(t, jobSpan.endMs, int64(0),
		"pending job span should not have zero end time")
}

func TestClampingEndToEnd(t *testing.T) {
	t.Run("step spans do not exceed parent job in full pipeline", func(t *testing.T) {
		mockClient := new(mockGitHubProvider)
		builder := &SpanBuilder{}

		run := githubapi.WorkflowRun{
			ID:           500,
			RunAttempt:   1,
			Name:         "Post-merge",
			Status:       "completed",
			Conclusion:   "failure",
			CreatedAt:    "2026-03-18T17:00:00Z",
			RunStartedAt: "2026-03-18T17:00:00Z",
			UpdatedAt:    "2026-03-18T17:10:00Z",
			HeadSHA:      "abc123",
			Repository: githubapi.RepoRef{
				Owner: githubapi.RepoOwner{Login: "owner"},
				Name:  "repo",
			},
		}

		// Job ends at 17:10, but a step claims to end at 17:20
		job := githubapi.Job{
			ID:          600,
			Name:        "Deploy",
			Status:      "completed",
			Conclusion:  "failure",
			CreatedAt:   "2026-03-18T17:00:00Z",
			StartedAt:   "2026-03-18T17:01:00Z",
			CompletedAt: "2026-03-18T17:10:00Z",
			RunnerName:  "runner-1",
			Steps: []githubapi.Step{
				{
					Name:        "Setup",
					Number:      1,
					Status:      "completed",
					Conclusion:  "success",
					StartedAt:   "2026-03-18T17:01:00Z",
					CompletedAt: "2026-03-18T17:03:00Z",
				},
				{
					Name:        "Run deploy",
					Number:      2,
					Status:      "completed",
					Conclusion:  "failure",
					StartedAt:   "2026-03-18T17:03:00Z",
					CompletedAt: "2026-03-18T17:20:00Z", // Exceeds job end by 10 minutes
				},
			},
		}

		jobsURL := "https://api.github.com/repos/owner/repo/actions/runs/500/jobs?per_page=100"
		mockClient.On("FetchJobsPaginated", mock.Anything, jobsURL).Return([]githubapi.Job{job}, nil)
		mockClient.On("FetchCheckRunsForCommit", mock.Anything, "owner", "repo", "abc123").Return([]githubapi.CheckRun{}, nil)
		mockClient.On("FetchRunTiming", mock.Anything, "owner", "repo", int64(500)).Return((*githubapi.RunTiming)(nil), nil)
		mockClient.On("ListArtifacts", mock.Anything, "owner", "repo", int64(500)).Return([]githubapi.Artifact{}, nil)

		createdAt, _ := utils.ParseTime(run.CreatedAt)
		earliestTime := createdAt.UnixMilli()

		_, _, _, _, err := processWorkflowRun(
			context.Background(), run, 0, 1001, earliestTime,
			"owner", "repo", "1", 0, "https://github.com/owner/repo/pull/1", "pr",
			nil, 0, 0, 0, mockClient, nil, builder, NewTraceEmitter(builder), AnalyzeOptions{NoArtifacts: true},
		)
		assert.NoError(t, err)

		// Find the job span end time
		jobSpans := findSpansByType(builder, "job")
		assert.Len(t, jobSpans, 1)
		jobEndMs := jobSpans[0].endMs

		// Find the workflow span end time
		wfSpans := findSpansByType(builder, "workflow")
		assert.Len(t, wfSpans, 1)
		wfEndMs := wfSpans[0].endMs

		// All step spans must be within job bounds
		stepSpans := findSpansByType(builder, "step")
		assert.GreaterOrEqual(t, len(stepSpans), 2, "expected at least 2 step spans")
		for _, step := range stepSpans {
			assert.LessOrEqual(t, step.endMs, jobEndMs,
				"step %q end time should not exceed job end time", step.name)
		}

		// Job span must be within workflow bounds
		assert.LessOrEqual(t, jobSpans[0].endMs, wfEndMs,
			"job span end time should not exceed workflow end time")
	})
}
