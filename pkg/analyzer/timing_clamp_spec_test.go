package analyzer

// Tests derived from the specs/timing-clamp TLA+ campaign witness traces
// (see specs/timing-clamp/README.md, findings 1-5). Each test encodes one
// TLC counterexample as a concrete analyzer scenario and checks the
// containment contract: every emitted child span (OTel AND ms/flamechart
// path) stays inside its parent's bounds.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// clampSpecRun builds a completed run with the given raw bounds.
func clampSpecRun(id int64, createdAt, updatedAt string) githubapi.WorkflowRun {
	return githubapi.WorkflowRun{
		ID:           id,
		RunAttempt:   1,
		Name:         "CI",
		Status:       "completed",
		Conclusion:   "success",
		CreatedAt:    createdAt,
		RunStartedAt: createdAt,
		UpdatedAt:    updatedAt,
		Repository: githubapi.RepoRef{
			Owner: githubapi.RepoOwner{Login: "owner"},
			Name:  "repo",
		},
	}
}

// clampSpecProcess runs the full per-run pipeline and returns the metrics
// plus the ms-path trace events (the flamechart JSON the spec models).
func clampSpecProcess(t *testing.T, run githubapi.WorkflowRun, jobs []githubapi.Job, builder *SpanBuilder) []TraceEvent {
	t.Helper()
	mockClient := new(mockGitHubProvider)
	jobsURL := fmt.Sprintf("https://api.github.com/repos/owner/repo/actions/runs/%d/jobs?per_page=100", run.ID)
	mockClient.On("FetchJobsPaginated", mock.Anything, jobsURL).Return(jobs, nil)
	if run.Status == "completed" {
		mockClient.On("FetchRunTiming", mock.Anything, "owner", "repo", run.ID).
			Return((*githubapi.RunTiming)(nil), nil).Maybe()
	}
	createdAt, _ := utils.ParseTime(run.CreatedAt)
	_, traceEvents, _, _, err := processWorkflowRun(
		context.Background(), run, 0, 1001, createdAt.UnixMilli(),
		"owner", "repo", "1", 0, "https://github.com/owner/repo/pull/1", "pr",
		nil, 0, 0, 0, mockClient, nil, builder, NewTraceEmitter(builder),
		AnalyzeOptions{NoArtifacts: true}, nil,
	)
	assert.NoError(t, err)
	return traceEvents
}

// msEvent finds the first ms-path trace event with the given Cat.
func msEvent(t *testing.T, events []TraceEvent, cat string) TraceEvent {
	t.Helper()
	for _, ev := range events {
		if ev.Cat == cat && ev.Ph == "X" {
			return ev
		}
	}
	t.Fatalf("no %q trace event emitted", cat)
	return TraceEvent{}
}

// Finding 3 (MCFinding3 witness): the ms/flamechart path used the RAW
// re-parsed job start, unclamped anywhere, so a job whose StartedAt is past
// runEnd escapes the workflow event by an UNBOUNDED amount. Spec invariant
// MsChildWithinParent.
func TestTimingClampSpec_MsPathJobStaysInsideWorkflow(t *testing.T) {
	builder := &SpanBuilder{}
	run := clampSpecRun(100, "2026-03-18T17:00:00Z", "2026-03-18T17:10:00Z")
	// In-progress job (does not extend runEnd) with a raw start PAST runEnd.
	job := githubapi.Job{
		ID:        200,
		Name:      "late-start",
		Status:    "in_progress",
		CreatedAt: "2026-03-18T17:00:00Z",
		StartedAt: "2026-03-18T17:20:00Z", // 10 min past runEnd
	}

	events := clampSpecProcess(t, run, []githubapi.Job{job}, builder)

	wf := msEvent(t, events, "workflow")
	jb := msEvent(t, events, "job")
	assert.GreaterOrEqual(t, jb.Ts, wf.Ts,
		"job ms event must not start before the workflow event")
	assert.LessOrEqual(t, jb.Ts+jb.Dur, wf.Ts+wf.Dur,
		"job ms event must not end past the workflow event")
}

// Finding 2 (MCFinding2 witness): no lower-bound clamp existed anywhere in
// the pipeline, so a child starting before its parent escapes on both the
// OTel and ms paths. Spec invariant ExportedChildStartInParent.
func TestTimingClampSpec_ChildStartClampedToParentStart(t *testing.T) {
	t.Run("job starting before run start", func(t *testing.T) {
		builder := &SpanBuilder{}
		run := clampSpecRun(101, "2026-03-18T17:00:00Z", "2026-03-18T17:30:00Z")
		job := githubapi.Job{
			ID:          201,
			Name:        "early-start",
			Status:      "completed",
			Conclusion:  "success",
			CreatedAt:   "2026-03-18T16:50:00Z",
			StartedAt:   "2026-03-18T16:55:00Z", // 5 min before run CreatedAt
			CompletedAt: "2026-03-18T17:10:00Z",
		}

		events := clampSpecProcess(t, run, []githubapi.Job{job}, builder)

		wfSpans := findSpansByType(builder, "workflow")
		jobSpans := findSpansByType(builder, "job")
		assert.Len(t, wfSpans, 1)
		assert.Len(t, jobSpans, 1)
		assert.GreaterOrEqual(t, jobSpans[0].startMs, wfSpans[0].startMs,
			"job OTel span must not start before its parent workflow span")

		wf := msEvent(t, events, "workflow")
		jb := msEvent(t, events, "job")
		assert.GreaterOrEqual(t, jb.Ts, wf.Ts,
			"job ms event must not start before the workflow event")
	})

	t.Run("step starting before job start", func(t *testing.T) {
		builder := &SpanBuilder{}
		run := clampSpecRun(102, "2026-03-18T17:00:00Z", "2026-03-18T17:30:00Z")
		job := githubapi.Job{
			ID:          202,
			Name:        "build",
			Status:      "completed",
			Conclusion:  "success",
			CreatedAt:   "2026-03-18T17:00:00Z",
			StartedAt:   "2026-03-18T17:05:00Z",
			CompletedAt: "2026-03-18T17:10:00Z",
			Steps: []githubapi.Step{{
				Name:        "checkout",
				Number:      1,
				Status:      "completed",
				Conclusion:  "success",
				StartedAt:   "2026-03-18T17:03:00Z", // 2 min before job start
				CompletedAt: "2026-03-18T17:08:00Z",
			}},
		}

		clampSpecProcess(t, run, []githubapi.Job{job}, builder)

		jobSpans := findSpansByType(builder, "job")
		stepSpans := findSpansByType(builder, "step")
		assert.Len(t, jobSpans, 1)
		assert.Len(t, stepSpans, 1)
		assert.GreaterOrEqual(t, stepSpans[0].startMs, jobSpans[0].startMs,
			"step OTel span must not start before its parent job span")
	})
}

// Finding 1 (MCFinding1 witness): the +1ms end floors re-applied AFTER the
// end clamp push a child sitting exactly at its parent's end one ms past it.
// Spec invariant ExportedChildEndInParent. Fixed code renders the child as a
// 1ms sliver ENDING at the parent's end instead.
func TestTimingClampSpec_EndFloorDoesNotEscapeParent(t *testing.T) {
	builder := &SpanBuilder{}
	run := clampSpecRun(103, "2026-03-18T17:00:00Z", "2026-03-18T17:30:00Z")
	job := githubapi.Job{
		ID:          203,
		Name:        "build",
		Status:      "completed",
		Conclusion:  "success",
		CreatedAt:   "2026-03-18T17:00:00Z",
		StartedAt:   "2026-03-18T17:00:00Z",
		CompletedAt: "2026-03-18T17:10:00Z",
		Steps: []githubapi.Step{{
			Name:       "post-cleanup",
			Number:     9,
			Status:     "completed",
			Conclusion: "success",
			// Zero-width step exactly at the job's end: the old floor made
			// its ms end jobEnd+1ms.
			StartedAt:   "2026-03-18T17:10:00Z",
			CompletedAt: "2026-03-18T17:10:00Z",
		}},
	}

	events := clampSpecProcess(t, run, []githubapi.Job{job}, builder)

	jb := msEvent(t, events, "job")
	var stepEv *TraceEvent
	for i := range events {
		if events[i].Ph == "X" && events[i].Cat != "workflow" && events[i].Cat != "job" && events[i].Cat != "queued" {
			stepEv = &events[i]
			break
		}
	}
	if assert.NotNil(t, stepEv, "expected a step ms event") {
		assert.LessOrEqual(t, stepEv.Ts+stepEv.Dur, jb.Ts+jb.Dur,
			"step ms event must not end past its parent job event")
		assert.GreaterOrEqual(t, stepEv.Dur, int64(1000),
			"hostile zero-width step should render as a >=1ms sliver, not disappear")
	}

	jobSpans := findSpansByType(builder, "job")
	stepSpans := findSpansByType(builder, "step")
	assert.Len(t, jobSpans, 1)
	assert.Len(t, stepSpans, 1)
	assert.LessOrEqual(t, stepSpans[0].endMs, jobSpans[0].endMs,
		"step OTel span must not end past its parent job span")
}

// Finding 4 (MCFinding4 witness): runEnd := UpdatedAt was never checked
// against runStart, so UpdatedAt < CreatedAt (server-side skew/stale cache)
// emitted a workflow event with NEGATIVE duration. Spec invariant
// RunBoundsOrdered.
func TestTimingClampSpec_RunBoundsOrdered(t *testing.T) {
	builder := &SpanBuilder{}
	run := clampSpecRun(104, "2026-03-18T17:00:00Z", "2026-03-18T16:00:00Z") // UpdatedAt < CreatedAt

	events := clampSpecProcess(t, run, nil, builder)

	wfSpans := findSpansByType(builder, "workflow")
	assert.Len(t, wfSpans, 1)
	assert.Greater(t, wfSpans[0].endMs, wfSpans[0].startMs,
		"workflow OTel span must have ordered bounds")

	wf := msEvent(t, events, "workflow")
	assert.Greater(t, wf.Dur, int64(0),
		"workflow ms event must not have negative duration")
}

// Finding 5 (MCFinding5 witness): the 24h sanity recalc re-maxed over the
// same CompletedAt field that triggered the anomaly, so a poisoned job end
// defeated the heal. Spec invariant SanityHeals24h. Fixed code excludes job
// ends beyond runStart+24h from the recalc (and caps at runStart+24h when
// nothing usable remains).
func TestTimingClampSpec_SanityHealsPoisonedJobEnd(t *testing.T) {
	const dayMs = int64(24 * 3600 * 1000)
	runStart, _ := utils.ParseTime("2026-03-18T17:00:00Z")
	runStartTs := runStart.UnixMilli()

	poisoned := githubapi.Job{
		ID:          205,
		Name:        "poisoned",
		Status:      "completed",
		Conclusion:  "success",
		CreatedAt:   "2026-03-18T17:00:00Z",
		StartedAt:   "2026-03-18T17:05:00Z",
		CompletedAt: "2026-03-19T23:00:00Z", // 30h after run start
	}
	good := githubapi.Job{
		ID:          206,
		Name:        "good",
		Status:      "completed",
		Conclusion:  "success",
		CreatedAt:   "2026-03-18T17:00:00Z",
		StartedAt:   "2026-03-18T17:01:00Z",
		CompletedAt: "2026-03-18T17:08:00Z",
	}

	t.Run("good job end wins over poisoned end", func(t *testing.T) {
		builder := &SpanBuilder{}
		run := clampSpecRun(105, "2026-03-18T17:00:00Z", "2026-03-18T17:10:00Z")

		clampSpecProcess(t, run, []githubapi.Job{poisoned, good}, builder)

		wfSpans := findSpansByType(builder, "workflow")
		assert.Len(t, wfSpans, 1)
		goodEnd, _ := utils.ParseTime(good.CompletedAt)
		assert.Equal(t, goodEnd.UnixMilli(), wfSpans[0].endMs,
			"sanity recalc must heal to the max non-poisoned job end")

		// Children (incl. the poisoned job) must still fit the healed parent.
		for _, jb := range findSpansByType(builder, "job") {
			assert.LessOrEqual(t, jb.endMs, wfSpans[0].endMs,
				"job %q must not escape the healed workflow end", jb.name)
		}
	})

	t.Run("only poisoned ends: cap at runStart+24h", func(t *testing.T) {
		builder := &SpanBuilder{}
		run := clampSpecRun(106, "2026-03-18T17:00:00Z", "2026-03-18T17:10:00Z")

		clampSpecProcess(t, run, []githubapi.Job{poisoned}, builder)

		wfSpans := findSpansByType(builder, "workflow")
		assert.Len(t, wfSpans, 1)
		assert.Equal(t, runStartTs+dayMs, wfSpans[0].endMs,
			"with no usable job end the run end is capped at runStart+24h")
	})
}

// The shared clamp helper itself (unit-level pin of the spec's ClampSpan
// operator): start lands in [parentStart, parentEnd-1], end in
// [start+1, parentEnd].
func TestTimingClampSpec_ClampSpanToParent(t *testing.T) {
	cases := []struct {
		name               string
		s, e, pS, pE       int64
		wantStart, wantEnd int64
	}{
		{"already inside", 2, 3, 1, 4, 2, 3},
		{"end past parent", 2, 9, 1, 4, 2, 4},
		{"start before parent", 0, 3, 1, 4, 1, 3},
		{"start past parent end -> sliver at edge", 9, 12, 1, 4, 3, 4},
		{"inverted child -> 1ms sliver", 3, 2, 1, 4, 3, 4},
		{"zero-width at parent end", 4, 4, 1, 4, 3, 4},
		{"degenerate parent", 5, 5, 4, 4, 4, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, e := clampSpanToParent(tc.s, tc.e, tc.pS, tc.pE)
			assert.Equal(t, tc.wantStart, s, "start")
			assert.Equal(t, tc.wantEnd, e, "end")
			assert.GreaterOrEqual(t, s, tc.pS, "start >= parentStart")
			assert.GreaterOrEqual(t, e, s+1, "end >= start+1")
		})
	}
}
