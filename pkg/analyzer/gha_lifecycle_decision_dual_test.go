package analyzer

// Dual check: isJobPending / FailedJobs / countsQueueTime (handwritten)
// vs ClassifyPending / ClassifyFailed / ClassifyQueue from the
// GhaLifecycleDecision core (specgen → ghalifecyclespec).
//
// Spec: specs/gha-lifecycle/decision/Decision.tla
// Gen:  pkg/analyzer/ghalifecyclespec (never hand-edit)

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer/ghalifecyclespec"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countsFailed mirrors analyzer.go:903-906 (faithful, Bug=FALSE).
func countsFailed(job githubapi.Job) bool {
	return !isJobPending(job) &&
		(job.Conclusion == "failure" || job.Conclusion == "timed_out")
}

// countsQueue mirrors analyzer.go:924-926 (faithful; CreatedAt abstracted
// as always present; skipped/cancelled not in the decision-core domain).
func countsQueue(job githubapi.Job) bool {
	return !isJobPending(job) &&
		job.Conclusion != "skipped" && job.Conclusion != "cancelled"
}

// TestGhaLifecycleDecisionDual_InitPendingFailure: completed-looking
// failure with no completed_at is pending — never failed, never queued.
func TestGhaLifecycleDecisionDual_InitPendingFailure(t *testing.T) {
	t.Parallel()

	job := githubapi.Job{
		Status:      "completed",
		CompletedAt: "", // fault: missing timestamp
		Conclusion:  "failure",
	}
	assert.True(t, isJobPending(job))
	assert.False(t, countsFailed(job), "pending must not count failed")
	assert.False(t, countsQueue(job), "pending must not count queue")

	st := ghalifecyclespec.Init()
	assert.False(t, st.HasCompletedAt)
	assert.Equal(t, "failure", st.Conclusion)
	require.True(t, st.CanClassifyPending())
	assert.False(t, st.CanClassifyFailed(),
		"ClassifyFailed disabled while pending (Bug=FALSE)")
	assert.False(t, st.CanClassifyQueue(),
		"ClassifyQueue disabled while pending (Bug=FALSE)")

	st = st.ClassifyPending()
	assert.True(t, st.CountedPending)
	assert.False(t, st.CountedFailed)
	// PendingNeverFailed
	assert.False(t, st.CountedPending && st.CountedFailed)
}

// TestGhaLifecycleDecisionDual_CompletedFailure: after Reset, a completed
// failure/timed_out is classified failed + queue (not pending).
func TestGhaLifecycleDecisionDual_CompletedFailure(t *testing.T) {
	t.Parallel()

	st := ghalifecyclespec.Init()
	st = st.Reset() // hasCompletedAt=true, conclusion=timed_out
	assert.True(t, st.HasCompletedAt)
	assert.Equal(t, "timed_out", st.Conclusion)

	job := githubapi.Job{
		Status:      "completed",
		CompletedAt: "2026-06-01T12:00:00Z",
		Conclusion:  "timed_out",
		CreatedAt:   "2026-06-01T11:00:00Z",
	}
	assert.False(t, isJobPending(job))
	assert.True(t, countsFailed(job))
	assert.True(t, countsQueue(job))

	assert.False(t, st.CanClassifyPending())
	require.True(t, st.CanClassifyFailed())
	require.True(t, st.CanClassifyQueue())

	st = st.ClassifyFailed()
	st = st.ClassifyQueue()
	assert.True(t, st.CountedFailed)
	assert.True(t, st.QueueCounted)
	assert.False(t, st.CountedPending)
	// QueueOnlyNotPending
	assert.True(t, st.HasCompletedAt)
}

// TestGhaLifecycleDecisionDual_CompletedSuccess: success counts queue,
// not failed.
func TestGhaLifecycleDecisionDual_CompletedSuccess(t *testing.T) {
	t.Parallel()

	st := ghalifecyclespec.Init()
	st = st.Reset() // timed_out
	st = st.Reset() // success
	assert.True(t, st.HasCompletedAt)
	assert.Equal(t, "success", st.Conclusion)

	job := githubapi.Job{
		Status:      "completed",
		CompletedAt: "2026-06-01T12:00:00Z",
		Conclusion:  "success",
		CreatedAt:   "2026-06-01T11:00:00Z",
	}
	assert.False(t, isJobPending(job))
	assert.False(t, countsFailed(job))
	assert.True(t, countsQueue(job))

	assert.False(t, st.CanClassifyFailed())
	require.True(t, st.CanClassifyQueue())
	st = st.ClassifyQueue()
	assert.True(t, st.QueueCounted)
	assert.False(t, st.CountedFailed)
}

// TestGhaLifecycleDecisionDual_Table checks isJobPending / fail / queue
// against decision-core flags for several job shapes.
func TestGhaLifecycleDecisionDual_Table(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		status         string
		completedAt    string
		conclusion     string
		wantPending    bool
		wantFailed     bool
		wantQueue      bool
	}{
		{
			name: "completed failure no ts (Init witness)",
			status: "completed", completedAt: "", conclusion: "failure",
			wantPending: true, wantFailed: false, wantQueue: false,
		},
		{
			name: "completed failure with ts",
			status: "completed", completedAt: "2026-01-01T00:00:00Z", conclusion: "failure",
			wantPending: false, wantFailed: true, wantQueue: true,
		},
		{
			name: "completed timed_out with ts",
			status: "completed", completedAt: "2026-01-01T00:00:00Z", conclusion: "timed_out",
			wantPending: false, wantFailed: true, wantQueue: true,
		},
		{
			name: "completed success with ts",
			status: "completed", completedAt: "2026-01-01T00:00:00Z", conclusion: "success",
			wantPending: false, wantFailed: false, wantQueue: true,
		},
		{
			name: "in_progress",
			status: "in_progress", completedAt: "", conclusion: "",
			wantPending: true, wantFailed: false, wantQueue: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := githubapi.Job{
				Status:      tc.status,
				CompletedAt: tc.completedAt,
				Conclusion:  tc.conclusion,
				CreatedAt:   "2026-01-01T00:00:00Z",
			}
			assert.Equal(t, tc.wantPending, isJobPending(job), "isJobPending")
			assert.Equal(t, tc.wantFailed, countsFailed(job), "countsFailed")
			assert.Equal(t, tc.wantQueue, countsQueue(job), "countsQueue")

			// Decision core abstracts status into hasCompletedAt for the
			// completed-but-no-ts fault. Only dual when status=completed.
			if tc.status != "completed" {
				return
			}
			st := ghalifecyclespec.State{
				HasCompletedAt: tc.completedAt != "",
				Conclusion:     tc.conclusion,
			}
			assert.Equal(t, tc.wantPending, st.CanClassifyPending() || (tc.wantPending && st.CountedPending),
				"pending gate")
			// CanClassifyPending requires ~countedPending; for a fresh state
			// it equals isPending.
			assert.Equal(t, tc.wantPending, !st.HasCompletedAt)

			if tc.wantFailed {
				require.True(t, st.CanClassifyFailed(), "ClassifyFailed enabled")
			} else if tc.conclusion == "failure" || tc.conclusion == "timed_out" {
				// failure/timeout but pending → must not classify failed
				assert.False(t, st.CanClassifyFailed())
			}

			if tc.wantQueue {
				require.True(t, st.CanClassifyQueue())
			} else {
				assert.False(t, st.CanClassifyQueue())
			}
		})
	}
}
