package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "ote.db"))
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	return st
}

func sampleRuns(base time.Time) []analyzer.RunData {
	return []analyzer.RunData{
		{
			ID: 1, WorkflowName: "CI", HeadSHA: "aaa", Status: "completed", Conclusion: "success",
			CreatedAt: base, StartedAt: base, UpdatedAt: base.Add(10 * time.Minute), Duration: 600_000,
			Jobs: []analyzer.JobData{{
				ID: 11, Name: "build", URL: "https://github.com/o/r/actions/runs/1/job/11",
				Status: "completed", Conclusion: "success",
				CreatedAt: base, StartedAt: base.Add(5 * time.Second), CompletedAt: base.Add(5 * time.Minute),
				Duration: 295_000, QueueTime: 5_000,
			}},
		},
		{
			ID: 2, WorkflowName: "CI", HeadSHA: "bbb", Status: "completed", Conclusion: "failure",
			CreatedAt: base.Add(time.Hour), StartedAt: base.Add(time.Hour), UpdatedAt: base.Add(time.Hour + 5*time.Minute), Duration: 300_000,
			// No job detail fetched for this run
		},
	}
}

func TestStoreRoundTrip(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	require.NoError(t, st.UpsertRuns("o", "r", sampleRuns(base)))

	got, err := st.LoadRuns("o", "r", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, int64(1), got[0].ID, "runs load oldest-first")
	assert.Equal(t, "CI", got[0].WorkflowName)
	assert.Equal(t, "success", got[0].Conclusion)
	assert.True(t, got[0].CreatedAt.Equal(base), "timestamps survive the round trip")
	require.Len(t, got[0].Jobs, 1)
	assert.Equal(t, "build", got[0].Jobs[0].Name)
	assert.Equal(t, int64(295_000), got[0].Jobs[0].Duration)
	assert.Equal(t, int64(5_000), got[0].Jobs[0].QueueTime)
	assert.Empty(t, got[1].Jobs, "run 2 has no job detail")
}

func TestStoreUpsertIdempotentAndJobReplacing(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	runs := sampleRuns(base)

	require.NoError(t, st.UpsertRuns("o", "r", runs))
	require.NoError(t, st.UpsertRuns("o", "r", runs)) // idempotent

	// Re-sync run 2, now completed with job detail: jobs must replace, not duplicate.
	runs[1].Jobs = []analyzer.JobData{{ID: 21, Name: "test", Status: "completed", Conclusion: "failure",
		CreatedAt: base.Add(time.Hour), StartedAt: base.Add(time.Hour), CompletedAt: base.Add(time.Hour + 4*time.Minute), Duration: 240_000}}
	require.NoError(t, st.UpsertRuns("o", "r", runs))
	require.NoError(t, st.UpsertRuns("o", "r", runs))

	got, err := st.LoadRuns("o", "r", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Len(t, got[0].Jobs, 1)
	assert.Len(t, got[1].Jobs, 1)
	assert.Equal(t, "test", got[1].Jobs[0].Name)
}

func TestStoreWindowFiltering(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, st.UpsertRuns("o", "r", sampleRuns(base)))

	got, err := st.LoadRuns("o", "r", base.Add(30*time.Minute), base.Add(2*time.Hour))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, int64(2), got[0].ID)

	// Different repo: empty.
	got, err = st.LoadRuns("o", "other", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestStoreWatermark(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	wm, err := st.Watermark("o", "r")
	require.NoError(t, err)
	assert.True(t, wm.IsZero(), "empty store has zero watermark")

	require.NoError(t, st.UpsertRuns("o", "r", sampleRuns(base)))
	wm, err = st.Watermark("o", "r")
	require.NoError(t, err)
	assert.True(t, wm.Equal(base.Add(time.Hour)), "watermark = newest run CreatedAt, got %v", wm)
}

func TestStoreJobsFetchedTracking(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, st.UpsertRuns("o", "r", sampleRuns(base)))

	ids, err := st.RunsNeedingJobs("o", "r", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, []int64{2}, ids, "only the run without fetched jobs needs fetching")
}

func TestStoreUpsertJobsDoesNotClobberRun(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, st.UpsertRuns("o", "r", sampleRuns(base)))

	// Attach jobs to run 2 without touching its run row.
	jobs := []analyzer.JobData{{ID: 21, Name: "test", Status: "completed", Conclusion: "failure",
		CreatedAt: base.Add(time.Hour), StartedAt: base.Add(time.Hour), CompletedAt: base.Add(time.Hour + 4*time.Minute), Duration: 240_000}}
	require.NoError(t, st.UpsertJobs("o", "r", 2, jobs))

	got, err := st.LoadRuns("o", "r", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "failure", got[1].Conclusion, "run row untouched")
	assert.True(t, got[1].CreatedAt.Equal(base.Add(time.Hour)), "run timestamps untouched")
	require.Len(t, got[1].Jobs, 1)

	// And the run no longer needs job fetching.
	ids, err := st.RunsNeedingJobs("o", "r", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestStoreRunAttemptRoundTrip(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	runs := sampleRuns(base)
	runs[0].Attempt = 3

	require.NoError(t, st.UpsertRuns("o", "r", runs))
	got, err := st.LoadRuns("o", "r", base.Add(-time.Hour), base.Add(2*time.Hour))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, int64(3), got[0].Attempt)
	assert.Equal(t, int64(1), got[1].Attempt, "unset attempt defaults to 1")
}
