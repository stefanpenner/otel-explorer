package store

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProvider serves a fixed run listing and counts API calls.
type fakeProvider struct {
	githubapi.GitHubProvider
	runs      []githubapi.WorkflowRun
	listCalls atomic.Int64
	jobCalls  atomic.Int64
	jobErr    error
}

func (f *fakeProvider) FetchRecentWorkflowRuns(ctx context.Context, owner, repo string, days int, branch, workflow string, onPage func(fetched, total int)) ([]githubapi.WorkflowRun, error) {
	f.listCalls.Add(1)
	// Honor the window like the real API: only runs created within `days`.
	cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	var out []githubapi.WorkflowRun
	for _, run := range f.runs {
		created, _ := time.Parse(time.RFC3339, run.CreatedAt)
		if created.After(cutoff) {
			out = append(out, run)
		}
	}
	return out, nil
}

func (f *fakeProvider) FetchJobsPaginated(ctx context.Context, urlValue string) ([]githubapi.Job, error) {
	f.jobCalls.Add(1)
	if f.jobErr != nil {
		return nil, f.jobErr
	}
	var runID int64
	idx := strings.LastIndex(urlValue, "/runs/")
	if _, err := fmt.Sscanf(urlValue[idx:], "/runs/%d/jobs", &runID); err != nil {
		return nil, err
	}
	return []githubapi.Job{{
		ID: runID * 100, Name: "build", Status: "completed", Conclusion: "success",
		CreatedAt:   time.Now().Add(-time.Hour).Format(time.RFC3339),
		StartedAt:   time.Now().Add(-time.Hour).Format(time.RFC3339),
		CompletedAt: time.Now().Add(-30 * time.Minute).Format(time.RFC3339),
	}}, nil
}

func TestSyncIncremental(t *testing.T) {
	t.Parallel()
	st, err := Open(filepath.Join(t.TempDir(), "ote.db"))
	require.NoError(t, err)
	defer st.Close()

	now := time.Now()
	provider := &fakeProvider{}
	for i := 0; i < 5; i++ {
		provider.runs = append(provider.runs, githubapi.WorkflowRun{
			ID: int64(i + 1), RunAttempt: 1, Name: "CI", Status: "completed", Conclusion: "success",
			HeadSHA:    fmt.Sprintf("sha%d", i),
			CreatedAt:  now.Add(-time.Duration(i+1) * time.Hour).Format(time.RFC3339),
			UpdatedAt:  now.Add(-time.Duration(i) * time.Hour).Format(time.RFC3339),
			Repository: githubapi.RepoRef{Owner: githubapi.RepoOwner{Login: "o"}, Name: "r"},
		})
	}

	stats, err := Sync(context.Background(), provider, st, "o", "r", 7, nil)
	require.NoError(t, err)
	assert.Equal(t, 5, stats.RunsFetched)
	assert.Equal(t, 5, stats.JobsFetched, "first sync fetches all job detail")
	assert.Equal(t, int64(5), provider.jobCalls.Load())

	// Second sync: nothing new — zero job fetches.
	stats, err = Sync(context.Background(), provider, st, "o", "r", 7, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, stats.JobsFetched, "re-sync must not refetch job detail")
	assert.Equal(t, int64(5), provider.jobCalls.Load(), "no additional job API calls")

	// Store holds everything with jobs attached.
	runs, err := st.LoadRuns("o", "r", now.Add(-7*24*time.Hour), now)
	require.NoError(t, err)
	require.Len(t, runs, 5)
	for _, run := range runs {
		assert.Len(t, run.Jobs, 1, "run %d has its job", run.ID)
	}
}

func TestSyncBackfillsDeeperHistory(t *testing.T) {
	t.Parallel()
	st, err := Open(filepath.Join(t.TempDir(), "ote.db"))
	require.NoError(t, err)
	defer st.Close()

	now := time.Now()
	provider := &fakeProvider{}
	for i := 0; i < 10; i++ {
		provider.runs = append(provider.runs, githubapi.WorkflowRun{
			ID: int64(i + 1), RunAttempt: 1, Name: "CI", Status: "completed", Conclusion: "success",
			HeadSHA:    fmt.Sprintf("sha%d", i),
			CreatedAt:  now.Add(-time.Duration(i)*24*time.Hour - time.Hour).Format(time.RFC3339),
			UpdatedAt:  now.Add(-time.Duration(i) * 24 * time.Hour).Format(time.RFC3339),
			Repository: githubapi.RepoRef{Owner: githubapi.RepoOwner{Login: "o"}, Name: "r"},
		})
	}

	// First sync: shallow 2-day window.
	_, err = Sync(context.Background(), provider, st, "o", "r", 2, nil)
	require.NoError(t, err)

	// Second sync asks for 10 days: the watermark must NOT clamp the listing
	// window below the requested depth when the store lacks older history.
	stats, err := Sync(context.Background(), provider, st, "o", "r", 10, nil)
	require.NoError(t, err)
	assert.Equal(t, 10, stats.RunsFetched, "deeper request must re-list the full window")

	runs, err := st.LoadRuns("o", "r", now.Add(-11*24*time.Hour), now)
	require.NoError(t, err)
	assert.Len(t, runs, 10, "all 10 days of history stored after backfill")
	for _, run := range runs {
		assert.NotEmpty(t, run.Jobs, "backfilled run %d has job detail", run.ID)
	}
}

func TestSyncPropagatesFetchJobsError(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)

	now := time.Now()
	provider := &fakeProvider{jobErr: fmt.Errorf("api down")}
	provider.runs = append(provider.runs, githubapi.WorkflowRun{
		ID: int64(1), RunAttempt: 1, Name: "CI", Status: "completed", Conclusion: "success",
		HeadSHA:    "sha1",
		CreatedAt:  now.Add(-time.Hour).Format(time.RFC3339),
		UpdatedAt:  now.Format(time.RFC3339),
		Repository: githubapi.RepoRef{Owner: githubapi.RepoOwner{Login: "o"}, Name: "r"},
	})

	_, err := Sync(context.Background(), provider, st, "o", "r", 7, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "api down")
	assert.Equal(t, int64(1), provider.jobCalls.Load(), "the failing run must still have been attempted")
}
