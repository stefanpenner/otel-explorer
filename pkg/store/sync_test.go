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
}

func (f *fakeProvider) FetchRecentWorkflowRuns(ctx context.Context, owner, repo string, days int, branch, workflow string, onPage func(fetched, total int)) ([]githubapi.WorkflowRun, error) {
	f.listCalls.Add(1)
	return f.runs, nil
}

func (f *fakeProvider) FetchJobsPaginated(ctx context.Context, urlValue string) ([]githubapi.Job, error) {
	f.jobCalls.Add(1)
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
			HeadSHA:   fmt.Sprintf("sha%d", i),
			CreatedAt: now.Add(-time.Duration(i+1) * time.Hour).Format(time.RFC3339),
			UpdatedAt: now.Add(-time.Duration(i) * time.Hour).Format(time.RFC3339),
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
