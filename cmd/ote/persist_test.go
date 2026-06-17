package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubJobProvider serves one job per run, deriving a unique job ID from the
// run ID in the URL so the jobs table never collides across runs.
type stubJobProvider struct {
	githubapi.GitHubProvider
}

func (stubJobProvider) FetchJobsPaginated(_ context.Context, url string) ([]githubapi.Job, error) {
	var runID int64
	if idx := strings.LastIndex(url, "/runs/"); idx >= 0 {
		_, _ = fmt.Sscanf(url[idx:], "/runs/%d/jobs", &runID)
	}
	return []githubapi.Job{{
		ID: runID*100 + 1, Name: "build", Status: "completed", Conclusion: "success",
	}}, nil
}

func makeRawRun(id int64, status, conclusion string, created time.Time) githubapi.WorkflowRun {
	return githubapi.WorkflowRun{
		ID:         id,
		RunAttempt: 1,
		Status:     status,
		Conclusion: conclusion,
		CreatedAt:  created.Format(time.RFC3339),
		UpdatedAt:  created.Add(5 * time.Minute).Format(time.RFC3339),
		Repository: githubapi.RepoRef{Owner: githubapi.RepoOwner{Login: "o"}, Name: "r"},
	}
}

func TestPersistRunsToStore(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	ctx := context.Background()
	now := time.Now()

	results := []analyzer.URLResult{{
		Owner: "o",
		Repo:  "r",
		RawRuns: []githubapi.WorkflowRun{
			makeRawRun(1, "completed", "success", now.Add(-2*time.Hour)),
			makeRawRun(2, "in_progress", "", now.Add(-5*time.Minute)),
		},
	}}
	provider := stubJobProvider{}

	dbPath, err := store.DefaultPath()
	require.NoError(t, err)

	// Opt-in gate: with no store yet, persistence must not create one.
	persistRunsToStore(ctx, provider, results)
	_, statErr := os.Stat(dbPath)
	assert.True(t, os.IsNotExist(statErr), "must not create a store when none exists")

	// User has synced before (store exists): now persistence should seed it.
	st, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, st.Close())

	persistRunsToStore(ctx, provider, results)

	st2, err := store.Open(dbPath)
	require.NoError(t, err)
	defer st2.Close()
	runs, err := st2.LoadRuns("o", "r", now.Add(-24*time.Hour), now.Add(time.Hour))
	require.NoError(t, err)

	require.Len(t, runs, 1, "only the completed run should be persisted")
	assert.Equal(t, int64(1), runs[0].ID)
	assert.NotEmpty(t, runs[0].Jobs, "completed run should carry its fetched jobs")
}
