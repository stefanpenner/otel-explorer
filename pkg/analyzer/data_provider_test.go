package analyzer

import (
	"context"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCommitFetchFallsBackToUnfilteredRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sha := "abc12345deadbeef"
	baseURL := "https://api.github.com/repos/owner/repo"
	commitURL := "https://github.com/owner/repo/commit/" + sha

	// The unfiltered runs that exist for this commit (e.g. pull_request event on feature branch)
	unfilteredRuns := []githubapi.WorkflowRun{
		{
			ID:        1,
			Name:      "CI",
			Status:    "completed",
			CreatedAt: "2026-01-15T10:00:00Z",
			UpdatedAt: "2026-01-15T10:05:00Z",
		},
	}

	mockClient := new(mockGitHubProvider)

	// Commit has no associated PRs, so branch resolution falls back to repo default
	mockClient.On("FetchCommitAssociatedPRs", mock.Anything, "owner", "repo", sha).
		Return([]githubapi.PullAssociated{}, nil)
	mockClient.On("FetchRepository", mock.Anything, baseURL).
		Return(&githubapi.RepoMeta{DefaultBranch: "main"}, nil)

	// Unfiltered fetch returns runs; filtered (branch=main, event=push) returns nothing
	mockClient.On("FetchWorkflowRuns", mock.Anything, baseURL, sha, "", "").
		Return(unfilteredRuns, nil)
	mockClient.On("FetchWorkflowRuns", mock.Anything, baseURL, sha, "main", "push").
		Return([]githubapi.WorkflowRun{}, nil)

	// Commit metadata
	mockClient.On("FetchCommit", mock.Anything, baseURL, sha).
		Return(&githubapi.CommitResponse{
			Commit: githubapi.CommitDetails{
				Author:    githubapi.CommitAuthor{Date: "2026-01-15T09:59:00Z"},
				Committer: githubapi.CommitAuthor{Date: "2026-01-15T09:59:00Z"},
			},
		}, nil)

	// Jobs for compute time calculation
	mockClient.On("FetchJobsPaginated", mock.Anything, mock.Anything).
		Return([]githubapi.Job{}, nil)

	// Branch protection
	mockClient.On("FetchBranchProtection", mock.Anything, "owner", "repo", "main").
		Return((*githubapi.BranchProtection)(nil), nil)

	provider := NewDataProvider(mockClient)
	result, err := provider.Fetch(ctx, commitURL, 0, nil, AnalyzeOptions{})

	assert.NoError(t, err)
	assert.NotNil(t, result, "expected non-nil result when unfiltered runs exist")
	assert.Equal(t, unfilteredRuns, result.Runs, "should fall back to unfiltered runs when branch+push filter returns nothing")
}

func TestCommitFetchUsesFilteredRunsWhenAvailable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sha := "abc12345deadbeef"
	baseURL := "https://api.github.com/repos/owner/repo"
	commitURL := "https://github.com/owner/repo/commit/" + sha

	unfilteredRuns := []githubapi.WorkflowRun{
		{ID: 1, Name: "CI", Status: "completed", CreatedAt: "2026-01-15T10:00:00Z", UpdatedAt: "2026-01-15T10:05:00Z"},
		{ID: 2, Name: "Deploy", Status: "completed", CreatedAt: "2026-01-15T10:00:00Z", UpdatedAt: "2026-01-15T10:10:00Z"},
	}
	filteredRuns := []githubapi.WorkflowRun{
		{ID: 1, Name: "CI", Status: "completed", CreatedAt: "2026-01-15T10:00:00Z", UpdatedAt: "2026-01-15T10:05:00Z"},
	}

	mockClient := new(mockGitHubProvider)

	mockClient.On("FetchCommitAssociatedPRs", mock.Anything, "owner", "repo", sha).
		Return([]githubapi.PullAssociated{}, nil)
	mockClient.On("FetchRepository", mock.Anything, baseURL).
		Return(&githubapi.RepoMeta{DefaultBranch: "main"}, nil)

	mockClient.On("FetchWorkflowRuns", mock.Anything, baseURL, sha, "", "").
		Return(unfilteredRuns, nil)
	mockClient.On("FetchWorkflowRuns", mock.Anything, baseURL, sha, "main", "push").
		Return(filteredRuns, nil)

	mockClient.On("FetchCommit", mock.Anything, baseURL, sha).
		Return(&githubapi.CommitResponse{
			Commit: githubapi.CommitDetails{
				Author:    githubapi.CommitAuthor{Date: "2026-01-15T09:59:00Z"},
				Committer: githubapi.CommitAuthor{Date: "2026-01-15T09:59:00Z"},
			},
		}, nil)

	mockClient.On("FetchJobsPaginated", mock.Anything, mock.Anything).
		Return([]githubapi.Job{}, nil)

	mockClient.On("FetchBranchProtection", mock.Anything, "owner", "repo", "main").
		Return((*githubapi.BranchProtection)(nil), nil)

	provider := NewDataProvider(mockClient)
	result, err := provider.Fetch(ctx, commitURL, 0, nil, AnalyzeOptions{})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, filteredRuns, result.Runs, "should use filtered runs when branch+push filter returns results")
}

func TestPendingReviewsAreDropped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseURL := "https://api.github.com/repos/owner/repo"

	mockClient := new(mockGitHubProvider)
	mockClient.On("FetchPullRequest", mock.Anything, baseURL, "1").
		Return(&githubapi.PullRequest{
			Number: 1,
			Head:   githubapi.PRRef{Ref: "feature", SHA: "abc123"},
			Base:   githubapi.PRRef{Ref: "main"},
		}, nil)
	// A PENDING (unsubmitted) review has submitted_at == null → empty string.
	mockClient.On("FetchPRReviews", mock.Anything, "owner", "repo", "1").
		Return([]githubapi.Review{
			{State: "PENDING", SubmittedAt: "", User: githubapi.UserInfo{Login: "drafter"}},
			{State: "APPROVED", SubmittedAt: "2026-01-15T10:00:00Z", User: githubapi.UserInfo{Login: "reviewer"}},
		}, nil)
	mockClient.On("FetchPRComments", mock.Anything, "owner", "repo", "1").
		Return([]githubapi.Review{}, nil)
	mockClient.On("FetchWorkflowRuns", mock.Anything, baseURL, "abc123", "", "").
		Return([]githubapi.WorkflowRun{
			{ID: 1, Name: "CI", Status: "completed", CreatedAt: "2026-01-15T10:05:00Z", UpdatedAt: "2026-01-15T10:15:00Z"},
		}, nil)
	mockClient.On("FetchBranchProtection", mock.Anything, "owner", "repo", "main").
		Return((*githubapi.BranchProtection)(nil), nil)

	provider := NewDataProvider(mockClient)
	raw, err := provider.Fetch(ctx, "https://github.com/owner/repo/pull/1", 0, nil, AnalyzeOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, raw)

	assert.Len(t, raw.ReviewEvents, 1, "pending review without submitted_at must be dropped")
	assert.Equal(t, "APPROVED", raw.ReviewEvents[0].State)
	for _, ev := range raw.ReviewEvents {
		assert.Greater(t, ev.TimeMillis(), int64(0), "no review event may carry the 0 timestamp sentinel")
	}
}
