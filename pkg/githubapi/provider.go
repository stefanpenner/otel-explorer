package githubapi

import (
	"context"
)

// GitHubProvider defines the interface for interacting with GitHub's API.
type GitHubProvider interface {
	FetchWorkflowRuns(ctx context.Context, baseURL, headSHA string, branch, event string) ([]WorkflowRun, error)
	FetchRecentWorkflowRuns(ctx context.Context, owner, repo string, days int, branch, workflow string, onPage func(fetched, total int)) ([]WorkflowRun, error)
	FetchRepository(ctx context.Context, baseURL string) (*RepoMeta, error)
	FetchCommitAssociatedPRs(ctx context.Context, owner, repo, sha string) ([]PullAssociated, error)
	FetchCommit(ctx context.Context, baseURL, sha string) (*CommitResponse, error)
	FetchPullRequest(ctx context.Context, baseURL, identifier string) (*PullRequest, error)
	FetchPRReviews(ctx context.Context, owner, repo, prNumber string) ([]Review, error)
	FetchPRComments(ctx context.Context, owner, repo, prNumber string) ([]Review, error)
	FetchJobsPaginated(ctx context.Context, urlValue string) ([]Job, error)
	FetchBranchProtection(ctx context.Context, owner, repo, branch string) (*BranchProtection, error)
	FetchRunTiming(ctx context.Context, owner, repo string, runID int64) (*RunTiming, error)
	FetchCheckRunsForCommit(ctx context.Context, owner, repo, sha string) ([]CheckRun, error)
	FetchAnnotations(ctx context.Context, owner, repo string, checkRunID int64) ([]Annotation, error)
	ListArtifacts(ctx context.Context, owner, repo string, runID int64) ([]Artifact, error)
	DownloadArtifact(ctx context.Context, url string) ([]byte, error)
	FetchJobLog(ctx context.Context, owner, repo string, jobID int64) ([]byte, error)
	FetchWorkflowRun(ctx context.Context, owner, repo string, runID int64) (*WorkflowRun, error)
}
