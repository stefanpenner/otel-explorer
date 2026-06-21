package analyzer

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// RawData contains all the raw data fetched from GitHub for a specific URL.
type RawData struct {
	Parsed                 utils.ParsedGitHubURL
	URLIndex               int
	HeadSHA                string
	BranchName             string
	DisplayName            string
	DisplayURL             string
	ReviewEvents           []ReviewEvent
	MergedAtMs             *int64
	CommitTimeMs           *int64
	CommitPushedAtMs       *int64
	Runs                   []githubapi.WorkflowRun
	AllCommitRunsCount     int
	AllCommitRunsComputeMs int64
	RequiredContexts       []string
	// VCS change stats (from PR or commit metadata — no extra API call)
	ChangedFilesCount int
	ChangedAdditions  int
	ChangedDeletions  int
}

// DataProvider handles fetching data from GitHub.
type DataProvider struct {
	client githubapi.GitHubProvider
}

func NewDataProvider(client githubapi.GitHubProvider) *DataProvider {
	return &DataProvider{client: client}
}

func (p *DataProvider) Fetch(ctx context.Context, githubURL string, urlIndex int, reporter ProgressReporter, opts AnalyzeOptions) (*RawData, error) {
	parsed, err := utils.ParseGitHubURL(githubURL)
	if err != nil {
		return nil, err
	}
	if reporter != nil {
		reporter.SetPhase("Parsing URL")
		reporter.SetDetail(fmt.Sprintf("%s/%s", parsed.Owner, parsed.Repo))
	}
	baseURL := fmt.Sprintf("https://api.github.com/repos/%s/%s", parsed.Owner, parsed.Repo)

	var headSHA, branchName, displayName, displayURL string
	var reviewEvents []ReviewEvent
	var mergedAtMs *int64
	var commitTimeMs *int64
	var commitPushedAtMs *int64
	var runs []githubapi.WorkflowRun
	var changedFilesCount, changedAdditions, changedDeletions int
	allCommitRunsCount := 0
	var allCommitRunsComputeMs int64
	var requiredContexts []string
	var protectionTargetBranch string

	if parsed.Type == "pr" {
		if reporter != nil {
			reporter.SetPhase("Fetching PR metadata")
			reporter.SetDetail(parsed.Identifier)
		}
		analyzingPRURL := fmt.Sprintf("https://github.com/%s/%s/pull/%s", parsed.Owner, parsed.Repo, parsed.Identifier)
		prData, err := p.client.FetchPullRequest(ctx, baseURL, parsed.Identifier)
		if err != nil {
			return nil, err
		}
		if prData.Head.Ref == "" || prData.Head.SHA == "" {
			return nil, errors.New("invalid PR response - missing head or base information")
		}
		headSHA = prData.Head.SHA
		branchName = prData.Head.Ref
		displayName = fmt.Sprintf("PR #%s", parsed.Identifier)
		displayURL = analyzingPRURL

		if reporter != nil {
			reporter.SetPhase("Fetching PR reviews and comments")
			reporter.SetDetail(parsed.Identifier)
		}

		reviews, err := p.client.FetchPRReviews(ctx, parsed.Owner, parsed.Repo, parsed.Identifier)
		if err != nil {
			return nil, err
		}
		for _, review := range reviews {
			// Pending (unsubmitted) reviews have submitted_at == null; drop
			// them so the 0 timestamp sentinel never enters min/max math.
			if review.SubmittedAt == "" {
				continue
			}
			reviewEvents = append(reviewEvents, ReviewEvent{
				Type:     "review",
				State:    review.State,
				Time:     review.SubmittedAt,
				Reviewer: review.User.Login,
				URL:      firstNonEmpty(review.HTMLURL, analyzingPRURL),
			})
		}

		comments, err := p.client.FetchPRComments(ctx, parsed.Owner, parsed.Repo, parsed.Identifier)
		if err == nil {
			for _, comment := range comments {
				reviewEvents = append(reviewEvents, ReviewEvent{
					Type:     "comment",
					Time:     comment.SubmittedAt,
					Reviewer: comment.User.Login,
					URL:      firstNonEmpty(comment.HTMLURL, analyzingPRURL),
				})
			}
		}

		if prData.MergedAt != nil && *prData.MergedAt != "" {
			reviewEvents = append(reviewEvents, ReviewEvent{
				Type:     "merged",
				Time:     *prData.MergedAt,
				MergedBy: resolvedUser(prData.MergedBy),
				URL:      analyzingPRURL,
				PRNumber: prData.Number,
				PRTitle:  prData.Title,
			})
			if t, ok := utils.ParseTime(*prData.MergedAt); ok {
				ms := t.UnixMilli()
				mergedAtMs = &ms
			}
		}

		// PR stats are already in the response — no extra API call
		changedFilesCount = prData.ChangedFiles
		changedAdditions = prData.Additions
		changedDeletions = prData.Deletions

		if reporter != nil {
			reporter.SetPhase("Fetching workflow runs")
			reporter.SetDetail(headSHA)
		}
		runs, err = p.client.FetchWorkflowRuns(ctx, baseURL, headSHA, "", "")
		if err != nil {
			return nil, err
		}
		// Track target branch for branch protection lookup (base branch of PR)
		protectionTargetBranch = prData.Base.Ref
	} else if parsed.Type == "run" {
		runID, err := strconv.ParseInt(parsed.Identifier, 10, 64)
		if err != nil {
			return nil, errors.Newf("invalid run ID %q: %w", parsed.Identifier, err)
		}
		if reporter != nil {
			reporter.SetPhase("Fetching workflow run")
			reporter.SetDetail(parsed.Identifier)
		}
		run, err := p.client.FetchWorkflowRun(ctx, parsed.Owner, parsed.Repo, runID)
		if err != nil {
			return nil, err
		}
		headSHA = run.HeadSHA
		branchName = run.HeadBranch
		displayName = fmt.Sprintf("run %s", parsed.Identifier)
		displayURL = fmt.Sprintf("https://github.com/%s/%s/actions/runs/%s", parsed.Owner, parsed.Repo, parsed.Identifier)
		runs = []githubapi.WorkflowRun{*run}
	} else {
		analyzingCommitURL := fmt.Sprintf("https://github.com/%s/%s/commit/%s", parsed.Owner, parsed.Repo, parsed.Identifier)
		headSHA = parsed.Identifier
		displayName = fmt.Sprintf("commit %s", headSHA[:minInt(8, len(headSHA))])
		displayURL = analyzingCommitURL

		targetBranch := ""
		if reporter != nil {
			reporter.SetPhase("Resolving commit branch")
			reporter.SetDetail(headSHA)
		}
		prs, err := p.client.FetchCommitAssociatedPRs(ctx, parsed.Owner, parsed.Repo, headSHA)
		if err == nil && len(prs) > 0 {
			targetBranch = prs[0].Base.Ref

			// Fetch reviews and comments for the first associated PR
			prNumber := fmt.Sprintf("%d", prs[0].Number)
			if reporter != nil {
				reporter.SetPhase("Fetching associated PR metadata")
				reporter.SetDetail(prNumber)
			}

			reviews, err := p.client.FetchPRReviews(ctx, parsed.Owner, parsed.Repo, prNumber)
			if err == nil {
				for _, review := range reviews {
					// Pending (unsubmitted) reviews have submitted_at == null;
					// drop them so the 0 timestamp sentinel never enters
					// min/max math.
					if review.SubmittedAt == "" {
						continue
					}
					reviewEvents = append(reviewEvents, ReviewEvent{
						Type:     "review",
						State:    review.State,
						Time:     review.SubmittedAt,
						Reviewer: review.User.Login,
						URL:      firstNonEmpty(review.HTMLURL, prs[0].HTMLURL),
					})
				}
			}

			comments, err := p.client.FetchPRComments(ctx, parsed.Owner, parsed.Repo, prNumber)
			if err == nil {
				for _, comment := range comments {
					reviewEvents = append(reviewEvents, ReviewEvent{
						Type:     "comment",
						Time:     comment.SubmittedAt,
						Reviewer: comment.User.Login,
						URL:      firstNonEmpty(comment.HTMLURL, prs[0].HTMLURL),
					})
				}
			}

			if prs[0].MergedAt != nil && *prs[0].MergedAt != "" {
				reviewEvents = append(reviewEvents, ReviewEvent{
					Type:     "merged",
					Time:     *prs[0].MergedAt,
					MergedBy: resolvedUser(prs[0].MergedBy),
					URL:      prs[0].HTMLURL,
					PRNumber: prs[0].Number,
					PRTitle:  prs[0].Title,
				})
				if t, ok := utils.ParseTime(*prs[0].MergedAt); ok {
					ms := t.UnixMilli()
					mergedAtMs = &ms
				}
			}
		}
		if targetBranch == "" {
			if repoMeta, err := p.client.FetchRepository(ctx, baseURL); err == nil && repoMeta != nil && repoMeta.DefaultBranch != "" {
				targetBranch = repoMeta.DefaultBranch
			}
		}
		branchName = targetBranch
		if branchName == "" {
			branchName = "unknown"
		}

		if reporter != nil {
			reporter.SetPhase("Fetching commit runs")
			reporter.SetDetail(headSHA)
		}
		allRunsForHead, err := p.client.FetchWorkflowRuns(ctx, baseURL, headSHA, "", "")
		if err != nil {
			return nil, err
		}
		allCommitRunsCount = len(allRunsForHead)

		runs, err = p.client.FetchWorkflowRuns(ctx, baseURL, headSHA, branchName, "push")
		if err != nil {
			return nil, err
		}
		if len(runs) == 0 {
			runs = allRunsForHead
		}
		if reporter != nil {
			reporter.SetPhase("Fetching commit metadata")
			reporter.SetDetail(headSHA)
		}
		commitMeta, err := p.client.FetchCommit(ctx, baseURL, headSHA)
		if err == nil {
			changedFilesCount = commitMeta.Stats.Total
			changedAdditions = commitMeta.Stats.Additions
			changedDeletions = commitMeta.Stats.Deletions
			dateStr := commitMeta.Commit.Committer.Date
			if dateStr == "" {
				dateStr = commitMeta.Commit.Author.Date
			}
			if t, ok := utils.ParseTime(dateStr); ok {
				ms := t.UnixMilli()
				commitTimeMs = &ms
			}

			// For commit pushed time, we'll use the earliest workflow run's created_at
			// as a proxy if we don't have a more direct way to get the push event time.
			if len(allRunsForHead) > 0 {
				earliestRun := allRunsForHead[0]
				for _, run := range allRunsForHead {
					if t1, ok1 := utils.ParseTime(run.CreatedAt); ok1 {
						if t2, ok2 := utils.ParseTime(earliestRun.CreatedAt); ok2 {
							if t1.Before(t2) {
								earliestRun = run
							}
						}
					}
				}
				if t, ok := utils.ParseTime(earliestRun.CreatedAt); ok {
					ms := t.UnixMilli()
					commitPushedAtMs = &ms
				}
			}
		}

		if commitTimeMs != nil {
			filtered := []githubapi.WorkflowRun{}
			for _, run := range runs {
				if t, ok := utils.ParseTime(run.CreatedAt); ok {
					if t.UnixMilli() >= *commitTimeMs {
						filtered = append(filtered, run)
					}
				}
			}
			runs = filtered
		}

		if reporter != nil {
			reporter.SetPhase("Computing commit job durations")
			reporter.SetDetail(fmt.Sprintf("%d runs", len(allRunsForHead)))
		}

		// Parallelize fetching jobs for all runs to calculate total compute time.
		// Concurrency is controlled by the HTTP client's semaphore.
		var computeMu sync.Mutex
		var computeWg sync.WaitGroup

		for _, run := range allRunsForHead {
			if run.Status != "completed" {
				continue
			}
			computeWg.Add(1)
			go func(r githubapi.WorkflowRun) {
				defer computeWg.Done()

				jobsURL := fmt.Sprintf("%s/actions/runs/%d/jobs?per_page=100", baseURL, r.ID)
				jobs, err := p.client.FetchJobsPaginated(ctx, jobsURL)
				if err != nil {
					return
				}

				currentAttempt := r.RunAttempt
				if currentAttempt == 0 {
					currentAttempt = 1
				}
				var runCompute int64
				for _, job := range jobs {
					// Skip jobs from previous retry attempts to avoid double-counting
					if job.RunAttempt != 0 && job.RunAttempt != currentAttempt {
						continue
					}
					if start, ok := utils.ParseTime(job.StartedAt); ok {
						if end, ok := utils.ParseTime(job.CompletedAt); ok {
							if end.After(start) {
								runCompute += end.Sub(start).Milliseconds()
							}
						}
					}
				}
				computeMu.Lock()
				allCommitRunsComputeMs += runCompute
				computeMu.Unlock()
			}(run)
		}
		computeWg.Wait()

		// Track target branch for branch protection lookup
		protectionTargetBranch = branchName
	}

	// Fetch branch protection for target branch
	if protectionTargetBranch != "" && protectionTargetBranch != "unknown" {
		if reporter != nil {
			reporter.SetPhase("Fetching branch protection")
			reporter.SetDetail(protectionTargetBranch)
		}
		protection, err := p.client.FetchBranchProtection(ctx, parsed.Owner, parsed.Repo, protectionTargetBranch)
		if err == nil && protection != nil && protection.RequiredStatusChecks != nil {
			requiredContexts = append(requiredContexts, protection.RequiredStatusChecks.Contexts...)
			for _, check := range protection.RequiredStatusChecks.Checks {
				requiredContexts = append(requiredContexts, check.Context)
			}
		}
	}

	if len(runs) == 0 && len(reviewEvents) == 0 && commitTimeMs == nil && commitPushedAtMs == nil {
		return nil, nil
	}

	// Apply window filtering if requested
	if opts.Window > 0 {
		var anchorTimeMs int64
		if mergedAtMs != nil {
			anchorTimeMs = *mergedAtMs
		} else {
			// Fallback to latest activity
			anchorTimeMs = FindLatestTimestamp(runs)
			for _, event := range reviewEvents {
				ms := event.TimeMillis()
				if ms > anchorTimeMs {
					anchorTimeMs = ms
				}
			}
		}

		startTimeMs := anchorTimeMs - opts.Window.Milliseconds()

		// Filter runs
		var filteredRuns []githubapi.WorkflowRun
		for _, run := range runs {
			runTime := int64(0)
			if t, ok := utils.ParseTime(run.UpdatedAt); ok {
				runTime = t.UnixMilli()
			} else if t, ok := utils.ParseTime(run.CreatedAt); ok {
				runTime = t.UnixMilli()
			}
			if runTime >= startTimeMs {
				filteredRuns = append(filteredRuns, run)
			}
		}
		runs = filteredRuns

		// Filter review events
		var filteredReviews []ReviewEvent
		for _, event := range reviewEvents {
			if event.TimeMillis() >= startTimeMs {
				filteredReviews = append(filteredReviews, event)
			}
		}
		reviewEvents = filteredReviews

		// Filter commit time
		if commitTimeMs != nil && *commitTimeMs < startTimeMs {
			commitTimeMs = nil
		}
		// Filter push time
		if commitPushedAtMs != nil && *commitPushedAtMs < startTimeMs {
			commitPushedAtMs = nil
		}
	}

	if len(runs) == 0 && len(reviewEvents) == 0 && commitTimeMs == nil && commitPushedAtMs == nil {
		return nil, nil
	}

	return &RawData{
		Parsed:                 parsed,
		URLIndex:               urlIndex,
		HeadSHA:                headSHA,
		BranchName:             branchName,
		DisplayName:            displayName,
		DisplayURL:             displayURL,
		ReviewEvents:           reviewEvents,
		MergedAtMs:             mergedAtMs,
		CommitTimeMs:           commitTimeMs,
		CommitPushedAtMs:       commitPushedAtMs,
		Runs:                   runs,
		AllCommitRunsCount:     allCommitRunsCount,
		AllCommitRunsComputeMs: allCommitRunsComputeMs,
		RequiredContexts:       requiredContexts,
		ChangedFilesCount:      changedFilesCount,
		ChangedAdditions:       changedAdditions,
		ChangedDeletions:       changedDeletions,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func resolvedUser(user *githubapi.UserInfo) string {
	if user == nil {
		return ""
	}
	if user.Login != "" {
		return user.Login
	}
	return user.Name
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
