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
	report(reporter, "Parsing URL", fmt.Sprintf("%s/%s", parsed.Owner, parsed.Repo))

	var raw *RawData
	switch parsed.Type {
	case "pr":
		raw, err = p.fetchPullRequest(ctx, parsed, reporter)
	case "run":
		raw, err = p.fetchWorkflowRun(ctx, parsed, reporter)
	default:
		raw, err = p.fetchCommit(ctx, parsed, reporter)
	}
	if err != nil || !hasActivity(raw) {
		return nil, err
	}

	if opts.Window > 0 {
		clipToWindow(raw, opts.Window.Milliseconds())
	}

	if !hasActivity(raw) {
		return nil, nil
	}

	raw.Parsed = parsed
	raw.URLIndex = urlIndex
	return raw, nil
}

func (p *DataProvider) fetchPullRequest(ctx context.Context, parsed utils.ParsedGitHubURL, reporter ProgressReporter) (*RawData, error) {
	baseURL := repoAPI(parsed)
	report(reporter, "Fetching PR metadata", parsed.Identifier)

	prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%s", parsed.Owner, parsed.Repo, parsed.Identifier)
	pr, err := p.client.FetchPullRequest(ctx, baseURL, parsed.Identifier)
	if err != nil {
		return nil, err
	}
	if pr.Head.Ref == "" || pr.Head.SHA == "" {
		return nil, errors.New("invalid PR response - missing head or base information")
	}

	report(reporter, "Fetching PR reviews and comments", parsed.Identifier)
	reviews, err := p.client.FetchPRReviews(ctx, parsed.Owner, parsed.Repo, parsed.Identifier)
	if err != nil {
		return nil, err
	}
	comments, err := p.client.FetchPRComments(ctx, parsed.Owner, parsed.Repo, parsed.Identifier)
	if err != nil {
		comments = nil
	}
	events, mergedAt := reviewAndMerge(reviews, comments, prURL, pr.MergedAt, pr.MergedBy, pr.Number, pr.Title)

	report(reporter, "Fetching workflow runs", pr.Head.SHA)
	runs, err := p.client.FetchWorkflowRuns(ctx, baseURL, pr.Head.SHA, "", "")
	if err != nil {
		return nil, err
	}

	return &RawData{
		HeadSHA:           pr.Head.SHA,
		BranchName:        pr.Head.Ref,
		DisplayName:       fmt.Sprintf("PR #%s", parsed.Identifier),
		DisplayURL:        prURL,
		ReviewEvents:      events,
		MergedAtMs:        mergedAt,
		Runs:              runs,
		RequiredContexts:  p.requiredContexts(ctx, parsed, pr.Base.Ref, reporter),
		ChangedFilesCount: pr.ChangedFiles,
		ChangedAdditions:  pr.Additions,
		ChangedDeletions:  pr.Deletions,
	}, nil
}

func (p *DataProvider) fetchWorkflowRun(ctx context.Context, parsed utils.ParsedGitHubURL, reporter ProgressReporter) (*RawData, error) {
	runID, err := strconv.ParseInt(parsed.Identifier, 10, 64)
	if err != nil {
		return nil, errors.Newf("invalid run ID %q: %w", parsed.Identifier, err)
	}

	report(reporter, "Fetching workflow run", parsed.Identifier)
	run, err := p.client.FetchWorkflowRun(ctx, parsed.Owner, parsed.Repo, runID)
	if err != nil {
		return nil, err
	}

	return &RawData{
		HeadSHA:     run.HeadSHA,
		BranchName:  run.HeadBranch,
		DisplayName: fmt.Sprintf("run %s", parsed.Identifier),
		DisplayURL:  fmt.Sprintf("https://github.com/%s/%s/actions/runs/%s", parsed.Owner, parsed.Repo, parsed.Identifier),
		Runs:        []githubapi.WorkflowRun{*run},
	}, nil
}

func (p *DataProvider) fetchCommit(ctx context.Context, parsed utils.ParsedGitHubURL, reporter ProgressReporter) (*RawData, error) {
	baseURL := repoAPI(parsed)
	headSHA := parsed.Identifier

	branch, events, mergedAt := p.associatedPull(ctx, parsed, reporter)
	if branch == "" {
		branch = p.defaultBranch(ctx, baseURL)
	}
	if branch == "" {
		branch = "unknown"
	}

	report(reporter, "Fetching commit runs", headSHA)
	allRuns, err := p.client.FetchWorkflowRuns(ctx, baseURL, headSHA, "", "")
	if err != nil {
		return nil, err
	}
	runs, err := p.client.FetchWorkflowRuns(ctx, baseURL, headSHA, branch, "push")
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		runs = allRuns
	}

	files, adds, dels, commitAt, pushedAt := p.commitMeta(ctx, baseURL, headSHA, allRuns, reporter)
	runs = runsAtOrAfter(runs, commitAt)
	computeMs := p.commitComputeMs(ctx, baseURL, allRuns, reporter)

	return &RawData{
		HeadSHA:                headSHA,
		BranchName:             branch,
		DisplayName:            fmt.Sprintf("commit %s", headSHA[:minInt(8, len(headSHA))]),
		DisplayURL:             fmt.Sprintf("https://github.com/%s/%s/commit/%s", parsed.Owner, parsed.Repo, headSHA),
		ReviewEvents:           events,
		MergedAtMs:             mergedAt,
		CommitTimeMs:           commitAt,
		CommitPushedAtMs:       pushedAt,
		Runs:                   runs,
		AllCommitRunsCount:     len(allRuns),
		AllCommitRunsComputeMs: computeMs,
		RequiredContexts:       p.requiredContexts(ctx, parsed, branch, reporter),
		ChangedFilesCount:      files,
		ChangedAdditions:       adds,
		ChangedDeletions:       dels,
	}, nil
}

func (p *DataProvider) associatedPull(ctx context.Context, parsed utils.ParsedGitHubURL, reporter ProgressReporter) (string, []ReviewEvent, *int64) {
	report(reporter, "Resolving commit branch", parsed.Identifier)
	prs, err := p.client.FetchCommitAssociatedPRs(ctx, parsed.Owner, parsed.Repo, parsed.Identifier)
	if err != nil || len(prs) == 0 {
		return "", nil, nil
	}
	pr := prs[0]

	prNumber := fmt.Sprintf("%d", pr.Number)
	report(reporter, "Fetching associated PR metadata", prNumber)

	var reviews []githubapi.Review
	if got, err := p.client.FetchPRReviews(ctx, parsed.Owner, parsed.Repo, prNumber); err == nil {
		reviews = got
	}
	var comments []githubapi.Review
	if got, err := p.client.FetchPRComments(ctx, parsed.Owner, parsed.Repo, prNumber); err == nil {
		comments = got
	}

	events, mergedAt := reviewAndMerge(reviews, comments, pr.HTMLURL, pr.MergedAt, pr.MergedBy, pr.Number, pr.Title)
	return pr.Base.Ref, events, mergedAt
}

func (p *DataProvider) defaultBranch(ctx context.Context, baseURL string) string {
	repoMeta, err := p.client.FetchRepository(ctx, baseURL)
	if err != nil || repoMeta == nil || repoMeta.DefaultBranch == "" {
		return ""
	}
	return repoMeta.DefaultBranch
}

func reviewAndMerge(reviews, comments []githubapi.Review, pageURL string, mergedAt *string, mergedBy *githubapi.UserInfo, number int, title string) ([]ReviewEvent, *int64) {
	var events []ReviewEvent
	for _, review := range reviews {
		// Pending (unsubmitted) reviews have submitted_at == null; drop
		// them so the 0 timestamp sentinel never enters min/max math.
		if review.SubmittedAt == "" {
			continue
		}
		events = append(events, ReviewEvent{
			Type:     "review",
			State:    review.State,
			Time:     review.SubmittedAt,
			Reviewer: review.User.Login,
			URL:      firstNonEmpty(review.HTMLURL, pageURL),
		})
	}
	for _, comment := range comments {
		events = append(events, ReviewEvent{
			Type:     "comment",
			Time:     comment.SubmittedAt,
			Reviewer: comment.User.Login,
			URL:      firstNonEmpty(comment.HTMLURL, pageURL),
		})
	}
	if mergedAt == nil || *mergedAt == "" {
		return events, nil
	}
	events = append(events, ReviewEvent{
		Type:     "merged",
		Time:     *mergedAt,
		MergedBy: resolvedUser(mergedBy),
		URL:      pageURL,
		PRNumber: number,
		PRTitle:  title,
	})
	if t, ok := utils.ParseTime(*mergedAt); ok {
		ms := t.UnixMilli()
		return events, &ms
	}
	return events, nil
}

func (p *DataProvider) commitMeta(ctx context.Context, baseURL, headSHA string, allRuns []githubapi.WorkflowRun, reporter ProgressReporter) (files, adds, dels int, commitAt, pushedAt *int64) {
	report(reporter, "Fetching commit metadata", headSHA)
	meta, err := p.client.FetchCommit(ctx, baseURL, headSHA)
	if err != nil {
		return 0, 0, 0, nil, nil
	}

	dateStr := meta.Commit.Committer.Date
	if dateStr == "" {
		dateStr = meta.Commit.Author.Date
	}
	if t, ok := utils.ParseTime(dateStr); ok {
		ms := t.UnixMilli()
		commitAt = &ms
	}

	return meta.Stats.Total, meta.Stats.Additions, meta.Stats.Deletions, commitAt, earliestPushMs(allRuns)
}

// earliestPushMs proxies push time from the earliest workflow run created_at.
func earliestPushMs(runs []githubapi.WorkflowRun) *int64 {
	if len(runs) == 0 {
		return nil
	}
	earliest := runs[0]
	for _, run := range runs {
		t1, ok1 := utils.ParseTime(run.CreatedAt)
		t2, ok2 := utils.ParseTime(earliest.CreatedAt)
		if ok1 && ok2 && t1.Before(t2) {
			earliest = run
		}
	}
	t, ok := utils.ParseTime(earliest.CreatedAt)
	if !ok {
		return nil
	}
	ms := t.UnixMilli()
	return &ms
}

func runsAtOrAfter(runs []githubapi.WorkflowRun, commitAt *int64) []githubapi.WorkflowRun {
	if commitAt == nil {
		return runs
	}
	filtered := []githubapi.WorkflowRun{}
	for _, run := range runs {
		if t, ok := utils.ParseTime(run.CreatedAt); ok && t.UnixMilli() >= *commitAt {
			filtered = append(filtered, run)
		}
	}
	return filtered
}

func (p *DataProvider) commitComputeMs(ctx context.Context, baseURL string, runs []githubapi.WorkflowRun, reporter ProgressReporter) int64 {
	report(reporter, "Computing commit job durations", fmt.Sprintf("%d runs", len(runs)))

	// Parallelize fetching jobs for all runs to calculate total compute time.
	// Concurrency is controlled by the HTTP client's semaphore.
	var total int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, run := range runs {
		if run.Status != "completed" {
			continue
		}
		wg.Add(1)
		go func(r githubapi.WorkflowRun) {
			defer wg.Done()
			jobsURL := fmt.Sprintf("%s/actions/runs/%d/jobs?per_page=100", baseURL, r.ID)
			jobs, err := p.client.FetchJobsPaginated(ctx, jobsURL)
			if err != nil {
				return
			}
			ms := attemptComputeMs(jobs, r.RunAttempt)
			mu.Lock()
			total += ms
			mu.Unlock()
		}(run)
	}
	wg.Wait()
	return total
}

func attemptComputeMs(jobs []githubapi.Job, runAttempt int64) int64 {
	if runAttempt == 0 {
		runAttempt = 1
	}
	var sum int64
	for _, job := range jobs {
		// Skip jobs from previous retry attempts to avoid double-counting.
		if job.RunAttempt != 0 && job.RunAttempt != runAttempt {
			continue
		}
		start, startOK := utils.ParseTime(job.StartedAt)
		end, endOK := utils.ParseTime(job.CompletedAt)
		if startOK && endOK && end.After(start) {
			sum += end.Sub(start).Milliseconds()
		}
	}
	return sum
}

func (p *DataProvider) requiredContexts(ctx context.Context, parsed utils.ParsedGitHubURL, branch string, reporter ProgressReporter) []string {
	if branch == "" || branch == "unknown" {
		return nil
	}
	report(reporter, "Fetching branch protection", branch)
	protection, err := p.client.FetchBranchProtection(ctx, parsed.Owner, parsed.Repo, branch)
	if err != nil || protection == nil || protection.RequiredStatusChecks == nil {
		return nil
	}
	var contexts []string
	contexts = append(contexts, protection.RequiredStatusChecks.Contexts...)
	for _, check := range protection.RequiredStatusChecks.Checks {
		contexts = append(contexts, check.Context)
	}
	return contexts
}

func clipToWindow(raw *RawData, windowMs int64) {
	anchor := int64(0)
	if raw.MergedAtMs != nil {
		anchor = *raw.MergedAtMs
	} else {
		anchor = FindLatestTimestamp(raw.Runs)
		for _, event := range raw.ReviewEvents {
			if ms := event.TimeMillis(); ms > anchor {
				anchor = ms
			}
		}
	}
	start := anchor - windowMs

	var runs []githubapi.WorkflowRun
	for _, run := range raw.Runs {
		runTime := int64(0)
		if t, ok := utils.ParseTime(run.UpdatedAt); ok {
			runTime = t.UnixMilli()
		} else if t, ok := utils.ParseTime(run.CreatedAt); ok {
			runTime = t.UnixMilli()
		}
		if runTime >= start {
			runs = append(runs, run)
		}
	}
	raw.Runs = runs

	var events []ReviewEvent
	for _, event := range raw.ReviewEvents {
		if event.TimeMillis() >= start {
			events = append(events, event)
		}
	}
	raw.ReviewEvents = events

	if raw.CommitTimeMs != nil && *raw.CommitTimeMs < start {
		raw.CommitTimeMs = nil
	}
	if raw.CommitPushedAtMs != nil && *raw.CommitPushedAtMs < start {
		raw.CommitPushedAtMs = nil
	}
}

func hasActivity(raw *RawData) bool {
	return raw != nil && (len(raw.Runs) > 0 || len(raw.ReviewEvents) > 0 || raw.CommitTimeMs != nil || raw.CommitPushedAtMs != nil)
}

func report(reporter ProgressReporter, phase, detail string) {
	if reporter == nil {
		return
	}
	reporter.SetPhase(phase)
	reporter.SetDetail(detail)
}

func repoAPI(parsed utils.ParsedGitHubURL) string {
	return fmt.Sprintf("https://api.github.com/repos/%s/%s", parsed.Owner, parsed.Repo)
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
