package githubapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func fetchWithAuth(ctx context.Context, client *Client, urlValue string, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", urlValue, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token "+client.context.GitHubToken)
	if accept == "" {
		accept = "application/vnd.github.v3+json"
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", "Node")

	resp, err := client.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, handleGithubError(resp, urlValue)
	}
	return resp, nil
}

func decodeJSON[T any](resp *http.Response, target *T) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}

func handleGithubError(response *http.Response, requestURL string) error {
	defer response.Body.Close()
	status := response.StatusCode
	statusText := response.Status

	var bodyText string
	var message string
	var documentationURL string

	contentType := response.Header.Get("content-type")
	body, _ := io.ReadAll(response.Body)
	if strings.Contains(contentType, "application/json") {
		var data GitHubError
		if err := json.Unmarshal(body, &data); err == nil {
			message = data.Message
			documentationURL = data.DocumentationURL
		}
	} else {
		bodyText = string(body)
	}

	ssoHeader := response.Header.Get("x-github-sso")
	oauthScopes := response.Header.Get("x-oauth-scopes")
	acceptedScopes := response.Header.Get("x-accepted-oauth-scopes")
	rateRemaining := response.Header.Get("x-ratelimit-remaining")

	base := fmt.Sprintf("Error fetching %s: %d %s", requestURL, status, statusText)
	detail := message
	if detail == "" {
		detail = bodyText
	}

	if status == 401 {
		lines := []string{
			base + formatDetail(detail),
			"➡️  Authentication failed. Ensure a valid token (env GITHUB_TOKEN or CLI arg).",
			"   - Fine-grained PAT: grant repository access and Read for: Contents, Actions, Pull requests, Checks.",
			"   - Classic PAT: include repo scope for private repos.",
		}
		if documentationURL != "" {
			lines = append(lines, "   Docs: "+documentationURL)
		}
		return errors.New(strings.Join(lines, "\n"))
	}

	if status == 403 {
		if ssoHeader != "" && strings.Contains(strings.ToLower(ssoHeader), "required") {
			lines := []string{
				base + formatDetail(detail),
				"❌ GitHub API request forbidden due to SSO requirement for this token.",
			}
			if ssoURL := extractSSOURL(ssoHeader); ssoURL != "" {
				lines = append(lines, fmt.Sprintf("➡️  Authorize SSO for this token by visiting:\n   %s\nThen re-run the command.", ssoURL))
			} else {
				lines = append(lines, "➡️  Authorize SSO for this token in your organization, then re-run.")
			}
			return errors.New(strings.Join(lines, "\n"))
		}

		if rateRemaining == "0" {
			lines := []string{
				base + formatDetail(detail),
				"➡️  API rate limit reached. Wait for reset or use an authenticated token with higher limits.",
			}
			return errors.New(strings.Join(lines, "\n"))
		}

		lines := []string{
			base + formatDetail(detail),
			"➡️  Permission issue. Verify token access to this repository and required scopes.",
		}
		if acceptedScopes != "" {
			lines = append(lines, fmt.Sprintf("   Required scopes (server hint): %s", acceptedScopes))
		}
		if oauthScopes != "" {
			lines = append(lines, fmt.Sprintf("   Your token scopes: %s", oauthScopes))
		}
		lines = append(lines, "   - Fine-grained PAT: grant repo access and Read for Contents, Actions, Pull requests, Checks.")
		lines = append(lines, "   - Classic PAT: include repo scope for private repos.")
		if documentationURL != "" {
			lines = append(lines, "   Docs: "+documentationURL)
		}
		return errors.New(strings.Join(lines, "\n"))
	}

	if status == 404 {
		lines := []string{
			base + formatDetail(detail),
			"➡️  Not found. On private repos, 404 can indicate insufficient token access. Check repository access and scopes.",
		}
		return errors.New(strings.Join(lines, "\n"))
	}

	if detail != "" {
		return fmt.Errorf("%s - %s", base, detail)
	}
	return errors.New(base)
}

func formatDetail(detail string) string {
	if detail == "" {
		return ""
	}
	return " - " + detail
}

func extractSSOURL(header string) string {
	parts := strings.Split(header, "url=")
	if len(parts) < 2 {
		return ""
	}
	segment := parts[1]
	if i := strings.IndexAny(segment, "; "); i >= 0 {
		return segment[:i]
	}
	return segment
}

func (c *Client) FetchWorkflowRuns(ctx context.Context, baseURL, headSHA string, branch, event string) ([]WorkflowRun, error) {
	ctx, span := getTracer().Start(ctx, "FetchWorkflowRuns", trace.WithAttributes(
		attribute.String("github.baseURL", baseURL),
		attribute.String("github.headSHA", headSHA),
		attribute.String("github.branch", branch),
		attribute.String("github.event", event),
	))
	defer span.End()

	params := url.Values{}
	params.Set("head_sha", headSHA)
	params.Set("per_page", "100")
	if branch != "" {
		params.Set("branch", branch)
	}
	if event != "" {
		params.Set("event", event)
	}
	runsURL := fmt.Sprintf("%s/actions/runs?%s", baseURL, params.Encode())
	return fetchWorkflowRunsPaginated(ctx, c, runsURL, nil)
}

func (c *Client) FetchWorkflowRun(ctx context.Context, owner, repo string, runID int64) (*WorkflowRun, error) {
	ctx, span := getTracer().Start(ctx, "FetchWorkflowRun", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.Int64("github.run_id", runID),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%d", owner, repo, runID)
	resp, err := fetchWithAuth(ctx, c, endpoint, "")
	if err != nil {
		return nil, err
	}
	var run WorkflowRun
	if err := decodeJSON(resp, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

// FetchWorkflowRunsSince fetches workflow runs for a repository created on or
// after `since`. The caller computes `since` once (e.g. from the `now` a sync
// captured at its start) — recomputing it here from a fresh time.Now() let a
// clock jump mid-sync (laptop suspend) slide the listing window and silently
// skip runs. The optional onPage callback is called after each page with
// (fetchedSoFar, totalCount).
func (c *Client) FetchWorkflowRunsSince(ctx context.Context, owner, repo string, since time.Time, branch, workflow string, onPage func(fetched, total int)) ([]WorkflowRun, error) {
	ctx, span := getTracer().Start(ctx, "FetchWorkflowRunsSince", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.String("since", since.UTC().Format("2006-01-02")),
		attribute.String("github.branch", branch),
		attribute.String("github.workflow", workflow),
	))
	defer span.End()

	// Created date filter (YYYY-MM-DD format). GitHub evaluates the
	// qualifier in UTC; formatting a local date in a timezone ahead of
	// UTC started the window up to ~14h late and silently dropped runs.
	sinceDate := since.UTC().Format("2006-01-02")

	params := url.Values{}
	params.Set("per_page", "100")
	params.Set("created", ">="+sinceDate) // Filter by creation date

	// Add optional branch filter
	if branch != "" {
		params.Set("branch", branch)
	}
	// Note: workflow_id API parameter doesn't work reliably (includes triggered workflows)
	// We'll filter client-side after fetching

	baseURL := fmt.Sprintf("https://api.github.com/repos/%s/%s", owner, repo)
	runsURL := fmt.Sprintf("%s/actions/runs?%s", baseURL, params.Encode())

	runs, err := fetchWorkflowRunsPaginated(ctx, c, runsURL, onPage)
	if err != nil {
		return nil, err
	}

	// Client-side workflow filtering (more reliable than API parameter)
	if workflow != "" {
		filtered := make([]WorkflowRun, 0, len(runs))
		for _, run := range runs {
			// Match against filename or full path
			if strings.HasSuffix(run.Path, workflow) || run.Path == workflow {
				filtered = append(filtered, run)
			}
		}
		return filtered, nil
	}

	return runs, nil
}

func (c *Client) FetchRepository(ctx context.Context, baseURL string) (*RepoMeta, error) {
	ctx, span := getTracer().Start(ctx, "FetchRepository", trace.WithAttributes(
		attribute.String("github.baseURL", baseURL),
	))
	defer span.End()

	resp, err := fetchWithAuth(ctx, c, baseURL, "")
	if err != nil {
		return nil, err
	}
	var repo RepoMeta
	if err := decodeJSON(resp, &repo); err != nil {
		return nil, err
	}
	return &repo, nil
}

const maxPages = 500

// paginate fetches all pages from a GitHub list endpoint. It follows the
// Link "rel=next" header, decoding each page into T and extracting items via
// the extract callback. The optional onPage callback (pass nil to skip) is
// invoked after each page with the decoded page and the running item count.
// `name` identifies the caller in the maxPages-exceeded error.
func paginate[T any, Item any](
	ctx context.Context, c *Client, firstURL, accept, name string,
	extract func(T) []Item,
	onPage func(page T, fetchedSoFar int),
) ([]Item, error) {
	var all []Item
	nextURL := firstURL
	for page := 0; nextURL != ""; page++ {
		if page >= maxPages {
			return nil, fmt.Errorf("pagination limit of %d exceeded for %s", maxPages, name)
		}
		resp, err := fetchWithAuth(ctx, c, nextURL, accept)
		if err != nil {
			return nil, err
		}
		var data T
		if err := decodeJSON(resp, &data); err != nil {
			return nil, err
		}
		all = append(all, extract(data)...)
		if onPage != nil {
			onPage(data, len(all))
		}
		nextURL = parseNextLink(resp.Header.Get("Link"))
	}
	return all, nil
}

func (c *Client) FetchCommitAssociatedPRs(ctx context.Context, owner, repo, sha string) ([]PullAssociated, error) {
	ctx, span := getTracer().Start(ctx, "FetchCommitAssociatedPRs", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.String("github.sha", sha),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s/pulls?per_page=100", owner, repo, sha)
	return paginate[[]PullAssociated, PullAssociated](ctx, c, endpoint, "application/vnd.github+json", "FetchCommitAssociatedPRs",
		func(s []PullAssociated) []PullAssociated { return s }, nil)
}

func (c *Client) FetchCommit(ctx context.Context, baseURL, sha string) (*CommitResponse, error) {
	ctx, span := getTracer().Start(ctx, "FetchCommit", trace.WithAttributes(
		attribute.String("github.baseURL", baseURL),
		attribute.String("github.sha", sha),
	))
	defer span.End()

	commitURL := fmt.Sprintf("%s/commits/%s", baseURL, sha)
	resp, err := fetchWithAuth(ctx, c, commitURL, "")
	if err != nil {
		return nil, err
	}
	var commit CommitResponse
	if err := decodeJSON(resp, &commit); err != nil {
		return nil, err
	}
	return &commit, nil
}

func (c *Client) FetchPullRequest(ctx context.Context, baseURL, identifier string) (*PullRequest, error) {
	ctx, span := getTracer().Start(ctx, "FetchPullRequest", trace.WithAttributes(
		attribute.String("github.baseURL", baseURL),
		attribute.String("github.identifier", identifier),
	))
	defer span.End()

	prURL := fmt.Sprintf("%s/pulls/%s", baseURL, identifier)
	resp, err := fetchWithAuth(ctx, c, prURL, "")
	if err != nil {
		return nil, err
	}
	var pr PullRequest
	if err := decodeJSON(resp, &pr); err != nil {
		return nil, err
	}
	return &pr, nil
}

func (c *Client) FetchPRReviews(ctx context.Context, owner, repo, prNumber string) ([]Review, error) {
	ctx, span := getTracer().Start(ctx, "FetchPRReviews", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.String("github.prNumber", prNumber),
	))
	defer span.End()

	reviewsURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/pulls/%s/reviews?per_page=100", owner, repo, prNumber)
	return fetchReviewsPaginated(ctx, c, reviewsURL)
}

func (c *Client) FetchPRComments(ctx context.Context, owner, repo, prNumber string) ([]Review, error) {
	ctx, span := getTracer().Start(ctx, "FetchPRComments", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.String("github.prNumber", prNumber),
	))
	defer span.End()

	// Use Review struct for simplicity as they share similar fields (ID, User, Body, SubmittedAt/CreatedAt)
	commentsURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/issues/%s/comments?per_page=100", owner, repo, prNumber)
	return fetchCommentsPaginated(ctx, c, commentsURL)
}

func (c *Client) FetchJobsPaginated(ctx context.Context, urlValue string) ([]Job, error) {
	ctx, span := getTracer().Start(ctx, "FetchJobsPaginated", trace.WithAttributes(
		attribute.String("github.url", urlValue),
	))
	defer span.End()

	return paginate[JobsResponse, Job](ctx, c, urlValue, "", "FetchJobsPaginated",
		func(r JobsResponse) []Job { return r.Jobs }, nil)
}

func fetchWorkflowRunsPaginated(ctx context.Context, c *Client, urlValue string, onPage func(fetched, total int)) ([]WorkflowRun, error) {
	ctx, span := getTracer().Start(ctx, "fetchWorkflowRunsPaginated", trace.WithAttributes(
		attribute.String("github.url", urlValue),
	))
	defer span.End()

	// Adapt the caller's (fetched, total) callback to paginate's page callback.
	var pageCB func(WorkflowRunsResponse, int)
	if onPage != nil {
		pageCB = func(data WorkflowRunsResponse, fetched int) {
			onPage(fetched, data.TotalCount)
		}
	}
	return paginate[WorkflowRunsResponse, WorkflowRun](ctx, c, urlValue, "", "fetchWorkflowRunsPaginated",
		func(r WorkflowRunsResponse) []WorkflowRun { return r.WorkflowRuns }, pageCB)
}

func fetchReviewsPaginated(ctx context.Context, c *Client, urlValue string) ([]Review, error) {
	ctx, span := getTracer().Start(ctx, "fetchReviewsPaginated", trace.WithAttributes(
		attribute.String("github.url", urlValue),
	))
	defer span.End()

	return paginate[[]Review, Review](ctx, c, urlValue, "", "fetchReviewsPaginated",
		func(s []Review) []Review { return s }, nil)
}

func fetchCommentsPaginated(ctx context.Context, c *Client, urlValue string) ([]Review, error) {
	ctx, span := getTracer().Start(ctx, "fetchCommentsPaginated", trace.WithAttributes(
		attribute.String("github.url", urlValue),
	))
	defer span.End()

	type Comment struct {
		ID        int64    `json:"id"`
		User      UserInfo `json:"user"`
		Body      string   `json:"body"`
		CreatedAt string   `json:"created_at"`
		HTMLURL   string   `json:"html_url"`
	}
	return paginate[[]Comment, Review](ctx, c, urlValue, "", "fetchCommentsPaginated",
		func(comments []Comment) []Review {
			reviews := make([]Review, len(comments))
			for i, cm := range comments {
				reviews[i] = Review{
					ID:          cm.ID,
					User:        cm.User,
					Body:        cm.Body,
					SubmittedAt: cm.CreatedAt,
					HTMLURL:     cm.HTMLURL,
				}
			}
			return reviews
		}, nil)
}

func parseNextLink(linkHeader string) string {
	if linkHeader == "" {
		return ""
	}
	parts := strings.Split(linkHeader, ",")
	for _, part := range parts {
		section := strings.TrimSpace(part)
		if strings.Contains(section, `rel="next"`) {
			start := strings.Index(section, "<")
			end := strings.Index(section, ">")
			if start >= 0 && end > start {
				return section[start+1 : end]
			}
		}
	}
	return ""
}

func (c *Client) FetchBranchProtection(ctx context.Context, owner, repo, branch string) (*BranchProtection, error) {
	ctx, span := getTracer().Start(ctx, "FetchBranchProtection", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.String("github.branch", branch),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/branches/%s/protection/required_status_checks", owner, repo, branch)
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token "+c.context.GitHubToken)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "Node")

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 404 means no branch protection configured - this is not an error
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, handleGithubError(resp, endpoint)
	}

	var protection RequiredStatusChecks
	if err := json.NewDecoder(resp.Body).Decode(&protection); err != nil {
		return nil, err
	}

	return &BranchProtection{RequiredStatusChecks: &protection}, nil
}

func (c *Client) FetchRunTiming(ctx context.Context, owner, repo string, runID int64) (*RunTiming, error) {
	ctx, span := getTracer().Start(ctx, "FetchRunTiming", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.Int64("github.run_id", runID),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%d/timing", owner, repo, runID)
	resp, err := fetchWithAuth(ctx, c, endpoint, "")
	if err != nil {
		return nil, err
	}
	var timing RunTiming
	if err := decodeJSON(resp, &timing); err != nil {
		return nil, err
	}
	return &timing, nil
}

func (c *Client) FetchCheckRunsForCommit(ctx context.Context, owner, repo, sha string) ([]CheckRun, error) {
	ctx, span := getTracer().Start(ctx, "FetchCheckRunsForCommit", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.String("github.sha", sha),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s/check-runs?per_page=100", owner, repo, sha)
	type checkRunsPage struct {
		CheckRuns []CheckRun `json:"check_runs"`
	}
	return paginate[checkRunsPage, CheckRun](ctx, c, endpoint, "", "FetchCheckRunsForCommit",
		func(r checkRunsPage) []CheckRun { return r.CheckRuns }, nil)
}

func (c *Client) FetchAnnotations(ctx context.Context, owner, repo string, checkRunID int64) ([]Annotation, error) {
	ctx, span := getTracer().Start(ctx, "FetchAnnotations", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.Int64("github.check_run_id", checkRunID),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/check-runs/%d/annotations?per_page=100", owner, repo, checkRunID)
	return paginate[[]Annotation, Annotation](ctx, c, endpoint, "", "FetchAnnotations",
		func(s []Annotation) []Annotation { return s }, nil)
}

func (c *Client) ListArtifacts(ctx context.Context, owner, repo string, runID int64) ([]Artifact, error) {
	ctx, span := getTracer().Start(ctx, "ListArtifacts", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.Int64("github.run_id", runID),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%d/artifacts?per_page=100", owner, repo, runID)
	type artifactsPage struct {
		Artifacts []Artifact `json:"artifacts"`
	}
	return paginate[artifactsPage, Artifact](ctx, c, endpoint, "", "ListArtifacts",
		func(r artifactsPage) []Artifact { return r.Artifacts }, nil)
}

func (c *Client) DownloadArtifact(ctx context.Context, downloadURL string) ([]byte, error) {
	ctx, span := getTracer().Start(ctx, "DownloadArtifact", trace.WithAttributes(
		attribute.String("github.url", downloadURL),
	))
	defer span.End()

	resp, err := fetchWithAuth(ctx, c, downloadURL, "application/vnd.github.v3+json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
