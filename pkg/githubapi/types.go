package githubapi

type WorkflowRunsResponse struct {
	TotalCount   int           `json:"total_count"`
	WorkflowRuns []WorkflowRun `json:"workflow_runs"`
}

type WorkflowRun struct {
	ID           int64   `json:"id"`
	RunAttempt   int64   `json:"run_attempt"`
	Name         string  `json:"name"`
	Path         string  `json:"path"`
	Status       string  `json:"status"`
	Conclusion   string  `json:"conclusion"`
	CreatedAt    string  `json:"created_at"`
	RunStartedAt string  `json:"run_started_at"`
	UpdatedAt    string  `json:"updated_at"`
	HeadSHA      string  `json:"head_sha"`
	HeadBranch   string  `json:"head_branch"`
	Event        string  `json:"event"`
	Repository   RepoRef `json:"repository"`
}

type RepoRef struct {
	Owner RepoOwner `json:"owner"`
	Name  string    `json:"name"`
}

type RepoOwner struct {
	Login string `json:"login"`
}

type JobsResponse struct {
	Jobs []Job `json:"jobs"`
}

type Job struct {
	ID          int64    `json:"id"`
	RunAttempt  int64    `json:"run_attempt"`
	Name        string   `json:"name"`
	Status      string   `json:"status"`
	Conclusion  string   `json:"conclusion"`
	CreatedAt   string   `json:"created_at"`
	StartedAt   string   `json:"started_at"`
	CompletedAt string   `json:"completed_at"`
	RunnerName  string   `json:"runner_name"`
	Labels      []string `json:"labels"`
	HTMLURL     string   `json:"html_url"`
	Steps       []Step   `json:"steps"`
}

type Step struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	Number      int    `json:"number"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at"`
}

type PullRequest struct {
	Number       int       `json:"number"`
	Title        string    `json:"title"`
	Head         PRRef     `json:"head"`
	Base         PRRef     `json:"base"`
	MergedAt     *string   `json:"merged_at"`
	MergedBy     *UserInfo `json:"merged_by"`
	ChangedFiles int       `json:"changed_files"`
	Additions    int       `json:"additions"`
	Deletions    int       `json:"deletions"`
}

type PRRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type Review struct {
	ID          int64    `json:"id"`
	State       string   `json:"state"`
	SubmittedAt string   `json:"submitted_at"`
	User        UserInfo `json:"user"`
	Body        string   `json:"body"`
	HTMLURL     string   `json:"html_url"`
}

type UserInfo struct {
	Login string `json:"login"`
	Name  string `json:"name"`
}

type CommitDetails struct {
	Committer CommitAuthor `json:"committer"`
	Author    CommitAuthor `json:"author"`
}

type CommitResponse struct {
	Commit CommitDetails `json:"commit"`
	Stats  CommitStats   `json:"stats"`
	Files  []CommitFile  `json:"files"`
}

type CommitStats struct {
	Total     int `json:"total"`
	Additions int `json:"additions"`
	Deletions int `json:"deletions"`
}

type CommitFile struct {
	Filename  string `json:"filename"`
	Status    string `json:"status"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

type CommitAuthor struct {
	Date string `json:"date"`
}

type RepoMeta struct {
	DefaultBranch string `json:"default_branch"`
}

type PullAssociated struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Base   struct {
		Ref string `json:"ref"`
	} `json:"base"`
	MergedAt *string   `json:"merged_at"`
	MergedBy *UserInfo `json:"merged_by"`
	HTMLURL  string    `json:"html_url"`
}

type GitHubError struct {
	Message          string `json:"message"`
	DocumentationURL string `json:"documentation_url"`
}

type BranchProtection struct {
	RequiredStatusChecks *RequiredStatusChecks `json:"required_status_checks"`
}

type RequiredStatusChecks struct {
	Strict   bool     `json:"strict"`
	Contexts []string `json:"contexts"`
	Checks   []Check  `json:"checks,omitempty"`
}

type Check struct {
	Context string `json:"context"`
	AppID   *int64 `json:"app_id,omitempty"`
}

// RunTiming represents the billing timing for a workflow run.
type RunTiming struct {
	Billable map[string]BillableOS `json:"billable"`
}

// BillableOS represents billable time for an OS.
type BillableOS struct {
	TotalMs int64 `json:"total_ms"`
}

// CheckRun represents a GitHub check run.
type CheckRun struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

// Annotation represents a check run annotation.
type Annotation struct {
	Path      string `json:"path"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	Level     string `json:"annotation_level"`
	Message   string `json:"message"`
	Title     string `json:"title"`
}

// Artifact represents a GitHub Actions artifact.
type Artifact struct {
	ID                 int64  `json:"id"`
	Name               string `json:"name"`
	SizeInBytes        int64  `json:"size_in_bytes"`
	ArchiveDownloadURL string `json:"archive_download_url"`
	Expired            bool   `json:"expired"`
}
