package analyzer

import "regexp"

// FacetDimension names a way to slice a trend analysis into comparable groups.
type FacetDimension string

const (
	FacetBranch FacetDimension = "branch" // upstream (trunk/release) vs feature
	FacetEvent  FacetDimension = "event"  // push, pull_request, schedule, …
	FacetRunner FacetDimension = "runner" // job-level: by requested runner labels
)

// Branch facet bucket labels.
const (
	branchUpstream = "upstream"
	branchFeature  = "feature"
)

// releaseBranchPatterns recognize long-lived release/maintenance lines that
// should count as "upstream" alongside the default branch. Heuristic and
// deliberately broad — matching covers the common conventions (GitHub
// release/*, Node-style vN.x, Rails-style N-N-stable, semver tags-as-branches).
var releaseBranchPatterns = []*regexp.Regexp{
	regexp.MustCompile(`^release[-/]`),
	regexp.MustCompile(`(?i)^v?\d+(\.\d+)*(\.x)?$`), // v20.x, 20.x, 1.2.3
	regexp.MustCompile(`-stable$`),                  // 7-1-stable
	regexp.MustCompile(`(?i)^(stable|maint|lts)\b`),
}

// classifyBranch buckets a run's head branch into "upstream" or "feature".
// Upstream = the repo default branch, the conventional trunk names main/master
// (so it still works when the default branch couldn't be fetched), or a branch
// matching a release/maintenance pattern. Everything else — PR topic branches,
// dependabot, forks — is "feature" and aggregates into a single bucket so N
// feature branches don't explode into N tiny groups.
func classifyBranch(branch, defaultBranch string) string {
	if branch == "" {
		return branchFeature
	}
	if branch == defaultBranch || branch == "main" || branch == "master" {
		return branchUpstream
	}
	for _, re := range releaseBranchPatterns {
		if re.MatchString(branch) {
			return branchUpstream
		}
	}
	return branchFeature
}
