package analyzer

import (
	"regexp"
	"sort"
	"strings"
)

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

// FacetComparison is a trend analysis sliced into comparable buckets along one
// dimension. Level is "run" for run-level facets (branch, event) or "job" for
// job-level facets (runner), which tells the renderer which columns apply.
type FacetComparison struct {
	Dimension FacetDimension
	Level     string
	Rows      []FacetRow
}

// FacetRow holds one bucket's headline metrics. FlakyJobs is meaningful only
// for run-level facets; AvgQueueSec only for job-level (runner) facets.
type FacetRow struct {
	Key               string
	Count             int // runs (run-level) or jobs (job-level)
	AvgDurationSec    float64
	MedianDurationSec float64
	SuccessRatePct    float64
	FlakyJobs         int     // run-level only
	AvgQueueSec       float64 // job-level only
}

// computeFacets slices runs along the given dimension. Branch and event facet
// at the run level; runner facets at the job level (over whatever job detail
// was fetched). Returns nil for an unknown dimension.
func computeFacets(runs []RunData, dim FacetDimension, defaultBranch string) *FacetComparison {
	switch dim {
	case FacetBranch:
		return facetRuns(dim, runs, func(r RunData) string { return classifyBranch(r.Branch, defaultBranch) })
	case FacetEvent:
		return facetRuns(dim, runs, func(r RunData) string {
			if r.Event == "" {
				return "unknown"
			}
			return r.Event
		})
	case FacetRunner:
		return facetJobs(runs)
	default:
		return nil
	}
}

// facetRuns groups runs by keyFn and computes run-level metrics per bucket,
// reusing the same summary and flaky-detection logic as the top-level report.
func facetRuns(dim FacetDimension, runs []RunData, keyFn func(RunData) string) *FacetComparison {
	groups := make(map[string][]RunData)
	for _, r := range runs {
		k := keyFn(r)
		groups[k] = append(groups[k], r)
	}
	rows := make([]FacetRow, 0, len(groups))
	for k, g := range groups {
		s := calculateTrendSummary(g)
		rows = append(rows, FacetRow{
			Key:               k,
			Count:             len(g),
			AvgDurationSec:    s.AvgDuration,
			MedianDurationSec: s.MedianDuration,
			SuccessRatePct:    s.AvgSuccessRate,
			FlakyJobs:         len(detectFlakyJobs(g)),
		})
	}
	sortFacetRows(rows)
	return &FacetComparison{Dimension: dim, Level: "run", Rows: rows}
}

// facetJobs groups every fetched job by its runner key and computes job-level
// metrics per bucket.
func facetJobs(runs []RunData) *FacetComparison {
	type agg struct {
		durations []float64
		queues    []float64
		success   int
		decisive  int
		count     int
	}
	groups := make(map[string]*agg)
	for _, r := range runs {
		for _, j := range r.Jobs {
			a := groups[runnerKey(j)]
			if a == nil {
				a = &agg{}
				groups[runnerKey(j)] = a
			}
			a.count++
			if j.Duration > 0 {
				a.durations = append(a.durations, float64(j.Duration)/1000.0)
			}
			if j.QueueTime > 0 {
				a.queues = append(a.queues, float64(j.QueueTime)/1000.0)
			}
			switch j.Conclusion {
			case "success":
				a.success++
				a.decisive++
			case "failure":
				a.decisive++
			}
		}
	}
	rows := make([]FacetRow, 0, len(groups))
	for k, a := range groups {
		successRate := 0.0
		if a.decisive > 0 {
			successRate = float64(a.success) / float64(a.decisive) * 100
		}
		rows = append(rows, FacetRow{
			Key:               k,
			Count:             a.count,
			AvgDurationSec:    average(a.durations),
			MedianDurationSec: calculateMedian(a.durations),
			SuccessRatePct:    successRate,
			AvgQueueSec:       average(a.queues),
		})
	}
	sortFacetRows(rows)
	return &FacetComparison{Dimension: FacetRunner, Level: "job", Rows: rows}
}

// runnerKey identifies a job's runner bucket: the requested labels (the
// meaningful "by runner type" axis), falling back to the specific runner
// instance name, then "unlabeled".
func runnerKey(j JobData) string {
	if len(j.Labels) > 0 {
		return strings.Join(j.Labels, ",")
	}
	if j.RunnerName != "" {
		return j.RunnerName
	}
	return "unlabeled"
}

// sortFacetRows orders buckets by descending count (most-trafficked first),
// breaking ties on key for deterministic output.
func sortFacetRows(rows []FacetRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		return rows[i].Key < rows[j].Key
	})
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
