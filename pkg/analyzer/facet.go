package analyzer

import (
	"fmt"
	"math"
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

// maxFacetRows caps a comparison table. A cross-product of dimensions can
// produce many sparse combinations; the busiest buckets are kept and the rest
// reported as a remainder so the table stays legible.
const maxFacetRows = 25

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
// or more crossed dimensions. Level is "run" for run-level facets (branch,
// event combinations) or "job" when runner is among the dimensions, which tells
// the renderer which columns apply. Truncated is the number of buckets dropped
// by the row cap.
type FacetComparison struct {
	Dimensions []FacetDimension
	Level      string
	Rows       []FacetRow
	Truncated  int
	// Estimated is true when job-level counts were extrapolated from a sampled
	// subset of runs (i.e. sampling was active). Counts are then population
	// estimates rather than exact observations.
	Estimated bool
}

// FacetRow holds one bucket's headline metrics. Keys holds one value per
// dimension (in Dimensions order). FlakyJobs is meaningful only for run-level
// facets; AvgQueueSec only for job-level (runner) facets.
type FacetRow struct {
	Keys              []string
	Count             int // exact runs (run-level) or estimated population jobs (job-level; see facetJobsCross)
	AvgDurationSec    float64
	MedianDurationSec float64
	SuccessRatePct    float64
	FlakyJobs         int     // run-level only
	AvgQueueSec       float64 // job-level only
}

// ParseFacets turns a comma-separated --facet value into an ordered, de-duped
// dimension list. "all" expands to branch,event,runner. An empty string yields
// no dimensions (faceting off); an unknown token is an error.
func ParseFacets(s string) ([]FacetDimension, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if s == "all" {
		return []FacetDimension{FacetBranch, FacetEvent, FacetRunner}, nil
	}
	seen := make(map[FacetDimension]bool)
	var dims []FacetDimension
	for _, part := range strings.Split(s, ",") {
		d := FacetDimension(strings.TrimSpace(part))
		switch d {
		case FacetBranch, FacetEvent, FacetRunner:
		default:
			return nil, fmt.Errorf("invalid facet %q (expected branch, event, runner, or all)", part)
		}
		if !seen[d] {
			seen[d] = true
			dims = append(dims, d)
		}
	}
	return dims, nil
}

// computeFacets slices runs along the cross-product of the given dimensions.
// Branch/event combinations facet at the run level; including runner drops the
// whole comparison to the job level (run-level dimensions become attributes of
// each job's parent run). Returns nil for an empty or unknown dimension list.
func computeFacets(runs []RunData, dims []FacetDimension, defaultBranch string) *FacetComparison {
	if len(dims) == 0 {
		return nil
	}
	jobLevel := false
	for _, d := range dims {
		switch d {
		case FacetBranch, FacetEvent:
		case FacetRunner:
			jobLevel = true
		default:
			return nil
		}
	}
	if jobLevel {
		return facetJobsCross(dims, runs, defaultBranch)
	}
	return facetRunsCross(dims, runs, defaultBranch)
}

// runDimValue is a run's bucket value for a run-level dimension.
func runDimValue(d FacetDimension, r RunData, defaultBranch string) string {
	switch d {
	case FacetBranch:
		return classifyBranch(r.Branch, defaultBranch)
	case FacetEvent:
		if r.Event == "" {
			return "unknown"
		}
		return r.Event
	default:
		return ""
	}
}

// facetRunsCross groups runs by the tuple of run-level dimension values and
// computes run-level metrics per bucket, reusing the same summary and
// flaky-detection logic as the top-level report.
func facetRunsCross(dims []FacetDimension, runs []RunData, defaultBranch string) *FacetComparison {
	groups := make(map[string][]RunData)
	keysOf := make(map[string][]string)
	for _, r := range runs {
		parts := make([]string, len(dims))
		for i, d := range dims {
			parts[i] = runDimValue(d, r, defaultBranch)
		}
		jk := strings.Join(parts, "\x00")
		if _, ok := groups[jk]; !ok {
			keysOf[jk] = parts
		}
		groups[jk] = append(groups[jk], r)
	}
	rows := make([]FacetRow, 0, len(groups))
	for jk, g := range groups {
		s := calculateTrendSummary(g)
		rows = append(rows, FacetRow{
			Keys:              keysOf[jk],
			Count:             len(g),
			AvgDurationSec:    s.AvgDuration,
			MedianDurationSec: s.MedianDuration,
			SuccessRatePct:    s.AvgSuccessRate,
			FlakyJobs:         len(detectFlakyJobs(g)),
		})
	}
	return finishComparison(dims, "run", rows)
}

// facetJobsCross groups every fetched job by the tuple of dimension values
// (run-level dimensions taken from the job's parent run, runner from the job)
// and computes job-level metrics per bucket.
//
// Because job detail is fetched for only a sampled subset of runs — and the
// sampler allocates different rates per workflow — the raw job count in a
// bucket understates reality, unevenly across buckets. Count is therefore the
// *estimated population* count: each observed job is weighted by its workflow's
// inverse sampling fraction (total completed runs ÷ runs whose jobs we fetched).
// With no sampling every run carries jobs, weight is 1, and the estimate equals
// the exact count. Per-job duration/queue/success stats stay sample-based (the
// sampler keeps those accurate; see cmd/sample-eval) — only the count, which
// drives the busiest-first ranking and the row cap, is corrected.
func facetJobsCross(dims []FacetDimension, runs []RunData, defaultBranch string) *FacetComparison {
	// Per-workflow inverse sampling weight from run-level data (complete). A
	// run counts toward the denominator once its jobs were fetched, even if it
	// had none — that keeps a genuinely job-less run (skipped/filtered) from
	// being mistaken for an unsampled one, which would inflate the estimate.
	// When no run is marked fetched (hand-built data, older callers), fall back
	// to "has jobs" so counts equal the raw observation with weight 1.
	totalByWF := make(map[string]int)
	fetchedByWF := make(map[string]int)
	anyFetched := false
	for _, r := range runs {
		if r.JobsFetched {
			anyFetched = true
			break
		}
	}
	fetchedSignal := func(r RunData) bool {
		if anyFetched {
			return r.JobsFetched
		}
		return len(r.Jobs) > 0
	}
	for _, r := range runs {
		totalByWF[r.WorkflowName]++
		if fetchedSignal(r) {
			fetchedByWF[r.WorkflowName]++
		}
	}
	weightOf := func(wf string) float64 {
		if s := fetchedByWF[wf]; s > 0 {
			return float64(totalByWF[wf]) / float64(s)
		}
		return 1
	}
	estimated := false
	for wf, total := range totalByWF {
		if fetchedByWF[wf] > 0 && fetchedByWF[wf] < total {
			estimated = true
			break
		}
	}

	type agg struct {
		keys      []string
		durations []float64
		queues    []float64
		success   int
		decisive  int
		countEst  float64 // sum of per-job population weights
	}
	groups := make(map[string]*agg)
	for _, r := range runs {
		w := weightOf(r.WorkflowName)
		for _, j := range r.Jobs {
			parts := make([]string, len(dims))
			for i, d := range dims {
				if d == FacetRunner {
					parts[i] = runnerKey(j)
				} else {
					parts[i] = runDimValue(d, r, defaultBranch)
				}
			}
			jk := strings.Join(parts, "\x00")
			a := groups[jk]
			if a == nil {
				a = &agg{keys: parts}
				groups[jk] = a
			}
			a.countEst += w
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
	for _, a := range groups {
		successRate := 0.0
		if a.decisive > 0 {
			successRate = float64(a.success) / float64(a.decisive) * 100
		}
		rows = append(rows, FacetRow{
			Keys:              a.keys,
			Count:             int(math.Round(a.countEst)),
			AvgDurationSec:    average(a.durations),
			MedianDurationSec: calculateMedian(a.durations),
			SuccessRatePct:    successRate,
			AvgQueueSec:       average(a.queues),
		})
	}
	c := finishComparison(dims, "job", rows)
	c.Estimated = estimated
	return c
}

// finishComparison sorts buckets by descending count (busiest first, tie-broken
// on key for determinism) and applies the row cap, recording the remainder.
func finishComparison(dims []FacetDimension, level string, rows []FacetRow) *FacetComparison {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		return strings.Join(rows[i].Keys, "\x00") < strings.Join(rows[j].Keys, "\x00")
	})
	truncated := 0
	if len(rows) > maxFacetRows {
		truncated = len(rows) - maxFacetRows
		rows = rows[:maxFacetRows]
	}
	return &FacetComparison{Dimensions: dims, Level: level, Rows: rows, Truncated: truncated}
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
