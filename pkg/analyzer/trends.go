package analyzer

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// SamplingInfo describes whether and how sampling was applied
type SamplingInfo struct {
	Enabled       bool
	SampleSize    int // job-level sample count
	TotalRuns     int // total runs fetched from API
	WorkflowCount int // distinct workflows in the window
	MajorTarget   int // per-workflow observation target (≥1% compute share)
	MinorTarget   int // per-workflow observation target (minor workflows)
	MarginOfError float64
	Rationale     string // human-readable "why we think" explanation
}

// TrendAnalysis holds the result of analyzing historical workflow trends for a repository.
type TrendAnalysis struct {
	Owner            string
	Repo             string
	TimeRange        TimeRange
	Sampling         SamplingInfo
	Summary          TrendSummary
	DurationTrend    []DataPoint
	SuccessRateTrend []DataPoint
	JobTrends        []JobTrend
	FlakyJobs        []FlakyJob
	TopRegressions   []JobRegression
	TopImprovements  []JobImprovement
	QueueTimeStats   QueueTimeStats
	Typical          *TypicalRun
	Hourly           *HourlyPatterns
}

// Changepoint identifies the approximate point in time where a job's duration shifted.
type Changepoint struct {
	Date         time.Time // timestamp of the first observation after the shift
	BeforeSHA    string    // last commit SHA before the changepoint
	AfterSHA     string    // first commit SHA after the changepoint
	BeforeRunURL string    // URL of the last run before the changepoint
	AfterRunURL  string    // URL of the first run after the changepoint
	DiffURL      string    // GitHub compare URL: BeforeSHA...AfterSHA
	BeforeAvg    float64   // average duration (seconds) before the changepoint
	AfterAvg     float64   // average duration (seconds) after the changepoint
	Index        int       // index of the first observation after the shift
	TotalPoints  int       // total number of observations
}

// JobRegression represents a job that got slower
type JobRegression struct {
	Name            string
	URLs            []string // sample recent job URLs (newest first)
	OldAvgDuration  float64
	NewAvgDuration  float64
	PercentIncrease float64
	AbsoluteChange  float64
	Changepoint     *Changepoint // nil when insufficient data
}

// JobImprovement represents a job that got faster
type JobImprovement struct {
	Name            string
	URLs            []string // sample recent job URLs (newest first)
	OldAvgDuration  float64
	NewAvgDuration  float64
	PercentDecrease float64
	AbsoluteChange  float64
	Changepoint     *Changepoint // nil when insufficient data
}

// QueueTimeStats contains queue time analysis
type QueueTimeStats struct {
	AvgQueueTime    float64
	MedianQueueTime float64
	P95QueueTime    float64
	AvgRunTime      float64
	MedianRunTime   float64
	QueueTimeRatio  float64 // queue time / total time
}

// TimeRange represents a time period
type TimeRange struct {
	Start time.Time
	End   time.Time
	Days  int
}

// TrendSummary provides high-level statistics
type TrendSummary struct {
	TotalRuns          int
	AvgDuration        float64
	MedianDuration     float64
	P95Duration        float64
	AvgSuccessRate     float64
	TrendDirection     string // "improving", "stable", "degrading"
	TrendDescription   string // human-readable explanation of the trend
	PercentChange      float64
	MostFlakyJobsCount int
	RerunRuns          int   // runs that were re-run attempts (attempt > 1)
	RerunComputeMs     int64 // compute spent on those re-run attempts — retry burn
}

// DataPoint represents a single data point in a trend
type DataPoint struct {
	Timestamp time.Time
	Value     float64
	Count     int // number of runs at this point
}

// JobTrend contains trend data for a specific job
type JobTrend struct {
	Name           string
	URLs           []string // sample recent job URLs (newest first)
	AvgDuration    float64
	MedianDuration float64
	SuccessRate    float64
	TotalRuns      int
	TrendDirection string
	DurationPoints []DataPoint
}

// FlakyJob represents a job with inconsistent outcomes
type FlakyJob struct {
	Name            string
	URLs            []string // sample recent failure URLs (newest first)
	TotalRuns       int
	SuccessCount    int
	FailureCount    int
	FlakeRate       float64 // percentage of failures among decisive outcomes
	RecentFailures  int     // failures in last 10 runs
	LastFailure     time.Time
	SameSHAFlakes   int     // commits where this job both passed and failed (definitive flakiness)
	TransitionScore float64 // 0..1: pass/fail state changes over chronological observations (Buildkite-style)
}

// RunData represents simplified workflow run data
type RunData struct {
	ID           int64
	Attempt      int64 // run attempt number; >1 means this is a re-run
	WorkflowName string
	HeadSHA      string
	Branch       string // head_branch the run executed against (for branch faceting)
	Event        string // trigger event: push, pull_request, schedule, … (for event faceting)
	Status       string
	Conclusion   string
	CreatedAt    time.Time
	StartedAt    time.Time // effective start of this attempt (RunStartedAt for retries)
	UpdatedAt    time.Time
	Duration     int64 // milliseconds
	Jobs         []JobData
}

// JobData represents simplified job data
type JobData struct {
	ID          int64
	Name        string
	URL         string
	Status      string
	Conclusion  string
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	Duration    int64    // milliseconds
	QueueTime   int64    // milliseconds
	RunnerName  string   // specific runner instance that executed the job
	Labels      []string // runner labels requested (e.g. ubuntu-24.04) — for runner faceting
}

// TrendOptions configures the trend analysis behavior
type TrendOptions struct {
	NoSample      bool
	MarginOfError float64 // e.g. 0.10 for ±10% — drives per-workflow observation targets
	DumpRunsPath  string  // when set, write the fetched RunData as JSON for offline analysis
}

// RunDump is the on-disk format written by TrendOptions.DumpRunsPath: the raw
// per-run (and, where fetched, per-job) data backing a trend analysis. Use
// with --no-sample to capture a complete ground-truth dataset.
type RunDump struct {
	Owner string
	Repo  string
	Days  int
	Runs  []RunData
}

// AnalyzeTrends analyzes historical trends for a repository using GitHub API.
// All run pages are always fetched to ensure accurate trend detection.
// When opts.NoSample is false (default), job detail fetching uses statistical
// sampling to reduce API calls. Run-level metrics use all fetched runs.
func AnalyzeTrends(ctx context.Context, client githubapi.GitHubProvider, owner, repo string, days int, branch, workflow string, opts TrendOptions, reporter ProgressReporter) (*TrendAnalysis, error) {
	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(days) * 24 * time.Hour)

	marginOfError := opts.MarginOfError
	if marginOfError <= 0 {
		marginOfError = 0.10
	}

	fetchDetail := fmt.Sprintf("%s/%s, last %d days", owner, repo, days)
	if branch != "" {
		fetchDetail += fmt.Sprintf(", branch: %s", branch)
	}
	if workflow != "" {
		fetchDetail += fmt.Sprintf(", workflow: %s", workflow)
	}

	if reporter != nil {
		reporter.SetPhase("Fetching workflow runs")
		reporter.SetDetail(fetchDetail)
	}

	sampling := SamplingInfo{
		MarginOfError: marginOfError,
	}

	// Fetch all run pages — run listing is cheap (1 API call per 100 runs)
	// and complete data is needed for accurate trend detection.
	var onPage func(fetched, total int)
	if reporter != nil {
		onPage = func(fetched, total int) {
			if total > 0 {
				reporter.SetDetail(fmt.Sprintf("%s — %d/%d runs", fetchDetail, fetched, total))
			} else {
				reporter.SetDetail(fmt.Sprintf("%s — %d runs", fetchDetail, fetched))
			}
		}
	}
	runs, err := client.FetchRecentWorkflowRuns(ctx, owner, repo, days, branch, workflow, onPage)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workflow runs: %w", err)
	}

	if len(runs) == 0 {
		return nil, fmt.Errorf("no workflow runs found for %s/%s in the last %d days", owner, repo, days)
	}

	// Keep only completed runs. Queued/in-progress runs have no conclusion
	// (dragging down success rates) and only partial durations, and because
	// they always cluster at the recent end of the window they would
	// systematically bias the first-half/second-half trend comparison.
	completed := runs[:0]
	for _, run := range runs {
		if run.Status == "completed" {
			completed = append(completed, run)
		}
	}
	runs = completed

	if len(runs) == 0 {
		return nil, fmt.Errorf("no completed workflow runs found for %s/%s in the last %d days", owner, repo, days)
	}

	// Convert all runs to RunData (no job fetching yet)
	runData := ConvertRuns(runs)

	// Determine job-level sampling. Allocation is per workflow rather than
	// global: a global sample spreads observations so thinly across jobs
	// that per-job percentiles (especially tails) become noise. Targets are
	// derived from the margin knob and calibrated with cmd/sample-eval
	// against full-scan ground truth.
	totalRuns := len(runData)
	sampling.TotalRuns = totalRuns
	majorTarget, minorTarget := JobSampleTargets(marginOfError)
	sampleIndices := SelectSampleIndices(runs, majorTarget, minorTarget)
	sampling.WorkflowCount = countWorkflows(runs)
	sampling.MajorTarget = majorTarget
	sampling.MinorTarget = minorTarget

	// Only sample when it saves ≥25% of API calls; otherwise the marginal
	// savings aren't worth the per-job accuracy loss on borderline trends.
	if !opts.NoSample && len(sampleIndices) < totalRuns*3/4 {
		sampling.Enabled = true
		sampling.SampleSize = len(sampleIndices)
	} else {
		sampling.SampleSize = totalRuns
		sampleIndices = make([]int, totalRuns)
		for i := range sampleIndices {
			sampleIndices[i] = i
		}
	}

	// Generate rationale
	sampling.Rationale = generateRationale(sampling)

	if reporter != nil {
		reporter.SetURLRuns(sampling.SampleSize)
		reporter.SetPhase("Fetching job details")
		if sampling.Enabled {
			reporter.SetDetail(fmt.Sprintf("sampling %d/%d runs across %d workflows — %d/%d obs targets (±%.0f%% margin)",
				sampling.SampleSize, sampling.TotalRuns, sampling.WorkflowCount,
				majorTarget, minorTarget, sampling.MarginOfError*100))
		} else {
			reporter.SetDetail(fmt.Sprintf("%d runs", totalRuns))
		}
	}

	// Fetch jobs for sampled runs
	fetchedRuns, err := fetchJobsForRuns(ctx, client, runData, runs, sampleIndices, reporter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch job data: %w", err)
	}
	// Record the achieved sample size so the output reflects reality when
	// transient fetch errors dropped some sampled runs, and warn that the
	// stated confidence/margin may no longer hold.
	if fetchedRuns < sampling.SampleSize {
		sampling.Rationale += fmt.Sprintf(" Warning: job details were fetched for only %d of %d sampled runs; job-level statistics may not meet the stated confidence/margin.",
			fetchedRuns, sampling.SampleSize)
		sampling.SampleSize = fetchedRuns
	}

	if reporter != nil {
		reporter.SetPhase("Analyzing trends")
	}

	// Sort runs chronologically (oldest first) so first-half/second-half
	// comparisons correctly treat early data as "before" and recent data as "after".
	// The GitHub API returns runs newest-first by default.
	sort.Slice(runData, func(i, j int) bool {
		return runData[i].CreatedAt.Before(runData[j].CreatedAt)
	})

	if opts.DumpRunsPath != "" {
		if err := dumpRuns(opts.DumpRunsPath, owner, repo, days, runData); err != nil {
			return nil, fmt.Errorf("failed to dump runs: %w", err)
		}
	}

	return analyzeRunData(owner, repo, runData, sampling, TimeRange{Start: startTime, End: endTime, Days: days}), nil
}

// AnalyzeTrendsFromRuns runs the full trend analysis over locally stored
// runs — exact job detail, no API calls, no sampling. Incomplete runs are
// filtered out the same way the API path filters them.
func AnalyzeTrendsFromRuns(owner, repo string, days int, runs []RunData) *TrendAnalysis {
	completed := make([]RunData, 0, len(runs))
	for _, run := range runs {
		if run.Status == "completed" {
			completed = append(completed, run)
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		return completed[i].CreatedAt.Before(completed[j].CreatedAt)
	})
	withJobs := 0
	for _, run := range completed {
		if len(run.Jobs) > 0 {
			withJobs++
		}
	}
	sampling := SamplingInfo{
		SampleSize: withJobs,
		TotalRuns:  len(completed),
		Rationale: fmt.Sprintf("%s runs loaded from the local store (exact job detail for %d; run `ote sync` to refresh).",
			formatCount(len(completed)), withJobs),
	}
	endTime := time.Now()
	return analyzeRunData(owner, repo, completed, sampling,
		TimeRange{Start: endTime.Add(-time.Duration(days) * 24 * time.Hour), End: endTime, Days: days})
}

// analyzeRunData computes the full TrendAnalysis over chronologically
// sorted, completed runs.
func analyzeRunData(owner, repo string, runData []RunData, sampling SamplingInfo, window TimeRange) *TrendAnalysis {
	analysis := &TrendAnalysis{
		Owner:     owner,
		Repo:      repo,
		TimeRange: window,
		Sampling:  sampling,
	}

	// Calculate summary statistics (uses all runs — run-level data)
	analysis.Summary = calculateTrendSummary(runData)

	// Generate duration trend (uses all runs)
	analysis.DurationTrend = generateDurationTrend(runData)

	// Generate success rate trend (uses all runs)
	analysis.SuccessRateTrend = generateSuccessRateTrend(runData)

	// Analyze individual jobs (uses sampled job data)
	analysis.JobTrends = analyzeJobTrends(runData)

	// Detect flaky jobs (uses sampled job data)
	analysis.FlakyJobs = detectFlakyJobs(runData)
	analysis.Summary.MostFlakyJobsCount = len(analysis.FlakyJobs)

	// Calculate regressions and improvements (uses sampled job data)
	analysis.TopRegressions, analysis.TopImprovements = calculateJobChanges(runData)

	// Populate diff URLs on changepoints
	for i, reg := range analysis.TopRegressions {
		if reg.Changepoint != nil {
			analysis.TopRegressions[i].Changepoint.DiffURL = fmt.Sprintf(
				"https://github.com/%s/%s/compare/%s...%s", owner, repo, reg.Changepoint.BeforeSHA, reg.Changepoint.AfterSHA)
		}
	}
	for i, imp := range analysis.TopImprovements {
		if imp.Changepoint != nil {
			analysis.TopImprovements[i].Changepoint.DiffURL = fmt.Sprintf(
				"https://github.com/%s/%s/compare/%s...%s", owner, repo, imp.Changepoint.BeforeSHA, imp.Changepoint.AfterSHA)
		}
	}

	// Calculate queue time statistics (uses sampled job data)
	analysis.QueueTimeStats = calculateQueueTimeStats(runData)

	// Aggregate sampled jobs into the statistically typical run
	analysis.Typical = computeTypicalRun(runData)

	// Hour-of-day patterns (needs a large dataset; usually store-backed)
	analysis.Hourly = computeHourlyPatterns(runData)

	return analysis
}

// stratifiedSampleIndices selects indices spread evenly across time buckets.
// Runs are bucketed by CreatedAt timestamp into equal-width time intervals,
// then samples are drawn proportionally from each bucket using deterministic
// Fisher-Yates selection. Returns sorted indices into the original runs slice.
func stratifiedSampleIndices(runs []githubapi.WorkflowRun, sampleSize int) []int {
	total := len(runs)
	if sampleSize >= total {
		indices := make([]int, total)
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Deterministic seed from run IDs
	h := fnv.New64a()
	for _, run := range runs {
		fmt.Fprintf(h, "%d", run.ID)
	}
	rng := rand.New(rand.NewSource(int64(h.Sum64())))

	// Build time-ordered index pairs: (parsedTime, originalIndex)
	type indexedRun struct {
		t   time.Time
		idx int
	}
	indexed := make([]indexedRun, total)
	for i, run := range runs {
		t, _ := utils.ParseTime(run.CreatedAt)
		indexed[i] = indexedRun{t: t, idx: i}
	}
	sort.Slice(indexed, func(i, j int) bool {
		return indexed[i].t.Before(indexed[j].t)
	})

	// Find time range
	minT := indexed[0].t
	maxT := indexed[total-1].t
	timeSpan := maxT.Sub(minT)

	// Divide into equal-width time buckets
	numBuckets := sampleSize
	if numBuckets > 10 {
		numBuckets = 10
	}
	if numBuckets < 1 {
		numBuckets = 1
	}

	// Assign runs to buckets. If all timestamps are identical (timeSpan==0),
	// put everything in bucket 0.
	buckets := make([][]int, numBuckets) // each bucket holds original indices
	for _, ir := range indexed {
		b := 0
		if timeSpan > 0 {
			b = int(float64(ir.t.Sub(minT)) / float64(timeSpan) * float64(numBuckets))
			if b >= numBuckets {
				b = numBuckets - 1
			}
		}
		buckets[b] = append(buckets[b], ir.idx)
	}

	// Allocate samples per bucket proportionally
	perBucket := make([]int, numBuckets)
	allocated := 0
	for i, bucket := range buckets {
		perBucket[i] = int(math.Round(float64(len(bucket)) / float64(total) * float64(sampleSize)))
		// Clamp to bucket size
		if perBucket[i] > len(bucket) {
			perBucket[i] = len(bucket)
		}
		allocated += perBucket[i]
	}

	// Adjust to hit exact sampleSize (rounding may over/under-allocate)
	for allocated < sampleSize {
		// Add to the largest under-sampled bucket
		bestIdx := -1
		bestSlack := 0
		for i, bucket := range buckets {
			slack := len(bucket) - perBucket[i]
			if slack > bestSlack {
				bestSlack = slack
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		perBucket[bestIdx]++
		allocated++
	}
	for allocated > sampleSize {
		// Remove from the bucket with the most samples (that has >0)
		bestIdx := -1
		bestCount := 0
		for i := range buckets {
			if perBucket[i] > bestCount {
				bestCount = perBucket[i]
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		perBucket[bestIdx]--
		allocated--
	}

	// Within each bucket, select deterministically using Fisher-Yates
	var selected []int
	for i, bucket := range buckets {
		n := perBucket[i]
		if n <= 0 || len(bucket) == 0 {
			continue
		}
		if n >= len(bucket) {
			selected = append(selected, bucket...)
			continue
		}
		// Partial Fisher-Yates shuffle
		perm := make([]int, len(bucket))
		copy(perm, bucket)
		for j := 0; j < n; j++ {
			k := j + rng.Intn(len(perm)-j)
			perm[j], perm[k] = perm[k], perm[j]
		}
		selected = append(selected, perm[:n]...)
	}

	sort.Ints(selected)
	return selected
}

// generateRationale produces a human-readable explanation of sampling decisions.
func generateRationale(s SamplingInfo) string {
	parts := []string{fmt.Sprintf("%s runs analyzed.", formatCount(s.TotalRuns))}

	if s.Enabled {
		parts = append(parts, fmt.Sprintf("%d sampled for job details across %d workflows (%d obs per major workflow, %d minor; temporally stratified).",
			s.SampleSize, s.WorkflowCount, s.MajorTarget, s.MinorTarget))
	} else {
		parts = append(parts, "Full job details fetched for all runs.")
	}

	return strings.Join(parts, " ")
}

// formatCount formats a number with commas for readability.
func formatCount(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

// ConvertRuns converts WorkflowRun data to RunData without fetching jobs.
func ConvertRuns(runs []githubapi.WorkflowRun) []RunData {
	runData := make([]RunData, len(runs))
	for i, run := range runs {
		createdAt, _ := utils.ParseTime(run.CreatedAt)
		updatedAt, _ := utils.ParseTime(run.UpdatedAt)
		attempt := run.RunAttempt
		if attempt == 0 {
			attempt = 1
		}
		rd := RunData{
			ID:           run.ID,
			Attempt:      attempt,
			WorkflowName: run.Name,
			HeadSHA:      run.HeadSHA,
			Branch:       run.HeadBranch,
			Event:        run.Event,
			Status:       run.Status,
			Conclusion:   run.Conclusion,
			CreatedAt:    createdAt,
			UpdatedAt:    updatedAt,
		}
		// For retried runs, CreatedAt is from the original attempt, so
		// UpdatedAt-CreatedAt would also span the idle gap between attempts.
		// Use RunStartedAt as the effective start of this attempt.
		startAt := createdAt
		if run.RunAttempt > 1 && run.RunStartedAt != "" {
			if retryStart, ok := utils.ParseTime(run.RunStartedAt); ok {
				startAt = retryStart
			}
		}
		rd.StartedAt = startAt
		if !startAt.IsZero() && !updatedAt.IsZero() {
			rd.Duration = updatedAt.Sub(startAt).Milliseconds()
		}
		runData[i] = rd
	}
	return runData
}

// CollectCompletedRunData converts the completed runs in `runs` to store-ready
// RunData and fetches their job detail, so a normal analysis can seed the local
// store with the same records `ote sync` would (job fetches are served from the
// HTTP cache the analysis already populated). In-progress and queued runs are
// skipped: their timings are not yet final and would pollute the typical-run
// baseline. Returns nil when nothing is completed, without touching the client.
func CollectCompletedRunData(ctx context.Context, client githubapi.GitHubProvider, runs []githubapi.WorkflowRun, reporter ProgressReporter) []RunData {
	var completed []githubapi.WorkflowRun
	for _, run := range runs {
		if run.Status == "completed" {
			completed = append(completed, run)
		}
	}
	if len(completed) == 0 {
		return nil
	}
	runData := ConvertRuns(completed)
	indices := make([]int, len(completed))
	for i := range completed {
		indices[i] = i
	}
	_, _ = fetchJobsForRuns(ctx, client, runData, completed, indices, reporter)
	return runData
}

// fetchJobsForRuns fetches job details for runs at the given indices.
// It returns the number of runs whose job details were successfully fetched;
// individual fetch errors are skipped, but a cancelled/expired context aborts.
// jobFetchWorkers bounds concurrent job-detail requests. GitHub's secondary
// limits allow 100 concurrent / 900 points-min; 8 stays far inside both
// while cutting wall time ~8x versus serial fetching.
const jobFetchWorkers = 8

func fetchJobsForRuns(ctx context.Context, client githubapi.GitHubProvider, runData []RunData, runs []githubapi.WorkflowRun, indices []int, reporter ProgressReporter) (int, error) {
	var (
		fetched atomic.Int64
		wg      sync.WaitGroup
		sem     = make(chan struct{}, jobFetchWorkers)
	)
	for _, idx := range indices {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			fetchJobsForRun(ctx, client, runData, runs[idx], idx, reporter, &fetched)
		}(idx)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return int(fetched.Load()), fmt.Errorf("job fetching aborted: %w", ctx.Err())
	}
	return int(fetched.Load()), nil
}

// fetchJobsForRun fetches one run's jobs and fills runData[idx]. Each
// goroutine writes only its own runData element, so no locking is needed.
func fetchJobsForRun(ctx context.Context, client githubapi.GitHubProvider, runData []RunData, run githubapi.WorkflowRun, idx int, reporter ProgressReporter, fetched *atomic.Int64) {
	jobsURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%d/jobs",
		run.Repository.Owner.Login, run.Repository.Name, run.ID)
	jobs, err := client.FetchJobsPaginated(ctx, jobsURL)
	if reporter != nil {
		reporter.ProcessRun()
	}
	if err != nil {
		return
	}
	fetched.Add(1)
	runData[idx].Jobs = append(runData[idx].Jobs, ConvertJobs(jobs, run.RunAttempt)...)
}

// ConvertJobs converts API jobs into JobData, skipping jobs from previous
// retry attempts to avoid double-counting. runAttempt 0 accepts all
// attempts (used when the caller has no listing context for the run).
func ConvertJobs(jobs []githubapi.Job, runAttempt int64) []JobData {
	var out []JobData
	for _, job := range jobs {
		if runAttempt != 0 && job.RunAttempt != 0 && job.RunAttempt != runAttempt {
			continue
		}
		createdAt, _ := utils.ParseTime(job.CreatedAt)
		startedAt, _ := utils.ParseTime(job.StartedAt)
		completedAt, _ := utils.ParseTime(job.CompletedAt)

		duration := int64(0)
		if !startedAt.IsZero() && !completedAt.IsZero() {
			duration = completedAt.Sub(startedAt).Milliseconds()
		}

		queueTime := int64(0)
		if !createdAt.IsZero() && !startedAt.IsZero() {
			queueTime = startedAt.Sub(createdAt).Milliseconds()
		}

		out = append(out, JobData{
			ID:          job.ID,
			Name:        job.Name,
			URL:         job.HTMLURL,
			Status:      job.Status,
			Conclusion:  job.Conclusion,
			CreatedAt:   createdAt,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    duration,
			QueueTime:   queueTime,
			RunnerName:  job.RunnerName,
			Labels:      job.Labels,
		})
	}
	return out
}

// calculateTrendSummary computes summary statistics
func calculateTrendSummary(runs []RunData) TrendSummary {
	if len(runs) == 0 {
		return TrendSummary{}
	}

	durations := make([]float64, 0, len(runs))
	successCount := 0
	rerunRuns := 0
	var rerunComputeMs int64

	for _, run := range runs {
		if run.Duration > 0 {
			durationSec := float64(run.Duration) / 1000.0
			durations = append(durations, durationSec)
		}
		if run.Conclusion == "success" {
			successCount++
		}
		if run.Attempt > 1 {
			rerunRuns++
			if run.Duration > 0 {
				rerunComputeMs += run.Duration
			}
		}
	}

	avgDuration := average(durations)
	medianDuration := calculateMedian(durations)
	p95Duration := calculatePercentile(durations, 95)
	avgSuccessRate := float64(successCount) / float64(len(runs)) * 100

	// Determine trend direction
	trendDirection := "stable"
	percentChange := 0.0
	if len(durations) >= 4 {
		midpoint := len(durations) / 2
		firstHalf := average(durations[:midpoint])
		secondHalf := average(durations[midpoint:])

		if firstHalf > 0 {
			percentChange = ((secondHalf - firstHalf) / firstHalf) * 100
			if percentChange < -5 {
				trendDirection = "improving"
			} else if percentChange > 5 {
				trendDirection = "degrading"
			}
		}
	}

	// Generate human-readable trend description
	trendDescription := ""
	switch trendDirection {
	case "improving":
		trendDescription = fmt.Sprintf("Workflow durations decreased by %.1f%% over this period. Runs are completing faster in the more recent half of the analysis window.", -percentChange)
	case "degrading":
		trendDescription = fmt.Sprintf("Workflow durations increased by %.1f%% over this period. Runs are taking longer in the more recent half of the analysis window. Investigate recent changes for added overhead, cache issues, or resource contention.", percentChange)
	case "stable":
		trendDescription = "Workflow durations are stable (within 5% variation) over this period."
	}

	return TrendSummary{
		TotalRuns:        len(runs),
		RerunRuns:        rerunRuns,
		RerunComputeMs:   rerunComputeMs,
		AvgDuration:      avgDuration,
		MedianDuration:   medianDuration,
		P95Duration:      p95Duration,
		AvgSuccessRate:   avgSuccessRate,
		TrendDirection:   trendDirection,
		TrendDescription: trendDescription,
		PercentChange:    percentChange,
	}
}

// generateDurationTrend creates time-series data for duration
func generateDurationTrend(runs []RunData) []DataPoint {
	dayBuckets := make(map[string][]float64)

	for _, run := range runs {
		if run.Duration > 0 {
			dayKey := run.CreatedAt.Format("2006-01-02")
			durationSec := float64(run.Duration) / 1000.0
			dayBuckets[dayKey] = append(dayBuckets[dayKey], durationSec)
		}
	}

	var points []DataPoint
	for dayKey, durations := range dayBuckets {
		timestamp, _ := time.Parse("2006-01-02", dayKey)
		avgDuration := average(durations)
		points = append(points, DataPoint{
			Timestamp: timestamp,
			Value:     avgDuration,
			Count:     len(durations),
		})
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return points
}

// generateSuccessRateTrend creates time-series data for success rate
func generateSuccessRateTrend(runs []RunData) []DataPoint {
	dayBuckets := make(map[string]struct{ success, total int })

	for _, run := range runs {
		dayKey := run.CreatedAt.Format("2006-01-02")
		bucket := dayBuckets[dayKey]
		bucket.total++
		if run.Conclusion == "success" {
			bucket.success++
		}
		dayBuckets[dayKey] = bucket
	}

	var points []DataPoint
	for dayKey, bucket := range dayBuckets {
		timestamp, _ := time.Parse("2006-01-02", dayKey)
		successRate := float64(bucket.success) / float64(bucket.total) * 100
		points = append(points, DataPoint{
			Timestamp: timestamp,
			Value:     successRate,
			Count:     bucket.total,
		})
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return points
}

// analyzeJobTrends analyzes individual job trends
func analyzeJobTrends(runs []RunData) []JobTrend {
	// Collect all jobs by name
	jobMap := make(map[string][]JobData)

	for _, run := range runs {
		for _, job := range run.Jobs {
			jobMap[job.Name] = append(jobMap[job.Name], job)
		}
	}

	var trends []JobTrend

	for name, jobs := range jobMap {
		durations := make([]float64, 0, len(jobs))
		successCount := 0

		for _, job := range jobs {
			if job.Duration > 0 {
				durationSec := float64(job.Duration) / 1000.0
				durations = append(durations, durationSec)
			}
			if job.Conclusion == "success" {
				successCount++
			}
		}

		avgDuration := average(durations)
		medianDuration := calculateMedian(durations)
		successRate := float64(successCount) / float64(len(jobs)) * 100

		// Determine trend direction
		trendDirection := "stable"
		if len(durations) >= 4 {
			midpoint := len(durations) / 2
			firstHalf := average(durations[:midpoint])
			secondHalf := average(durations[midpoint:])

			if firstHalf > 0 {
				percentChange := ((secondHalf - firstHalf) / firstHalf) * 100
				if percentChange < -5 {
					trendDirection = "improving"
				} else if percentChange > 5 {
					trendDirection = "degrading"
				}
			}
		}

		// Collect up to 5 most recent URLs (jobs are oldest-first)
		var urls []string
		for i := len(jobs) - 1; i >= 0 && len(urls) < 5; i-- {
			if jobs[i].URL != "" {
				urls = append(urls, jobs[i].URL)
			}
		}

		trends = append(trends, JobTrend{
			Name:           name,
			URLs:           urls,
			AvgDuration:    avgDuration,
			MedianDuration: medianDuration,
			SuccessRate:    successRate,
			TotalRuns:      len(jobs),
			TrendDirection: trendDirection,
		})
	}

	// Sort by average duration (slowest first)
	sort.Slice(trends, func(i, j int) bool {
		return trends[i].AvgDuration > trends[j].AvgDuration
	})

	return trends
}

// detectFlakyJobs identifies jobs with inconsistent outcomes
func detectFlakyJobs(runs []RunData) []FlakyJob {
	jobMap := make(map[string][]flakeObs)

	for _, run := range runs {
		for _, job := range run.Jobs {
			jobMap[job.Name] = append(jobMap[job.Name], flakeObs{job: job, headSHA: run.HeadSHA})
		}
	}

	var flakyJobs []FlakyJob

	for name, obs := range jobMap {
		if len(obs) < 5 {
			continue // Not enough data
		}

		successCount := 0
		failureCount := 0
		var lastFailure time.Time
		var failureURLs []string

		// Sort observations by time (most recent first)
		sort.Slice(obs, func(i, j int) bool {
			return obs[i].job.CompletedAt.After(obs[j].job.CompletedAt)
		})

		recentFailures := 0
		recentLimit := 10
		if len(obs) < recentLimit {
			recentLimit = len(obs)
		}

		// Same-SHA divergence: a commit on which this job both passed and
		// failed proves the job, not the code, is unstable.
		shaOutcomes := make(map[string]uint8) // bit 1 = success seen, bit 2 = failure seen
		sameSHAFlakes := 0

		for i, o := range obs {
			job := o.job
			switch job.Conclusion {
			case "success":
				successCount++
				if o.headSHA != "" {
					if shaOutcomes[o.headSHA]&2 != 0 && shaOutcomes[o.headSHA]&1 == 0 {
						sameSHAFlakes++
					}
					shaOutcomes[o.headSHA] |= 1
				}
			case "failure":
				failureCount++
				if lastFailure.IsZero() || job.CompletedAt.After(lastFailure) {
					lastFailure = job.CompletedAt
				}
				if job.URL != "" && len(failureURLs) < 5 {
					failureURLs = append(failureURLs, job.URL)
				}
				if i < recentLimit {
					recentFailures++
				}
				if o.headSHA != "" {
					if shaOutcomes[o.headSHA]&1 != 0 && shaOutcomes[o.headSHA]&2 == 0 {
						sameSHAFlakes++
					}
					shaOutcomes[o.headSHA] |= 2
				}
			}
		}

		// Flakiness requires mixed outcomes: a job that never failed isn't
		// flaky, and one that never succeeded is consistently broken, not flaky.
		if failureCount == 0 || successCount == 0 {
			continue
		}

		// Transition score: pass/fail state changes over chronological
		// (oldest-first) decisive observations, normalized to 0..1.
		transitions := 0
		decisive := 0
		prev := ""
		for i := len(obs) - 1; i >= 0; i-- { // obs is newest-first
			c := obs[i].job.Conclusion
			if c != "success" && c != "failure" {
				continue
			}
			decisive++
			if prev != "" && c != prev {
				transitions++
			}
			prev = c
		}
		transitionScore := 0.0
		if decisive > 1 {
			transitionScore = float64(transitions) / float64(decisive-1)
		}

		// Rate over decisive outcomes only, so skipped/cancelled/in-progress
		// observations don't dilute the flake rate.
		flakeRate := float64(failureCount) / float64(successCount+failureCount) * 100

		// Include on either signal: >10% flake rate, or definitive same-SHA
		// divergence regardless of rate.
		if flakeRate > 10 || sameSHAFlakes > 0 {
			flakyJobs = append(flakyJobs, FlakyJob{
				Name:            name,
				URLs:            failureURLs,
				TotalRuns:       len(obs),
				SuccessCount:    successCount,
				FailureCount:    failureCount,
				FlakeRate:       flakeRate,
				RecentFailures:  recentFailures,
				LastFailure:     lastFailure,
				SameSHAFlakes:   sameSHAFlakes,
				TransitionScore: transitionScore,
			})
		}
	}

	// Definitive same-SHA flakes first, then transition score, then rate.
	sort.Slice(flakyJobs, func(i, j int) bool {
		if flakyJobs[i].SameSHAFlakes != flakyJobs[j].SameSHAFlakes {
			return flakyJobs[i].SameSHAFlakes > flakyJobs[j].SameSHAFlakes
		}
		if flakyJobs[i].TransitionScore != flakyJobs[j].TransitionScore {
			return flakyJobs[i].TransitionScore > flakyJobs[j].TransitionScore
		}
		return flakyJobs[i].FlakeRate > flakyJobs[j].FlakeRate
	})

	return flakyJobs
}

// flakeObs pairs a job observation with the commit it ran on.
type flakeObs struct {
	job     JobData
	headSHA string
}

// Utility functions

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func calculatePercentile(values []float64, p int) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	index := int(math.Ceil(float64(len(sorted))*float64(p)/100.0)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

// jobObservation captures per-observation metadata for changepoint detection.
type jobObservation struct {
	DurationSec  float64
	RunCreatedAt time.Time
	HeadSHA      string
	JobURL       string
}

// detectChangepoint finds the single split point that minimizes total
// sum-of-squared residuals. Returns nil if len(observations) < 2*minSideSize.
func detectChangepoint(observations []jobObservation, minSideSize int) *Changepoint {
	n := len(observations)
	if n < 2*minSideSize {
		return nil
	}

	bestSSR := math.MaxFloat64
	bestIdx := -1

	// Prefix sums of durations and squared durations make each split's
	// sum-of-squared-residuals O(1): SSR = Σx² - (Σx)²/n per side,
	// keeping the whole scan O(n) instead of O(n²).
	prefixSum := make([]float64, n+1)
	prefixSq := make([]float64, n+1)
	for i, o := range observations {
		prefixSum[i+1] = prefixSum[i] + o.DurationSec
		prefixSq[i+1] = prefixSq[i] + o.DurationSec*o.DurationSec
	}

	for i := minSideSize; i <= n-minSideSize; i++ {
		leftSum := prefixSum[i]
		leftSSR := prefixSq[i] - leftSum*leftSum/float64(i)

		rightSum := prefixSum[n] - prefixSum[i]
		rightSSR := (prefixSq[n] - prefixSq[i]) - rightSum*rightSum/float64(n-i)

		ssr := leftSSR + rightSSR
		if ssr < bestSSR {
			bestSSR = ssr
			bestIdx = i
		}
	}

	if bestIdx < 0 {
		return nil
	}

	beforeAvg := avgObservations(observations[:bestIdx])
	afterAvg := avgObservations(observations[bestIdx:])
	last := observations[bestIdx-1]
	first := observations[bestIdx]

	return &Changepoint{
		Date:         first.RunCreatedAt,
		BeforeSHA:    last.HeadSHA,
		AfterSHA:     first.HeadSHA,
		BeforeRunURL: last.JobURL,
		AfterRunURL:  first.JobURL,
		BeforeAvg:    beforeAvg,
		AfterAvg:     afterAvg,
		Index:        bestIdx,
		TotalPoints:  n,
	}
}

// avgObservations computes the mean DurationSec of a slice of observations.
func avgObservations(obs []jobObservation) float64 {
	if len(obs) == 0 {
		return 0
	}
	sum := 0.0
	for _, o := range obs {
		sum += o.DurationSec
	}
	return sum / float64(len(obs))
}

// calculateJobChanges finds jobs that got significantly slower or faster
func calculateJobChanges(runs []RunData) ([]JobRegression, []JobImprovement) {
	if len(runs) < 4 {
		return nil, nil // Not enough data
	}

	// Group jobs by name, preserving per-observation metadata (chronological order)
	jobMap := make(map[string][]jobObservation)

	for _, run := range runs {
		for _, job := range run.Jobs {
			if job.Duration <= 0 {
				continue
			}
			jobMap[job.Name] = append(jobMap[job.Name], jobObservation{
				DurationSec:  float64(job.Duration) / 1000.0,
				RunCreatedAt: run.CreatedAt,
				HeadSHA:      run.HeadSHA,
				JobURL:       job.URL,
			})
		}
	}

	var regressions []JobRegression
	var improvements []JobImprovement

	for name, observations := range jobMap {
		midpoint := len(observations) / 2
		firstHalf := observations[:midpoint]
		secondHalf := observations[midpoint:]

		if len(firstHalf) < 2 || len(secondHalf) < 2 {
			continue
		}

		// Snap averages to the table's display grid so the reported percent
		// change reproduces from the Was/Now columns the user sees.
		oldAvg := utils.RoundTrendDuration(avgObservations(firstHalf))
		newAvg := utils.RoundTrendDuration(avgObservations(secondHalf))
		if oldAvg == 0 {
			continue
		}
		change := newAvg - oldAvg
		percentChange := (change / oldAvg) * 100

		// Only include significant changes (>10%)
		if math.Abs(percentChange) < 10 {
			continue
		}

		// Take up to 5 most recent URLs (newest first)
		var urls []string
		for i := len(secondHalf) - 1; i >= 0 && len(urls) < 5; i-- {
			if secondHalf[i].JobURL != "" {
				urls = append(urls, secondHalf[i].JobURL)
			}
		}

		cp := detectChangepoint(observations, 3)

		if change > 0 {
			// Regression (got slower)
			regressions = append(regressions, JobRegression{
				Name:            name,
				URLs:            urls,
				OldAvgDuration:  oldAvg,
				NewAvgDuration:  newAvg,
				PercentIncrease: percentChange,
				AbsoluteChange:  change,
				Changepoint:     cp,
			})
		} else {
			// Improvement (got faster)
			improvements = append(improvements, JobImprovement{
				Name:            name,
				URLs:            urls,
				OldAvgDuration:  oldAvg,
				NewAvgDuration:  newAvg,
				PercentDecrease: -percentChange,
				AbsoluteChange:  -change,
				Changepoint:     cp,
			})
		}
	}

	// Sort by percent change (worst first)
	sort.Slice(regressions, func(i, j int) bool {
		return regressions[i].PercentIncrease > regressions[j].PercentIncrease
	})
	sort.Slice(improvements, func(i, j int) bool {
		return improvements[i].PercentDecrease > improvements[j].PercentDecrease
	})

	// Limit to top 10
	if len(regressions) > 10 {
		regressions = regressions[:10]
	}
	if len(improvements) > 10 {
		improvements = improvements[:10]
	}

	return regressions, improvements
}

// calculateQueueTimeStats computes queue time statistics
func calculateQueueTimeStats(runs []RunData) QueueTimeStats {
	var queueTimes []float64
	var runTimes []float64

	for _, run := range runs {
		for _, job := range run.Jobs {
			if job.QueueTime > 0 {
				queueTimes = append(queueTimes, float64(job.QueueTime)/1000.0)
			}
			if job.Duration > 0 {
				runTimes = append(runTimes, float64(job.Duration)/1000.0)
			}
		}
	}

	if len(queueTimes) == 0 {
		return QueueTimeStats{}
	}

	avgQueue := average(queueTimes)
	avgRun := average(runTimes)
	totalTime := avgQueue + avgRun
	queueRatio := 0.0
	if totalTime > 0 {
		queueRatio = (avgQueue / totalTime) * 100
	}

	return QueueTimeStats{
		AvgQueueTime:    avgQueue,
		MedianQueueTime: calculateMedian(queueTimes),
		P95QueueTime:    calculatePercentile(queueTimes, 95),
		AvgRunTime:      avgRun,
		MedianRunTime:   calculateMedian(runTimes),
		QueueTimeRatio:  queueRatio,
	}
}

// dumpRuns writes the fetched run data (including any job detail) as JSON,
// for offline analysis such as sampling-strategy evaluation.
func dumpRuns(path, owner, repo string, days int, runs []RunData) error {
	data, err := json.MarshalIndent(RunDump{Owner: owner, Repo: repo, Days: days, Runs: runs}, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// jobSampleTargets maps the margin-of-error knob to per-workflow observation
// targets. Calibrated empirically with cmd/sample-eval against full-scan
// ground truth (nodejs/node, rails/rails): ~50 observations per significant
// workflow keeps worst-job p50 error ≲10% and p95 error ≲30% at 90% of
// trials, while trivial workflows need only enough to be represented.
func JobSampleTargets(marginOfError float64) (major, minor int) {
	major = int(math.Round(5.0 / marginOfError)) // 0.10 → 50
	if major < 20 {
		major = 20
	}
	if major > 200 {
		major = 200
	}
	minor = major * 2 / 5 // 0.10 → 20
	if minor < 10 {
		minor = 10
	}
	return major, minor
}

// selectSampleIndices chooses which runs get job-detail fetches by sampling
// each workflow independently: workflows holding ≥1% of total run time get
// min(N, major) runs, the rest min(N, minor), drawn via deterministic
// temporal stratification within the workflow. Per-workflow allocation keeps
// job-level percentiles honest — a global sample starves every individual
// job of observations — and makes the API cost scale with the number of
// workflows rather than the size of the window. Returns sorted indices.
func SelectSampleIndices(runs []githubapi.WorkflowRun, major, minor int) []int {
	byWorkflow := make(map[string][]int)
	durations := make(map[string]float64)
	var totalDuration float64
	for i, run := range runs {
		byWorkflow[run.Name] = append(byWorkflow[run.Name], i)
		created, _ := utils.ParseTime(run.CreatedAt)
		updated, _ := utils.ParseTime(run.UpdatedAt)
		if !created.IsZero() && !updated.IsZero() && updated.After(created) {
			d := updated.Sub(created).Seconds()
			durations[run.Name] += d
			totalDuration += d
		}
	}

	names := make([]string, 0, len(byWorkflow))
	for name := range byWorkflow {
		names = append(names, name)
	}
	sort.Strings(names)

	var selected []int
	for _, name := range names {
		group := byWorkflow[name]
		target := minor
		if totalDuration > 0 && durations[name]/totalDuration >= 0.01 {
			target = major
		}
		if target >= len(group) {
			selected = append(selected, group...)
			continue
		}
		subset := make([]githubapi.WorkflowRun, len(group))
		for i, idx := range group {
			subset[i] = runs[idx]
		}
		for _, si := range stratifiedSampleIndices(subset, target) {
			selected = append(selected, group[si])
		}
	}
	sort.Ints(selected)
	return selected
}

// countWorkflows returns the number of distinct workflow names in runs.
func countWorkflows(runs []githubapi.WorkflowRun) int {
	names := make(map[string]struct{})
	for _, run := range runs {
		names[run.Name] = struct{}{}
	}
	return len(names)
}
