package analyzer

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

func InitializeMetrics() Metrics {
	return Metrics{
		JobDurations:    []float64{},
		JobNames:        []string{},
		JobURLs:         []string{},
		StepDurations:   []StepDuration{},
		RunnerJobCounts: map[string]int{},
		RunnerDurations: map[string]float64{},
		QueueTimes:      []float64{},
		BillableMs:      map[string]int64{},
		JobTimeline:     []TimelineJob{},
		LongestJob:      JobDuration{Name: "", Duration: 0},
		ShortestJob:     JobDuration{Name: "", Duration: math.Inf(1)},
	}
}

func FindEarliestTimestamp(runs []githubapi.WorkflowRun) int64 {
	earliest := int64(math.MaxInt64)
	for _, run := range runs {
		if t, ok := utils.ParseTime(run.CreatedAt); ok {
			if t.UnixMilli() < earliest {
				earliest = t.UnixMilli()
			}
		}
	}
	if earliest == int64(math.MaxInt64) {
		return int64(math.MaxInt64)
	}
	return earliest
}

func FindLatestTimestamp(runs []githubapi.WorkflowRun) int64 {
	latest := int64(0)
	for _, run := range runs {
		runTime := int64(0)
		if t, ok := utils.ParseTime(run.UpdatedAt); ok {
			runTime = t.UnixMilli()
		} else if t, ok := utils.ParseTime(run.CreatedAt); ok {
			runTime = t.UnixMilli()
		}
		if runTime > latest {
			latest = runTime
		}
	}
	return latest
}

// sortJobEventsEndFirst orders events by timestamp, processing "end" events
// before "start" events at equal timestamps. This keeps back-to-back jobs
// (one ending exactly when the next starts, common with second-granularity
// GitHub timestamps and `needs:` chains) from being counted as concurrent,
// consistent with FindOverlappingJobs treating touching intervals as
// non-overlapping.
func sortJobEventsEndFirst(events []JobEvent) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Ts == events[j].Ts {
			return events[i].Type == "end" && events[j].Type == "start"
		}
		return events[i].Ts < events[j].Ts
	})
}

func CalculateMaxConcurrency(jobStartTimes, jobEndTimes []JobEvent) int {
	if len(jobStartTimes) == 0 {
		return 0
	}
	all := append([]JobEvent{}, jobStartTimes...)
	all = append(all, jobEndTimes...)
	sortJobEventsEndFirst(all)

	current := 0
	maxConcurrency := 0
	for _, event := range all {
		if event.Type == "start" {
			current++
			if current > maxConcurrency {
				maxConcurrency = current
			}
		} else {
			current--
		}
	}
	return maxConcurrency
}

func CalculateFinalMetrics(metrics Metrics, totalRuns int, jobStartTimes, jobEndTimes []JobEvent) FinalMetrics {
	avgJob := 0.0
	if len(metrics.JobDurations) > 0 {
		sum := 0.0
		for _, d := range metrics.JobDurations {
			sum += d
		}
		avgJob = sum / float64(len(metrics.JobDurations))
	}

	avgStep := 0.0
	if len(metrics.StepDurations) > 0 {
		sum := 0.0
		for _, d := range metrics.StepDurations {
			sum += d.Duration
		}
		avgStep = sum / float64(len(metrics.StepDurations))
	}

	successRate := "0"
	if metrics.TotalRuns > 0 {
		successRate = formatPercent(float64(metrics.SuccessfulRuns) / float64(metrics.TotalRuns) * 100)
	}
	jobSuccessRate := "0"
	if metrics.TotalJobs > 0 {
		jobSuccessRate = formatPercent(float64(metrics.TotalJobs-metrics.FailedJobs) / float64(metrics.TotalJobs) * 100)
	}

	// Queue time stats
	avgQueueTime := 0.0
	maxQueueTime := 0.0
	if len(metrics.QueueTimes) > 0 {
		sum := 0.0
		for _, qt := range metrics.QueueTimes {
			sum += qt
			if qt > maxQueueTime {
				maxQueueTime = qt
			}
		}
		avgQueueTime = sum / float64(len(metrics.QueueTimes))
	}

	// Retry rate
	retryRate := "0"
	if metrics.TotalRuns > 0 {
		retryRate = formatPercent(float64(metrics.RetriedRuns) / float64(metrics.TotalRuns) * 100)
	}

	return FinalMetrics{
		Metrics:         metrics,
		AvgJobDuration:  avgJob,
		AvgStepDuration: avgStep,
		SuccessRate:     successRate,
		JobSuccessRate:  jobSuccessRate,
		MaxConcurrency:  CalculateMaxConcurrency(jobStartTimes, jobEndTimes),
		AvgQueueTime:    avgQueueTime,
		MaxQueueTime:    maxQueueTime,
		RetryRate:       retryRate,
	}
}

func AnalyzeSlowJobs(metrics Metrics, limit int) []JobWithURL {
	if limit <= 0 {
		limit = 5
	}
	jobs := []JobWithURL{}
	for i, duration := range metrics.JobDurations {
		name := "Job"
		if i < len(metrics.JobNames) {
			name = metrics.JobNames[i]
		}
		url := ""
		if i < len(metrics.JobURLs) {
			url = metrics.JobURLs[i]
		}
		jobs = append(jobs, JobWithURL{Name: name, Duration: duration, URL: url})
	}

	SortJobDurationsDesc(jobs)
	if len(jobs) > limit {
		return jobs[:limit]
	}
	return jobs
}

func AnalyzeSlowSteps(metrics Metrics, limit int) []StepDuration {
	if limit <= 0 {
		limit = 5
	}
	steps := append([]StepDuration{}, metrics.StepDurations...)
	SortStepDurationsDesc(steps)
	if len(steps) > limit {
		return steps[:limit]
	}
	return steps
}

func FindOverlappingJobs(jobs []TimelineJob) [][2]TimelineJob {
	overlaps := [][2]TimelineJob{}
	for i := 0; i < len(jobs); i++ {
		for j := i + 1; j < len(jobs); j++ {
			job1 := jobs[i]
			job2 := jobs[j]
			if job1.StartTime < job2.EndTime && job2.StartTime < job1.EndTime {
				overlaps = append(overlaps, [2]TimelineJob{job1, job2})
			}
		}
	}
	return overlaps
}

func FindBottleneckJobs(jobs []TimelineJob) []TimelineJob {
	if len(jobs) == 0 {
		return nil
	}

	significant := []TimelineJob{}
	for _, job := range jobs {
		duration := job.EndTime - job.StartTime
		if duration > 1000 {
			significant = append(significant, job)
		}
	}
	if len(significant) == 0 {
		return nil
	}

	SortTimelineByDurationDesc(significant)

	pipelineStart := significant[0].StartTime
	pipelineEnd := significant[0].EndTime
	for _, job := range jobs {
		if job.StartTime < pipelineStart {
			pipelineStart = job.StartTime
		}
		if job.EndTime > pipelineEnd {
			pipelineEnd = job.EndTime
		}
	}
	totalPipeline := float64(pipelineEnd - pipelineStart)
	threshold := totalPipeline * 0.1

	bottlenecks := []TimelineJob{}
	for _, job := range significant {
		duration := float64(job.EndTime - job.StartTime)
		if duration > threshold {
			bottlenecks = append(bottlenecks, job)
		}
	}

	if len(bottlenecks) == 0 {
		if len(significant) > 2 {
			return significant[:2]
		}
		return significant
	}
	return bottlenecks
}

func CalculateCombinedMetrics(urlResults []URLResult, totalRuns int, allJobStartTimes, allJobEndTimes []JobEvent) CombinedMetrics {
	totalJobs := 0
	totalSteps := 0
	for _, result := range urlResults {
		totalJobs += result.Metrics.TotalJobs
		totalSteps += result.Metrics.TotalSteps
	}

	jobTimeline := []CombinedTimelineJob{}
	for _, result := range urlResults {
		for _, job := range result.Metrics.JobTimeline {
			jobTimeline = append(jobTimeline, CombinedTimelineJob{
				TimelineJob: job,
				URLIndex:    result.URLIndex,
				SourceURL:   result.DisplayURL,
				SourceName:  result.DisplayName,
			})
		}
	}

	return CombinedMetrics{
		TotalRuns:      totalRuns,
		TotalJobs:      totalJobs,
		TotalSteps:     totalSteps,
		SuccessRate:    CalculateCombinedSuccessRate(urlResults),
		JobSuccessRate: CalculateCombinedJobSuccessRate(urlResults),
		MaxConcurrency: maxConcurrencyAtTimes(allJobStartTimes, allJobEndTimes),
		JobTimeline:    jobTimeline,
	}
}

func CalculateCombinedSuccessRate(urlResults []URLResult) string {
	totalSuccessful := 0
	totalRuns := 0
	for _, result := range urlResults {
		totalSuccessful += result.Metrics.SuccessfulRuns
		totalRuns += result.Metrics.TotalRuns
	}
	if totalRuns == 0 {
		return "0.0"
	}
	return formatPercent(float64(totalSuccessful) / float64(totalRuns) * 100)
}

func CalculateCombinedJobSuccessRate(urlResults []URLResult) string {
	totalSuccessful := 0
	totalJobs := 0
	for _, result := range urlResults {
		totalSuccessful += result.Metrics.TotalJobs - result.Metrics.FailedJobs
		totalJobs += result.Metrics.TotalJobs
	}
	if totalJobs == 0 {
		return "0.0"
	}
	return formatPercent(float64(totalSuccessful) / float64(totalJobs) * 100)
}

func maxConcurrencyAtTimes(startEvents, endEvents []JobEvent) int {
	if len(startEvents) == 0 {
		return 0
	}
	all := append([]JobEvent{}, startEvents...)
	all = append(all, endEvents...)
	sortJobEventsEndFirst(all)

	current := 0
	max := 0
	for _, event := range all {
		if event.Type == "start" {
			current++
			if current > max {
				max = current
			}
		} else {
			current--
		}
	}
	return max
}

func formatPercent(value float64) string {
	return formatFloat(value, 1)
}

func formatFloat(value float64, decimals int) string {
	format := "%." + strconv.Itoa(decimals) + "f"
	return fmt.Sprintf(format, value)
}
