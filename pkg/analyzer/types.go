package analyzer

import (
	"sort"

	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

type Metrics struct {
	TotalRuns       int
	SuccessfulRuns  int
	FailedRuns      int
	RetriedRuns     int
	TotalJobs       int
	FailedJobs      int
	TotalSteps      int
	FailedSteps     int
	JobDurations    []float64
	JobNames        []string
	JobURLs         []string
	StepDurations   []StepDuration
	RunnerJobCounts map[string]int
	RunnerDurations map[string]float64
	QueueTimes      []float64
	BillableMs      map[string]int64
	TotalDuration   float64
	LongestJob      JobDuration
	ShortestJob     JobDuration
	JobTimeline     []TimelineJob
	PendingJobs     []PendingJob
}

type StepDuration struct {
	Name     string
	Duration float64
	URL      string
	JobName  string
}

type JobDuration struct {
	Name     string
	Duration float64
}

type TimelineJob struct {
	Name       string
	StartTime  int64
	EndTime    int64
	Conclusion string
	Status     string
	URL        string
	IsRequired bool
}

type PendingJob struct {
	Name       string
	Status     string
	StartedAt  string
	URL        string
	IsRequired bool
}

type FinalMetrics struct {
	Metrics
	AvgJobDuration  float64
	AvgStepDuration float64
	SuccessRate     string
	JobSuccessRate  string
	MaxConcurrency  int
	AvgQueueTime    float64
	MaxQueueTime    float64
	RetryRate       string
}

type JobEvent struct {
	Ts   int64
	Type string
}

type JobWithURL struct {
	Name     string
	Duration float64
	URL      string
	JobName  string
}

type ReviewEvent struct {
	Type     string
	State    string
	Time     string
	Reviewer string
	MergedBy string
	URL      string
	PRNumber int
	PRTitle  string
}

func (r ReviewEvent) TimeMillis() int64 {
	if t, ok := utils.ParseTime(r.Time); ok {
		return t.UnixMilli()
	}
	return 0
}

type URLResult struct {
	Owner                  string
	Repo                   string
	Identifier             string
	BranchName             string
	HeadSHA                string
	Metrics                FinalMetrics
	TraceEvents            []TraceEvent
	Type                   string
	DisplayName            string
	DisplayURL             string
	URLIndex               int
	JobStartTimes          []JobEvent
	JobEndTimes            []JobEvent
	EarliestTime           int64
	ReviewEvents           []ReviewEvent
	MergedAtMs             *int64
	CommitTimeMs           *int64
	CommitPushedAtMs       *int64
	AllCommitRunsCount     int
	AllCommitRunsComputeMs int64
}

type TraceEvent struct {
	Name string                 `json:"name"`
	Ph   string                 `json:"ph"`
	Ts   int64                  `json:"ts"`
	Dur  int64                  `json:"dur,omitempty"`
	Pid  int                    `json:"pid"`
	Tid  int                    `json:"tid,omitempty"`
	Cat  string                 `json:"cat,omitempty"`
	Args map[string]interface{} `json:"args,omitempty"`
	S    string                 `json:"s,omitempty"`
}

type CombinedMetrics struct {
	TotalRuns      int
	TotalJobs      int
	TotalSteps     int
	SuccessRate    string
	JobSuccessRate string
	MaxConcurrency int
	JobTimeline    []CombinedTimelineJob
}

type CombinedTimelineJob struct {
	TimelineJob
	URLIndex   int
	SourceURL  string
	SourceName string
}

func SortJobEvents(events []JobEvent) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Ts == events[j].Ts {
			// End before start at the same timestamp, so back-to-back jobs
			// are not counted as concurrent (matches CalculateConcurrency).
			return events[i].Type == "end" && events[j].Type == "start"
		}
		return events[i].Ts < events[j].Ts
	})
}

func SortJobDurationsDesc(jobs []JobWithURL) {
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Duration > jobs[j].Duration
	})
}

func SortStepDurationsDesc(steps []StepDuration) {
	sort.Slice(steps, func(i, j int) bool {
		return steps[i].Duration > steps[j].Duration
	})
}

func SortTimelineByDurationDesc(jobs []TimelineJob) {
	sort.Slice(jobs, func(i, j int) bool {
		return (jobs[i].EndTime - jobs[i].StartTime) > (jobs[j].EndTime - jobs[j].StartTime)
	})
}

func SortCombinedJobsByDuration(jobs []CombinedTimelineJob) {
	sort.Slice(jobs, func(i, j int) bool {
		return (jobs[i].EndTime - jobs[i].StartTime) > (jobs[j].EndTime - jobs[j].StartTime)
	})
}
