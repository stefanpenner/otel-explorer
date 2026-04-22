// Package arc provides an OpenTelemetry trace recorder for
// Actions Runner Controller (ARC). It implements the MetricsRecorder
// interface from github.com/actions/scaleset/listener, emitting
// per-job spans that correlate with otel-explorer's GitHub Actions
// trace view via deterministic trace/span IDs.
package arc

import "time"

// JobMessageBase mirrors scaleset.JobMessageBase — the common fields
// present on every job lifecycle message from GitHub's Actions backend.
type JobMessageBase struct {
	RunnerRequestID    int64
	RepositoryName     string
	OwnerName          string
	JobID              string
	JobWorkflowRef     string
	JobDisplayName     string
	WorkflowRunID      int64
	EventName          string
	RequestLabels      []string
	QueueTime          time.Time
	ScaleSetAssignTime time.Time
	RunnerAssignTime   time.Time
	FinishTime         time.Time
}

// JobStarted mirrors scaleset.JobStarted.
type JobStarted struct {
	JobMessageBase
	RunnerID   int
	RunnerName string
}

// JobCompleted mirrors scaleset.JobCompleted.
type JobCompleted struct {
	JobMessageBase
	Result     string
	RunnerID   int
	RunnerName string
}

// RunnerScaleSetStatistic mirrors scaleset.RunnerScaleSetStatistic.
type RunnerScaleSetStatistic struct {
	TotalAvailableJobs     int
	TotalAcquiredJobs      int
	TotalAssignedJobs      int
	TotalRunningJobs       int
	TotalRegisteredRunners int
	TotalBusyRunners       int
	TotalIdleRunners       int
}

// MetricsRecorder mirrors the listener.MetricsRecorder interface from
// github.com/actions/scaleset/listener. Implement this interface and
// pass it via listener.WithMetricsRecorder() to emit OTel traces from
// ARC's listener process.
type MetricsRecorder interface {
	RecordJobStarted(msg *JobStarted)
	RecordJobCompleted(msg *JobCompleted)
	RecordStatistics(stats *RunnerScaleSetStatistic)
	RecordDesiredRunners(count int)
}
