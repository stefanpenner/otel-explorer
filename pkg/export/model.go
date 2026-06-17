// Package export defines a normalized, serializable intermediate
// representation (the Report) for ote's analysis output, plus renderers that
// project it to JSON, XLSX, DOCX, and HTML. Every renderer consumes the same
// Report, so the analysis/trends data is mapped to a stable schema exactly
// once. Durations are integers with the unit in the field name (_ms / _sec);
// rates are floats in percent (_pct).
package export

// SchemaVersion is the version of the Report JSON schema. Bump on any
// breaking change to field names, units, or structure.
const SchemaVersion = "1.0"

// Report kinds.
const (
	KindRunAnalysis = "run_analysis"
	KindTrends      = "trends"
)

// Report is the top-level export document. Exactly one of Run/Trends is set,
// selected by Kind.
type Report struct {
	SchemaVersion string       `json:"schema_version"`
	Kind          string       `json:"kind"`
	Meta          Meta         `json:"meta"`
	Highlights    []Insight    `json:"highlights,omitempty"`
	Run           *RunReport   `json:"run,omitempty"`
	Trends        *TrendReport `json:"trends,omitempty"`
}

// Meta carries provenance common to every report.
type Meta struct {
	Tool        string `json:"tool"`
	GeneratedAt string `json:"generated_at"` // RFC3339; injected by the caller
	Repo        string `json:"repo,omitempty"`
}

// --- Run analysis (PR/commit/trace) ---

// RunReport is the analysis of one or more workflow runs (the PR/commit/trace
// path).
type RunReport struct {
	Summary RunSummary `json:"summary"`
	Runs    []Run      `json:"runs"`
}

// RunSummary aggregates across every analyzed run.
type RunSummary struct {
	TotalRuns         int      `json:"total_runs"`
	SuccessfulRuns    int      `json:"successful_runs"`
	FailedRuns        int      `json:"failed_runs"`
	TotalJobs         int      `json:"total_jobs"`
	FailedJobs        int      `json:"failed_jobs"`
	TotalSteps        int      `json:"total_steps"`
	SuccessRatePct    *float64 `json:"success_rate_pct"`     // null = unknown (untyped traces)
	JobSuccessRatePct *float64 `json:"job_success_rate_pct"` // null = unknown
	MaxConcurrency    int      `json:"max_concurrency"`
	WallClockMs       int64    `json:"wall_clock_ms"`
}

// Run is a single analyzed workflow run (or PR/commit aggregate).
type Run struct {
	Repo              string   `json:"repo"`
	Identifier        string   `json:"identifier"`
	Type              string   `json:"type"`
	Branch            string   `json:"branch,omitempty"`
	HeadSHA           string   `json:"head_sha,omitempty"`
	DisplayName       string   `json:"display_name,omitempty"`
	URL               string   `json:"url,omitempty"`
	TotalJobs         int      `json:"total_jobs"`
	FailedJobs        int      `json:"failed_jobs"`
	TotalSteps        int      `json:"total_steps"`
	JobSuccessRatePct *float64 `json:"job_success_rate_pct"` // null = unknown
	AvgQueueMs        int64    `json:"avg_queue_ms"`
	MaxQueueMs        int64    `json:"max_queue_ms"`
	Jobs              []Job    `json:"jobs"`
	Steps             []Step   `json:"steps,omitempty"`

	conclusion string // run outcome, used only to derive summary counts
}

// Job is one job within a run.
type Job struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	StartMs    int64  `json:"start_ms"`
	EndMs      int64  `json:"end_ms"`
	DurationMs int64  `json:"duration_ms"`
	Required   bool   `json:"required"`
	URL        string `json:"url,omitempty"`
}

// Step is one step within a job.
type Step struct {
	Job        string `json:"job"`
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
	URL        string `json:"url,omitempty"`
}

// --- Trends ---

// TrendReport is the historical trend analysis (the `ote trends` path).
type TrendReport struct {
	Days          int               `json:"days"`
	Summary       TrendSummary      `json:"summary"`
	QueueStats    QueueStats        `json:"queue_stats"`
	Typical       []TypicalWorkflow `json:"typical_workflows,omitempty"`
	FlakyJobs     []FlakyJob        `json:"flaky_jobs,omitempty"`
	Regressions   []JobChange       `json:"regressions,omitempty"`
	Improvements  []JobChange       `json:"improvements,omitempty"`
	Hourly        []HourBucket      `json:"hourly,omitempty"`
	DailyDuration []DailyPoint      `json:"daily_duration,omitempty"`
	DailySuccess  []DailyPoint      `json:"daily_success,omitempty"`
}

// TrendSummary is the headline of a trend report.
type TrendSummary struct {
	TotalRuns         int     `json:"total_runs"`
	AvgDurationSec    float64 `json:"avg_duration_sec"`
	MedianDurationSec float64 `json:"median_duration_sec"`
	P95DurationSec    float64 `json:"p95_duration_sec"`
	AvgSuccessRatePct float64 `json:"avg_success_rate_pct"`
	TrendDirection    string  `json:"trend_direction"`
	TrendDescription  string  `json:"trend_description"`
	PercentChange     float64 `json:"percent_change"`
	RerunRuns         int     `json:"rerun_runs"`
	RerunComputeMs    int64   `json:"rerun_compute_ms"`
}

// QueueStats summarizes queue vs run time across the window.
type QueueStats struct {
	AvgQueueSec    float64 `json:"avg_queue_sec"`
	MedianQueueSec float64 `json:"median_queue_sec"`
	P95QueueSec    float64 `json:"p95_queue_sec"`
	QueueRatioPct  float64 `json:"queue_ratio_pct"`
}

// TypicalWorkflow is a workflow's typical-run profile.
type TypicalWorkflow struct {
	Name        string       `json:"name"`
	SampledRuns int          `json:"sampled_runs"`
	TotalRuns   int          `json:"total_runs"`
	RunDuration Quantiles    `json:"run_duration_sec"`
	Jobs        []TypicalJob `json:"jobs"`
}

// TypicalJob is a job's typical-run profile within a workflow.
type TypicalJob struct {
	Name            string    `json:"name"`
	Samples         int       `json:"samples"`
	PresenceRatePct float64   `json:"presence_rate_pct"`
	SuccessRatePct  float64   `json:"success_rate_pct"`
	StartOffset     Quantiles `json:"start_offset_sec"`
	Duration        Quantiles `json:"duration_sec"`
	QueueTime       Quantiles `json:"queue_time_sec"`
	TrendDirection  string    `json:"trend_direction"`
	P50URL          string    `json:"p50_url,omitempty"`
	P95URL          string    `json:"p95_url,omitempty"`
}

// Quantiles holds a five-number summary (seconds, where used for durations).
type Quantiles struct {
	P5  float64 `json:"p5"`
	P25 float64 `json:"p25"`
	P50 float64 `json:"p50"`
	P75 float64 `json:"p75"`
	P95 float64 `json:"p95"`
}

// FlakyJob is a job with inconsistent outcomes.
type FlakyJob struct {
	Name            string  `json:"name"`
	TotalRuns       int     `json:"total_runs"`
	SuccessCount    int     `json:"success_count"`
	FailureCount    int     `json:"failure_count"`
	FlakeRatePct    float64 `json:"flake_rate_pct"`
	RecentFailures  int     `json:"recent_failures"`
	SameSHAFlakes   int     `json:"same_sha_flakes"`
	TransitionScore float64 `json:"transition_score"`
	SampleURL       string  `json:"sample_url,omitempty"`
}

// JobChange is a regression or improvement in a job's duration.
type JobChange struct {
	Name          string  `json:"name"`
	OldAvgSec     float64 `json:"old_avg_sec"`
	NewAvgSec     float64 `json:"new_avg_sec"`
	PercentChange float64 `json:"percent_change"` // signed: + slower, - faster
	AbsoluteSec   float64 `json:"absolute_sec"`
	DiffURL       string  `json:"diff_url,omitempty"`
}

// HourBucket is one UTC hour of activity.
type HourBucket struct {
	Hour           int     `json:"hour"`
	RunCount       int     `json:"run_count"`
	QueueP50Sec    float64 `json:"queue_p50_sec"`
	DurationP50Sec float64 `json:"duration_p50_sec"`
}

// DailyPoint is one day of a time series.
type DailyPoint struct {
	Date  string  `json:"date"` // YYYY-MM-DD (UTC)
	Value float64 `json:"value"`
	Count int     `json:"count"`
}
