package export

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenderJSON_Golden pins the exact JSON for a known input. It is the
// consumer contract: any change to field names, units, ordering, or
// null-vs-zero semantics must be intentional and update this golden.
func TestRenderJSON_Golden(t *testing.T) {
	rep := &Report{
		SchemaVersion: SchemaVersion, Kind: KindRunAnalysis,
		Meta: Meta{Tool: "ote", GeneratedAt: "2026-06-17T00:00:00Z", Repo: "o/r"},
		Run: &RunReport{
			Summary: RunSummary{
				TotalRuns: 1, SuccessfulRuns: 1, FailedRuns: 0,
				TotalJobs: 2, FailedJobs: 1, TotalSteps: 1,
				SuccessRatePct: f64(100), JobSuccessRatePct: nil, // nil → null (unknown)
				MaxConcurrency: 2, WallClockMs: 3000,
			},
			Runs: []Run{{
				Repo: "o/r", Identifier: "42", Type: "pr", DisplayName: "PR #42",
				TotalJobs: 2, FailedJobs: 1, TotalSteps: 1, JobSuccessRatePct: f64(50),
				AvgQueueMs: 1500, MaxQueueMs: 4000,
				Jobs:  []Job{{Name: "build", Status: "completed", Conclusion: "success", StartMs: 1000, EndMs: 4000, DurationMs: 3000, Required: true, URL: "u1"}},
				Steps: []Step{{Job: "build", Name: "checkout", DurationMs: 500, URL: "s1"}},
			}},
		},
	}

	want := `{
  "schema_version": "1.0",
  "kind": "run_analysis",
  "meta": {
    "tool": "ote",
    "generated_at": "2026-06-17T00:00:00Z",
    "repo": "o/r"
  },
  "run": {
    "summary": {
      "total_runs": 1,
      "successful_runs": 1,
      "failed_runs": 0,
      "total_jobs": 2,
      "failed_jobs": 1,
      "total_steps": 1,
      "success_rate_pct": 100,
      "job_success_rate_pct": null,
      "max_concurrency": 2,
      "wall_clock_ms": 3000
    },
    "runs": [
      {
        "repo": "o/r",
        "identifier": "42",
        "type": "pr",
        "display_name": "PR #42",
        "total_jobs": 2,
        "failed_jobs": 1,
        "total_steps": 1,
        "job_success_rate_pct": 50,
        "avg_queue_ms": 1500,
        "max_queue_ms": 4000,
        "jobs": [
          {
            "name": "build",
            "status": "completed",
            "conclusion": "success",
            "start_ms": 1000,
            "end_ms": 4000,
            "duration_ms": 3000,
            "required": true,
            "url": "u1"
          }
        ],
        "steps": [
          {
            "job": "build",
            "name": "checkout",
            "duration_ms": 500,
            "url": "s1"
          }
        ]
      }
    ]
  }
}
`

	var buf bytes.Buffer
	require.NoError(t, RenderJSON(&buf, rep))
	assert.Equal(t, want, buf.String())
}
