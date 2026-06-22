package analyzer

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNativeRunnerContract verifies that a trace emitted by the real
// self-hosted GitHub Actions runner (actions/runner#4366 native OpenTelemetry)
// is classified into non-zero runs/jobs/steps.
//
// The runner's contract is two levels deep: a SERVER job span is the trace
// root (its parent is a workflow-run span that is NOT exported, so the ref
// dangles), carrying run-level attributes (cicd.pipeline.run.id, vcs.*) and
// task-level attributes (cicd.pipeline.task.*). Step spans are its children
// and carry cicd.pipeline.task.*.
//
// Before the fix, the job-root was classified as a plain "job" (its dangling
// parent made isRootSpan false) and the step children were classified as jobs
// (cicd task spans were never marked as leaves), so the summary read
// Runs=0, Jobs=N, Steps=0.
func TestNativeRunnerContract(t *testing.T) {
	t.Parallel()

	spans, err := otlpfile.ParseFile("testdata/native_runner_contract.json")
	require.NoError(t, err)
	require.NotEmpty(t, spans)

	enricher := enrichment.DefaultEnricher()
	cm := CombinedMetricsFromSpans(spans, enricher)

	// Job-as-root collapses to one run and one job; the 4 step children count
	// as steps.
	assert.Equal(t, 1, cm.TotalRuns, "the job-as-root is the run")
	assert.Equal(t, 1, cm.TotalJobs, "the job-as-root is also the job")
	assert.Equal(t, 4, cm.TotalSteps, "the 4 task spans are steps")

	// Run succeeded → 100% workflow success.
	assert.Equal(t, "100.0", cm.SuccessRate, "the run (job-root) result is success")

	// RunsFromSpans should reconstruct the same shape: one run, one job, steps
	// nested under the job.
	runs := RunsFromSpans(spans, enricher)
	require.Len(t, runs, 1, "one run")
	require.Len(t, runs[0].Jobs, 1, "one job under the run")
	assert.Equal(t, 4, len(runs[0].Jobs[0].Steps), "4 steps under the job")
	assert.Equal(t, "27979259696", runs[0].Identifier, "run id from cicd.pipeline.run.id")
}
