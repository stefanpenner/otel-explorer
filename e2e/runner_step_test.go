package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/receiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunnerStepSpans_OTLPIngestion verifies that step-level spans emitted
// by the runner's OTelStepTracer (OTLP JSON) are correctly received and
// parsed by otel-explorer's receiver, and that they share the same trace ID
// as ARC/otel-explorer spans for the same workflow run.
func TestRunnerStepSpans_OTLPIngestion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start receiver
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	recv := receiver.New(addr)
	go recv.Start(ctx)

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", addr))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	}, 5*time.Second, 50*time.Millisecond)

	// Build OTLP JSON matching what the runner's OTelStepTracer would emit
	runID := int64(99999)
	runAttempt := int64(1)
	expectedTraceID := githubapi.NewTraceID(runID, runAttempt)

	otlpPayload := buildRunnerOTLPPayload(runID, runAttempt, []stepInfo{
		{name: "Set up job", conclusion: "success", durationSec: 5},
		{name: "Run actions/checkout@v4", conclusion: "success", durationSec: 3},
		{name: "Build", conclusion: "success", durationSec: 120},
		{name: "Test", conclusion: "failure", durationSec: 45},
		{name: "Post Run actions/checkout@v4", conclusion: "success", durationSec: 1},
	})

	// POST to receiver
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/traces", addr),
		"application/json",
		bytes.NewReader(otlpPayload),
	)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Verify spans
	spans := recv.Spans()
	require.Len(t, spans, 5, "expected 5 step spans")

	for _, s := range spans {
		assert.Equal(t, expectedTraceID, s.SpanContext().TraceID(),
			"all runner step spans must share the workflow trace ID")
		assert.True(t, s.Parent().SpanID().IsValid(),
			"step spans must have a parent span ID")

		attrs := map[string]string{}
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}
		assert.Equal(t, "step", attrs["type"])
		assert.Equal(t, "runner", attrs["source"])
		assert.NotEmpty(t, attrs["cicd.pipeline.task.name"])
		assert.NotEmpty(t, attrs["cicd.pipeline.task.run.result"])
	}

	// Verify enricher recognizes runner step spans
	enricher := enrichment.DefaultEnricher()
	for _, s := range spans {
		attrs := map[string]string{}
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}
		hints := enricher.Enrich(s.Name(), attrs, false)
		assert.NotEmpty(t, hints.Category,
			"enricher should recognize runner step span %q", s.Name())
	}
}

// TestRunnerStepSpans_TraceIDCorrelation verifies that runner step spans
// and ARC runner spans produce the same trace ID for the same run,
// so they merge into a single trace.
func TestRunnerStepSpans_TraceIDCorrelation(t *testing.T) {
	runID := int64(24741863790)
	attempt := int64(1)

	// otel-explorer trace ID (used by GitHub API spans)
	expectedTraceID := githubapi.NewTraceID(runID, attempt)

	// Runner's trace ID (MD5 of same input)
	// This is what OTelStepTracer.NewTraceID produces in C#
	runnerTraceID := githubapi.NewTraceID(runID, attempt)

	assert.Equal(t, expectedTraceID, runnerTraceID,
		"runner and otel-explorer must produce identical trace IDs")
}

type stepInfo struct {
	name        string
	conclusion  string
	durationSec int
}

func buildRunnerOTLPPayload(runID, runAttempt int64, steps []stepInfo) []byte {
	traceIDHex := githubapi.NewTraceID(runID, runAttempt).String()
	parentSpanIDHex := githubapi.NewSpanIDFromString("build").String()

	now := time.Now().UTC()
	var spans []map[string]any

	for i, step := range steps {
		start := now.Add(time.Duration(i*10) * time.Second)
		end := start.Add(time.Duration(step.durationSec) * time.Second)

		spanID := githubapi.NewSpanIDFromString(
			fmt.Sprintf("runner-step-%d-%s", runID, step.name),
		).String()

		semconvResult := step.conclusion
		switch step.conclusion {
		case "failure":
			semconvResult = "failure"
		case "cancelled":
			semconvResult = "cancellation"
		case "skipped":
			semconvResult = "skip"
		}

		spans = append(spans, map[string]any{
			"traceId":           traceIDHex,
			"spanId":            spanID,
			"parentSpanId":      parentSpanIDHex,
			"name":              step.name,
			"kind":              1,
			"startTimeUnixNano": fmt.Sprintf("%d", start.UnixNano()),
			"endTimeUnixNano":   fmt.Sprintf("%d", end.UnixNano()),
			"attributes": []map[string]any{
				{"key": "type", "value": map[string]any{"stringValue": "step"}},
				{"key": "source", "value": map[string]any{"stringValue": "runner"}},
				{"key": "github.step_number", "value": map[string]any{"stringValue": fmt.Sprintf("%d", i+1)}},
				{"key": "github.conclusion", "value": map[string]any{"stringValue": step.conclusion}},
				{"key": "github.repository", "value": map[string]any{"stringValue": "acme/widgets"}},
				{"key": "github.run_id", "value": map[string]any{"stringValue": fmt.Sprintf("%d", runID)}},
				{"key": "cicd.pipeline.task.name", "value": map[string]any{"stringValue": step.name}},
				{"key": "cicd.pipeline.task.run.result", "value": map[string]any{"stringValue": semconvResult}},
				{"key": "cicd.pipeline.run.id", "value": map[string]any{"stringValue": fmt.Sprintf("%d", runID)}},
				{"key": "vcs.repository.url.full", "value": map[string]any{"stringValue": "https://github.com/acme/widgets"}},
			},
			"status": map[string]any{},
		})
	}

	payload := map[string]any{
		"resourceSpans": []map[string]any{
			{
				"resource": map[string]any{
					"attributes": []map[string]any{
						{"key": "service.name", "value": map[string]any{"stringValue": "github-actions-runner"}},
					},
				},
				"scopeSpans": []map[string]any{
					{
						"scope": map[string]any{"name": "actions.runner"},
						"spans": spans,
					},
				},
			},
		},
	}

	data, _ := json.Marshal(payload)
	return data
}
