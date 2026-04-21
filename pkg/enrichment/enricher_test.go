package enrichment

import "testing"

func TestGHAEnricher_Workflow(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":              "workflow",
		"github.conclusion": "success",
		"github.url":        "https://github.com/owner/repo/actions/runs/123",
	}
	h := e.Enrich("CI", attrs, false)

	if h.Category != "workflow" {
		t.Errorf("expected category 'workflow', got %q", h.Category)
	}
	if !h.IsRoot {
		t.Error("expected IsRoot=true for workflow")
	}
	if h.Outcome != "success" {
		t.Errorf("expected outcome 'success', got %q", h.Outcome)
	}
	if h.Color != "green" {
		t.Errorf("expected color 'green', got %q", h.Color)
	}
	if h.Icon != "📋" {
		t.Errorf("expected icon '📋', got %q", h.Icon)
	}
	if h.BarChar != "█" {
		t.Errorf("expected bar char '█', got %q", h.BarChar)
	}
	if h.URL != "https://github.com/owner/repo/actions/runs/123" {
		t.Errorf("unexpected URL %q", h.URL)
	}
}

func TestGHAEnricher_Job(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":              "job",
		"github.conclusion": "failure",
		"github.is_required": "true",
	}
	h := e.Enrich("build", attrs, false)

	if h.Category != "job" {
		t.Errorf("expected category 'job', got %q", h.Category)
	}
	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure', got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected color 'red', got %q", h.Color)
	}
	if !h.IsRequired {
		t.Error("expected IsRequired=true")
	}
}

func TestGHAEnricher_Step(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":              "step",
		"github.conclusion": "skipped",
	}
	h := e.Enrich("Run tests", attrs, false)

	if h.Category != "step" {
		t.Errorf("expected category 'step', got %q", h.Category)
	}
	if !h.IsLeaf {
		t.Error("expected IsLeaf=true for step")
	}
	if h.Outcome != "skipped" {
		t.Errorf("expected outcome 'skipped', got %q", h.Outcome)
	}
	if h.BarChar != "▒" {
		t.Errorf("expected bar char '▒', got %q", h.BarChar)
	}
}

func TestGHAEnricher_MarkerMerged(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":               "marker",
		"github.event_type":  "merged",
		"github.event_id":    "evt-1",
		"github.event_time":  "2024-01-01T00:00:00Z",
		"github.user":        "alice",
	}
	h := e.Enrich("Merged", attrs, true)

	if h.Category != "marker" {
		t.Errorf("expected category 'marker', got %q", h.Category)
	}
	if !h.IsMarker {
		t.Error("expected IsMarker=true")
	}
	if h.Icon != "◆ " {
		t.Errorf("expected icon '◆ ', got %q", h.Icon)
	}
	if h.BarChar != "◆" {
		t.Errorf("expected bar char '◆', got %q", h.BarChar)
	}
	if h.GroupKey != "activity" {
		t.Errorf("expected group key 'activity', got %q", h.GroupKey)
	}
	if h.DedupKey == "" {
		t.Error("expected non-empty dedup key")
	}
	if h.SortPriority != -1 {
		t.Errorf("expected sort priority -1, got %d", h.SortPriority)
	}
	if h.User != "alice" {
		t.Errorf("expected user 'alice', got %q", h.User)
	}
}

func TestGHAEnricher_PendingStatus(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":          "job",
		"github.status": "in_progress",
	}
	h := e.Enrich("build", attrs, false)

	if h.Outcome != "pending" {
		t.Errorf("expected outcome 'pending', got %q", h.Outcome)
	}
	if h.Color != "blue" {
		t.Errorf("expected color 'blue', got %q", h.Color)
	}
}

func TestGHAEnricher_NonGHASpan(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"http.method": "GET",
	}
	h := e.Enrich("GET /api/v1/users", attrs, false)

	if h.Category != "" {
		t.Errorf("expected empty category for non-GHA span, got %q", h.Category)
	}
}

func TestGHAEnricher_RunnerQueue(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":               "runner.queue",
		"github.runner_name": "runner-abc",
	}
	h := e.Enrich("runner.queue", attrs, false)

	if h.Category != "runner.queue" {
		t.Errorf("expected category 'runner.queue', got %q", h.Category)
	}
	if !h.IsLeaf {
		t.Error("expected IsLeaf=true for runner.queue")
	}
	if h.Color != "yellow" {
		t.Errorf("expected color 'yellow', got %q", h.Color)
	}
	if h.Outcome != "pending" {
		t.Errorf("expected outcome 'pending', got %q", h.Outcome)
	}
}

func TestGHAEnricher_RunnerStartup(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type": "runner.startup",
	}
	h := e.Enrich("runner.startup", attrs, false)

	if h.Category != "runner.startup" {
		t.Errorf("expected category 'runner.startup', got %q", h.Category)
	}
	if h.Color != "blue" {
		t.Errorf("expected color 'blue', got %q", h.Color)
	}
}

func TestGHAEnricher_RunnerExecution(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":              "runner.execution",
		"github.conclusion": "success",
	}
	h := e.Enrich("runner.execution", attrs, false)

	if h.Category != "runner.execution" {
		t.Errorf("expected category 'runner.execution', got %q", h.Category)
	}
	if !h.IsLeaf {
		t.Error("expected IsLeaf=true for runner.execution")
	}
	if h.Outcome != "success" {
		t.Errorf("expected outcome 'success', got %q", h.Outcome)
	}
	if h.Color != "green" {
		t.Errorf("expected color 'green', got %q", h.Color)
	}
}

func TestGHAEnricher_RunnerExecutionFailure(t *testing.T) {
	e := &GHAEnricher{}
	attrs := map[string]string{
		"type":              "runner.execution",
		"github.conclusion": "failure",
	}
	h := e.Enrich("runner.execution", attrs, false)

	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure', got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected color 'red', got %q", h.Color)
	}
}

func TestGenericEnricher_NormalSpan(t *testing.T) {
	e := &GenericEnricher{}
	attrs := map[string]string{
		"otel.status_code": "OK",
	}
	h := e.Enrich("GET /users", attrs, false)

	if h.Category != "operation" {
		t.Errorf("expected category 'operation', got %q", h.Category)
	}
	if h.Outcome != "success" {
		t.Errorf("expected outcome 'success', got %q", h.Outcome)
	}
	if h.Color != "green" {
		t.Errorf("expected color 'green', got %q", h.Color)
	}
}

func TestGenericEnricher_ErrorSpan(t *testing.T) {
	e := &GenericEnricher{}
	attrs := map[string]string{
		"otel.status_code": "ERROR",
	}
	h := e.Enrich("POST /users", attrs, false)

	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure', got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected color 'red', got %q", h.Color)
	}
}

func TestGenericEnricher_ZeroDuration(t *testing.T) {
	e := &GenericEnricher{}
	h := e.Enrich("event", map[string]string{}, true)

	if !h.IsMarker {
		t.Error("expected IsMarker=true for zero-duration span")
	}
	if h.Category != "marker" {
		t.Errorf("expected category 'marker', got %q", h.Category)
	}
	if h.SortPriority != -1 {
		t.Errorf("expected sort priority -1, got %d", h.SortPriority)
	}
}

func TestGenericEnricher_SpanKind(t *testing.T) {
	e := &GenericEnricher{}
	attrs := map[string]string{
		"otel.span_kind": "SERVER",
	}
	h := e.Enrich("handle request", attrs, false)

	if h.Icon != "⇣ " {
		t.Errorf("expected icon '⇣ ', got %q", h.Icon)
	}
}

func TestChainEnricher_GHAFirst(t *testing.T) {
	chain := DefaultEnricher()
	attrs := map[string]string{
		"type":              "workflow",
		"github.conclusion": "success",
	}
	h := chain.Enrich("CI", attrs, false)

	if h.Category != "workflow" {
		t.Errorf("expected GHA enricher to match first, got category %q", h.Category)
	}
}

func TestChainEnricher_FallbackToGeneric(t *testing.T) {
	chain := DefaultEnricher()
	attrs := map[string]string{
		"http.method": "GET",
	}
	h := chain.Enrich("GET /api", attrs, false)

	if h.Category != "http" {
		t.Errorf("expected generic enricher to recognize http spans, got category %q", h.Category)
	}
}

func TestGenericEnricher_ArtifactGrouping(t *testing.T) {
	e := &GenericEnricher{}
	attrs := map[string]string{
		"github.artifact_name": "build-output",
	}
	h := e.Enrich("upload artifact", attrs, false)

	if h.GroupKey != "artifact" {
		t.Errorf("expected GroupKey 'artifact', got %q", h.GroupKey)
	}
}

func TestGenericEnricher_EmptyArtifactName(t *testing.T) {
	e := &GenericEnricher{}
	attrs := map[string]string{
		"github.artifact_name": "",
	}
	h := e.Enrich("upload artifact", attrs, false)

	if h.GroupKey != "" {
		t.Errorf("expected empty GroupKey for empty artifact name, got %q", h.GroupKey)
	}
}
