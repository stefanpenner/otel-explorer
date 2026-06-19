package enrichment

import (
	"testing"
)

func TestCICDEnricher_Pipeline(t *testing.T) {
	e := &CICDEnricher{}

	// Jenkins-style pipeline span
	attrs := map[string]string{
		"cicd.pipeline.name":            "my-build",
		"cicd.pipeline.task.run.result": "success",
		"vcs.repository.url.full":       "https://github.com/owner/repo",
	}
	h := e.Enrich("my-build", attrs, false)

	if h.Category != "pipeline" {
		t.Errorf("expected category 'pipeline', got %q", h.Category)
	}
	if !h.IsRoot {
		t.Error("expected IsRoot=true for pipeline span")
	}
	if h.Outcome != "success" {
		t.Errorf("expected outcome 'success', got %q", h.Outcome)
	}
	if h.URL != "https://github.com/owner/repo" {
		t.Errorf("expected URL from vcs.repository.url.full, got %q", h.URL)
	}
}

func TestCICDEnricher_Task(t *testing.T) {
	e := &CICDEnricher{}

	// GitLab CI task span
	attrs := map[string]string{
		"cicd.pipeline.task.name":       "unit-tests",
		"cicd.pipeline.task.type":       "test",
		"cicd.pipeline.task.run.result": "failure",
	}
	h := e.Enrich("unit-tests", attrs, false)

	if h.Category != "task" {
		t.Errorf("expected category 'task', got %q", h.Category)
	}
	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure', got %q", h.Outcome)
	}
	if h.Icon != "🧪 " {
		t.Errorf("expected test icon, got %q", h.Icon)
	}
}

func TestCICDEnricher_TaskTypes(t *testing.T) {
	e := &CICDEnricher{}

	tests := []struct {
		taskType string
		icon     string
	}{
		{"build", "🔨 "},
		{"test", "🧪 "},
		{"deploy", "🚀 "},
		{"other", "⚙ "},
	}

	for _, tt := range tests {
		t.Run(tt.taskType, func(t *testing.T) {
			attrs := map[string]string{
				"cicd.pipeline.task.name": "task1",
				"cicd.pipeline.task.type": tt.taskType,
			}
			h := e.Enrich("task1", attrs, false)
			if h.Icon != tt.icon {
				t.Errorf("taskType %q: expected icon %q, got %q", tt.taskType, tt.icon, h.Icon)
			}
		})
	}
}

func TestCICDEnricher_NoMatch(t *testing.T) {
	e := &CICDEnricher{}

	// Span with no CI/CD attributes
	attrs := map[string]string{
		"http.method": "GET",
	}
	h := e.Enrich("GET /api", attrs, false)

	if h.Category != "" {
		t.Errorf("expected empty category for non-CICD span, got %q", h.Category)
	}
}

func TestCICDEnricher_CancelledResult(t *testing.T) {
	e := &CICDEnricher{}

	attrs := map[string]string{
		"cicd.pipeline.task.name":       "deploy",
		"cicd.pipeline.task.run.result": "cancelled",
	}
	h := e.Enrich("deploy", attrs, false)

	if h.Outcome != "skipped" {
		t.Errorf("expected outcome 'skipped' for cancelled result, got %q", h.Outcome)
	}
}

func TestCICDEnricher_OTelStatusFallback(t *testing.T) {
	e := &CICDEnricher{}

	attrs := map[string]string{
		"cicd.pipeline.name": "pipeline",
		"otel.status_code":   "ERROR",
	}
	h := e.Enrich("pipeline", attrs, false)

	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure' from OTel status fallback, got %q", h.Outcome)
	}
}

func TestChainEnricher_CICDBeforeGeneric(t *testing.T) {
	chain := NewChainEnricher(&CICDEnricher{}, &GenericEnricher{})

	// CICD span should be caught by CICDEnricher, not GenericEnricher
	attrs := map[string]string{
		"cicd.pipeline.name": "my-pipeline",
	}
	h := chain.Enrich("my-pipeline", attrs, false)

	if h.Category != "pipeline" {
		t.Errorf("expected CICDEnricher to match first, got category %q", h.Category)
	}
}

func TestCICDEnricher_VcsChangeAndBase(t *testing.T) {
	e := &CICDEnricher{}
	h := e.Enrich("build", map[string]string{
		"cicd.pipeline.task.name": "build",
		"vcs.ref.head.revision":   "abc123",
		"vcs.ref.base.name":       "main",
		"vcs.change.id":           "42",
	}, false)
	if h.VCSRevision != "abc123" {
		t.Errorf("VCSRevision: got %q", h.VCSRevision)
	}
	if h.VCSBaseRef != "main" {
		t.Errorf("VCSBaseRef: got %q", h.VCSBaseRef)
	}
	if h.VCSChangeID != "42" {
		t.Errorf("VCSChangeID: got %q", h.VCSChangeID)
	}
}
