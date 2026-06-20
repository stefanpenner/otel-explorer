package enrichment

import (
	"strings"
	"testing"
)

func TestGenAIEnricher_Chat(t *testing.T) {
	e := &GenAIEnricher{}

	attrs := map[string]string{
		"gen_ai.system":              "anthropic",
		"gen_ai.operation.name":      "chat",
		"gen_ai.request.model":       "claude-opus-4",
		"gen_ai.usage.input_tokens":  "1200",
		"gen_ai.usage.output_tokens": "340",
	}
	h := e.Enrich("chat claude-opus-4", attrs, false)

	if h.Category != "genai" {
		t.Errorf("expected category 'genai', got %q", h.Category)
	}
	if h.Icon != "🤖 " {
		t.Errorf("expected robot icon, got %q", h.Icon)
	}
	if !strings.Contains(h.Detail, "claude-opus-4") {
		t.Errorf("expected Detail to mention model, got %q", h.Detail)
	}
	if !strings.Contains(h.Detail, "chat") {
		t.Errorf("expected Detail to mention operation, got %q", h.Detail)
	}
	// Token usage should be surfaced, compactly (1200 -> 1.2k).
	if !strings.Contains(h.Detail, "1.2k") {
		t.Errorf("expected Detail to surface input tokens compactly, got %q", h.Detail)
	}
	if !strings.Contains(h.Detail, "340") {
		t.Errorf("expected Detail to surface output tokens, got %q", h.Detail)
	}
}

func TestGenAIEnricher_ResponseModelPreferred(t *testing.T) {
	e := &GenAIEnricher{}

	// When both request and response models are present, the response model
	// (the one actually served) is the more accurate label.
	attrs := map[string]string{
		"gen_ai.system":         "openai",
		"gen_ai.request.model":  "gpt-4o",
		"gen_ai.response.model": "gpt-4o-2024-08-06",
	}
	h := e.Enrich("chat", attrs, false)

	if !strings.Contains(h.Detail, "gpt-4o-2024-08-06") {
		t.Errorf("expected Detail to use response model, got %q", h.Detail)
	}
}

func TestGenAIEnricher_Embeddings(t *testing.T) {
	e := &GenAIEnricher{}

	attrs := map[string]string{
		"gen_ai.system":         "openai",
		"gen_ai.operation.name": "embeddings",
		"gen_ai.request.model":  "text-embedding-3-small",
	}
	h := e.Enrich("embeddings", attrs, false)

	if h.Category != "genai" {
		t.Errorf("expected category 'genai', got %q", h.Category)
	}
	// Embeddings get a distinct icon so they read differently from chat calls.
	if h.Icon == "🤖 " {
		t.Errorf("expected embeddings to use a distinct icon, got %q", h.Icon)
	}
}

func TestGenAIEnricher_ExecuteTool(t *testing.T) {
	e := &GenAIEnricher{}

	// A tool-execution span has no model; the tool name is the subject.
	attrs := map[string]string{
		"gen_ai.system":         "anthropic",
		"gen_ai.operation.name": "execute_tool",
		"gen_ai.tool.name":      "web_search",
	}
	h := e.Enrich("execute_tool web_search", attrs, false)

	if h.Category != "genai" {
		t.Errorf("expected category 'genai', got %q", h.Category)
	}
	if h.Icon != "🔧 " {
		t.Errorf("expected wrench icon for execute_tool, got %q", h.Icon)
	}
	if !strings.Contains(h.Detail, "web_search") {
		t.Errorf("expected Detail to name the tool, got %q", h.Detail)
	}
}

func TestGenAIEnricher_InvokeAgent(t *testing.T) {
	e := &GenAIEnricher{}

	attrs := map[string]string{
		"gen_ai.system":         "anthropic",
		"gen_ai.operation.name": "invoke_agent",
		"gen_ai.agent.name":     "research-agent",
	}
	h := e.Enrich("invoke_agent research-agent", attrs, false)

	if h.Icon != "🧠 " {
		t.Errorf("expected brain icon for invoke_agent, got %q", h.Icon)
	}
	if !strings.Contains(h.Detail, "research-agent") {
		t.Errorf("expected Detail to name the agent, got %q", h.Detail)
	}
}

func TestGenAIEnricher_Error(t *testing.T) {
	e := &GenAIEnricher{}

	attrs := map[string]string{
		"gen_ai.system":         "anthropic",
		"gen_ai.request.model":  "claude-opus-4",
		"gen_ai.operation.name": "chat",
		"otel.status_code":      "ERROR",
		"error.type":            "overloaded_error",
	}
	h := e.Enrich("chat", attrs, false)

	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure', got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected color 'red', got %q", h.Color)
	}
	if !strings.Contains(h.Detail, "overloaded_error") {
		t.Errorf("expected Detail to surface error.type, got %q", h.Detail)
	}
}

func TestGenAIEnricher_ServiceContext(t *testing.T) {
	e := &GenAIEnricher{}

	attrs := map[string]string{
		"gen_ai.system":          "anthropic",
		"gen_ai.request.model":   "claude-opus-4",
		"service.name":           "rag-api",
		"deployment.environment": "production",
	}
	h := e.Enrich("chat", attrs, false)

	if h.ServiceName != "rag-api" {
		t.Errorf("expected ServiceName 'rag-api', got %q", h.ServiceName)
	}
	if h.Environment != "production" {
		t.Errorf("expected Environment 'production', got %q", h.Environment)
	}
}

func TestGenAIUsage_Aggregates(t *testing.T) {
	u := NewGenAIUsage()
	// Two chat calls + one embeddings call.
	u.Add(map[string]string{"gen_ai.request.model": "claude-opus-4", "gen_ai.usage.input_tokens": "1800", "gen_ai.usage.output_tokens": "210"})
	u.Add(map[string]string{"gen_ai.response.model": "claude-opus-4", "gen_ai.usage.input_tokens": "12500", "gen_ai.usage.output_tokens": "1450"})
	u.Add(map[string]string{"gen_ai.request.model": "text-embedding-3-small", "gen_ai.usage.input_tokens": "80"})
	// A wrapper span with no model/tokens must NOT count.
	u.Add(map[string]string{"gen_ai.operation.name": "invoke_agent", "gen_ai.agent.name": "research-agent"})
	// A non-genai span must not count.
	u.Add(map[string]string{"http.request.method": "GET"})

	if u.Calls != 3 {
		t.Errorf("expected 3 calls, got %d", u.Calls)
	}
	if u.InputTokens != 14380 {
		t.Errorf("expected 14380 input tokens, got %d", u.InputTokens)
	}
	if u.OutputTokens != 1660 {
		t.Errorf("expected 1660 output tokens, got %d", u.OutputTokens)
	}
	if got := u.Summary(); got != "3 calls · 14.4k → 1.7k tokens" {
		t.Errorf("unexpected summary: %q", got)
	}
	lines := u.ModelLines()
	if len(lines) != 2 || lines[0] != "claude-opus-4 ×2" || lines[1] != "text-embedding-3-small ×1" {
		t.Errorf("unexpected model lines: %v", lines)
	}
}

func TestGenAIUsage_Empty(t *testing.T) {
	u := NewGenAIUsage()
	u.Add(map[string]string{"db.system": "postgresql"})
	if u.HasData() {
		t.Error("expected no data for non-genai spans")
	}
}

func TestGenAIEnricher_NoMatch(t *testing.T) {
	e := &GenAIEnricher{}

	// A plain HTTP span must not be claimed by the GenAI enricher.
	h := e.Enrich("GET /api", map[string]string{"http.request.method": "GET"}, false)
	if h.Category != "" {
		t.Errorf("expected empty category for non-genai span, got %q", h.Category)
	}
}

func TestGenAIEnricher_TokensOnly(t *testing.T) {
	e := &GenAIEnricher{}

	// Some instrumentations emit token usage without an explicit model.
	attrs := map[string]string{
		"gen_ai.system":              "cohere",
		"gen_ai.usage.input_tokens":  "50",
		"gen_ai.usage.output_tokens": "0",
	}
	h := e.Enrich("rerank", attrs, false)

	if h.Category != "genai" {
		t.Errorf("expected category 'genai', got %q", h.Category)
	}
	// Small token counts render as plain integers (no spurious "k").
	if !strings.Contains(h.Detail, "50") {
		t.Errorf("expected Detail to surface small token count, got %q", h.Detail)
	}
}
