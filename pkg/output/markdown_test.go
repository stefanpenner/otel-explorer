package output

import (
	"bytes"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func mdSpan(name string, attrs []attribute.KeyValue, res *resource.Resource) sdktrace.ReadOnlySpan {
	return tracetest.SpanStub{
		Name:       name,
		Attributes: attrs,
		Resource:   res,
	}.Snapshot()
}

func TestRenderResourceMarkdown(t *testing.T) {
	res := resource.NewSchemaless(
		attribute.String("service.name", "api-gateway"),
		attribute.String("deployment.environment", "production"),
		attribute.String("cloud.provider", "aws"),
		attribute.String("cloud.region", "us-east-1"),
	)
	spans := []sdktrace.ReadOnlySpan{mdSpan("POST /x", nil, res)}

	var buf bytes.Buffer
	renderResourceMarkdown(&buf, spans)
	out := buf.String()

	if !strings.Contains(out, "## Resources") {
		t.Error("expected Resources heading")
	}
	if !strings.Contains(out, "api-gateway · production · aws/us-east-1") {
		t.Errorf("expected service context line, got:\n%s", out)
	}
}

func TestRenderResourceMarkdown_NoServiceNoOp(t *testing.T) {
	spans := []sdktrace.ReadOnlySpan{mdSpan("op", []attribute.KeyValue{attribute.String("db.system", "x")}, nil)}
	var buf bytes.Buffer
	renderResourceMarkdown(&buf, spans)
	if buf.Len() != 0 {
		t.Errorf("expected no output without service.name, got:\n%s", buf.String())
	}
}

func TestRenderGenAIUsageMarkdown(t *testing.T) {
	spans := []sdktrace.ReadOnlySpan{
		mdSpan("chat", []attribute.KeyValue{
			attribute.String("gen_ai.request.model", "claude-opus-4"),
			attribute.Int("gen_ai.usage.input_tokens", 1200),
			attribute.Int("gen_ai.usage.output_tokens", 340),
		}, nil),
	}
	var buf bytes.Buffer
	renderGenAIUsageMarkdown(&buf, spans)
	out := buf.String()

	if !strings.Contains(out, "## LLM Usage") {
		t.Error("expected LLM Usage heading")
	}
	if !strings.Contains(out, "1.2k → 340 tokens") {
		t.Errorf("expected token summary, got:\n%s", out)
	}
	if !strings.Contains(out, "claude-opus-4 ×1") {
		t.Errorf("expected model line, got:\n%s", out)
	}
}

func TestRenderGenAIUsageMarkdown_NoLLMNoOp(t *testing.T) {
	spans := []sdktrace.ReadOnlySpan{mdSpan("GET /x", []attribute.KeyValue{attribute.String("http.request.method", "GET")}, nil)}
	var buf bytes.Buffer
	renderGenAIUsageMarkdown(&buf, spans)
	if buf.Len() != 0 {
		t.Errorf("expected no output for non-LLM trace, got:\n%s", buf.String())
	}
}
