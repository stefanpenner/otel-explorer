package enrichment

import (
	"fmt"
	"strconv"
	"strings"
)

// GenAIEnricher recognizes OTel GenAI (LLM) semantic conventions (gen_ai.*).
// It matches spans emitted by LLM instrumentation — the Anthropic/OpenAI SDKs,
// OpenLLMetry, OpenInference, LangChain, LlamaIndex, etc. — and surfaces the
// model, operation, and token usage so an LLM call reads at a glance instead of
// as an anonymous blue dot.
type GenAIEnricher struct{}

// Enrich produces SpanHints for any span carrying gen_ai.* attributes.
// Returns empty hints (Category=="") if no gen_ai signal is present.
func (e *GenAIEnricher) Enrich(name string, attrs map[string]string, isZeroDuration bool) SpanHints {
	system := attrs["gen_ai.system"]
	reqModel := attrs["gen_ai.request.model"]
	respModel := attrs["gen_ai.response.model"]
	operation := attrs["gen_ai.operation.name"]

	// Require at least one gen_ai signal to claim the span.
	if system == "" && reqModel == "" && respModel == "" && operation == "" {
		return SpanHints{}
	}

	h := SpanHints{
		Category: "genai",
		Icon:     genAIIcon(operation),
		BarChar:  "█",
		Color:    "blue",
	}

	// Resource-level context, mirroring GenericEnricher.
	if svc := attrs["service.name"]; svc != "" {
		h.ServiceName = svc
	}
	if env := attrs["deployment.environment"]; env != "" {
		h.Environment = env
	}

	// Detail: "operation subject · 1.2k→340 tok [error.type]".
	// The subject is the most specific identifier available: the response model
	// (what was actually served) over the requested model, then the tool name
	// (for execute_tool spans), the agent name (for agent spans), and finally
	// the provider so the span still names its source.
	subject := respModel
	if subject == "" {
		subject = reqModel
	}
	if subject == "" {
		subject = attrs["gen_ai.tool.name"]
	}
	if subject == "" {
		subject = attrs["gen_ai.agent.name"]
	}
	if subject == "" {
		subject = system
	}

	var head []string
	if operation != "" {
		head = append(head, operation)
	}
	if subject != "" {
		head = append(head, subject)
	}

	var parts []string
	if len(head) > 0 {
		parts = append(parts, strings.Join(head, " "))
	}
	if tok := genAITokens(attrs); tok != "" {
		parts = append(parts, tok)
	}

	// Status: gen_ai errors surface via otel.status_code plus an error.type.
	switch attrs["otel.status_code"] {
	case "OK":
		h.Outcome = "success"
		h.Color = "green"
	case "ERROR":
		h.Outcome = "failure"
		h.Color = "red"
	}
	if errType := attrs["error.type"]; errType != "" {
		h.Outcome = "failure"
		h.Color = "red"
		parts = append(parts, errType)
	}

	h.Detail = strings.Join(parts, " · ")
	return h
}

// genAIIcon picks an icon based on the GenAI operation (per the
// gen_ai.operation.name well-known values) so the common shapes read
// differently in a timeline.
func genAIIcon(operation string) string {
	switch operation {
	case "embeddings":
		return "🔢 "
	case "execute_tool":
		return "🔧 "
	case "create_agent", "invoke_agent", "invoke_workflow":
		return "🧠 "
	case "retrieval":
		return "🔎 "
	default:
		// chat, text_completion, generate_content, and any unknown operation.
		return "🤖 "
	}
}

// genAITokens formats input/output token usage compactly, e.g. "1.2k→340 tok".
// Returns "" when no usage attributes are present.
func genAITokens(attrs map[string]string) string {
	in, hasIn := parseTokenCount(attrs["gen_ai.usage.input_tokens"])
	out, hasOut := parseTokenCount(attrs["gen_ai.usage.output_tokens"])
	switch {
	case hasIn && hasOut:
		return fmt.Sprintf("%s→%s tok", compactCount(in), compactCount(out))
	case hasIn:
		return fmt.Sprintf("%s tok in", compactCount(in))
	case hasOut:
		return fmt.Sprintf("%s tok out", compactCount(out))
	default:
		return ""
	}
}

// parseTokenCount parses a token-count attribute, returning ok=false for
// missing or unparseable values.
func parseTokenCount(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

// compactCount renders a count compactly: 340 -> "340", 1200 -> "1.2k",
// 1500000 -> "1.5M".
func compactCount(n int) string {
	switch {
	case n >= 1_000_000:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", float64(n)/1_000_000), ".0") + "M"
	case n >= 1000:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", float64(n)/1000), ".0") + "k"
	default:
		return strconv.Itoa(n)
	}
}
