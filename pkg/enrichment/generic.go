package enrichment

import (
	"fmt"
	"strconv"
	"strings"
)

// grpcStatusNames maps canonical gRPC status codes to their names.
var grpcStatusNames = map[int]string{
	1: "CANCELLED", 2: "UNKNOWN", 3: "INVALID_ARGUMENT", 4: "DEADLINE_EXCEEDED",
	5: "NOT_FOUND", 6: "ALREADY_EXISTS", 7: "PERMISSION_DENIED", 8: "RESOURCE_EXHAUSTED",
	9: "FAILED_PRECONDITION", 10: "ABORTED", 11: "OUT_OF_RANGE", 12: "UNIMPLEMENTED",
	13: "INTERNAL", 14: "UNAVAILABLE", 15: "DATA_LOSS", 16: "UNAUTHENTICATED",
}

// grpcStatusName returns the canonical gRPC status name for a non-zero code,
// falling back to "code N" for unknown codes.
func grpcStatusName(code int) string {
	if name, ok := grpcStatusNames[code]; ok {
		return name
	}
	return fmt.Sprintf("code %d", code)
}

// GenericEnricher handles any OTel span that wasn't recognized by a more
// specific enricher. It recognizes OTel semantic conventions for HTTP, database,
// RPC, and messaging spans, and provides sensible defaults for everything else.
type GenericEnricher struct{}

// Enrich produces SpanHints for any span using OTel-standard attributes.
func (e *GenericEnricher) Enrich(name string, attrs map[string]string, isZeroDuration bool) SpanHints {
	h := SpanHints{
		Category: "operation",
		Icon:     "● ",
		BarChar:  "█",
		Color:    "blue",
	}
	if isZeroDuration {
		markZeroDuration(&h)
	}

	applyOTelStatus(&h, attrs)
	groupArtifact(&h, attrs)
	applyResource(&h, attrs)

	if !h.IsMarker {
		// GraphQL spans usually also carry HTTP; the operation is the detail.
		switch {
		case attrs["graphql.operation.type"] != "" || attrs["graphql.operation.name"] != "":
			applyGraphQL(&h, attrs)
		case attrs["http.request.method"] != "" || attrs["http.method"] != "":
			applyHTTP(&h, attrs)
		case attrs["db.system.name"] != "" || attrs["db.system"] != "":
			applyDB(&h, attrs)
		case attrs["rpc.system"] != "":
			applyRPC(&h, attrs)
		case attrs["messaging.system"] != "":
			applyMessaging(&h, attrs)
		case attrs["faas.trigger"] != "":
			applyFaaS(&h, attrs)
		}

		applyCodeOrigin(&h, attrs)
		applyPeer(&h, attrs)
	}

	applySpanKind(&h, attrs)
	return h
}

func markZeroDuration(h *SpanHints) {
	h.IsMarker = true
	h.Category = "marker"
	h.SortPriority = -1
	h.Icon = "▲ "
	h.BarChar = "▲"
}

func applyOTelStatus(h *SpanHints, attrs map[string]string) {
	switch attrs["otel.status_code"] {
	case "OK":
		h.Outcome = "success"
		h.Color = "green"
	case "ERROR":
		h.Outcome = "failure"
		h.Color = "red"
	}
}

func groupArtifact(h *SpanHints, attrs map[string]string) {
	if attrs["github.artifact_name"] != "" {
		h.GroupKey = "artifact"
	}
}

func applyResource(h *SpanHints, attrs map[string]string) {
	if svc := attrs["service.name"]; svc != "" {
		h.ServiceName = svc
	}
	if env := firstNonEmpty(attrs, "deployment.environment.name", "deployment.environment"); env != "" {
		h.Environment = env
	}
}

func applyGraphQL(h *SpanHints, attrs map[string]string) {
	h.Category = "graphql"
	h.Icon = "◆ "
	opType := attrs["graphql.operation.type"]
	opName := attrs["graphql.operation.name"]
	switch {
	case opType != "" && opName != "":
		h.Detail = opType + " " + opName
	case opName != "":
		h.Detail = opName
	default:
		h.Detail = opType
	}
}

func applyHTTP(h *SpanHints, attrs map[string]string) {
	h.Category = "http"
	h.Icon = "⇄ "
	method := firstNonEmpty(attrs, "http.request.method", "http.method")
	// Low-cardinality route first; clients often have only url.full / http.url.
	route := firstNonEmpty(attrs, "http.route", "url.path", "url.full", "http.url")
	if method != "" && route != "" {
		h.Detail = method + " " + route
	} else if method != "" {
		h.Detail = method
	}
	if server := attrs["server.address"]; server != "" {
		if port := attrs["server.port"]; port != "" {
			h.Detail += fmt.Sprintf(" → %s:%s", server, port)
		}
	}
	if code, err := strconv.Atoi(firstNonEmpty(attrs, "http.response.status_code", "http.status_code")); err == nil {
		if code >= 400 {
			h.Outcome = "failure"
			h.Color = "red"
		}
		if h.Detail != "" {
			h.Detail += fmt.Sprintf(" [%d]", code)
		}
	}
}

func applyDB(h *SpanHints, attrs map[string]string) {
	h.Category = "database"
	h.Icon = "⛁ "
	// Stable v1.30 names, and the older db.system / db.statement / db.operation / db.sql.table.
	dbSystem := firstNonEmpty(attrs, "db.system.name", "db.system")
	h.Detail = dbSystem
	query := firstNonEmpty(attrs, "db.query.text", "db.statement")
	op := firstNonEmpty(attrs, "db.operation.name", "db.operation")
	collection := firstNonEmpty(attrs, "db.collection.name", "db.sql.table")
	switch {
	case query != "":
		const maxLen = 80
		if runes := []rune(query); len(runes) > maxLen {
			query = string(runes[:maxLen-3]) + "..."
		}
		h.Detail = dbSystem + ": " + query
	case op != "":
		h.Detail = dbSystem + ": " + op
		if collection != "" {
			h.Detail += " " + collection
		}
	case collection != "":
		h.Detail = dbSystem + ": " + collection
	}
}

func applyRPC(h *SpanHints, attrs map[string]string) {
	h.Category = "rpc"
	h.Icon = "⇌ "
	rpcSystem := attrs["rpc.system"]
	h.Detail = rpcSystem
	if svc := attrs["rpc.service"]; svc != "" {
		if method := attrs["rpc.method"]; method != "" {
			h.Detail = rpcSystem + " " + svc + "/" + method
		} else {
			h.Detail = rpcSystem + " " + svc
		}
	}
	// Non-zero rpc.grpc.status_code fails even when otel.status_code is unset.
	code, err := strconv.Atoi(attrs["rpc.grpc.status_code"])
	if err != nil {
		return
	}
	if code == 0 {
		if h.Outcome == "" {
			h.Outcome = "success"
			h.Color = "green"
		}
		return
	}
	h.Outcome = "failure"
	h.Color = "red"
	h.Detail += " [" + grpcStatusName(code) + "]"
}

func applyMessaging(h *SpanHints, attrs map[string]string) {
	h.Category = "messaging"
	h.Icon = "✉ "
	h.Detail = attrs["messaging.system"]
	if dest := attrs["messaging.destination.name"]; dest != "" {
		h.Detail += " " + dest
	}
	if op := firstNonEmpty(attrs, "messaging.operation.name", "messaging.operation.type", "messaging.operation"); op != "" {
		h.Detail += " (" + op + ")"
	}
}

func applyFaaS(h *SpanHints, attrs map[string]string) {
	h.Category = "faas"
	h.Icon = "λ "
	h.Detail = attrs["faas.trigger"]
	if fname := attrs["faas.name"]; fname != "" {
		h.Detail = fname + " (" + h.Detail + ")"
	}
}

func applyCodeOrigin(h *SpanHints, attrs map[string]string) {
	if h.Detail != "" {
		return
	}
	fn := firstNonEmpty(attrs, "code.function.name", "code.function", "code.namespace")
	if fn == "" {
		return
	}
	h.Detail = fn
	file := firstNonEmpty(attrs, "code.file.path", "code.filepath")
	if file == "" {
		return
	}
	base := file
	if idx := strings.LastIndexByte(base, '/'); idx >= 0 {
		base = base[idx+1:]
	}
	if line := firstNonEmpty(attrs, "code.line.number", "code.lineno"); line != "" {
		h.Detail += fmt.Sprintf(" (%s:%s)", base, line)
		return
	}
	h.Detail += fmt.Sprintf(" (%s)", base)
}

func applyPeer(h *SpanHints, attrs map[string]string) {
	peer := firstNonEmpty(attrs, "service.peer.name", "peer.service")
	if peer == "" || strings.Contains(h.Detail, peer) || strings.Contains(h.Detail, "→") {
		return
	}
	if h.Detail == "" {
		h.Detail = "→ " + peer
		return
	}
	h.Detail += " → " + peer
}

func applySpanKind(h *SpanHints, attrs map[string]string) {
	if h.Icon != "● " {
		return
	}
	switch attrs["otel.span_kind"] {
	case "SERVER":
		h.Icon = "⇣ "
	case "CLIENT":
		h.Icon = "⇢ "
	case "PRODUCER":
		h.Icon = "⇡ "
	case "CONSUMER":
		h.Icon = "⇠ "
	}
}
