package enrichment

import (
	"fmt"
	"strconv"
)

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

	// Zero-duration spans are markers
	if isZeroDuration {
		h.IsMarker = true
		h.Category = "marker"
		h.SortPriority = -1
		h.Icon = "▲ "
		h.BarChar = "▲"
	}

	// OTel status code → Outcome
	statusCode := attrs["otel.status_code"]
	switch statusCode {
	case "OK":
		h.Outcome = "success"
		h.Color = "green"
	case "ERROR":
		h.Outcome = "failure"
		h.Color = "red"
	}

	// Artifact trace spans get grouped under their parent workflow
	if artifactName, ok := attrs["github.artifact_name"]; ok && artifactName != "" {
		h.GroupKey = "artifact"
	}

	// Extract resource-level context
	if svc := attrs["service.name"]; svc != "" {
		h.ServiceName = svc
	}
	if env := attrs["deployment.environment"]; env != "" {
		h.Environment = env
	}

	// Recognize OTel semantic conventions for common span types.
	// These set category/icon and extract Detail for richer display.
	if !h.IsMarker {
		switch {
		case attrs["graphql.operation.type"] != "" || attrs["graphql.operation.name"] != "":
			// GraphQL server spans usually also carry HTTP attributes
			// (POST /graphql); the GraphQL operation is the meaningful detail,
			// so this case precedes the HTTP one.
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

		case attrs["http.request.method"] != "" || attrs["http.method"] != "":
			h.Category = "http"
			h.Icon = "⇄ "
			// Extract meaningful detail: "METHOD route" or "METHOD url_path"
			method := attrs["http.request.method"]
			if method == "" {
				method = attrs["http.method"]
			}
			route := attrs["http.route"]
			if route == "" {
				route = attrs["url.path"]
			}
			if method != "" && route != "" {
				h.Detail = method + " " + route
			} else if method != "" {
				h.Detail = method
			}
			// Add server context
			if server := attrs["server.address"]; server != "" {
				if port := attrs["server.port"]; port != "" {
					h.Detail += fmt.Sprintf(" → %s:%s", server, port)
				}
			}
			// HTTP error status codes
			statusStr := attrs["http.response.status_code"]
			if statusStr == "" {
				statusStr = attrs["http.status_code"]
			}
			if code, err := strconv.Atoi(statusStr); err == nil {
				if code >= 400 {
					h.Outcome = "failure"
					h.Color = "red"
				}
				if h.Detail != "" {
					h.Detail += fmt.Sprintf(" [%d]", code)
				}
			}

		case attrs["db.system"] != "":
			h.Category = "database"
			h.Icon = "⛁ "
			// Extract DB detail: "system: statement"
			dbSystem := attrs["db.system"]
			h.Detail = dbSystem
			if stmt := attrs["db.statement"]; stmt != "" {
				const maxLen = 80
				if len(stmt) > maxLen {
					stmt = stmt[:maxLen-3] + "..."
				}
				h.Detail = dbSystem + ": " + stmt
			} else if op := attrs["db.operation"]; op != "" {
				h.Detail = dbSystem + ": " + op
				if table := attrs["db.sql.table"]; table != "" {
					h.Detail += " " + table
				}
			}

		case attrs["rpc.system"] != "":
			h.Category = "rpc"
			h.Icon = "⇌ "
			// Extract RPC detail: "system service/method"
			rpcSystem := attrs["rpc.system"]
			h.Detail = rpcSystem
			if svc := attrs["rpc.service"]; svc != "" {
				if method := attrs["rpc.method"]; method != "" {
					h.Detail = rpcSystem + " " + svc + "/" + method
				} else {
					h.Detail = rpcSystem + " " + svc
				}
			}

		case attrs["messaging.system"] != "":
			h.Category = "messaging"
			h.Icon = "✉ "
			// Extract messaging detail: "system destination operation"
			msgSystem := attrs["messaging.system"]
			h.Detail = msgSystem
			if dest := attrs["messaging.destination.name"]; dest != "" {
				h.Detail += " " + dest
			}
			if op := attrs["messaging.operation"]; op != "" {
				h.Detail += " (" + op + ")"
			}

		case attrs["faas.trigger"] != "":
			h.Category = "faas"
			h.Icon = "λ "
			h.Detail = attrs["faas.trigger"]
			if fname := attrs["faas.name"]; fname != "" {
				h.Detail = fname + " (" + h.Detail + ")"
			}
		}
	}

	// Use span kind for icon variation (only if not already set by semconv)
	if h.Icon == "● " {
		spanKind := attrs["otel.span_kind"]
		switch spanKind {
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

	return h
}
