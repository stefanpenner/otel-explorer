package otlpfile

// Jaeger API response format parser.
// Jaeger's /api/traces/{traceID} returns a proprietary JSON format
// with top-level "data" array containing traces with "spans" and "processes".

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type jaegerResponse struct {
	Data []jaegerTrace `json:"data"`
}

type jaegerTrace struct {
	TraceID   string                    `json:"traceID"`
	Spans     []jaegerSpan              `json:"spans"`
	Processes map[string]jaegerProcess  `json:"processes"`
}

type jaegerSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	OperationName string            `json:"operationName"`
	References    []jaegerReference `json:"references"`
	StartTime     int64             `json:"startTime"` // microseconds
	Duration      int64             `json:"duration"`   // microseconds
	Tags          []jaegerTag       `json:"tags"`
	Logs          []jaegerLog       `json:"logs"`
	ProcessID     string            `json:"processID"`
}

type jaegerReference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

type jaegerProcess struct {
	ServiceName string      `json:"serviceName"`
	Tags        []jaegerTag `json:"tags"`
}

type jaegerTag struct {
	Key   string      `json:"key"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type jaegerLog struct {
	Timestamp int64       `json:"timestamp"` // microseconds
	Fields    []jaegerTag `json:"fields"`
}

// ParseJaeger reads Jaeger API JSON response and returns ReadOnlySpans.
func ParseJaeger(r io.Reader) ([]sdktrace.ReadOnlySpan, error) {
	var resp jaegerResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode Jaeger JSON: %w", err)
	}

	var stubs tracetest.SpanStubs
	for _, t := range resp.Data {
		for _, span := range t.Spans {
			var res *resource.Resource
			if proc, ok := t.Processes[span.ProcessID]; ok {
				attrs := []attribute.KeyValue{
					attribute.String("service.name", proc.ServiceName),
				}
				attrs = append(attrs, convertJaegerTags(proc.Tags)...)
				res = resource.NewSchemaless(attrs...)
			}
			stub, err := convertJaegerSpan(span, res)
			if err != nil {
				continue
			}
			stubs = append(stubs, stub)
		}
	}

	return stubs.Snapshots(), nil
}

func convertJaegerSpan(raw jaegerSpan, res *resource.Resource) (tracetest.SpanStub, error) {
	// Jaeger emits 16-hex-char trace IDs when the high 64 bits are zero
	// (common for Zipkin-originated or 64-bit-configured clients), and some
	// versions strip leading zeros entirely. Left-pad to the 32 chars that
	// trace.TraceIDFromHex requires.
	traceIDHex := raw.TraceID
	if len(traceIDHex) > 0 && len(traceIDHex) < 32 {
		traceIDHex = strings.Repeat("0", 32-len(traceIDHex)) + traceIDHex
	}
	traceID, err := trace.TraceIDFromHex(traceIDHex)
	if err != nil {
		return tracetest.SpanStub{}, fmt.Errorf("invalid trace ID %q: %w", raw.TraceID, err)
	}

	spanID, err := trace.SpanIDFromHex(raw.SpanID)
	if err != nil {
		return tracetest.SpanStub{}, fmt.Errorf("invalid span ID %q: %w", raw.SpanID, err)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	var parent trace.SpanContext
	for _, ref := range raw.References {
		if ref.RefType == "CHILD_OF" {
			parentSpanID, err := trace.SpanIDFromHex(ref.SpanID)
			if err == nil {
				parent = trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    traceID,
					SpanID:     parentSpanID,
					TraceFlags: trace.FlagsSampled,
				})
			}
			break
		}
	}

	attrs := convertJaegerTags(raw.Tags)

	// Jaeger uses microseconds
	startTime := time.Unix(0, raw.StartTime*1000)
	endTime := time.Unix(0, (raw.StartTime+raw.Duration)*1000)

	var events []sdktrace.Event
	for _, log := range raw.Logs {
		name := "log"
		eventAttrs := convertJaegerTags(log.Fields)
		for _, f := range log.Fields {
			if f.Key == "event" || f.Key == "message" {
				if s, ok := f.Value.(string); ok {
					name = s
				}
			}
		}
		events = append(events, sdktrace.Event{
			Name:       name,
			Time:       time.Unix(0, log.Timestamp*1000),
			Attributes: eventAttrs,
		})
	}

	// Extract status from tags (otel.status_code, otel.status_description)
	statusCode := ""
	statusMsg := ""
	for _, tag := range raw.Tags {
		if tag.Key == "otel.status_code" {
			if s, ok := tag.Value.(string); ok {
				statusCode = s
			}
		}
		if tag.Key == "otel.status_description" {
			if s, ok := tag.Value.(string); ok {
				statusMsg = s
			}
		}
	}

	return tracetest.SpanStub{
		Name:        raw.OperationName,
		SpanContext: sc,
		Parent:      parent,
		StartTime:   startTime,
		EndTime:     endTime,
		Attributes:  attrs,
		Events:      events,
		Status:      StatusFromCode(statusCode, statusMsg),
		Resource:    res,
	}, nil
}

func convertJaegerTags(tags []jaegerTag) []attribute.KeyValue {
	var result []attribute.KeyValue
	for _, t := range tags {
		key := attribute.Key(t.Key)
		switch t.Type {
		case "string":
			if s, ok := t.Value.(string); ok {
				result = append(result, key.String(s))
			}
		case "int64":
			switch v := t.Value.(type) {
			case float64:
				result = append(result, key.Int64(int64(v)))
			case string:
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					result = append(result, key.Int64(n))
				}
			}
		case "float64":
			if f, ok := t.Value.(float64); ok {
				result = append(result, key.Float64(f))
			}
		case "bool":
			if b, ok := t.Value.(bool); ok {
				result = append(result, key.Bool(b))
			}
		}
	}
	return result
}
