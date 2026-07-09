package otlpfile

// OTLP protobuf-JSON format (ExportTraceServiceRequest) as returned by
// Tempo, Jaeger v2, and other OTLP-compatible backends.
//
// Example:
//
//	{
//	  "resourceSpans": [{
//	    "scopeSpans": [{
//	      "spans": [{
//	        "traceId": "...", "spanId": "...", "parentSpanId": "...",
//	        "name": "...", "kind": 1,
//	        "startTimeUnixNano": "1234567890000000000",
//	        "endTimeUnixNano":   "1234567891000000000",
//	        "attributes": [{"key": "k", "value": {"stringValue": "v"}}],
//	        "status": {"code": 1}
//	      }]
//	    }]
//	  }]
//	}

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// exportRequest is the top-level ExportTraceServiceRequest.
type exportRequest struct {
	ResourceSpans []resourceSpanJSON `json:"resourceSpans"`
}

type resourceSpanJSON struct {
	Resource   *resourceJSON   `json:"resource,omitempty"`
	ScopeSpans []scopeSpanJSON `json:"scopeSpans"`
}

type scopeSpanJSON struct {
	Scope *scopeJSON      `json:"scope,omitempty"`
	Spans []protoSpanJSON `json:"spans"`
}

type resourceJSON struct {
	Attributes []protoAttrJSON `json:"attributes"`
}

type scopeJSON struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type protoSpanJSON struct {
	TraceID           string           `json:"traceId"`
	SpanID            string           `json:"spanId"`
	ParentSpanID      string           `json:"parentSpanId"`
	Name              string           `json:"name"`
	Kind              protoEnum        `json:"kind"`
	StartTimeUnixNano stringOrInt      `json:"startTimeUnixNano"`
	EndTimeUnixNano   stringOrInt      `json:"endTimeUnixNano"`
	Attributes        []protoAttrJSON  `json:"attributes"`
	Events            []protoEventJSON `json:"events"`
	Status            protoStatusJSON  `json:"status"`
}

// stringOrInt handles JSON values that can be either a string "123" or number 123.
type stringOrInt int64

func (s *stringOrInt) UnmarshalJSON(data []byte) error {
	// Try string first (quoted)
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		n, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid nano timestamp %q: %w", str, err)
		}
		*s = stringOrInt(n)
		return nil
	}
	// Try number
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("invalid nano timestamp: %s", string(data))
	}
	*s = stringOrInt(n)
	return nil
}

// protoEnum handles OTLP enum fields (span kind, status code) that may be encoded
// as a number (1) or as the canonical protojson enum name ("SPAN_KIND_SERVER").
// Tempo returns the string form; the OTel Collector file exporter returns numbers.
type protoEnum int

var protoEnumNames = map[string]int{
	"SPAN_KIND_UNSPECIFIED": 0, "SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2,
	"SPAN_KIND_CLIENT": 3, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5,
	"STATUS_CODE_UNSET": 0, "STATUS_CODE_OK": 1, "STATUS_CODE_ERROR": 2,
}

func (e *protoEnum) UnmarshalJSON(data []byte) error {
	var n int
	if err := json.Unmarshal(data, &n); err == nil {
		*e = protoEnum(n)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*e = protoEnum(protoEnumNames[s]) // unknown name -> 0
		return nil
	}
	return nil
}

type protoAttrJSON struct {
	Key   string         `json:"key"`
	Value protoValueJSON `json:"value"`
}

type protoValueJSON struct {
	StringValue *string  `json:"stringValue,omitempty"`
	IntValue    *intText `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
}

// intText accepts proto-JSON int64 in both its spec form ("42", a string)
// and the bare-number form (42) common in hand-rolled emitters — rejecting
// the number form hard-failed the entire document.
type intText string

func (i *intText) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*i = intText(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("invalid intValue: %s", string(data))
	}
	*i = intText(n.String())
	return nil
}

type protoEventJSON struct {
	Name         string          `json:"name"`
	TimeUnixNano stringOrInt     `json:"timeUnixNano"`
	Attributes   []protoAttrJSON `json:"attributes"`
}

type protoStatusJSON struct {
	Code    protoEnum `json:"code"`
	Message string    `json:"message"`
}

// ParseProto reads OTLP protobuf-JSON (ExportTraceServiceRequest) and returns ReadOnlySpans.
func ParseProto(r io.Reader) (spans []sdktrace.ReadOnlySpan, err error) {
	defer utils.RecoverBoundary0(&err)
	return parseProto(r)
}

func parseProto(r io.Reader) ([]sdktrace.ReadOnlySpan, error) {
	var req exportRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return nil, fmt.Errorf("decode OTLP JSON: %w", err)
	}

	var stubs tracetest.SpanStubs
	for _, rs := range req.ResourceSpans {
		var res *resource.Resource
		if rs.Resource != nil {
			res = resource.NewSchemaless(convertProtoAttrs(rs.Resource.Attributes)...)
		}
		for _, ss := range rs.ScopeSpans {
			var scope instrumentation.Scope
			if ss.Scope != nil {
				scope = instrumentation.Scope{
					Name:    ss.Scope.Name,
					Version: ss.Scope.Version,
				}
			}
			for _, span := range ss.Spans {
				stub, err := convertProtoSpan(span, res, scope)
				if err != nil {
					continue
				}
				stubs = append(stubs, stub)
			}
		}
	}

	return stubs.Snapshots(), nil
}

// normalizeID returns a hex ID. protojson (Tempo) encodes ID bytes as base64,
// while the OTel Collector file exporter and our hand-built JSON use hex.
func normalizeID(s string) string {
	if _, err := hex.DecodeString(s); err == nil {
		return s
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return hex.EncodeToString(b)
	}
	return s
}

func convertProtoSpan(raw protoSpanJSON, res *resource.Resource, scope instrumentation.Scope) (tracetest.SpanStub, error) {
	// Zero/empty IDs denote "no ID": keep the span with a zero SpanContext
	// (matching the stdout path) instead of silently dropping it.
	var sc trace.SpanContext
	if !isZeroOrEmptyID(raw.TraceID) || !isZeroOrEmptyID(raw.SpanID) {
		traceID, err := trace.TraceIDFromHex(normalizeID(raw.TraceID))
		if err != nil {
			return tracetest.SpanStub{}, fmt.Errorf("invalid trace ID %q: %w", raw.TraceID, err)
		}

		spanID, err := trace.SpanIDFromHex(normalizeID(raw.SpanID))
		if err != nil {
			return tracetest.SpanStub{}, fmt.Errorf("invalid span ID %q: %w", raw.SpanID, err)
		}

		sc = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})
	}

	var parent trace.SpanContext
	if raw.ParentSpanID != "" {
		parentSpanID, err := trace.SpanIDFromHex(normalizeID(raw.ParentSpanID))
		if err == nil {
			parent = trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    sc.TraceID(), // same trace
				SpanID:     parentSpanID,
				TraceFlags: trace.FlagsSampled,
			})
		}
	}

	attrs := convertProtoAttrs(raw.Attributes)

	startTime := time.Unix(0, int64(raw.StartTimeUnixNano))
	endTime := time.Unix(0, int64(raw.EndTimeUnixNano))

	var events []sdktrace.Event
	for _, e := range raw.Events {
		events = append(events, sdktrace.Event{
			Name:       e.Name,
			Time:       time.Unix(0, int64(e.TimeUnixNano)),
			Attributes: convertProtoAttrs(e.Attributes),
		})
	}

	status := protoStatusToSDK(raw.Status)

	return tracetest.SpanStub{
		Name:                 raw.Name,
		SpanContext:          sc,
		Parent:               parent,
		SpanKind:             trace.SpanKind(raw.Kind),
		StartTime:            startTime,
		EndTime:              endTime,
		Attributes:           attrs,
		Events:               events,
		Status:               status,
		Resource:             res,
		InstrumentationScope: scope,
	}, nil
}

func convertProtoAttrs(raw []protoAttrJSON) []attribute.KeyValue {
	var result []attribute.KeyValue
	for _, a := range raw {
		key := attribute.Key(a.Key)
		v := a.Value
		switch {
		case v.StringValue != nil:
			result = append(result, key.String(*v.StringValue))
		case v.IntValue != nil:
			if n, err := strconv.ParseInt(string(*v.IntValue), 10, 64); err == nil {
				result = append(result, key.Int64(n))
			}
		case v.DoubleValue != nil:
			result = append(result, key.Float64(*v.DoubleValue))
		case v.BoolValue != nil:
			result = append(result, key.Bool(*v.BoolValue))
		}
	}
	return result
}

func protoStatusToSDK(s protoStatusJSON) sdktrace.Status {
	// OTLP status codes: 0=Unset, 1=Ok, 2=Error
	switch s.Code {
	case 1:
		return StatusFromCode("OK", s.Message)
	case 2:
		return StatusFromCode("ERROR", s.Message)
	default:
		return StatusFromCode("", s.Message)
	}
}
