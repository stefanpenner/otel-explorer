package otlpfile

// ParseProtobuf reads length-prefixed binary protobuf messages as written by
// the OTel Collector's fileexporter (format: proto). Each message is a 4-byte
// big-endian uint32 length prefix followed by that many bytes of protobuf-encoded
// ExportTraceServiceRequest (TracesData).

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// ParseRawProtobuf parses a raw (non-length-prefixed) protobuf message as
// sent by OTLP/HTTP exporters. The wire format of ExportTraceServiceRequest
// and TracesData is identical for the resource_spans field.
func ParseRawProtobuf(data []byte) ([]sdktrace.ReadOnlySpan, error) {
	var td v1.TracesData
	if err := proto.Unmarshal(data, &td); err != nil {
		return nil, fmt.Errorf("unmarshaling raw protobuf: %w", err)
	}

	var stubs tracetest.SpanStubs
	for _, rs := range td.ResourceSpans {
		var res *resource.Resource
		if rs.Resource != nil {
			res = resource.NewSchemaless(convertProtobufAttrs(rs.Resource.Attributes)...)
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
				stubs = append(stubs, convertProtobufSpan(span, res, scope))
			}
		}
	}
	return stubs.Snapshots(), nil
}

// ParseProtobuf reads length-prefixed binary protobuf messages from a reader
// and returns ReadOnlySpans. Each message is a 4-byte big-endian uint32 length
// prefix followed by a protobuf-encoded TracesData message.
func ParseProtobuf(r io.Reader) ([]sdktrace.ReadOnlySpan, error) {
	var stubs tracetest.SpanStubs

	for {
		// Read the 4-byte length prefix.
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading protobuf length prefix: %w", err)
		}

		// Sanity check: reject absurdly large messages (>100MB).
		if length > 100*1024*1024 {
			return nil, fmt.Errorf("protobuf message length %d exceeds 100MB limit", length)
		}

		// Read the protobuf message bytes.
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("reading protobuf message: %w", err)
		}

		// Unmarshal as TracesData.
		var td v1.TracesData
		if err := proto.Unmarshal(buf, &td); err != nil {
			return nil, fmt.Errorf("unmarshaling protobuf TracesData: %w", err)
		}

		// Convert each span to a SpanStub.
		for _, rs := range td.ResourceSpans {
			var res *resource.Resource
			if rs.Resource != nil {
				res = resource.NewSchemaless(convertProtobufAttrs(rs.Resource.Attributes)...)
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
					stub := convertProtobufSpan(span, res, scope)
					stubs = append(stubs, stub)
				}
			}
		}
	}

	return stubs.Snapshots(), nil
}

// convertProtobufSpan converts a protobuf Span to a tracetest.SpanStub.
func convertProtobufSpan(span *v1.Span, res *resource.Resource, scope instrumentation.Scope) tracetest.SpanStub {
	var traceID trace.TraceID
	var spanID trace.SpanID
	var parentSpanID trace.SpanID

	if len(span.TraceId) == 16 {
		copy(traceID[:], span.TraceId)
	}
	if len(span.SpanId) == 8 {
		copy(spanID[:], span.SpanId)
	}
	if len(span.ParentSpanId) == 8 {
		copy(parentSpanID[:], span.ParentSpanId)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	var parent trace.SpanContext
	if parentSpanID.IsValid() {
		parent = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     parentSpanID,
			TraceFlags: trace.FlagsSampled,
		})
	}

	attrs := convertProtobufAttrs(span.Attributes)
	startTime := time.Unix(0, int64(span.StartTimeUnixNano))
	endTime := time.Unix(0, int64(span.EndTimeUnixNano))

	var events []sdktrace.Event
	for _, e := range span.Events {
		events = append(events, sdktrace.Event{
			Name:       e.Name,
			Time:       time.Unix(0, int64(e.TimeUnixNano)),
			Attributes: convertProtobufAttrs(e.Attributes),
		})
	}

	status := protobufStatusToSDK(span.Status)

	// Map protobuf SpanKind to OTel SpanKind.
	// Protobuf: 0=UNSPECIFIED, 1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER
	// OTel SDK: 0=Unspecified, 1=Internal, 2=Server, 3=Client, 4=Producer, 5=Consumer
	spanKind := trace.SpanKind(span.Kind)

	return tracetest.SpanStub{
		Name:                 span.Name,
		SpanContext:          sc,
		Parent:               parent,
		SpanKind:             spanKind,
		StartTime:            startTime,
		EndTime:              endTime,
		Attributes:           attrs,
		Events:               events,
		Status:               status,
		Resource:             res,
		InstrumentationScope: scope,
	}
}

// convertProtobufAttrs converts protobuf KeyValue attributes to OTel attribute.KeyValue.
func convertProtobufAttrs(kvs []*v1common.KeyValue) []attribute.KeyValue {
	var result []attribute.KeyValue
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		key := attribute.Key(kv.Key)
		switch v := kv.Value.Value.(type) {
		case *v1common.AnyValue_StringValue:
			result = append(result, key.String(v.StringValue))
		case *v1common.AnyValue_IntValue:
			result = append(result, key.Int64(v.IntValue))
		case *v1common.AnyValue_DoubleValue:
			result = append(result, key.Float64(v.DoubleValue))
		case *v1common.AnyValue_BoolValue:
			result = append(result, key.Bool(v.BoolValue))
		default:
			result = append(result, key.String(fmt.Sprintf("%v", kv.Value)))
		}
	}
	return result
}

// protobufStatusToSDK converts a protobuf Status to an SDK Status.
func protobufStatusToSDK(s *v1.Status) sdktrace.Status {
	if s == nil {
		return StatusFromCode("", "")
	}
	// OTLP status codes: 0=Unset, 1=Ok, 2=Error
	switch s.Code {
	case v1.Status_STATUS_CODE_OK:
		return StatusFromCode("OK", s.Message)
	case v1.Status_STATUS_CODE_ERROR:
		return StatusFromCode("ERROR", s.Message)
	default:
		return StatusFromCode("", s.Message)
	}
}

// looksLikeProtobuf checks if data looks like length-prefixed binary protobuf.
// The first 4 bytes should be a reasonable length prefix (big-endian uint32),
// and the byte at offset 4 should look like a protobuf field tag.
func looksLikeProtobuf(data []byte) bool {
	if len(data) < 5 {
		return false
	}

	// Read the length prefix.
	length := binary.BigEndian.Uint32(data[:4])

	// Length must be positive and not exceed remaining data.
	if length == 0 || length > uint32(len(data)-4) {
		return false
	}

	// Check if byte at offset 4 looks like a protobuf field tag.
	// Protobuf field tags encode (field_number << 3 | wire_type).
	// Wire types: 0=varint, 1=64bit, 2=length-delimited, 5=32bit.
	// TracesData has field 1 (resource_spans) with wire type 2 (length-delimited),
	// so we expect tag byte = (1 << 3) | 2 = 0x0A.
	tag := data[4]
	wireType := tag & 0x07
	fieldNumber := tag >> 3

	// Accept reasonable field numbers (1-15 fit in one byte) with valid wire types.
	return fieldNumber >= 1 && fieldNumber <= 15 && (wireType == 0 || wireType == 1 || wireType == 2 || wireType == 5)
}
