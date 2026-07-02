// Package otlpfile parses OTel span JSON files as produced by the
// stdouttrace exporter (--otel flag) into ReadOnlySpan slices.
//
// The format is newline-delimited JSON where each line is a span object:
//
//	{"Name":"...","SpanContext":{...},"Parent":{...},"StartTime":"...","EndTime":"...","Attributes":[...], ...}
package otlpfile

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// stdoutSpan is the JSON structure emitted by stdouttrace exporter.
type stdoutSpan struct {
	Name                 string                      `json:"Name"`
	SpanContext          spanContextJSON             `json:"SpanContext"`
	Parent               spanContextJSON             `json:"Parent"`
	SpanKind             int                         `json:"SpanKind"`
	StartTime            time.Time                   `json:"StartTime"`
	EndTime              time.Time                   `json:"EndTime"`
	Attributes           []attrJSON                  `json:"Attributes"`
	Events               []eventJSON                 `json:"Events"`
	Links                []linkJSON                  `json:"Links"`
	Status               statusJSON                  `json:"Status"`
	Resource             []attrJSON                  `json:"Resource"`
	InstrumentationScope *stdoutInstrumentationScope `json:"InstrumentationScope,omitempty"`
}

// stdoutInstrumentationScope mirrors the InstrumentationScope object
// emitted by the stdouttrace exporter.
type stdoutInstrumentationScope struct {
	Name      string `json:"Name"`
	Version   string `json:"Version"`
	SchemaURL string `json:"SchemaURL"`
}

type spanContextJSON struct {
	TraceID    string `json:"TraceID"`
	SpanID     string `json:"SpanID"`
	TraceFlags string `json:"TraceFlags"`
}

type attrJSON struct {
	Key   string    `json:"Key"`
	Value valueJSON `json:"Value"`
}

type valueJSON struct {
	Type  string      `json:"Type"`
	Value interface{} `json:"Value"`
}

type eventJSON struct {
	Name       string     `json:"Name"`
	Attributes []attrJSON `json:"Attributes"`
	Time       time.Time  `json:"Time"`
}

type linkJSON struct {
	SpanContext spanContextJSON `json:"SpanContext"`
	Attributes  []attrJSON      `json:"Attributes"`
}

type statusJSON struct {
	Code        string `json:"Code"`
	Description string `json:"Description"`
}

// ParseFile reads an OTel JSON file and returns ReadOnlySpans.
// Auto-detects format: OTLP protobuf-JSON (ExportTraceServiceRequest with
// "resourceSpans") or stdouttrace (newline-delimited/array of span objects).
func ParseFile(path string) ([]sdktrace.ReadOnlySpan, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open trace file: %w", err)
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads OTel span JSON from a reader and returns ReadOnlySpans.
// Auto-detects format: OTLP protobuf-JSON ("resourceSpans"), Chrome Tracing
// ("traceEvents"/"ph"), Zipkin v2 JSON ("localEndpoint"),
// flat JSON ("ParentSpanID" with map-style attributes),
// or stdouttrace (newline-delimited/array).
func Parse(r io.Reader) ([]sdktrace.ReadOnlySpan, error) {
	// Read all content so we can inspect it for format detection.
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading trace data: %w", err)
	}

	// Transparently decompress gzip or zstd content.
	data, err = maybeDecompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompressing trace data: %w", err)
	}

	// Detect binary protobuf (length-prefixed) before JSON-based detection.
	if looksLikeProtobuf(data) {
		return ParseProtobuf(bytes.NewReader(data))
	}

	// Detect OTLP protobuf-JSON format by looking for "resourceSpans" key.
	// Handles both single-object and JSONL (newline-delimited) formats.
	if bytes.Contains(data, []byte(`"resourceSpans"`)) {
		return parseProtoJSONL(data)
	}

	// Detect Jaeger API format by looking for top-level "data" with "traceID".
	if bytes.Contains(data, []byte(`"traceID"`)) && bytes.Contains(data, []byte(`"operationName"`)) {
		return ParseJaeger(bytes.NewReader(data))
	}

	// Detect Chrome Tracing format by looking for "traceEvents" key or
	// "ph" field (event phase indicator unique to Chrome Tracing).
	if bytes.Contains(data, []byte(`"traceEvents"`)) || looksLikeChromeTrace(data) {
		return ParseChrome(bytes.NewReader(data))
	}

	// Detect Zipkin v2 JSON format by looking for "localEndpoint" key.
	if bytes.Contains(data, []byte(`"localEndpoint"`)) {
		return ParseZipkin(bytes.NewReader(data))
	}

	// Detect flat JSON format: has "ParentSpanID" (stdouttrace uses "Parent").
	if bytes.Contains(data, []byte(`"ParentSpanID"`)) {
		return parseFlatJSON(data)
	}

	return parseStdout(bytes.NewReader(data))
}

// parseProtoJSONL handles OTLP protobuf-JSON data that may be a single object
// (compact or pretty-printed) or a stream of concatenated/newline-delimited
// (JSONL) documents, each a TracesData object with "resourceSpans". It uses a
// json.Decoder so indented documents spanning multiple lines parse correctly,
// decodes each document with ParseProto, and concatenates all spans.
func parseProtoJSONL(data []byte) ([]sdktrace.ReadOnlySpan, error) {
	var allSpans []sdktrace.ReadOnlySpan

	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("parsing OTLP JSON document: %w", err)
		}

		spans, err := ParseProto(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("parsing OTLP JSON document: %w", err)
		}
		allSpans = append(allSpans, spans...)
	}

	return allSpans, nil
}

// parseStdout reads OTel stdouttrace JSON (newline-delimited or array).
func parseStdout(r io.Reader) ([]sdktrace.ReadOnlySpan, error) {
	var stubs tracetest.SpanStubs

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		trimmed := trimSpace(line)
		if len(trimmed) == 0 || trimmed[0] == '[' || trimmed[0] == ']' {
			continue
		}

		// Remove trailing comma for array format
		if trimmed[len(trimmed)-1] == ',' {
			trimmed = trimmed[:len(trimmed)-1]
		}

		var raw stdoutSpan
		if err := json.Unmarshal(trimmed, &raw); err != nil {
			continue
		}

		stub, err := convertToStub(raw)
		if err != nil {
			continue
		}
		stubs = append(stubs, stub)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading trace file: %w", err)
	}

	return stubs.Snapshots(), nil
}

func convertToStub(raw stdoutSpan) (tracetest.SpanStub, error) {
	sc, err := parseSpanContext(raw.SpanContext)
	if err != nil {
		return tracetest.SpanStub{}, err
	}

	parent, _ := parseSpanContext(raw.Parent)

	attrs := convertAttrs(raw.Attributes)
	status := StatusFromCode(raw.Status.Code, raw.Status.Description)

	var events []sdktrace.Event
	for _, e := range raw.Events {
		events = append(events, sdktrace.Event{
			Name:       e.Name,
			Attributes: convertAttrs(e.Attributes),
			Time:       e.Time,
		})
	}

	var links []sdktrace.Link
	for _, l := range raw.Links {
		lsc, _ := parseSpanContext(l.SpanContext)
		links = append(links, sdktrace.Link{
			SpanContext: lsc,
			Attributes:  convertAttrs(l.Attributes),
		})
	}

	// Build Resource from stdouttrace "Resource" field.
	var res *resource.Resource
	if len(raw.Resource) > 0 {
		res = resource.NewSchemaless(convertAttrs(raw.Resource)...)
	}

	// Build InstrumentationScope from stdouttrace "InstrumentationScope" field.
	var scope instrumentation.Scope
	if raw.InstrumentationScope != nil {
		scope = instrumentation.Scope{
			Name:      raw.InstrumentationScope.Name,
			Version:   raw.InstrumentationScope.Version,
			SchemaURL: raw.InstrumentationScope.SchemaURL,
		}
	}

	return tracetest.SpanStub{
		Name:                 raw.Name,
		SpanContext:          sc,
		Parent:               parent,
		SpanKind:             trace.SpanKind(raw.SpanKind),
		StartTime:            raw.StartTime,
		EndTime:              raw.EndTime,
		Attributes:           attrs,
		Events:               events,
		Links:                links,
		Status:               status,
		Resource:             res,
		InstrumentationScope: scope,
	}, nil
}

// isZeroOrEmptyID reports whether an ID string is absent — empty or all-zero
// hex. The stdouttrace exporter writes a zero TraceID/SpanID as "00…00" while
// other inputs omit it (""); both denote "no ID" and must be handled the same
// so parse→emit→re-parse is idempotent.
func isZeroOrEmptyID(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

func parseSpanContext(sc spanContextJSON) (trace.SpanContext, error) {
	if isZeroOrEmptyID(sc.TraceID) && isZeroOrEmptyID(sc.SpanID) {
		return trace.SpanContext{}, nil
	}

	traceID, err := trace.TraceIDFromHex(sc.TraceID)
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("invalid trace ID %q: %w", sc.TraceID, err)
	}

	spanID, err := trace.SpanIDFromHex(sc.SpanID)
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("invalid span ID %q: %w", sc.SpanID, err)
	}

	cfg := trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}

	return trace.NewSpanContext(cfg), nil
}

func convertAttrs(raw []attrJSON) []attribute.KeyValue {
	var result []attribute.KeyValue
	for _, a := range raw {
		result = append(result, convertAttr(a))
	}
	return result
}

func convertAttr(a attrJSON) attribute.KeyValue {
	key := attribute.Key(a.Key)
	switch a.Value.Type {
	case "STRING":
		if s, ok := a.Value.Value.(string); ok {
			return key.String(s)
		}
	case "INT64":
		switch v := a.Value.Value.(type) {
		case float64:
			return key.Int64(int64(v))
		case json.Number:
			if i, err := v.Int64(); err == nil {
				return key.Int64(i)
			}
		}
	case "FLOAT64":
		if f, ok := a.Value.Value.(float64); ok {
			return key.Float64(f)
		}
	case "BOOL":
		if b, ok := a.Value.Value.(bool); ok {
			return key.Bool(b)
		}
	}
	return key.String(fmt.Sprintf("%v", a.Value.Value))
}

// looksLikeChromeTrace checks for bare-array Chrome Tracing format: a JSON
// array whose first element carries a top-level "ph" key (event phase). A
// structural check, not a byte scan — "ph" appearing inside nested maps
// (e.g. Zipkin tags or span attributes) must not claim the file for Chrome.
func looksLikeChromeTrace(data []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') {
		return false
	}
	var first map[string]json.RawMessage
	if err := dec.Decode(&first); err != nil {
		return false
	}
	_, ok := first["ph"]
	return ok
}

// maxDecompressedBytes caps decompressed trace data to guard against
// compression bombs expanding unbounded in memory.
const maxDecompressedBytes = 1 << 30 // 1GB

// readAllLimited reads from r, erroring if more than maxDecompressedBytes
// would be read.
func readAllLimited(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxDecompressedBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxDecompressedBytes {
		return nil, fmt.Errorf("decompressed data exceeds %d byte limit", maxDecompressedBytes)
	}
	return data, nil
}

// maybeDecompress detects gzip or zstd compression by checking magic bytes
// and decompresses the data. Returns the original data unchanged if no
// compression is detected.
func maybeDecompress(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return data, nil
	}

	// gzip: magic bytes 0x1f 0x8b
	if data[0] == 0x1f && data[1] == 0x8b {
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gr.Close()
		return readAllLimited(gr)
	}

	// zstd: magic bytes 0x28 0xb5 0x2f 0xfd
	if len(data) >= 4 && data[0] == 0x28 && data[1] == 0xb5 && data[2] == 0x2f && data[3] == 0xfd {
		dec, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("zstd reader: %w", err)
		}
		defer dec.Close()
		return readAllLimited(dec)
	}

	return data, nil
}

func trimSpace(b []byte) []byte {
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\t' || b[start] == '\r') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}

// flatSpan is the JSON structure used by some Go OTel exporters that serialize
// attributes as a plain map and use "ParentSpanID" instead of a "Parent" object.
type flatSpan struct {
	Name         string                 `json:"Name"`
	SpanContext  spanContextJSON        `json:"SpanContext"`
	ParentSpanID string                 `json:"ParentSpanID"`
	SpanKind     int                    `json:"SpanKind"`
	StartTime    time.Time              `json:"StartTime"`
	EndTime      time.Time              `json:"EndTime"`
	Attributes   map[string]interface{} `json:"Attributes"`
	Events       []flatEventJSON        `json:"Events"`
	Status       statusJSON             `json:"Status"`
	Resource     map[string]interface{} `json:"Resource"`
}

type flatEventJSON struct {
	Name       string                 `json:"Name"`
	Attributes map[string]interface{} `json:"Attributes"`
	Time       time.Time              `json:"Time"`
}

// parseFlatJSON parses spans with flat attribute maps and "ParentSpanID".
// Supports a single object, newline-delimited objects, or a JSON array.
func parseFlatJSON(data []byte) ([]sdktrace.ReadOnlySpan, error) {
	var stubs tracetest.SpanStubs

	// Try single object first.
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var raw flatSpan
		if err := json.Unmarshal(trimmed, &raw); err == nil {
			if stub, err := convertFlatToStub(raw); err == nil {
				stubs = append(stubs, stub)
				return stubs.Snapshots(), nil
			}
		}
	}

	// Try JSON array.
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var raws []flatSpan
		if err := json.Unmarshal(trimmed, &raws); err == nil {
			for _, raw := range raws {
				if stub, err := convertFlatToStub(raw); err == nil {
					stubs = append(stubs, stub)
				}
			}
			return stubs.Snapshots(), nil
		}
	}

	// Fall back to newline-delimited.
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	for scanner.Scan() {
		line := trimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if line[len(line)-1] == ',' {
			line = line[:len(line)-1]
		}
		var raw flatSpan
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}
		if stub, err := convertFlatToStub(raw); err == nil {
			stubs = append(stubs, stub)
		}
	}
	return stubs.Snapshots(), nil
}

func convertFlatToStub(raw flatSpan) (tracetest.SpanStub, error) {
	sc, err := parseSpanContext(raw.SpanContext)
	if err != nil {
		return tracetest.SpanStub{}, err
	}

	// Build parent span context from ParentSpanID + same TraceID.
	var parent trace.SpanContext
	if raw.ParentSpanID != "" && raw.ParentSpanID != "0000000000000000" {
		parentSpanID, err := trace.SpanIDFromHex(raw.ParentSpanID)
		if err == nil {
			parent = trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    sc.TraceID(),
				SpanID:     parentSpanID,
				TraceFlags: trace.FlagsSampled,
			})
		}
	}

	attrs := convertFlatAttrs(raw.Attributes)

	// Add resource attributes with "resource." prefix for backward compatibility.
	var res *resource.Resource
	if len(raw.Resource) > 0 {
		var resAttrs []attribute.KeyValue
		for k, v := range raw.Resource {
			attrs = append(attrs, flatAttr("resource."+k, v))
			resAttrs = append(resAttrs, flatAttr(k, v))
		}
		res = resource.NewSchemaless(resAttrs...)
	}

	status := StatusFromCode(raw.Status.Code, raw.Status.Description)

	var events []sdktrace.Event
	for _, e := range raw.Events {
		events = append(events, sdktrace.Event{
			Name:       e.Name,
			Attributes: convertFlatAttrs(e.Attributes),
			Time:       e.Time,
		})
	}

	return tracetest.SpanStub{
		Name:        raw.Name,
		SpanContext: sc,
		Parent:      parent,
		SpanKind:    trace.SpanKind(raw.SpanKind),
		StartTime:   raw.StartTime,
		EndTime:     raw.EndTime,
		Attributes:  attrs,
		Events:      events,
		Status:      status,
		Resource:    res,
	}, nil
}

func convertFlatAttrs(m map[string]interface{}) []attribute.KeyValue {
	var result []attribute.KeyValue
	for k, v := range m {
		result = append(result, flatAttr(k, v))
	}
	return result
}

// flatAttr converts a key and an untyped JSON value to an attribute.KeyValue,
// inferring the type from the Go type that encoding/json produces.
func flatAttr(k string, v interface{}) attribute.KeyValue {
	key := attribute.Key(k)
	switch val := v.(type) {
	case string:
		return key.String(val)
	case float64:
		if val >= math.MinInt64 && val <= math.MaxInt64 && val == float64(int64(val)) {
			return key.Int64(int64(val))
		}
		return key.Float64(val)
	case bool:
		return key.Bool(val)
	default:
		return key.String(fmt.Sprintf("%v", v))
	}
}
