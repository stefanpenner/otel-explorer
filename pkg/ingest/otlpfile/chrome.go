package otlpfile

// Chrome Tracing format parser.
//
// Supports the Trace Event Format used by Chrome DevTools, Perfetto,
// Bazel --profile, and other profiling tools. Events are converted to
// OTel ReadOnlySpans with parent-child hierarchy derived from temporal
// nesting within threads.
//
// Supported event phases:
//   - "X" (complete): has ts + dur
//   - "B"/"E" (begin/end): an E closes the most recent unclosed B on its thread
//   - "i"/"I" (instant): zero-duration span
//   - "M" (metadata): thread_name, process_name
//
// Reference: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// chromeTrace is the top-level Chrome Tracing JSON structure.
type chromeTrace struct {
	TraceEvents []chromeEvent  `json:"traceEvents"`
	OtherData   map[string]any `json:"otherData,omitempty"`
}

// chromeEvent represents a single event in Chrome Tracing format.
type chromeEvent struct {
	Name string         `json:"name"`
	Ph   string         `json:"ph"`
	Ts   float64        `json:"ts"`  // microseconds
	Dur  float64        `json:"dur"` // microseconds (for "X" events)
	Pid  json.Number    `json:"pid"`
	Tid  json.Number    `json:"tid"`
	Cat  string         `json:"cat,omitempty"`
	Args map[string]any `json:"args,omitempty"`
	ID   string         `json:"id,omitempty"`
}

// minDurationMicros is the minimum event duration to include (1ms).
// Events shorter than this are filtered out to reduce noise from
// profilers like Bazel that emit hundreds of sub-millisecond module init events.
const minDurationMicros = 1000

// ParseChrome reads Chrome Tracing JSON and returns ReadOnlySpans.
// Accepts either {"traceEvents": [...]} or a bare [...] array.
func ParseChrome(r io.Reader) ([]sdktrace.ReadOnlySpan, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading chrome trace: %w", err)
	}

	var events []chromeEvent
	var otherData map[string]any

	// Try wrapped format first: {"traceEvents": [...]}
	var wrapped chromeTrace
	if err := json.Unmarshal(data, &wrapped); err == nil && len(wrapped.TraceEvents) > 0 {
		events = wrapped.TraceEvents
		otherData = wrapped.OtherData
	} else {
		// Try bare array: [...]
		if err := json.Unmarshal(data, &events); err != nil {
			return nil, fmt.Errorf("decode chrome trace JSON: %w", err)
		}
	}

	return chromeEventsToSpans(events, otherData)
}

// resolvedEvent is a chrome event resolved to a complete duration span.
type resolvedEvent struct {
	ev    chromeEvent
	index int
}

func chromeEventsToSpans(events []chromeEvent, otherData map[string]any) ([]sdktrace.ReadOnlySpan, error) {
	traceID := syntheticTraceID(events)

	// If profile_start_ts is present (e.g. Bazel profiles), event timestamps
	// are relative offsets in microseconds. Convert to absolute by adding the
	// start timestamp (which is in milliseconds since epoch).
	if startTsMs, ok := profileStartTsMs(otherData); ok {
		offsetMicros := startTsMs * 1000 // ms → µs
		for i := range events {
			events[i].Ts += offsetMicros
		}
	}

	// Sort by timestamp to ensure B events come before their E events.
	// Stable so a B is not reordered after its E when timestamps are equal.
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Ts < events[j].Ts
	})

	// Extract metadata: thread names and process names.
	threadNames := make(map[string]string)  // "pid:tid" → name
	processNames := make(map[string]string) // "pid" → name
	for _, ev := range events {
		if ev.Ph != "M" {
			continue
		}
		nameVal, _ := ev.Args["name"].(string)
		if nameVal == "" {
			continue
		}
		switch ev.Name {
		case "thread_name":
			threadNames[ev.Pid.String()+":"+ev.Tid.String()] = nameVal
		case "process_name":
			processNames[ev.Pid.String()] = nameVal
		}
	}

	// Resolve B/E pairs and collect complete events. Per the Trace Event
	// Format spec, an E event closes the most recent unclosed B event on the
	// same thread (its name is optional), so keep a stack of open B events
	// per (pid,tid) rather than matching by name.
	type beginKey struct {
		Pid string
		Tid string
	}
	begins := make(map[beginKey][]chromeEvent)

	var resolved []resolvedEvent
	spanIndex := 0

	for _, ev := range events {
		switch ev.Ph {
		case "X": // Complete event
			if ev.Dur >= minDurationMicros {
				resolved = append(resolved, resolvedEvent{ev: ev, index: spanIndex})
				spanIndex++
			}

		case "B": // Begin
			key := beginKey{ev.Pid.String(), ev.Tid.String()}
			begins[key] = append(begins[key], ev)

		case "E": // End
			key := beginKey{ev.Pid.String(), ev.Tid.String()}
			if stack := begins[key]; len(stack) > 0 {
				begin := stack[len(stack)-1]
				begins[key] = stack[:len(stack)-1]

				merged := begin
				merged.Dur = ev.Ts - begin.Ts
				if len(ev.Args) > 0 {
					if merged.Args == nil {
						merged.Args = ev.Args
					} else {
						maps.Copy(merged.Args, ev.Args)
					}
				}
				if merged.Dur >= minDurationMicros {
					resolved = append(resolved, resolvedEvent{ev: merged, index: spanIndex})
					spanIndex++
				}
			}

		case "i", "I": // Instant events — always include
			resolved = append(resolved, resolvedEvent{ev: ev, index: spanIndex})
			spanIndex++
		}
	}

	// Build parent-child hierarchy from temporal nesting within same thread.
	// For each thread, maintain a stack of open spans. A span is a child of
	// the innermost span on the stack that fully contains it.
	type threadKey struct {
		Pid string
		Tid string
	}

	// Group resolved events by thread, sorted by start time then descending duration.
	byThread := make(map[threadKey][]resolvedEvent)
	for _, r := range resolved {
		tk := threadKey{r.ev.Pid.String(), r.ev.Tid.String()}
		byThread[tk] = append(byThread[tk], r)
	}

	// parentIndex maps span index → parent span index (-1 for roots)
	parentIndex := make(map[int]int)

	for _, threadEvents := range byThread {
		// Sort: by start time, then by descending duration (wider spans first).
		sort.Slice(threadEvents, func(i, j int) bool {
			if threadEvents[i].ev.Ts != threadEvents[j].ev.Ts {
				return threadEvents[i].ev.Ts < threadEvents[j].ev.Ts
			}
			return threadEvents[i].ev.Dur > threadEvents[j].ev.Dur
		})

		// Stack-based nesting: each entry is a resolved event whose time range
		// is still "open" (current events might be children of it).
		type stackEntry struct {
			index int
			end   float64 // ts + dur
		}
		var stack []stackEntry

		for _, r := range threadEvents {
			evEnd := r.ev.Ts + r.ev.Dur

			// Pop stack entries that have ended before this event starts.
			for len(stack) > 0 && stack[len(stack)-1].end <= r.ev.Ts {
				stack = stack[:len(stack)-1]
			}

			if len(stack) > 0 {
				parentIndex[r.index] = stack[len(stack)-1].index
			} else {
				parentIndex[r.index] = -1
			}

			// Only push non-instant events onto the stack.
			if r.ev.Dur > 0 {
				stack = append(stack, stackEntry{index: r.index, end: evEnd})
			}
		}
	}

	// Convert to span stubs with parent references.
	spanIDs := make(map[int]trace.SpanID)
	for _, r := range resolved {
		spanIDs[r.index] = syntheticSpanID(traceID, r.index)
	}

	var stubs tracetest.SpanStubs
	for _, r := range resolved {
		stub := chromeEventToStub(r.ev, traceID, r.index, threadNames, processNames, otherData)

		// Set parent span context if this event has a parent.
		if pi, ok := parentIndex[r.index]; ok && pi >= 0 {
			if parentSID, ok := spanIDs[pi]; ok {
				stub.Parent = trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    traceID,
					SpanID:     parentSID,
					TraceFlags: trace.FlagsSampled,
				})
			}
		}

		stubs = append(stubs, stub)
	}

	return stubs.Snapshots(), nil
}

func chromeEventToStub(ev chromeEvent, traceID trace.TraceID, index int, threadNames, processNames map[string]string, otherData map[string]any) tracetest.SpanStub {
	spanID := syntheticSpanID(traceID, index)

	startMicros := ev.Ts
	durMicros := ev.Dur

	startTime := time.Unix(0, int64(startMicros*1000)) // micros → nanos
	endTime := time.Unix(0, int64((startMicros+durMicros)*1000))

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	pid := ev.Pid.String()
	tid := ev.Tid.String()

	var attrs []attribute.KeyValue
	if ev.Cat != "" {
		attrs = append(attrs, attribute.String("chrome.category", ev.Cat))
	}
	attrs = append(attrs, attribute.String("chrome.pid", pid))
	attrs = append(attrs, attribute.String("chrome.tid", tid))

	// Add thread/process name if available.
	if name, ok := threadNames[pid+":"+tid]; ok {
		attrs = append(attrs, attribute.String("chrome.thread_name", name))
	}
	if name, ok := processNames[pid]; ok {
		attrs = append(attrs, attribute.String("chrome.process_name", name))
	}

	// Add otherData metadata (e.g. bazel_version, build_id).
	for k, v := range otherData {
		attrs = append(attrs, attribute.String("chrome.metadata."+k, fmt.Sprintf("%v", v)))
	}

	for k, v := range ev.Args {
		key := attribute.Key("chrome.args." + k)
		switch val := v.(type) {
		case string:
			attrs = append(attrs, key.String(val))
		case float64:
			attrs = append(attrs, key.Float64(val))
		case bool:
			attrs = append(attrs, key.Bool(val))
		default:
			attrs = append(attrs, key.String(fmt.Sprintf("%v", val)))
		}
	}

	return tracetest.SpanStub{
		Name:        ev.Name,
		SpanContext: sc,
		StartTime:   startTime,
		EndTime:     endTime,
		Attributes:  attrs,
	}
}

// profileStartTsMs extracts profile_start_ts from otherData as milliseconds.
// Bazel profiles store this as an integer (ms since epoch).
func profileStartTsMs(otherData map[string]any) (float64, bool) {
	if otherData == nil {
		return 0, false
	}
	v, ok := otherData["profile_start_ts"]
	if !ok {
		return 0, false
	}
	if n, ok := v.(json.Number); ok {
		if f, err := n.Float64(); err == nil {
			return f, true
		}
	}
	if n, ok := v.(float64); ok {
		return n, true
	}
	return 0, false
}

// syntheticTraceID generates a deterministic trace ID by hashing event data.
func syntheticTraceID(events []chromeEvent) trace.TraceID {
	h := sha256.New()
	for _, ev := range events {
		fmt.Fprintf(h, "%s:%s:%.0f", ev.Name, ev.Ph, ev.Ts)
	}
	var tid trace.TraceID
	copy(tid[:], h.Sum(nil)[:16])
	return tid
}

// syntheticSpanID generates a deterministic span ID from trace ID and index.
func syntheticSpanID(traceID trace.TraceID, index int) trace.SpanID {
	h := sha256.New()
	h.Write(traceID[:])
	binary.Write(h, binary.BigEndian, int64(index))
	var sid trace.SpanID
	copy(sid[:], h.Sum(nil)[:8])
	return sid
}
