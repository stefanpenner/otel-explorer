package output

import (
	"bytes"
	"github.com/charmbracelet/lipgloss"
	"strings"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestRenderOTelTimelineDeduplication(t *testing.T) {
	now := time.Now()

	// Helper to create a read-only span
	createSpan := func(name string, eventType string, eventID string, url string, startTime time.Time) sdktrace.ReadOnlySpan {
		return &mockReadOnlySpan{
			name:      name,
			startTime: startTime,
			endTime:   startTime.Add(time.Second),
			spanID:    trace.SpanID{1, 2, 3, 4, 5, 6, 7, byte(len(eventID))}, // Unique-ish spanID
			attrs: []attribute.KeyValue{
				attribute.String("type", "marker"),
				attribute.String("github.event_type", eventType),
				attribute.String("github.event_id", eventID),
				attribute.String("github.url", url),
				attribute.String("github.event_time", startTime.Format(time.RFC3339)),
			},
		}
	}

	t.Run("Deduplicates identical events with same eventID and time", func(t *testing.T) {
		eventTime := now.Truncate(time.Second)
		eventID := "review-123"

		span1 := createSpan("Review: APPROVED", "approved", eventID, "https://github.com/1", eventTime)
		span2 := createSpan("Review: APPROVED", "approved", eventID, "https://github.com/1", eventTime)

		var buf bytes.Buffer
		RenderOTelTimeline(&buf, []sdktrace.ReadOnlySpan{span1, span2}, time.Time{}, time.Time{}, enrichment.DefaultEnricher())

		output := buf.String()
		// Should only contain one "Review: APPROVED"
		assert.Equal(t, 1, countOccurrences(output, "Review: APPROVED"))
	})

	t.Run("Preserves distinct events with same timestamp but different eventIDs", func(t *testing.T) {
		eventTime := now.Truncate(time.Second)

		// Same time, but different eventIDs (e.g. a review and a comment at the same time)
		span1 := createSpan("Review: APPROVED", "approved", "review-123-url1", "https://github.com/1#review", eventTime)
		span2 := createSpan("Comment", "comment", "comment-123-url2", "https://github.com/1#comment", eventTime)

		var buf bytes.Buffer
		RenderOTelTimeline(&buf, []sdktrace.ReadOnlySpan{span1, span2}, time.Time{}, time.Time{}, enrichment.DefaultEnricher())

		output := buf.String()
		// We check for the presence of the labels which are now clickable links
		assert.Contains(t, output, "APPROVED")
		assert.Contains(t, output, "Comment")
	})

	t.Run("Sorts markers before workflows when timestamps are identical", func(t *testing.T) {
		eventTime := now.Truncate(time.Second)

		workflowSpan := &mockReadOnlySpan{
			name:      "Workflow: Test",
			startTime: eventTime,
			endTime:   eventTime.Add(time.Minute),
			spanID:    trace.SpanID{1, 1, 1, 1, 1, 1, 1, 1},
			attrs: []attribute.KeyValue{
				attribute.String("type", "workflow"),
			},
		}

		markerSpan := &mockReadOnlySpan{
			name:      "Commit Pushed",
			startTime: eventTime,
			endTime:   eventTime.Add(time.Millisecond),
			spanID:    trace.SpanID{2, 2, 2, 2, 2, 2, 2, 2},
			attrs: []attribute.KeyValue{
				attribute.String("type", "marker"),
				attribute.String("github.event_type", "push"),
			},
		}

		var buf bytes.Buffer
		// Provide spans in "wrong" order to test sorting
		RenderOTelTimeline(&buf, []sdktrace.ReadOnlySpan{workflowSpan, markerSpan}, time.Time{}, time.Time{}, enrichment.DefaultEnricher())

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")

		// Find the lines containing the labels
		markerLineIdx := -1
		workflowLineIdx := -1
		for i, line := range lines {
			if strings.Contains(line, "Commit Pushed") {
				markerLineIdx = i
			}
			if strings.Contains(line, "Workflow: Test") {
				workflowLineIdx = i
			}
		}

		assert.True(t, markerLineIdx != -1, "Marker not found in output")
		assert.True(t, workflowLineIdx != -1, "Workflow not found in output")
		assert.True(t, markerLineIdx < workflowLineIdx, "Marker should appear before workflow in waterfall")
	})
}

type mockReadOnlySpan struct {
	sdktrace.ReadOnlySpan
	name      string
	startTime time.Time
	endTime   time.Time
	spanID    trace.SpanID
	attrs     []attribute.KeyValue
}

func (m *mockReadOnlySpan) Name() string                     { return m.name }
func (m *mockReadOnlySpan) StartTime() time.Time             { return m.startTime }
func (m *mockReadOnlySpan) EndTime() time.Time               { return m.endTime }
func (m *mockReadOnlySpan) Attributes() []attribute.KeyValue { return m.attrs }
func (m *mockReadOnlySpan) SpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		SpanID: m.spanID,
	})
}
func (m *mockReadOnlySpan) Parent() trace.SpanContext    { return trace.SpanContext{} }
func (m *mockReadOnlySpan) Resource() *resource.Resource { return nil }
func (m *mockReadOnlySpan) InstrumentationLibrary() instrumentation.Library {
	return instrumentation.Library{}
}
func (m *mockReadOnlySpan) InstrumentationScope() instrumentation.Scope {
	return instrumentation.Scope{}
}
func (m *mockReadOnlySpan) ChildSpanCount() int                 { return 0 }
func (m *mockReadOnlySpan) Links() []sdktrace.Link              { return nil }
func (m *mockReadOnlySpan) Events() []sdktrace.Event            { return nil }
func (m *mockReadOnlySpan) Status() sdktrace.Status             { return sdktrace.Status{} }
func (m *mockReadOnlySpan) DroppedAttributesCount() int         { return 0 }
func (m *mockReadOnlySpan) DroppedEventsCount() int             { return 0 }
func (m *mockReadOnlySpan) DroppedLinksCount() int              { return 0 }
func (m *mockReadOnlySpan) ChildSpans() []sdktrace.ReadOnlySpan { return nil }

func countOccurrences(s, substr string) int {
	count := 0
	for {
		idx := bytes.Index([]byte(s), []byte(substr))
		if idx == -1 {
			break
		}
		count++
		s = s[idx+len(substr):]
	}
	return count
}

func TestRequiredEmoji(t *testing.T) {
	t.Run("Returns lock emoji for required checks", func(t *testing.T) {
		assert.Equal(t, " 🔒", requiredEmoji(true))
	})

	t.Run("Returns empty string for optional checks", func(t *testing.T) {
		assert.Equal(t, "", requiredEmoji(false))
	})
}

func TestJobSpanRequiredEmoji(t *testing.T) {
	now := time.Now()

	createJobSpan := func(name string, isRequired bool) sdktrace.ReadOnlySpan {
		return &mockReadOnlySpan{
			name:      name,
			startTime: now,
			endTime:   now.Add(time.Second * 5),
			spanID:    trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
			attrs: []attribute.KeyValue{
				attribute.String("type", "job"),
				attribute.String("github.status", "completed"),
				attribute.String("github.conclusion", "success"),
				attribute.String("github.url", "https://github.com/test/repo/actions/runs/1/job/1"),
				attribute.Bool("is_required", isRequired),
			},
		}
	}

	t.Run("Shows lock emoji for required job", func(t *testing.T) {
		span := createJobSpan("required-check 🔒", true)

		var buf bytes.Buffer
		RenderOTelTimeline(&buf, []sdktrace.ReadOnlySpan{span}, time.Time{}, time.Time{}, enrichment.DefaultEnricher())

		output := buf.String()
		assert.Contains(t, output, "required-check 🔒")
		assert.NotContains(t, output, "📋")
	})

	t.Run("Shows no emoji for optional job", func(t *testing.T) {
		span := createJobSpan("optional-check", false)

		var buf bytes.Buffer
		RenderOTelTimeline(&buf, []sdktrace.ReadOnlySpan{span}, time.Time{}, time.Time{}, enrichment.DefaultEnricher())

		output := buf.String()
		assert.Contains(t, output, "optional-check")
		assert.NotContains(t, output, "📋")
		assert.NotContains(t, output, "🔒")
	})
}

func TestRenderOTelTimelineZeroDuration(t *testing.T) {
	// A trace whose spans all share a single timestamp used to make
	// totalDuration zero, producing 0/0 = NaN in the bar math. int(NaN) is
	// architecture-dependent (0 on arm64, minInt64 on amd64), garbling the
	// waterfall on amd64. The timeline must clamp and stay aligned.
	now := time.Now().Truncate(time.Second)

	span := &mockReadOnlySpan{
		name:      "instant-op",
		startTime: now,
		endTime:   now, // zero duration
		spanID:    trace.SpanID{9, 9, 9, 9, 9, 9, 9, 9},
	}

	var buf bytes.Buffer
	RenderOTelTimeline(&buf, []sdktrace.ReadOnlySpan{span}, now, now, enrichment.DefaultEnricher())

	output := utils.StripANSI(buf.String())
	assert.Contains(t, output, "Duration: 0s")
	assert.Contains(t, output, "instant-op")

	// Every row must keep the 62-cell interior between the box borders.
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	assert.NotEmpty(t, lines)
	for _, line := range lines {
		runes := []rune(line)
		if len(runes) == 0 {
			continue
		}
		switch runes[0] {
		case '┌', '└', '├':
			assert.Equal(t, 64, len(runes), "border width mismatch: %q", line)
		case '│':
			// Find the closing border of the waterfall area.
			closing := -1
			for i := 1; i < len(runes); i++ {
				if runes[i] == '│' {
					closing = i
					break
				}
			}
			assert.NotEqual(t, -1, closing, "row missing closing border: %q", line)
			// Marker rows reserve 2 display cells for the marker char, so
			// their rune count can be one short of the 63-rune border column.
			assert.True(t, closing == 62 || closing == 63, "row interior width mismatch (closing border at rune %d): %q", closing, line)
		}
	}
}

func TestRenderOTelTimelineMarkerAlignment(t *testing.T) {
	// Marker glyph widths were guessed from the event type (assuming e.g. ✅
	// for "approved") while enrichment actually emits ✓ — a 1-cell glyph
	// reserved 2 cells, leaving marker rows one column short. Reserved width
	// must equal the rendered glyph's display width for every marker kind.
	now := time.Now().Truncate(time.Second)

	spans := []sdktrace.ReadOnlySpan{
		&mockReadOnlySpan{
			name:      "Workflow: CI",
			startTime: now,
			endTime:   now.Add(10 * time.Minute),
			spanID:    trace.SpanID{1, 1, 1, 1, 1, 1, 1, 1},
			attrs:     []attribute.KeyValue{attribute.String("type", "workflow")},
		},
	}
	for i, eventType := range []string{"approved", "merged", "comment", "changes_requested", "push", "unknown-kind"} {
		spans = append(spans, &mockReadOnlySpan{
			name:      "Review: " + eventType,
			startTime: now.Add(time.Duration(i) * time.Minute),
			endTime:   now.Add(time.Duration(i) * time.Minute),
			spanID:    trace.SpanID{2, 2, 2, 2, 2, 2, 2, byte(i + 1)},
			attrs: []attribute.KeyValue{
				attribute.String("type", "marker"),
				attribute.String("github.event_type", eventType),
			},
		})
	}

	var buf bytes.Buffer
	RenderOTelTimeline(&buf, spans, now, now.Add(10*time.Minute), enrichment.DefaultEnricher())

	output := utils.StripANSI(buf.String())
	for _, line := range strings.Split(strings.TrimRight(output, "\n"), "\n") {
		runes := []rune(line)
		if len(runes) == 0 || runes[0] != '│' {
			continue
		}
		closing := -1
		for i := 1; i < len(runes); i++ {
			if runes[i] == '│' {
				closing = i
				break
			}
		}
		if closing < 0 {
			t.Fatalf("row missing closing border: %q", line)
		}
		interior := string(runes[1:closing])
		assert.Equal(t, 62, lipgloss.Width(interior), "interior display width mismatch: %q", line)
	}
}
