package results

import (
	"strings"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stretchr/testify/assert"
)

func TestGetCharWidth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		char     string
		expected int
	}{
		// Wide characters (emojis)
		{"💬", 2},
		{"📋", 2},
		{"⚙️", 2},
		{"❌", 2},
		{"🔒", 2},
		{"🔥", 2},
		// Single-width characters
		{"◆", 1},
		{"✓", 1},
		{"✗", 1},
		{"▲", 1},
		{"|", 1},
		{"↳", 1},
		{"◷", 1},
		{"○", 1},
		{"●", 1},
		{"▼", 1},
		{"▶", 1},
		{" ", 1},
		// Two-char combinations
		{"◆ ", 2},
		{"▲ ", 2},
		{"• ", 2},
		{"● ", 2},
	}

	for _, tc := range cases {
		t.Run(tc.char, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetCharWidth(tc.char))
		})
	}
}

func TestGetBarStyle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		item         TreeItem
		expectedChar string
	}{
		// Step styles
		{"step success", TreeItem{Hints: enrichment.SpanHints{Category: "step", IsLeaf: true, Outcome: "success", Color: "green", BarChar: "▒"}}, "▒"},
		{"step failure", TreeItem{Hints: enrichment.SpanHints{Category: "step", IsLeaf: true, Outcome: "failure", Color: "red", BarChar: "▒"}}, "▒"},
		{"step skipped", TreeItem{Hints: enrichment.SpanHints{Category: "step", IsLeaf: true, Outcome: "skipped", Color: "gray", BarChar: "░"}}, "░"},
		{"step cancelled", TreeItem{Hints: enrichment.SpanHints{Category: "step", IsLeaf: true, Outcome: "skipped", Color: "gray", BarChar: "░"}}, "░"},
		{"step pending", TreeItem{Hints: enrichment.SpanHints{Category: "step", IsLeaf: true, Outcome: "pending", Color: "blue", BarChar: "▒"}}, "▒"},

		// Marker styles
		{"marker merged", TreeItem{Hints: enrichment.SpanHints{Category: "marker", IsMarker: true, BarChar: "◆", Color: "green"}}, "◆"},
		{"marker approved", TreeItem{Hints: enrichment.SpanHints{Category: "marker", IsMarker: true, BarChar: "✓", Color: "green"}}, "✓"},
		{"marker comment", TreeItem{Hints: enrichment.SpanHints{Category: "marker", IsMarker: true, BarChar: "●", Color: "blue"}}, "●"},
		{"marker changes_requested", TreeItem{Hints: enrichment.SpanHints{Category: "marker", IsMarker: true, BarChar: "✗", Color: "red"}}, "✗"},
		{"marker unknown", TreeItem{Hints: enrichment.SpanHints{Category: "marker", IsMarker: true, BarChar: "▲", Color: "blue"}}, "▲"},

		// Job/workflow styles
		{"job pending", TreeItem{Hints: enrichment.SpanHints{Category: "job", Outcome: "pending", Color: "blue", BarChar: "▒"}}, "▒"},
		{"job success", TreeItem{Hints: enrichment.SpanHints{Category: "job", Outcome: "success", Color: "green", BarChar: "█"}}, "█"},
		{"job failure required", TreeItem{Hints: enrichment.SpanHints{Category: "job", Outcome: "failure", Color: "red", BarChar: "█", IsRequired: true}}, "█"},
		{"job failure optional", TreeItem{Hints: enrichment.SpanHints{Category: "job", Outcome: "failure", Color: "yellow", BarChar: "░"}}, "░"},
		{"job skipped", TreeItem{Hints: enrichment.SpanHints{Category: "job", Outcome: "skipped", Color: "gray", BarChar: "░"}}, "░"},
		{"job unknown", TreeItem{Hints: enrichment.SpanHints{Category: "job", Color: "gray", BarChar: "░"}}, "░"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			char, _ := getBarStyle(tc.item)
			assert.Equal(t, tc.expectedChar, char)
		})
	}
}

func TestRenderTimelineBar(t *testing.T) {
	t.Parallel()

	now := time.Now()
	globalStart := now
	globalEnd := now.Add(10 * time.Second)
	width := 20

	t.Run("returns spaces for invalid time range", func(t *testing.T) {
		// End before start
		result := RenderTimelineBar(TreeItem{}, now, now.Add(-time.Second), width, "")
		assert.Equal(t, strings.Repeat(" ", width), result)

		// Equal times
		result = RenderTimelineBar(TreeItem{}, now, now, width, "")
		assert.Equal(t, strings.Repeat(" ", width), result)
	})

	t.Run("returns spaces for zero width", func(t *testing.T) {
		result := RenderTimelineBar(TreeItem{}, globalStart, globalEnd, 0, "")
		assert.Equal(t, "", result)
	})

	t.Run("renders bar at start position", func(t *testing.T) {
		item := TreeItem{
			Hints:     enrichment.SpanHints{Category: "job", Outcome: "success", Color: "green", BarChar: "█"},
			StartTime: globalStart,
			EndTime:   globalStart.Add(2 * time.Second),
		}

		result := RenderTimelineBar(item, globalStart, globalEnd, width, "")

		assert.False(t, strings.HasPrefix(result, "     "), "Bar should start near beginning")
	})

	t.Run("renders bar at end position", func(t *testing.T) {
		item := TreeItem{
			Hints:     enrichment.SpanHints{Category: "job", Outcome: "success", Color: "green", BarChar: "█"},
			StartTime: globalEnd.Add(-2 * time.Second),
			EndTime:   globalEnd,
		}

		result := RenderTimelineBar(item, globalStart, globalEnd, width, "")

		assert.True(t, strings.HasPrefix(result, "    "), "Bar should be near end with leading spaces")
	})

	t.Run("clamps item outside global bounds", func(t *testing.T) {
		item := TreeItem{
			Hints:     enrichment.SpanHints{Category: "job", Outcome: "success", Color: "green", BarChar: "█"},
			StartTime: globalStart.Add(-5 * time.Second),
			EndTime:   globalEnd.Add(5 * time.Second),
		}

		result := RenderTimelineBar(item, globalStart, globalEnd, width, "")

		assert.NotEmpty(t, result)
	})

	t.Run("renders marker for zero-duration item", func(t *testing.T) {
		item := TreeItem{
			Hints:     enrichment.SpanHints{Category: "marker", IsMarker: true, BarChar: "✓", Color: "green"},
			StartTime: globalStart.Add(5 * time.Second),
			EndTime:   globalStart.Add(5 * time.Second),
		}

		result := RenderTimelineBar(item, globalStart, globalEnd, width, "")

		assert.NotEmpty(t, result)
	})

	t.Run("renders pipe for zero-duration non-marker", func(t *testing.T) {
		item := TreeItem{
			Hints:     enrichment.SpanHints{Category: "job", Outcome: "success", Color: "green", BarChar: "█"},
			StartTime: globalStart.Add(5 * time.Second),
			EndTime:   globalStart.Add(5 * time.Second),
		}

		result := RenderTimelineBar(item, globalStart, globalEnd, width, "")

		assert.Contains(t, result, "|")
	})
}

func TestRenderTimelineBarSelected(t *testing.T) {
	t.Parallel()

	now := time.Now()
	globalStart := now
	globalEnd := now.Add(10 * time.Second)
	width := 20

	t.Run("returns spaces for invalid time range", func(t *testing.T) {
		result := RenderTimelineBarSelected(TreeItem{}, now, now.Add(-time.Second), width, "")
		assert.Equal(t, strings.Repeat(" ", width), result)
	})

	t.Run("renders bar for selected items", func(t *testing.T) {
		item := TreeItem{
			Hints:     enrichment.SpanHints{Category: "job", Outcome: "success", Color: "green", BarChar: "█"},
			StartTime: globalStart,
			EndTime:   globalEnd,
		}

		result := RenderTimelineBarSelected(item, globalStart, globalEnd, width, "")

		assert.Contains(t, result, "█")
	})
}

func TestRenderMarker(t *testing.T) {
	t.Parallel()

	width := 20

	t.Run("positions marker correctly", func(t *testing.T) {
		result := renderMarker("○", BarPendingStyle, 5, width, "", false)

		// Should have 5 leading spaces
		assert.True(t, strings.HasPrefix(result, "     "), "Should have 5 leading spaces")
	})

	t.Run("clamps negative position to 0", func(t *testing.T) {
		result := renderMarker("○", BarPendingStyle, -5, width, "", false)

		// Should start with marker, not spaces
		assert.True(t, strings.HasPrefix(result, "○"), "Should clamp to position 0")
	})

	t.Run("clamps position beyond width", func(t *testing.T) {
		result := renderMarker("○", BarPendingStyle, 100, width, "", false)

		// Should not panic and should be within width
		// The marker should be at the end
		trimmed := strings.TrimRight(result, " ")
		assert.True(t, strings.HasSuffix(trimmed, "○") || strings.Contains(result, "○"))
	})

	t.Run("adds hyperlink when URL provided", func(t *testing.T) {
		result := renderMarker("○", BarPendingStyle, 5, width, "https://example.com", false)

		// Should contain OSC 8 hyperlink sequence with id parameter
		assert.Contains(t, result, "\x1b]8;id=https://example.com;https://example.com\x07")
		assert.Contains(t, result, "\x1b]8;;\x07") // Closing sequence
	})
}

func TestTimelineHyperlink(t *testing.T) {
	t.Parallel()

	t.Run("returns text unchanged when URL empty", func(t *testing.T) {
		result := timelineHyperlink("", "text")
		assert.Equal(t, "text", result)
	})

	t.Run("wraps text in OSC 8 hyperlink", func(t *testing.T) {
		result := timelineHyperlink("https://example.com", "click me")

		// Should contain OSC 8 hyperlink sequence with id parameter
		assert.Contains(t, result, "\x1b]8;id=https://example.com;https://example.com\x07")
		assert.Contains(t, result, "click me")
		assert.Contains(t, result, "\x1b]8;;\x07")
	})

	t.Run("disables underline in hyperlink", func(t *testing.T) {
		result := timelineHyperlink("https://example.com", "text")

		// Should contain \x1b[24m to disable underline
		assert.Contains(t, result, "\x1b[24m")
	})
}

func TestRenderTimelineBarWithChildren(t *testing.T) {
	t.Parallel()

	now := time.Now()
	globalStart := now
	globalEnd := now.Add(10 * time.Second)
	width := 20

	wfHints := enrichment.SpanHints{Category: "workflow", IsRoot: true, Outcome: "success", Color: "green", BarChar: "█"}
	childSuccess := enrichment.SpanHints{Outcome: "success"}
	childFailure := enrichment.SpanHints{Outcome: "failure"}

	t.Run("child markers appear at correct positions", func(t *testing.T) {
		item := TreeItem{
			Hints:       wfHints,
			StartTime:   globalStart,
			EndTime:     globalStart.Add(4 * time.Second),
			HasChildren: true,
			Children: []*TreeItem{
				{StartTime: globalStart.Add(6 * time.Second), EndTime: globalStart.Add(7 * time.Second), Hints: childSuccess},
				{StartTime: globalStart.Add(9 * time.Second), EndTime: globalStart.Add(10 * time.Second), Hints: childFailure},
			},
		}

		result := RenderTimelineBarWithChildren(item, globalStart, globalEnd, width, "", nil)

		assert.Contains(t, result, "·", "Should contain child marker dots")
		assert.NotEmpty(t, result)
	})

	t.Run("parent bar overwrites child markers where they overlap", func(t *testing.T) {
		item := TreeItem{
			Hints:       wfHints,
			StartTime:   globalStart,
			EndTime:     globalEnd,
			HasChildren: true,
			Children: []*TreeItem{
				{StartTime: globalStart.Add(5 * time.Second), EndTime: globalStart.Add(6 * time.Second), Hints: childSuccess},
			},
		}

		result := RenderTimelineBarWithChildren(item, globalStart, globalEnd, width, "", nil)

		assert.Contains(t, result, "█", "Parent bar should be present")
		assert.NotContains(t, result, "·", "Child marker should be overwritten by parent bar")
	})

	t.Run("no children produces same structure as normal RenderTimelineBar", func(t *testing.T) {
		item := TreeItem{
			Hints:       wfHints,
			StartTime:   globalStart,
			EndTime:     globalStart.Add(5 * time.Second),
			HasChildren: true,
			Children:    []*TreeItem{},
		}

		withChildren := RenderTimelineBarWithChildren(item, globalStart, globalEnd, width, "", nil)
		normal := RenderTimelineBar(item, globalStart, globalEnd, width, "")

		assert.Contains(t, withChildren, "█")
		assert.Contains(t, normal, "█")
	})

	t.Run("failure children use failure dim style", func(t *testing.T) {
		item := TreeItem{
			Hints:       wfHints,
			StartTime:   globalStart,
			EndTime:     globalStart.Add(2 * time.Second),
			HasChildren: true,
			Children: []*TreeItem{
				{StartTime: globalStart.Add(8 * time.Second), EndTime: globalStart.Add(9 * time.Second), Hints: childFailure},
			},
		}

		result := RenderTimelineBarWithChildren(item, globalStart, globalEnd, width, "", nil)

		assert.Contains(t, result, "█", "Parent bar should be present")
		assert.Contains(t, result, "·", "Child marker should be present")
	})

	t.Run("returns spaces for invalid time range", func(t *testing.T) {
		item := TreeItem{
			HasChildren: true,
			Children:    []*TreeItem{},
		}
		result := RenderTimelineBarWithChildren(item, now, now.Add(-time.Second), width, "", nil)
		assert.Equal(t, strings.Repeat(" ", width), result)
	})

	t.Run("children with zero start time are skipped", func(t *testing.T) {
		item := TreeItem{
			Hints:       wfHints,
			StartTime:   globalStart,
			EndTime:     globalStart.Add(2 * time.Second),
			HasChildren: true,
			Children: []*TreeItem{
				{EndTime: globalStart.Add(5 * time.Second), Hints: childSuccess},
			},
		}

		result := RenderTimelineBarWithChildren(item, globalStart, globalEnd, width, "", nil)

		assert.NotContains(t, result, "·")
		assert.Contains(t, result, "█", "Parent bar should still be present")
	})
}

func TestRenderTimelineBarWithChildrenSelected(t *testing.T) {
	t.Parallel()

	now := time.Now()
	globalStart := now
	globalEnd := now.Add(10 * time.Second)
	width := 20

	t.Run("renders with selection styles", func(t *testing.T) {
		item := TreeItem{
			Hints:       enrichment.SpanHints{Category: "workflow", IsRoot: true, Outcome: "success", Color: "green", BarChar: "█"},
			StartTime:   globalStart,
			EndTime:     globalStart.Add(4 * time.Second),
			HasChildren: true,
			Children: []*TreeItem{
				{StartTime: globalStart.Add(7 * time.Second), EndTime: globalStart.Add(8 * time.Second), Hints: enrichment.SpanHints{Outcome: "success"}},
			},
		}

		result := RenderTimelineBarWithChildrenSelected(item, globalStart, globalEnd, width, "", nil)

		assert.Contains(t, result, "█", "Parent bar should be present")
		assert.Contains(t, result, "·", "Child marker should be present")
	})

	t.Run("returns selection bg for invalid time range", func(t *testing.T) {
		item := TreeItem{
			HasChildren: true,
			Children:    []*TreeItem{},
		}
		result := RenderTimelineBarWithChildrenSelected(item, now, now.Add(-time.Second), width, "", nil)
		assert.NotEmpty(t, result)
	})
}

func TestComputeChildPositions(t *testing.T) {
	t.Parallel()

	now := time.Now()
	globalStart := now
	globalEnd := now.Add(10 * time.Second)
	width := 20

	t.Run("computes positions correctly", func(t *testing.T) {
		children := []*TreeItem{
			{StartTime: globalStart.Add(5 * time.Second), Hints: enrichment.SpanHints{Outcome: "success"}},
			{StartTime: globalEnd, Hints: enrichment.SpanHints{Outcome: "failure"}},
		}

		positions := computeChildPositions(children, globalStart, globalEnd, width, getChildMarkerStyle, nil)

		assert.Len(t, positions, 2)
		assert.Equal(t, 10, positions[0].pos)
		assert.Equal(t, 19, positions[1].pos)
	})

	t.Run("skips children with zero start time", func(t *testing.T) {
		children := []*TreeItem{
			{Hints: enrichment.SpanHints{Outcome: "success"}},
		}

		positions := computeChildPositions(children, globalStart, globalEnd, width, getChildMarkerStyle, nil)

		assert.Empty(t, positions)
	})

	t.Run("returns nil for zero width", func(t *testing.T) {
		children := []*TreeItem{
			{StartTime: globalStart.Add(5 * time.Second), Hints: enrichment.SpanHints{Outcome: "success"}},
		}

		positions := computeChildPositions(children, globalStart, globalEnd, 0, getChildMarkerStyle, nil)

		assert.Nil(t, positions)
	})

	t.Run("clamps positions before global start", func(t *testing.T) {
		children := []*TreeItem{
			{StartTime: globalStart.Add(-2 * time.Second), Hints: enrichment.SpanHints{Outcome: "success"}},
		}

		positions := computeChildPositions(children, globalStart, globalEnd, width, getChildMarkerStyle, nil)

		assert.Len(t, positions, 1)
		assert.Equal(t, 0, positions[0].pos)
	})
}

// TestComputeBarGeometry locks the shared timeline-bar geometry math extracted
// from the 5 Render*Bar functions. If the helper drifts, all of them drift.
func TestComputeBarGeometry(t *testing.T) {
	t.Parallel()

	now := time.Now()
	globalStart := now
	globalEnd := now.Add(10 * time.Second)
	width := 20

	t.Run("degenerate inputs return ok=false", func(t *testing.T) {
		_, ok := computeBarGeometry(TreeItem{}, now, now.Add(-time.Second), width)
		assert.False(t, ok, "end before start")

		_, ok = computeBarGeometry(TreeItem{}, now, now, width)
		assert.False(t, ok, "equal start and end")

		_, ok = computeBarGeometry(TreeItem{}, globalStart, globalEnd, 0)
		assert.False(t, ok, "zero width")
	})

	t.Run("item at start: startPos=0", func(t *testing.T) {
		item := TreeItem{StartTime: globalStart, EndTime: globalEnd}
		geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
		assert.True(t, ok)
		assert.Equal(t, 0, geo.startPos)
		assert.False(t, geo.isZero)
	})

	t.Run("zero-duration item flagged, marker clamped to width-1", func(t *testing.T) {
		mid := globalStart.Add(5 * time.Second)
		item := TreeItem{StartTime: mid, EndTime: mid}
		geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
		assert.True(t, ok)
		assert.True(t, geo.isZero)
		assert.Equal(t, 10, geo.startPos) // 5s/10s * 20 = 10

		// Marker beyond end clamps to last column.
		late := globalStart.Add(100 * time.Second)
		item2 := TreeItem{StartTime: late, EndTime: late}
		geo2, _ := computeBarGeometry(item2, globalStart, globalEnd, width)
		assert.Equal(t, width-1, geo2.startPos, "marker beyond window clamps to width-1")
	})

	t.Run("barLength at least 1, clamped to width", func(t *testing.T) {
		// Tiny sliver in the middle — barLength must still be >= 1.
		item := TreeItem{
			StartTime: globalStart.Add(49*time.Millisecond),
			EndTime:   globalStart.Add(51 * time.Millisecond),
		}
		geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
		assert.True(t, ok)
		assert.GreaterOrEqual(t, geo.barLength, 1)
		assert.LessOrEqual(t, geo.startPos+geo.barLength, width)
	})

	t.Run("clamps itemStart/itemEnd to global bounds", func(t *testing.T) {
		// Item extends past both ends → should fill the whole width.
		item := TreeItem{
			StartTime: globalStart.Add(-5 * time.Second),
			EndTime:   globalEnd.Add(5 * time.Second),
		}
		geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
		assert.True(t, ok)
		assert.Equal(t, 0, geo.startPos)
		assert.Equal(t, width, geo.startPos+geo.barLength)
	})
}

