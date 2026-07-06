package logparse

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests encode the TLC witness traces from specs/log-groups
// (MCFinding1-4 plus the nested-open and gap-baseline sub-findings of
// FINDINGS.md #12). Time N in a trace maps to specBase + N seconds;
// the parser runs with MinGap=2s, MinSpan=1s to match the spec bounds.

var specBase = time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

func specTime(n int) time.Time { return specBase.Add(time.Duration(n) * time.Second) }

func specParser() *TimestampParser {
	return &TimestampParser{
		MinGapDuration:  2 * time.Second,
		MinSpanDuration: time.Second,
	}
}

// assertSpanTreeWellFormed checks start <= end and child-in-parent for a
// whole span tree — the Inv_SpanStartLeEnd / Inv_ChildInParent invariants.
func assertSpanTreeWellFormed(t *testing.T, spans []ParsedSpan, path string) {
	t.Helper()
	for i, s := range spans {
		p := fmt.Sprintf("%s[%d] %q", path, i, s.Name)
		assert.False(t, s.EndTime.Before(s.StartTime),
			"%s: inverted span [%v, %v]", p, s.StartTime, s.EndTime)
		for j, c := range s.Children {
			cp := fmt.Sprintf("%s.child[%d] %q", p, j, c.Name)
			assert.False(t, c.StartTime.Before(s.StartTime),
				"%s: child starts before parent", cp)
			assert.False(t, c.EndTime.After(s.EndTime),
				"%s: child ends after parent", cp)
		}
		assertSpanTreeWellFormed(t, s.Children, p+".")
	}
}

func TestLogGroupsSpec_NonMonotonicWitnesses(t *testing.T) {
	cases := []struct {
		name    string
		lines   []LogLine
		stepEnd time.Time
		check   func(t *testing.T, spans []ParsedSpan)
	}{
		{
			// MCFinding1: open@2 -> EOF(stepEnd=1). The last-group end
			// override trusted stepEnd blindly, producing span [2, 1].
			name: "Finding1_last_group_end_clamped_to_start",
			lines: []LogLine{
				{Time: specTime(2), Content: "##[group]Alpha", LineNum: 1},
			},
			stepEnd: specTime(1),
			check: func(t *testing.T, spans []ParsedSpan) {
				require.Len(t, spans, 1)
				assert.Equal(t, specTime(2), spans[0].StartTime)
			},
		},
		{
			// MCFinding2: groups append in close order (open@2/close,
			// open@1) so the sorted-by-start assumption behind the
			// next-group end override was false -> broken tiling.
			name: "Finding2_groups_sorted_before_end_override",
			lines: []LogLine{
				{Time: specTime(2), Content: "##[group]Alpha", LineNum: 1},
				{Time: specTime(2), Content: "##[endgroup]", LineNum: 2},
				{Time: specTime(1), Content: "##[group]Beta", LineNum: 3},
			},
			stepEnd: specTime(4),
			check: func(t *testing.T, spans []ParsedSpan) {
				require.Len(t, spans, 2)
				assert.Equal(t, "Beta", spans[0].Name)
				assert.Equal(t, "Alpha", spans[1].Name)
				// Tiling: each span extends to the next span's start,
				// the last to stepEnd (logparse_test.go:67-68 rule).
				assert.Equal(t, spans[1].StartTime, spans[0].EndTime)
				assert.Equal(t, specTime(4), spans[1].EndTime)
			},
		},
		{
			// MCFinding3: open@1, plain@1, plain@3, EOF(stepEnd=1) ->
			// parent span [1,1] but gap-parsed child [1,3] stuck out.
			name: "Finding3_children_clamped_into_parent",
			lines: []LogLine{
				{Time: specTime(1), Content: "##[group]Alpha", LineNum: 1},
				{Time: specTime(1), Content: "first phase of work", LineNum: 2},
				{Time: specTime(3), Content: "second phase of work", LineNum: 3},
			},
			stepEnd: specTime(1),
			check: func(t *testing.T, spans []ParsedSpan) {
				require.Len(t, spans, 1)
			},
		},
		{
			// MCFinding4: open@2 -> close@1 -> open@1 -> EOF. Interior
			// span inverted purely via the next-group override — no
			// stepEnd involved (zero step window here).
			name: "Finding4_interior_span_not_inverted",
			lines: []LogLine{
				{Time: specTime(2), Content: "##[group]Alpha", LineNum: 1},
				{Time: specTime(1), Content: "##[endgroup]", LineNum: 2},
				{Time: specTime(1), Content: "##[group]Beta", LineNum: 3},
			},
			stepEnd: time.Time{},
			check: func(t *testing.T, spans []ParsedSpan) {
				require.Len(t, spans, 2)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spans := specParser().Parse(tc.lines, specBase, tc.stepEnd)
			assertSpanTreeWellFormed(t, spans, "span")
			tc.check(t, spans)
		})
	}
}

// Nested ##[group] used to overwrite the open group, silently discarding
// it and its collected lines (bait witness in specs/log-groups/README.md).
// Fixed: the outer group is implicit-closed at the nested open's time.
func TestLogGroupsSpec_NestedGroupImplicitClose(t *testing.T) {
	lines := []LogLine{
		{Time: specTime(1), Content: "##[group]Outer", LineNum: 1},
		{Time: specTime(1), Content: "outer group work", LineNum: 2},
		{Time: specTime(2), Content: "##[group]Inner", LineNum: 3},
		{Time: specTime(2), Content: "inner group work", LineNum: 4},
		{Time: specTime(3), Content: "##[endgroup]", LineNum: 5},
	}

	spans := specParser().Parse(lines, specBase, specTime(4))
	assertSpanTreeWellFormed(t, spans, "span")

	require.Len(t, spans, 2, "outer group must not be discarded")
	assert.Equal(t, "Outer", spans[0].Name)
	assert.Equal(t, "Inner", spans[1].Name)
	// Outer implicit-closes where Inner opens.
	assert.Equal(t, specTime(1), spans[0].StartTime)
	assert.Equal(t, specTime(2), spans[0].EndTime)
	assert.Equal(t, specTime(4), spans[1].EndTime)
}

// An out-of-order line dragged the gap baseline (current.end) backward,
// manufacturing a >= MinGap gap that is not real -> spurious splits.
// Fixed: current.end only advances forward. With the baseline held at 2,
// line@3 is only 1s away (< MinGap 2s), so there is a single gap group
// and no sub-step decomposition at all.
func TestLogGroupsSpec_GapBaselineOnlyMovesForward(t *testing.T) {
	lines := []LogLine{
		{Time: specTime(2), Content: "phase one work", LineNum: 1},
		{Time: specTime(1), Content: "phase two work", LineNum: 2},
		{Time: specTime(3), Content: "phase three work", LineNum: 3},
	}

	spans := specParser().Parse(lines, specBase, specTime(4))
	assert.Empty(t, spans, "backward timestamp must not manufacture a gap split")
}

// Adversarial regression table: raw log payloads (as fetched from the
// GitHub API) that previously drove the group state machine into
// inverted or mis-nested spans. Parsing must never panic and must always
// yield a well-formed span tree. Each new nasty payload gets a row.
func TestParseGroupsNeverPanics(t *testing.T) {
	payloads := []struct {
		name string
		raw  string
	}{
		{"finding1 stepEnd before group start", "" +
			"2024-01-15T10:30:02.0000000Z ##[group]Alpha\n"},
		{"finding2 close-order groups", "" +
			"2024-01-15T10:30:02.0000000Z ##[group]Alpha\n" +
			"2024-01-15T10:30:02.0000000Z ##[endgroup]\n" +
			"2024-01-15T10:30:01.0000000Z ##[group]Beta\n"},
		{"finding3 child past parent", "" +
			"2024-01-15T10:30:01.0000000Z ##[group]Alpha\n" +
			"2024-01-15T10:30:01.0000000Z first phase of work\n" +
			"2024-01-15T10:30:03.0000000Z second phase of work\n"},
		{"finding4 backward endgroup", "" +
			"2024-01-15T10:30:02.0000000Z ##[group]Alpha\n" +
			"2024-01-15T10:30:01.0000000Z ##[endgroup]\n" +
			"2024-01-15T10:30:01.0000000Z ##[group]Beta\n"},
		{"nested groups", "" +
			"2024-01-15T10:30:01.0000000Z ##[group]Outer\n" +
			"2024-01-15T10:30:01.0000000Z outer group work\n" +
			"2024-01-15T10:30:02.0000000Z ##[group]Inner\n" +
			"2024-01-15T10:30:02.0000000Z inner group work\n" +
			"2024-01-15T10:30:03.0000000Z ##[endgroup]\n"},
		{"stray endgroups", "" +
			"2024-01-15T10:30:01.0000000Z ##[endgroup]\n" +
			"2024-01-15T10:30:02.0000000Z ##[endgroup]\n" +
			"2024-01-15T10:30:03.0000000Z ##[group]Tail\n"},
		{"backward baseline no groups", "" +
			"2024-01-15T10:30:02.0000000Z phase one work\n" +
			"2024-01-15T10:30:01.0000000Z phase two work\n" +
			"2024-01-15T10:30:03.0000000Z phase three work\n"},
	}

	windows := []struct {
		name       string
		start, end time.Time
	}{
		{"sane", specBase, specTime(4)},
		{"inverted", specTime(4), specTime(1)},
		{"zero end", specBase, time.Time{}},
		{"end before groups", specBase, specTime(1)},
	}

	for _, pl := range payloads {
		for _, w := range windows {
			t.Run(pl.name+"/"+w.name, func(t *testing.T) {
				lines := ParseLogLines([]byte(pl.raw))
				spans := specParser().Parse(lines, w.start, w.end)
				assertSpanTreeWellFormed(t, spans, "span")
			})
		}
	}
}
