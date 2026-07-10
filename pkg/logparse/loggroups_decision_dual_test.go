package logparse

// Dual check: canOpenGroup / canCloseGroup (production) vs Open / Close
// guards from LogGroupsDecision (specgen → loggroupsspec).
//
// Spec: specs/log-groups/decision/Decision.tla
// Gen:  pkg/logparse/loggroupsspec (never hand-edit)

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/logparse/loggroupsspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupStackGatesMatchDecision(t *testing.T) {
	t.Parallel()

	s := loggroupsspec.Init()
	assert.Equal(t, int64(0), s.Depth)
	assert.True(t, s.CanOpen())
	assert.False(t, s.CanClose())
	assert.True(t, s.Inv_DepthNonNeg(), "generated pure Inv_DepthNonNeg at depth 0")
	assert.True(t, canOpenGroup(0, 3))
	assert.False(t, canCloseGroup(0), "CloseBug forbidden: no underflow")

	require.True(t, s.CanOpen())
	s = s.Open()
	assert.Equal(t, int64(1), s.Depth)
	assert.True(t, canCloseGroup(1))
	assert.True(t, s.CanClose())
	assert.True(t, canOpenGroup(1, 3))

	s = s.Open()
	s = s.Open()
	assert.Equal(t, int64(3), s.Depth)
	assert.False(t, s.CanOpen(), "at MaxDepth")
	assert.False(t, canOpenGroup(3, 3))
	assert.True(t, canOpenGroup(3, 0), "unbounded production maxDepth=0")

	s = s.Close()
	assert.Equal(t, int64(2), s.Depth)
	assert.True(t, canCloseGroup(int(s.Depth)))
}

func TestSplitGroupsStrayEndgroupNoUnderflow(t *testing.T) {
	t.Parallel()
	// Stray endgroups must not invent groups (depth-0 Close is a no-op).
	lines := []LogLine{
		{Content: "##[endgroup]", LineNum: 1},
		{Content: "##[endgroup]", LineNum: 2},
		{Content: "##[group]Tail", LineNum: 3},
		{Content: "work", LineNum: 4},
		{Content: "##[endgroup]", LineNum: 5},
	}
	top, groups := splitGroups(lines)
	assert.Empty(t, top)
	require.Len(t, groups, 1)
	assert.Equal(t, "Tail", groups[0].name)
}

