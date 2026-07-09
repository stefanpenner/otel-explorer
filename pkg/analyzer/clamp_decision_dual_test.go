package analyzer

// Dual check: production clampSpanToParent (thin wrapper around DoClamp)
// still matches applying DoClamp on a fresh timingclampspec.State.
// Catches wrapper drift if someone re-inlines the formula.
//
// Spec: specs/timing-clamp/decision/Decision.tla
// Gen:  pkg/analyzer/timingclampspec (never hand-edit)
// Production: analyzer.clampSpanToParent → State.DoClamp()

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer/timingclampspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClampDecisionDual compares clampSpanToParent against the generated
// DoClamp transition for a table of (start, end, parentStart, parentEnd).
func TestClampDecisionDual(t *testing.T) {
	cases := []struct {
		name               string
		s, e, pS, pE       int64
		wantStart, wantEnd int64
	}{
		{
			name: "child end past parent",
			s:    2, e: 4, pS: 1, pE: 3,
			wantStart: 2, wantEnd: 3,
		},
		{
			name: "child start before parent",
			s:    0, e: 2, pS: 1, pE: 3,
			wantStart: 1, wantEnd: 2,
		},
		{
			name: "child fully inside",
			s:    2, e: 3, pS: 1, pE: 4,
			wantStart: 2, wantEnd: 3,
		},
		{
			name: "child fully past parent",
			s:    5, e: 7, pS: 1, pE: 3,
			wantStart: 2, wantEnd: 3,
		},
		{
			name: "child fully before parent",
			s:    0, e: 1, pS: 3, pE: 5,
			wantStart: 3, wantEnd: 4,
		},
		{
			name: "inverted child inside parent",
			s:    3, e: 1, pS: 1, pE: 4,
			wantStart: 3, wantEnd: 4,
		},
		{
			name: "zero-width child at parent start",
			s:    1, e: 1, pS: 1, pE: 4,
			wantStart: 1, wantEnd: 2,
		},
		{
			name: "degenerate parent equal bounds",
			s:    0, e: 5, pS: 2, pE: 2,
			wantStart: 2, wantEnd: 3,
		},
		{
			name: "degenerate parent inverted",
			s:    1, e: 4, pS: 3, pE: 1,
			wantStart: 3, wantEnd: 4,
		},
		{
			name: "Init interesting case from Decision.tla",
			s:    2, e: 4, pS: 1, pE: 3,
			wantStart: 2, wantEnd: 3,
		},
		{
			name: "SetHostile inputs from Decision.tla",
			s:    0, e: 4, pS: 2, pE: 3,
			wantStart: 2, wantEnd: 3,
		},
		{
			name: "equal to parent window",
			s:    1, e: 3, pS: 1, pE: 3,
			wantStart: 1, wantEnd: 3,
		},
		{
			name: "start at parentEnd (sliver at edge)",
			s:    3, e: 5, pS: 1, pE: 3,
			wantStart: 2, wantEnd: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Handwritten production helper.
			gotS, gotE := clampSpanToParent(tc.s, tc.e, tc.pS, tc.pE)
			assert.Equal(t, tc.wantStart, gotS, "clampSpanToParent start")
			assert.Equal(t, tc.wantEnd, gotE, "clampSpanToParent end")

			// Generated decision module (DoClamp from Decision.tla).
			st := timingclampspec.State{
				Phase:       "init",
				Start:       tc.s,
				End:         tc.e,
				ParentStart: tc.pS,
				ParentEnd:   tc.pE,
				OutStart:    0,
				OutEnd:      0,
			}
			require.True(t, st.CanDoClamp(), "DoClamp must be enabled in init")
			st = st.DoClamp()
			assert.Equal(t, "clamped", st.Phase)
			assert.Equal(t, gotS, st.OutStart,
				"DoClamp outStart must match clampSpanToParent")
			assert.Equal(t, gotE, st.OutEnd,
				"DoClamp outEnd must match clampSpanToParent")
			assert.True(t, st.ClampedOrdered(), "generated pure ClampedOrdered")
			assert.True(t, st.ClampedContained(), "generated pure ClampedContained")

			// Containment contract when parent is valid.
			assert.Less(t, st.OutStart, st.OutEnd, "ordered")
			assert.GreaterOrEqual(t, st.OutStart, tc.pS, "start >= parentStart")
			if tc.pE > tc.pS {
				assert.LessOrEqual(t, st.OutEnd, tc.pE, "end <= parentEnd")
				assert.LessOrEqual(t, st.OutStart, tc.pE-1, "start <= parentEnd-1")
			}
		})
	}
}

// TestClampDecisionDual_InitPath drives the generated machine from Init
// through DoClamp and checks it matches clampSpanToParent on the Init case.
func TestClampDecisionDual_InitPath(t *testing.T) {
	st := timingclampspec.Init()
	gotS, gotE := clampSpanToParent(st.Start, st.End, st.ParentStart, st.ParentEnd)

	require.True(t, st.CanDoClamp())
	st = st.DoClamp()
	assert.Equal(t, gotS, st.OutStart)
	assert.Equal(t, gotE, st.OutEnd)
	assert.Equal(t, int64(2), st.OutStart)
	assert.Equal(t, int64(3), st.OutEnd)

	require.True(t, st.CanFinish())
	st = st.Finish()
	assert.Equal(t, "done", st.Phase)
}

// TestClampDecisionDual_HostilePath: SetHostile then DoClamp matches the
// handwritten helper on the hostile inputs.
func TestClampDecisionDual_HostilePath(t *testing.T) {
	st := timingclampspec.Init()
	require.True(t, st.CanSetHostile())
	st = st.SetHostile()
	// SetHostile: start=0, end=Tmax=4, parentStart=2, parentEnd=3
	assert.Equal(t, int64(0), st.Start)
	assert.Equal(t, int64(4), st.End)
	assert.Equal(t, int64(2), st.ParentStart)
	assert.Equal(t, int64(3), st.ParentEnd)

	gotS, gotE := clampSpanToParent(st.Start, st.End, st.ParentStart, st.ParentEnd)
	st = st.DoClamp()
	assert.Equal(t, gotS, st.OutStart)
	assert.Equal(t, gotE, st.OutEnd)
	assert.Equal(t, int64(2), st.OutStart)
	assert.Equal(t, int64(3), st.OutEnd)
}

// TestClampDecisionDual_BugPassthrough shows the mutation path does NOT
// match the faithful helper (raw passthrough escapes the parent).
func TestClampDecisionDual_BugPassthrough(t *testing.T) {
	st := timingclampspec.Init() // start=2, end=4, parent [1,3]
	// BugPassthrough is disabled when Bug=FALSE is baked into the gen module,
	// but we can still call the transition directly to model the mutation.
	st = st.BugPassthrough()
	assert.Equal(t, int64(2), st.OutStart)
	assert.Equal(t, int64(4), st.OutEnd)

	faithfulS, faithfulE := clampSpanToParent(2, 4, 1, 3)
	assert.NotEqual(t, faithfulE, st.OutEnd,
		"BugPassthrough end must differ from faithful clamp (escapes parent)")
	assert.Equal(t, int64(3), faithfulE)
	assert.Equal(t, int64(2), faithfulS)
	// Mutation violates containment: outEnd 4 > parentEnd 3
	assert.Greater(t, st.OutEnd, int64(3))
}
