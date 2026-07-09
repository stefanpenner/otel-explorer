package analyzer

// Dual check: dropAPIForRunnerTwin (production) vs DedupChoose keep rule
// from SpanTreeDecision (specgen → spantreespec).
//
// Spec: specs/span-tree/decision/Decision.tla
// Gen:  pkg/analyzer/spantreespec (never hand-edit)

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer/spantreespec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDropAPIForRunnerTwinMatchesDecision(t *testing.T) {
	t.Parallel()

	// Twin shape: both seen → DedupChoose keeps "runner".
	s := spantreespec.Init()
	s = s.SeeAPI().SeeRunner()
	require.True(t, s.CanDedupChoose())
	s = s.DedupChoose()
	assert.Equal(t, "runner", s.Kept)
	assert.True(t, s.Inv_RunnerWins(), "generated pure Inv_RunnerWins after DedupChoose")
	assert.True(t, dropAPIForRunnerTwin(1, 1, false),
		"API side of twin must drop")
	assert.False(t, dropAPIForRunnerTwin(1, 1, true),
		"runner side of twin must keep")

	// Singleton / non-twin shapes: never drop.
	assert.False(t, dropAPIForRunnerTwin(1, 0, false))
	assert.False(t, dropAPIForRunnerTwin(0, 1, true))
	assert.False(t, dropAPIForRunnerTwin(2, 1, false))
	assert.False(t, dropAPIForRunnerTwin(1, 2, false))
}

func TestSpanTreePurePredicatesRegistry(t *testing.T) {
	t.Parallel()
	preds := spantreespec.PurePredicates()
	require.NotEmpty(t, preds)
	s := spantreespec.Init()
	var saw bool
	for _, p := range preds {
		_ = p.Check(s)
		if p.Name == "Inv_RunnerWins" {
			saw = true
		}
	}
	assert.True(t, saw)
}
