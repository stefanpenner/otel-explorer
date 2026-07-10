package results

// Dual check: logFetchResultFresh (production) vs FetchAccept/FetchDiscard
// guards from TuiReloadDecision (specgen → tuireloadspec).
//
// Spec: specs/tui-reload/decision/Decision.tla
// Gen:  pkg/tui/results/tuireloadspec (never hand-edit)

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/tui/results/tuireloadspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogFetchResultFreshMatchesDecision(t *testing.T) {
	t.Parallel()

	// Fresh fetch under gen 0: Accept enabled, Discard not.
	s := tuireloadspec.Init()
	require.True(t, s.CanPressFetch1())
	s = s.PressFetch1()
	assert.True(t, s.CanFetchAccept())
	assert.False(t, s.CanFetchDiscard())
	assert.True(t, logFetchResultFresh(s.FetchJob, s.FetchJob, int(s.FetchGen), int(s.ReloadGen)),
		"production gate must match CanFetchAccept while gen matches")

	// Reload bumps gen; in-flight fetch stamped with old gen → Discard.
	require.True(t, s.CanPressReload())
	s = s.PressReload()
	require.True(t, s.CanReloadDone())
	s = s.ReloadDone()
	assert.Equal(t, int64(1), s.ReloadGen)
	assert.Equal(t, int64(0), s.FetchGen) // stamped before reload
	assert.False(t, s.CanFetchAccept())
	assert.True(t, s.CanFetchDiscard())
	assert.False(t, logFetchResultFresh(s.FetchJob, s.FetchJob, int(s.FetchGen), int(s.ReloadGen)),
		"production gate must match CanFetchDiscard after reload gen bump")
	// Faithful path never accepts stale: FetchDiscard leaves StaleAccepted false.
	require.True(t, s.CanFetchDiscard())
	s = s.FetchDiscard()
	assert.True(t, s.NoStaleAccepted(), "generated pure NoStaleAccepted after FetchDiscard")

	// Wrong job never accepted.
	assert.False(t, logFetchResultFresh(1, 2, 0, 0))
	assert.False(t, logFetchResultFresh(0, 1, 0, 0))
}

