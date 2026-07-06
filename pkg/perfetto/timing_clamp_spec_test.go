package perfetto

// Tests for the export-boundary time sanitizer, pinned against the
// specs/timing-clamp TLA+ spec's DoExport action (perfetto.go clamp + 1ms
// minimum). Key property from the campaign (Finding 1): spans the analyzer
// already clamped (end >= start+1ms) pass through UNCHANGED, so the 1ms
// bump can never push a pre-clamped child past its parent again.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimingClampSpec_SanitizeEventNs(t *testing.T) {
	const ms = int64(1_000_000)
	cases := []struct {
		name      string
		startNs   int64
		endNs     int64
		isMarker  bool
		wantStart uint64
		wantEnd   uint64
	}{
		// Analyzer-clamped spans (>=1ms duration): identity — no re-escape.
		{"pre-clamped span unchanged", 2 * ms, 3 * ms, false, uint64(2 * ms), uint64(3 * ms)},
		{"pre-clamped sliver unchanged", 3 * ms, 4 * ms, false, uint64(3 * ms), uint64(4 * ms)},
		// Hostile input from other span sources.
		{"negative start clamped to zero", -5 * ms, 2 * ms, false, 0, uint64(2 * ms)},
		{"inverted span floored to 1ms", 3 * ms, 1 * ms, false, uint64(3 * ms), uint64(4 * ms)},
		{"zero-duration non-marker gets 1ms", 2 * ms, 2 * ms, false, uint64(2 * ms), uint64(3 * ms)},
		{"zero-duration marker stays instant", 2 * ms, 2 * ms, true, uint64(2 * ms), uint64(2 * ms)},
		{"inverted marker clamped, no bump", 3 * ms, 1 * ms, true, uint64(3 * ms), uint64(3 * ms)},
		{"all-negative span pinned at zero", -3 * ms, -2 * ms, false, 0, uint64(ms)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStart, gotEnd := sanitizeEventNs(tc.startNs, tc.endNs, tc.isMarker)
			assert.Equal(t, tc.wantStart, gotStart, "start")
			assert.Equal(t, tc.wantEnd, gotEnd, "end")
			assert.GreaterOrEqual(t, gotEnd, gotStart, "never inverted")
		})
	}
}
