package store

// Dual check: UpsertJobs attempt guard (handwritten store) vs
// OfferNewer / OfferOlder from the SyncBoundsDecision core
// (specgen → syncboundsspec).
//
// Spec: specs/sync-bounds/decision/Decision.tla
// Gen:  pkg/store/syncboundsspec (never hand-edit)

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/store/syncboundsspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncBoundsDecisionDual_OfferPaths drives Store then Offer and
// checks accepted matches acceptJobsAttempt (and NoStaleAccepted).
func TestSyncBoundsDecisionDual_OfferPaths(t *testing.T) {
	t.Parallel()

	// Store2 → OfferNewer: matching attempt must be accepted.
	st := syncboundsspec.Init()
	require.True(t, st.CanStore2())
	st = st.Store2()
	assert.Equal(t, int64(2), st.StoredAttempt)
	require.True(t, st.CanOfferNewer())
	st = st.OfferNewer()
	assert.Equal(t, "decided", st.Phase)
	assert.True(t, st.Accepted)
	assert.Equal(t, st.StoredAttempt, st.IncomingAttempt)
	assert.True(t, acceptJobsAttempt(st.StoredAttempt, st.IncomingAttempt),
		"OfferNewer must match UpsertJobs accept guard")
	// Generated pure invariant NoStaleAccepted (and compact form).
	assert.True(t, st.NoStaleAccepted())
	assert.GreaterOrEqual(t, st.IncomingAttempt, st.StoredAttempt)

	// Store2 → OfferOlder: stale attempt must be rejected (Bug=FALSE).
	st = syncboundsspec.Init()
	st = st.Store2()
	require.True(t, st.CanOfferOlder())
	st = st.OfferOlder()
	assert.False(t, st.Accepted, "faithful OfferOlder rejects stale")
	assert.Equal(t, int64(1), st.IncomingAttempt)
	assert.Equal(t, int64(2), st.StoredAttempt)
	assert.False(t, acceptJobsAttempt(st.StoredAttempt, st.IncomingAttempt),
		"stale incoming must fail the UpsertJobs guard")
	// accepted is false, so NoStaleAccepted holds vacuously
}

// TestSyncBoundsDecisionDual_StoreBump models concurrent attempt bump
// then a stale worker (the FINDING 1 witness shape).
func TestSyncBoundsDecisionDual_StoreBump(t *testing.T) {
	t.Parallel()

	st := syncboundsspec.Init()
	st = st.Store1()
	assert.Equal(t, int64(1), st.StoredAttempt)
	// Concurrent writer advances attempt.
	st = st.Store2()
	assert.Equal(t, int64(2), st.StoredAttempt)
	// Stale worker still holding attempt 1.
	require.True(t, st.CanOfferOlder())
	st = st.OfferOlder()
	assert.False(t, st.Accepted)
	assert.Equal(t, int64(1), st.IncomingAttempt)
	assert.False(t, acceptJobsAttempt(2, 1))
}

// TestSyncBoundsDecisionDual_AgainstStore runs the real UpsertJobs path
// and checks it agrees with the decision-core outcomes.
func TestSyncBoundsDecisionDual_AgainstStore(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Matching attempt: OfferNewer equivalent.
	{
		st := openTestStore(t)
		require.NoError(t, st.UpsertRuns("o", "r", []analyzer.RunData{{
			ID: 1, WorkflowName: "CI", Status: "completed", Conclusion: "success",
			CreatedAt: base, Attempt: 2,
		}}))
		require.NoError(t, st.UpsertJobs("o", "r", 1, 2, []analyzer.JobData{{
			ID: 21, Name: "build", Status: "completed", Conclusion: "success",
			CreatedAt: base, Duration: 60_000,
		}}))
		ids, err := st.RunsNeedingJobs("o", "r", base.Add(-time.Hour), base.Add(time.Hour))
		require.NoError(t, err)
		assert.Empty(t, ids, "matching attempt is accepted (OfferNewer)")

		dec := syncboundsspec.Init().Store2().OfferNewer()
		assert.True(t, dec.Accepted)
	}

	// Stale attempt: OfferOlder equivalent.
	{
		st := openTestStore(t)
		require.NoError(t, st.UpsertRuns("o", "r", []analyzer.RunData{{
			ID: 1, WorkflowName: "CI", Status: "completed", Conclusion: "success",
			CreatedAt: base, Attempt: 2,
		}}))
		// Stale worker for attempt 1.
		require.NoError(t, st.UpsertJobs("o", "r", 1, 1, []analyzer.JobData{{
			ID: 11, Name: "build", Status: "completed", Conclusion: "failure",
			CreatedAt: base, Duration: 60_000,
		}}))
		ids, err := st.RunsNeedingJobs("o", "r", base.Add(-time.Hour), base.Add(time.Hour))
		require.NoError(t, err)
		assert.Equal(t, []int64{1}, ids,
			"stale attempt must not set jobs_fetched (OfferOlder reject)")

		dec := syncboundsspec.Init().Store2().OfferOlder()
		assert.False(t, dec.Accepted)
	}
}

func TestSyncBoundsPurePredicatesRegistry(t *testing.T) {
	t.Parallel()
	preds := syncboundsspec.PurePredicates()
	require.NotEmpty(t, preds)
	s := syncboundsspec.Init()
	var saw bool
	for _, p := range preds {
		_ = p.Check(s)
		if p.Name == "NoStaleAccepted" {
			saw = true
		}
	}
	assert.True(t, saw)
}
