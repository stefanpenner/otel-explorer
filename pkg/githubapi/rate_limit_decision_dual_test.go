package githubapi

// Dual test: decision core (ratelimitspec) ↔ rateLimiter.waitIfNeeded.
//
// Spec: specs/rate-limit/decision/Decision.tla (FINDING 13 wake recheck)
// Generated: pkg/githubapi/ratelimitspec (never hand-edit)
// Production: waitIfNeeded loop client.go:361-379

import (
	"context"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi/ratelimitspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRateLimitDecision_DualWaitIfNeededRecheck pairs the decision-core
// StartSleep → WakeRecheckResleep path with a live rateLimiter that has
// its window re-exhausted mid-sleep (same shape as
// TestRateLimitSpec_WakeRechecksLimiter).
func TestRateLimitDecision_DualWaitIfNeededRecheck(t *testing.T) {
	t.Parallel()

	// --- decision core: still-exhausted recheck stays sleeping ---
	s := ratelimitspec.Init()
	require.True(t, s.CanLearnExhausted())
	s = s.LearnExhausted()
	require.True(t, s.CanStartSleep())
	s = s.StartSleep()
	require.True(t, s.CanWakeRecheckResleep(),
		"decision core must re-sleep while WaitNeeded (FINDING 13)")
	require.False(t, s.CanWakeBugSend(), "Bug=FALSE: no fire-without-recheck")
	require.False(t, s.CanWakeRecheckSend(), "must not send while WaitNeeded")
	s = s.WakeRecheckResleep()
	assert.True(t, s.Sleeping)
	assert.False(t, s.SentWhileExhausted)

	// --- production: waitIfNeeded loops until waitDuration()==0 ---
	limiter := &rateLimiter{
		remaining: 0,
		resetTime: time.Now().Add(50 * time.Millisecond),
	}
	// While the waiter sleeps (+1s slack), re-exhaust further out.
	go func() {
		time.Sleep(100 * time.Millisecond)
		limiter.mu.Lock()
		limiter.remaining = 0
		limiter.resetTime = time.Now().Add(2 * time.Second)
		limiter.mu.Unlock()
	}()

	err := limiter.waitIfNeeded(context.Background())
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), limiter.waitDuration(),
		"waitIfNeeded must recheck after sleep (decision: WakeRecheckResleep)")
}

// TestRateLimitWaitNeededMatchesDecision pins the pure WaitNeeded gate
// used by waitDuration to the decision-core StartSleep precondition.
func TestRateLimitWaitNeededMatchesDecision(t *testing.T) {
	t.Parallel()

	s := ratelimitspec.Init()
	// Fresh window: not WaitNeeded.
	assert.False(t, rateLimitWaitNeeded(int(s.Remaining), s.ResetAt > 0, time.Duration(s.ResetAt-s.Clock)*time.Second))
	assert.False(t, s.CanStartSleep())

	s = s.LearnExhausted()
	// Exhausted, reset in future (resetAt = clock+1 in LearnExhausted).
	until := time.Duration(s.ResetAt-s.Clock) * time.Second
	assert.True(t, s.CanStartSleep())
	assert.True(t, rateLimitWaitNeeded(int(s.Remaining), s.ResetAt > 0, until),
		"production WaitNeeded must match CanStartSleep precondition")

	// Past reset: not needed.
	assert.False(t, rateLimitWaitNeeded(0, true, 0))
	assert.False(t, rateLimitWaitNeeded(0, true, -time.Second))
	assert.False(t, rateLimitWaitNeeded(1, true, time.Hour))
}
