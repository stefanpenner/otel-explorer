package githubapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRequestStatsCounting drives the real transport stack (rate-limited behind
// cache) against a stub server: a cache miss counts as a network request, an
// identical follow-up is served fresh from cache with no extra network call,
// and the rate-limit header is captured.
func TestRequestStatsCounting(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("x-ratelimit-remaining", "4999")
		w.Header().Set("x-ratelimit-reset", "9999999999")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()

	stats := &apiStats{}
	limiter := &rateLimiter{}
	var base http.RoundTripper = http.DefaultTransport
	base = &RateLimitedTransport{Base: base, Limiter: limiter, Semaphore: make(chan struct{}, 2), Stats: stats}
	ct := NewCachedTransport(base, t.TempDir())
	ct.Stats = stats
	client := &http.Client{Transport: ct}

	do := func() {
		req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
		resp, err := client.Do(req)
		assert.NoError(t, err)
		if resp != nil {
			resp.Body.Close()
		}
	}

	do() // miss → network
	assert.EqualValues(t, 1, stats.networkRequests.Load())
	assert.EqualValues(t, 0, stats.cacheHits.Load())

	do() // fresh hit → cache, no extra network
	assert.EqualValues(t, 1, stats.networkRequests.Load())
	assert.EqualValues(t, 1, stats.cacheHits.Load())
	assert.Equal(t, 1, calls, "server should be hit only once")

	rem, _, known := limiter.snapshot()
	assert.True(t, known)
	assert.Equal(t, 4999, rem)
}

func TestRequestStatsSummary(t *testing.T) {
	full := RequestStats{
		NetworkRequests: 47, CacheHits: 312,
		RateLimitRemaining: 4612, RateLimitReset: time.Unix(1_700_000_000, 0), RateLimitKnown: true,
	}
	s := full.Summary()
	assert.Contains(t, s, "47 requests")
	assert.Contains(t, s, "312 served from cache")
	assert.Contains(t, s, "4612 rate-limit remaining")
	assert.Contains(t, s, "resets ")

	// Singular, no cache, unknown rate limit: exact and no extra clauses.
	assert.Equal(t, "GitHub API: 1 request", RequestStats{NetworkRequests: 1}.Summary())
	assert.Equal(t, "GitHub API: 0 requests", RequestStats{}.Summary())
	assert.NotContains(t, RequestStats{NetworkRequests: 3}.Summary(), "rate-limit")
}
