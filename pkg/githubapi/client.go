package githubapi

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi/ratelimitspec"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxRetries            = 5
	maxRetryDelay         = 2 * time.Minute
	defaultMaxConcurrency = 5
)

// getTracer returns the current tracer from the global provider.
// This must be called at runtime (not package init) to pick up the correct provider.
func getTracer() trace.Tracer {
	return otel.Tracer("githubapi")
}

type Context struct {
	GitHubToken string
	CacheDir    string
}

// DefaultCacheDir returns the OS-appropriate cache directory for ote.
func DefaultCacheDir() string {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		// Fallback to current directory
		return ".gha-cache"
	}
	return filepath.Join(cacheDir, "ote")
}

func NewContext(token string) Context {
	return Context{GitHubToken: token, CacheDir: DefaultCacheDir()}
}

type Client struct {
	context    Context
	httpClient *http.Client
	semaphore  chan struct{}
	limiter    *rateLimiter
	stats      *apiStats
}

// apiStats counts GitHub API traffic for the run so the UI can show progress
// and rate-limit impact. networkRequests counts requests that reached GitHub
// (cache misses and conditional revalidations); cacheHits counts requests
// served entirely from the local cache with no network call.
type apiStats struct {
	networkRequests atomic.Int64
	cacheHits       atomic.Int64
}

// RequestStats is a point-in-time snapshot of API traffic and rate-limit state.
type RequestStats struct {
	NetworkRequests    int
	CacheHits          int
	RateLimitRemaining int
	RateLimitReset     time.Time
	RateLimitKnown     bool // false until a GitHub response has reported the limit
}

// RequestStats returns the running API-traffic snapshot for this client.
func (c *Client) RequestStats() RequestStats {
	s := RequestStats{}
	if c.stats != nil {
		s.NetworkRequests = int(c.stats.networkRequests.Load())
		s.CacheHits = int(c.stats.cacheHits.Load())
	}
	if c.limiter != nil {
		s.RateLimitRemaining, s.RateLimitReset, s.RateLimitKnown = c.limiter.snapshot()
	}
	return s
}

// Summary renders a one-line human summary for stderr.
func (s RequestStats) Summary() string {
	plural := "s"
	if s.NetworkRequests == 1 {
		plural = ""
	}
	out := fmt.Sprintf("GitHub API: %d request%s", s.NetworkRequests, plural)
	if s.CacheHits > 0 {
		out += fmt.Sprintf(" · %d served from cache", s.CacheHits)
	}
	if s.RateLimitKnown {
		out += fmt.Sprintf(" · %d rate-limit remaining", s.RateLimitRemaining)
		if !s.RateLimitReset.IsZero() {
			out += fmt.Sprintf(" (resets %s)", s.RateLimitReset.Local().Format("15:04"))
		}
	}
	return out
}

type Option func(*Client)

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.httpClient = client
	}
}

func WithCacheDir(dir string) Option {
	return func(c *Client) {
		c.context.CacheDir = dir
	}
}

func WithMaxConcurrency(max int) Option {
	return func(c *Client) {
		if max < 1 {
			max = 1
		}
		c.semaphore = make(chan struct{}, max)
	}
}

func NewClient(context Context, opts ...Option) *Client {
	client := &Client{
		context: context,
		limiter: &rateLimiter{},
		stats:   &apiStats{},
	}
	for _, opt := range opts {
		opt(client)
	}

	if client.semaphore == nil {
		client.semaphore = make(chan struct{}, defaultMaxConcurrency)
	}

	if client.httpClient == nil {
		// Base transport. Use a header timeout rather than http.Client.Timeout:
		// Client.Timeout covers reading the full body, so large artifact/log
		// downloads that take >60s would always fail. ResponseHeaderTimeout
		// still bounds unresponsive servers; per-call context deadlines bound
		// the rest.
		var baseTransport *http.Transport
		if t, ok := http.DefaultTransport.(*http.Transport); ok {
			baseTransport = t.Clone()
		} else {
			baseTransport = &http.Transport{}
		}
		baseTransport.ResponseHeaderTimeout = 60 * time.Second
		var base http.RoundTripper = baseTransport

		// Add rate limiting (MUST be behind cache)
		base = &RateLimitedTransport{
			Base:      base,
			Limiter:   client.limiter,
			Semaphore: client.semaphore,
			Stats:     client.stats,
		}

		// Add caching
		if client.context.CacheDir != "" {
			ct := NewCachedTransport(base, client.context.CacheDir)
			ct.Stats = client.stats
			base = ct
		}

		// Add OTel instrumentation
		base = otelhttp.NewTransport(base)

		client.httpClient = &http.Client{
			Transport: base,
		}
	}

	return client
}

type rateLimiter struct {
	mu        sync.Mutex
	remaining int
	resetTime time.Time
}

// rateLimitWaitNeeded is the RateLimitDecision WaitNeeded pure gate:
// remaining exhausted, reset known, and the reset is still in the future.
// Spec: specs/rate-limit/decision WaitNeeded → ratelimitspec.WaitNeeded.
// SSOT: production calls the generated pure (scalar encoding of duration).
func rateLimitWaitNeeded(remaining int, resetKnown bool, untilReset time.Duration) bool {
	resetAt := int64(0)
	if resetKnown {
		resetAt = 1
	}
	clock := int64(0)
	if untilReset <= 0 {
		// At or past reset: clock >= resetAt so WaitNeeded is false.
		clock = resetAt
	}
	return ratelimitspec.State{
		Remaining: int64(remaining),
		ResetAt:   resetAt,
		Clock:     clock,
	}.WaitNeeded()
}

// waitDuration computes how long the caller must wait before issuing a
// request. It only reads state under the lock; the caller sleeps outside
// the critical section so other goroutines are not blocked.
// Decision: rateLimitWaitNeeded / specs/rate-limit/decision WaitNeeded.
func (r *rateLimiter) waitDuration() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.resetTime.IsZero() {
		return 0
	}
	d := time.Until(r.resetTime)
	if !rateLimitWaitNeeded(r.remaining, true, d) {
		return 0
	}
	// +1s slack: a woken sleeper always observes a refilled window if the
	// header reset was accurate (rate-limit spec Tick / client.go).
	return d + time.Second
}

func (r *rateLimiter) waitIfNeeded(ctx context.Context) error {
	// Recheck after every sleep: while we slept another response may have
	// reported a newly exhausted window, and firing into it anyway would
	// join the post-reset stampede (rate-limit spec, wake-together race).
	for {
		d := r.waitDuration()
		if d <= 0 {
			return nil
		}
		// An exhausted primary limit can mean sleeping until the hourly reset —
		// say so instead of appearing frozen.
		if d > 5*time.Second {
			fmt.Fprintf(os.Stderr, "GitHub rate limit exhausted; waiting %s for reset (ctrl+c to abort)\n", d.Round(time.Second))
		}
		if err := sleepContext(ctx, d); err != nil {
			return err
		}
	}
}

// sleepContext sleeps for d or until ctx is cancelled, whichever comes first.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// snapshot reports the last-seen rate-limit state. known is false until a
// GitHub response has populated it (reset stays zero before then).
func (r *rateLimiter) snapshot() (remaining int, reset time.Time, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.remaining, r.resetTime, !r.resetTime.IsZero()
}

func (r *rateLimiter) updateFromHeaders(headers http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if remaining := headers.Get("x-ratelimit-remaining"); remaining != "" {
		if value, err := strconv.Atoi(remaining); err == nil {
			if value < 0 {
				// Hostile/buggy header: waitDuration tests remaining == 0
				// exactly, so a stored negative would disable waiting.
				value = 0
			}
			r.remaining = value
		}
	}
	if reset := headers.Get("x-ratelimit-reset"); reset != "" {
		if seconds, err := strconv.ParseInt(reset, 10, 64); err == nil && seconds > 0 {
			r.resetTime = time.Unix(seconds, 0)
		}
	}
}

type RateLimitedTransport struct {
	Base      http.RoundTripper
	Limiter   *rateLimiter
	Semaphore chan struct{}
	Stats     *apiStats
}

func (t *RateLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.countNetworkRequest()

	if err := t.takeSlot(req.Context()); err != nil {
		return nil, err
	}
	defer t.freeSlot()

	if err := t.Limiter.waitIfNeeded(req.Context()); err != nil {
		return nil, err
	}

	resp, err := t.send(req)
	if err != nil {
		return nil, err
	}
	return t.retryWhileRateLimited(req, resp)
}

// countNetworkRequest records a call that passed the cache: a miss or a
// conditional revalidation.
func (t *RateLimitedTransport) countNetworkRequest() {
	if t.Stats != nil {
		t.Stats.networkRequests.Add(1)
	}
}

func (t *RateLimitedTransport) takeSlot(ctx context.Context) error {
	select {
	case t.Semaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *RateLimitedTransport) freeSlot() { <-t.Semaphore }

func (t *RateLimitedTransport) send(req *http.Request) (*http.Response, error) {
	resp, err := t.Base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	t.Limiter.updateFromHeaders(resp.Header)
	return resp, nil
}

func (t *RateLimitedTransport) retryWhileRateLimited(req *http.Request, resp *http.Response) (*http.Response, error) {
	for attempt := 0; attempt < maxRetries && shouldRetry(resp); attempt++ {
		if err := pauseForRetry(req.Context(), attempt, resp); err != nil {
			return nil, err
		}
		if err := t.Limiter.waitIfNeeded(req.Context()); err != nil {
			return nil, err
		}
		next, err := t.send(req)
		if err != nil {
			return nil, err
		}
		resp = next
	}
	return resp, nil
}

func pauseForRetry(ctx context.Context, attempt int, resp *http.Response) error {
	delay := retryDelay(attempt, resp)
	fmt.Fprintf(os.Stderr, "Rate limited by GitHub API, retrying in %s (attempt %d/%d)\n", delay.Round(time.Millisecond), attempt+1, maxRetries)
	// Drain a bounded amount before closing so the keep-alive
	// connection can be reused for the retry.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	return sleepContext(ctx, delay)
}

func shouldRetry(resp *http.Response) bool {
	if resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if resp.StatusCode == http.StatusForbidden {
		if resp.Header.Get("x-ratelimit-remaining") == "0" {
			return true
		}
		if resp.Header.Get("Retry-After") != "" {
			return true
		}
	}
	return false
}

func waitDurationFromHeaders(resp *http.Response) time.Duration {
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	if reset := resp.Header.Get("x-ratelimit-reset"); reset != "" {
		if seconds, err := strconv.ParseInt(reset, 10, 64); err == nil {
			resetTime := time.Unix(seconds, 0)
			if d := time.Until(resetTime) + time.Second; d > 0 {
				return d
			}
		}
	}
	return 0
}

func retryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil {
		if d := waitDurationFromHeaders(resp); d > 0 {
			// Clamp header-derived delays (Retry-After / x-ratelimit-reset
			// can be up to an hour out) so a single retry never stalls the
			// transport longer than maxRetryDelay.
			if d > maxRetryDelay {
				d = maxRetryDelay
			}
			return d
		}
	}
	// Exponential backoff: 1s, 2s, 4s, 8s, 16s capped at maxRetryDelay
	base := time.Second * time.Duration(1<<uint(attempt))
	if base > maxRetryDelay {
		base = maxRetryDelay
	}
	// Add jitter: 0-25% of base
	jitter := time.Duration(rand.Int63n(int64(base / 4)))
	return base + jitter
}
