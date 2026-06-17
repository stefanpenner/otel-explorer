package githubapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResponse(req *http.Request, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func TestRateLimiterWaitRespectsContextCancellation(t *testing.T) {
	limiter := &rateLimiter{
		remaining: 0,
		resetTime: time.Now().Add(time.Hour),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := limiter.waitIfNeeded(ctx)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 5*time.Second, "wait should abort promptly on context cancellation, not sleep until reset")
}

func TestRateLimiterWaitDoesNotHoldLockWhileSleeping(t *testing.T) {
	limiter := &rateLimiter{
		remaining: 0,
		resetTime: time.Now().Add(time.Hour),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waiting := make(chan struct{})
	go func() {
		close(waiting)
		_ = limiter.waitIfNeeded(ctx)
	}()
	<-waiting
	time.Sleep(10 * time.Millisecond) // let the goroutine enter its sleep

	// updateFromHeaders must not block behind the sleeping waiter.
	done := make(chan struct{})
	go func() {
		headers := http.Header{}
		headers.Set("x-ratelimit-remaining", "100")
		limiter.updateFromHeaders(headers)
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("updateFromHeaders blocked behind a sleeping waitIfNeeded; mutex is held during sleep")
	}
}

func TestRateLimiterNoWaitWhenRemaining(t *testing.T) {
	limiter := &rateLimiter{
		remaining: 10,
		resetTime: time.Now().Add(time.Hour),
	}
	start := time.Now()
	err := limiter.waitIfNeeded(context.Background())
	assert.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

func TestRetryDelayClampsHeaderDelay(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("Retry-After", "3600")

	d := retryDelay(0, resp)
	assert.Equal(t, maxRetryDelay, d, "header-derived delay should be clamped to maxRetryDelay")

	resp2 := &http.Response{Header: http.Header{}}
	resp2.Header.Set("x-ratelimit-reset", fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()))
	d2 := retryDelay(0, resp2)
	assert.LessOrEqual(t, d2, maxRetryDelay)

	// Short header delays are honored as-is.
	resp3 := &http.Response{Header: http.Header{}}
	resp3.Header.Set("Retry-After", "3")
	assert.Equal(t, 3*time.Second, retryDelay(0, resp3))
}

func TestRoundTripRetrySleepRespectsContext(t *testing.T) {
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Set("Retry-After", "30")
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     "429 Too Many Requests",
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("slow down")),
			Request:    req,
		}, nil
	})

	transport := &RateLimitedTransport{
		Base:      base,
		Limiter:   &rateLimiter{},
		Semaphore: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/test", nil)
	assert.NoError(t, err)

	start := time.Now()
	resp, err := transport.RoundTrip(req)
	elapsed := time.Since(start)

	assert.Nil(t, resp)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 5*time.Second, "retry sleep should abort promptly on context cancellation")
}

// newPaginatedTestClient returns a Client whose transport serves page1 with a
// Link rel=next header pointing at nextURL, and page2 for nextURL itself.
func newPaginatedTestClient(t *testing.T, page1, page2, nextURL string) *Client {
	t.Helper()
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() == nextURL {
			return jsonResponse(req, page2, nil), nil
		}
		header := http.Header{}
		header.Set("Link", fmt.Sprintf(`<%s>; rel="next"`, nextURL))
		return jsonResponse(req, page1, header), nil
	})
	return NewClient(NewContext("test-token"), WithHTTPClient(&http.Client{Transport: rt}))
}

func TestFetchCheckRunsForCommitFollowsPagination(t *testing.T) {
	client := newPaginatedTestClient(t,
		`{"total_count": 2, "check_runs": [{"id": 1, "name": "build"}]}`,
		`{"total_count": 2, "check_runs": [{"id": 2, "name": "test"}]}`,
		"https://api.github.com/repos/o/r/commits/abc/check-runs?per_page=100&page=2",
	)

	runs, err := client.FetchCheckRunsForCommit(context.Background(), "o", "r", "abc")
	assert.NoError(t, err)
	assert.Len(t, runs, 2)
	assert.Equal(t, int64(1), runs[0].ID)
	assert.Equal(t, int64(2), runs[1].ID)
}

func TestListArtifactsFollowsPagination(t *testing.T) {
	client := newPaginatedTestClient(t,
		`{"total_count": 2, "artifacts": [{"id": 1, "name": "a", "size_in_bytes": 10}]}`,
		`{"total_count": 2, "artifacts": [{"id": 2, "name": "b", "size_in_bytes": 20}]}`,
		"https://api.github.com/repos/o/r/actions/runs/7/artifacts?per_page=100&page=2",
	)

	artifacts, err := client.ListArtifacts(context.Background(), "o", "r", 7)
	assert.NoError(t, err)
	assert.Len(t, artifacts, 2)
	assert.Equal(t, "a", artifacts[0].Name)
	assert.Equal(t, "b", artifacts[1].Name)
}

func TestFetchAnnotationsFollowsPagination(t *testing.T) {
	client := newPaginatedTestClient(t,
		`[{"path": "a.go", "message": "first"}]`,
		`[{"path": "b.go", "message": "second"}]`,
		"https://api.github.com/repos/o/r/check-runs/9/annotations?per_page=100&page=2",
	)

	annotations, err := client.FetchAnnotations(context.Background(), "o", "r", 9)
	assert.NoError(t, err)
	assert.Len(t, annotations, 2)
	assert.Equal(t, "first", annotations[0].Message)
	assert.Equal(t, "second", annotations[1].Message)
}

func TestFetchCommitAssociatedPRsFollowsPagination(t *testing.T) {
	client := newPaginatedTestClient(t,
		`[{"number": 1, "title": "one"}]`,
		`[{"number": 2, "title": "two"}]`,
		"https://api.github.com/repos/o/r/commits/abc/pulls?per_page=100&page=2",
	)

	prs, err := client.FetchCommitAssociatedPRs(context.Background(), "o", "r", "abc")
	assert.NoError(t, err)
	assert.Len(t, prs, 2)
	assert.Equal(t, 1, prs[0].Number)
	assert.Equal(t, 2, prs[1].Number)
}

func TestRoundTripDrainsBodyBeforeRetry(t *testing.T) {
	attempts := 0
	bodyDrained := false
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			header := http.Header{}
			header.Set("Retry-After", "0") // invalid (<=0) so exponential backoff (1s+jitter) applies
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Status:     "429 Too Many Requests",
				Header:     header,
				Body: &drainTrackingBody{
					Reader:  strings.NewReader("limited"),
					drained: &bodyDrained,
				},
				Request: req,
			}, nil
		}
		return jsonResponse(req, `{}`, nil), nil
	})

	transport := &RateLimitedTransport{
		Base:      base,
		Limiter:   &rateLimiter{},
		Semaphore: make(chan struct{}, 1),
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.github.com/test", nil)
	assert.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, attempts)
	assert.True(t, bodyDrained, "retry should drain the body before closing so the connection can be reused")
	_ = resp.Body.Close()
}

type drainTrackingBody struct {
	*strings.Reader
	drained *bool
}

func (b *drainTrackingBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF {
		*b.drained = true
	}
	return n, err
}

func (b *drainTrackingBody) Close() error { return nil }
