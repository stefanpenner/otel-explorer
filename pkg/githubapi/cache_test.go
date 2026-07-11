package githubapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCachedTransport(t *testing.T) {
	cacheDir, err := os.MkdirTemp("", "gha-cache-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
	}))
	defer server.Close()

	transport := NewCachedTransport(http.DefaultTransport, cacheDir)
	client := &http.Client{Transport: transport}

	// First call - should hit the server
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, callCount)

	// Second call - should hit the cache
	resp2, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, 1, callCount) // callCount should still be 1

	// Verify file exists
	files, _ := os.ReadDir(cacheDir)
	assert.Equal(t, 1, len(files))
}

func TestCachedTransportTTLExpiration(t *testing.T) {
	cacheDir, err := os.MkdirTemp("", "gha-cache-ttl-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
	}))
	defer server.Close()

	transport := NewCachedTransport(http.DefaultTransport, cacheDir)
	client := &http.Client{Transport: transport}

	// First call - should hit the server
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, callCount)

	// Verify cache file exists
	files, _ := os.ReadDir(cacheDir)
	assert.Equal(t, 1, len(files))
	cachedFileName := files[0].Name()

	// Backdate the cache file's modification time to be older than cacheTTL
	cachePath := cacheDir + "/" + cachedFileName
	staleTime := time.Now().Add(-(cacheTTL + time.Minute))
	err = os.Chtimes(cachePath, staleTime, staleTime)
	assert.NoError(t, err)

	// Second call - cache should be expired, should hit the server again
	resp2, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, 2, callCount)

	// Verify stale file was removed and a new cache file was created
	files2, _ := os.ReadDir(cacheDir)
	assert.Equal(t, 1, len(files2))
	assert.Equal(t, cachedFileName, files2[0].Name())

	// Verify the new cache file has a recent modification time
	info, err := os.Stat(cachePath)
	assert.NoError(t, err)
	assert.True(t, time.Since(info.ModTime()) < time.Minute, "new cache file should have a recent modification time")
}

func TestCacheKeyIncludesAuthorization(t *testing.T) {
	transport := NewCachedTransport(http.DefaultTransport, t.TempDir())

	reqA, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/o/r", nil)
	reqA.Header.Set("Authorization", "token aaa")
	reqB, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/o/r", nil)
	reqB.Header.Set("Authorization", "token bbb")

	assert.NotEqual(t, transport.getCacheKey(reqA), transport.getCacheKey(reqB),
		"responses fetched as one identity must not be served to another")

	// Same identity yields the same key.
	reqA2, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/o/r", nil)
	reqA2.Header.Set("Authorization", "token aaa")
	assert.Equal(t, transport.getCacheKey(reqA), transport.getCacheKey(reqA2))
}

func TestCachedTransportDoesNotShareAcrossTokens(t *testing.T) {
	cacheDir := t.TempDir()

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello " + r.Header.Get("Authorization")))
	}))
	defer server.Close()

	transport := NewCachedTransport(http.DefaultTransport, cacheDir)
	client := &http.Client{Transport: transport}

	reqA, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	reqA.Header.Set("Authorization", "token aaa")
	respA, err := client.Do(reqA)
	assert.NoError(t, err)
	bodyA, _ := io.ReadAll(respA.Body)
	respA.Body.Close()
	assert.Equal(t, 1, callCount)

	// Different token: must miss the cache and hit the server.
	reqB, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	reqB.Header.Set("Authorization", "token bbb")
	respB, err := client.Do(reqB)
	assert.NoError(t, err)
	bodyB, _ := io.ReadAll(respB.Body)
	respB.Body.Close()
	assert.Equal(t, 2, callCount, "request with a different token must not be served from another identity's cache")
	assert.NotEqual(t, string(bodyA), string(bodyB))
}

func TestCachedTransportRevalidatesWithETag(t *testing.T) {
	cacheDir := t.TempDir()

	var fullResponses, notModified int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			notModified++
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fullResponses++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world"))
	}))
	defer server.Close()

	transport := NewCachedTransport(http.DefaultTransport, cacheDir)
	client := &http.Client{Transport: transport}

	// First call: full 200, body cached along with its ETag.
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	assert.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello world", string(body))
	assert.Equal(t, 1, fullResponses)

	// Backdate the entry past the TTL so the next call must revalidate.
	files, _ := os.ReadDir(cacheDir)
	assert.Equal(t, 1, len(files))
	cachePath := cacheDir + "/" + files[0].Name()
	staleTime := time.Now().Add(-(cacheTTL + time.Minute))
	assert.NoError(t, os.Chtimes(cachePath, staleTime, staleTime))

	// Second call: stale entry carries an ETag, so the transport sends a
	// conditional request. The server answers 304, and the caller still gets
	// the cached body with a 200 — no full body transfer.
	resp2, err := client.Do(req)
	assert.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, "hello world", string(body2))
	assert.Equal(t, 1, fullResponses, "304 revalidation must not refetch the full body")
	assert.Equal(t, 1, notModified)

	// The 304 should have refreshed the entry's TTL, so an immediate third
	// call is served straight from cache without touching the server.
	resp3, err := client.Do(req)
	assert.NoError(t, err)
	body3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	assert.Equal(t, "hello world", string(body3))
	assert.Equal(t, 1, fullResponses)
	assert.Equal(t, 1, notModified, "refreshed entry should serve from cache, not revalidate again")

	// The caller's request must not be mutated with conditional headers.
	assert.Empty(t, req.Header.Get("If-None-Match"))
}

// rtFunc is a RoundTripper that never hits the network (unit-test double).
type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestCachedTransportImmutableCompletedRunSurvivesTTL(t *testing.T) {
	cacheDir := t.TempDir()
	callCount := 0
	body := `{"id":1,"status":"completed","conclusion":"success","name":"CI"}`
	base := rtFunc(func(req *http.Request) (*http.Response, error) {
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})
	transport := NewCachedTransport(base, cacheDir)
	client := &http.Client{Transport: transport}

	url := "https://api.github.com/repos/o/r/actions/runs/42"
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	assert.NoError(t, err)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, body, string(got))
	assert.Equal(t, 1, callCount)

	files, _ := os.ReadDir(cacheDir)
	assert.Len(t, files, 1)
	cachePath := cacheDir + "/" + files[0].Name()
	// Past short TTL, still within immutable window.
	stale := time.Now().Add(-(cacheTTL + time.Hour))
	assert.NoError(t, os.Chtimes(cachePath, stale, stale))

	resp2, err := client.Do(req)
	assert.NoError(t, err)
	got2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, body, string(got2))
	assert.Equal(t, 1, callCount, "completed run must stay cache-fresh past short TTL")
}

func TestCachedTransportInProgressRunExpires(t *testing.T) {
	cacheDir := t.TempDir()
	callCount := 0
	body := `{"id":1,"status":"in_progress","conclusion":null}`
	base := rtFunc(func(req *http.Request) (*http.Response, error) {
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})
	transport := NewCachedTransport(base, cacheDir)
	client := &http.Client{Transport: transport}

	url := "https://api.github.com/repos/o/r/actions/runs/99"
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	_, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	files, _ := os.ReadDir(cacheDir)
	cachePath := cacheDir + "/" + files[0].Name()
	stale := time.Now().Add(-(cacheTTL + time.Minute))
	assert.NoError(t, os.Chtimes(cachePath, stale, stale))

	_, err = client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount, "in_progress run must revalidate after short TTL")
}

func TestCachedTransportImmutableJobLogs(t *testing.T) {
	cacheDir := t.TempDir()
	callCount := 0
	logBody := "2026-01-01T00:00:00.0000000Z hello"
	base := rtFunc(func(req *http.Request) (*http.Response, error) {
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(logBody)),
			Request:    req,
		}, nil
	})
	transport := NewCachedTransport(base, cacheDir)
	client := &http.Client{Transport: transport}

	url := "https://api.github.com/repos/o/r/actions/jobs/7/logs"
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	_, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	files, _ := os.ReadDir(cacheDir)
	cachePath := cacheDir + "/" + files[0].Name()
	stale := time.Now().Add(-(cacheTTL + 2*time.Hour))
	assert.NoError(t, os.Chtimes(cachePath, stale, stale))

	resp, err := client.Do(req)
	assert.NoError(t, err)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, logBody, string(got))
	assert.Equal(t, 1, callCount, "job logs are immutable once fetched")
}

func TestCachedTransportConcurrentSameURL(t *testing.T) {
	cacheDir := t.TempDir()

	payload := strings.Repeat("abcdefgh", 64*1024) // 512KB body
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload))
	}))
	defer server.Close()

	transport := NewCachedTransport(http.DefaultTransport, cacheDir)
	client := &http.Client{Transport: transport}

	// Many goroutines hitting the same URL: every reader must observe either
	// a miss or a complete cached entry, never a torn partial write.
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
			resp, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				errs <- err
				return
			}
			if string(body) != payload {
				errs <- io.ErrUnexpectedEOF
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent cached request failed: %v", err)
	}

	// No temp files should be left behind.
	files, _ := os.ReadDir(cacheDir)
	for _, f := range files {
		assert.False(t, strings.HasPrefix(f.Name(), "tmp-"), "temp file %s left behind", f.Name())
	}
}
