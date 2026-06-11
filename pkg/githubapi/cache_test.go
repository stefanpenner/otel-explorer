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
