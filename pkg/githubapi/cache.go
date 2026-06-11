package githubapi

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// cacheTTL controls how long cached responses remain valid.
const cacheTTL = 5 * time.Minute

// cacheVersion is incremented to invalidate all existing cached responses.
// Bump this when response parsing or enrichment logic changes.
const cacheVersion = "v4"

// CachedTransport implements http.RoundTripper and caches GET requests to disk.
type CachedTransport struct {
	Base     http.RoundTripper
	CacheDir string
}

func NewCachedTransport(base http.RoundTripper, cacheDir string) *CachedTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &CachedTransport{
		Base:     base,
		CacheDir: cacheDir,
	}
}

func (t *CachedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only cache GET requests
	if req.Method != http.MethodGet {
		return t.Base.RoundTrip(req)
	}

	cacheKey := t.getCacheKey(req)
	cachePath := filepath.Join(t.CacheDir, cacheKey)

	// Try to read from cache
	if cachedResp := t.getFromCache(cachePath, req); cachedResp != nil {
		return cachedResp, nil
	}

	// Not in cache, perform request
	resp, err := t.Base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Only cache successful responses
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.saveToCache(cachePath, resp)
	}

	return resp, nil
}

func (t *CachedTransport) getCacheKey(req *http.Request) string {
	// Use version + URL + headers to create a unique hash.
	// Bumping cacheVersion invalidates all existing entries.
	h := sha256.New()
	h.Write([]byte(cacheVersion))
	// Strip query params from signed blob URLs (e.g. Azure artifact downloads).
	// GitHub redirects artifact downloads to signed URLs with expiring tokens,
	// so the query string changes each request while the path stays the same.
	u := req.URL.String()
	if strings.Contains(req.URL.Host, "blob.core.windows.net") {
		u = req.URL.Scheme + "://" + req.URL.Host + req.URL.Path
	}
	h.Write([]byte(u))
	h.Write([]byte(req.Header.Get("Accept")))
	// Include the auth credential so responses fetched as one identity are
	// never served to another (the key is a hash, so the token itself is
	// not exposed on disk).
	h.Write([]byte(req.Header.Get("Authorization")))
	return hex.EncodeToString(h.Sum(nil))
}

func (t *CachedTransport) getFromCache(path string, req *http.Request) *http.Response {
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}

	// Expire stale entries. Don't os.Remove here: another process may be
	// concurrently renaming a fresh entry into place, and removing would
	// delete it. The stale file is simply overwritten by the next save.
	if time.Since(info.ModTime()) > cacheTTL {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	// Parse the cached response
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), req)
	if err != nil {
		return nil
	}

	return resp
}

func (t *CachedTransport) saveToCache(path string, resp *http.Response) {
	if err := os.MkdirAll(t.CacheDir, 0755); err != nil {
		return
	}

	// We need to dump the response to save it. 
	// DumpResponse reads and CLOSES the body if it's not already buffered.
	// But it actually returns the full bytes including the body.
	dump, err := httputil.DumpResponse(resp, true)
	if err != nil {
		return
	}

	// Write to a temp file and rename it into place. Rename is atomic on
	// POSIX, so concurrent readers (other goroutines or other ote processes
	// sharing the cache dir) never observe a torn, partially written entry.
	t.writeFileAtomic(path, dump)

	// Since DumpResponse consumed the body, we MUST restore it so the 
	// caller of RoundTrip can still read it.
	// We parse it back to a temporary response to get the body out.
	restored, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(dump)), resp.Request)
	if err == nil {
		resp.Body = restored.Body
	}
}

// writeFileAtomic writes data to a temp file in the cache directory and
// renames it over path so readers always see a complete entry.
func (t *CachedTransport) writeFileAtomic(path string, data []byte) {
	tmp, err := os.CreateTemp(t.CacheDir, "tmp-*")
	if err != nil {
		return
	}
	_, werr := tmp.Write(data)
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(tmp.Name())
		return
	}
	_ = os.Chmod(tmp.Name(), 0644)
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
	}
}

// ClearCache removes all cached files.
func (t *CachedTransport) ClearCache() error {
	return os.RemoveAll(t.CacheDir)
}

func (t *CachedTransport) IsCached(req *http.Request) bool {
	cacheKey := t.getCacheKey(req)
	cachePath := filepath.Join(t.CacheDir, cacheKey)
	_, err := os.Stat(cachePath)
	return err == nil
}
