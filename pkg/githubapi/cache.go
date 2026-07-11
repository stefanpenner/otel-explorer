package githubapi

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// cacheTTL is the default freshness window for mutable GitHub responses
// (lists, in-progress runs, PR metadata, …).
const cacheTTL = 5 * time.Minute

// cacheTTLImmutable is how long completed GHA resources stay fresh without a
// network round-trip. Completed runs/jobs/logs do not change; re-fetching them
// wastes rate budget and wall clock. Cap avoids unbounded disk trust if GitHub
// ever mutates history (deletes, redactions).
const cacheTTLImmutable = 30 * 24 * time.Hour

// cacheVersion is incremented to invalidate all existing cached responses.
// Bump this when response parsing or enrichment logic changes.
const cacheVersion = "v5"

// CachedTransport implements http.RoundTripper and caches GET requests to disk.
type CachedTransport struct {
	Base     http.RoundTripper
	CacheDir string
	Stats    *apiStats // optional; counts requests served from cache without a network call
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

	// A fresh entry is served outright. A stale-but-present entry is kept so
	// we can revalidate it conditionally instead of refetching the full body.
	stored, fresh := t.readFromCache(cachePath, req)
	if fresh {
		if t.Stats != nil {
			t.Stats.cacheHits.Add(1)
		}
		return stored, nil
	}

	// If the stale entry carries validators, ask the server whether it still
	// holds (If-None-Match / If-Modified-Since). A 304 lets us reuse the body.
	outReq := req
	if stored != nil {
		if validators := revalidationHeaders(stored, req); len(validators) > 0 {
			outReq = req.Clone(req.Context())
			for k, v := range validators {
				outReq.Header.Set(k, v)
			}
		}
	}

	resp, err := t.Base.RoundTrip(outReq)
	if err != nil {
		if stored != nil {
			stored.Body.Close()
		}
		return nil, err
	}

	// Not Modified: the cached body is still valid. Refresh its TTL and serve
	// it, mapped back to a 200 so callers never see the 304.
	if resp.StatusCode == http.StatusNotModified && stored != nil {
		resp.Body.Close()
		t.touch(cachePath)
		return stored, nil
	}

	if stored != nil {
		stored.Body.Close()
	}

	// Only cache successful responses
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.saveToCache(cachePath, resp)
	}

	return resp, nil
}

// revalidationHeaders returns the conditional request headers derived from a
// stored response's validators, but only for validators the caller hasn't
// already set itself.
func revalidationHeaders(stored *http.Response, req *http.Request) map[string]string {
	h := map[string]string{}
	if etag := stored.Header.Get("ETag"); etag != "" && req.Header.Get("If-None-Match") == "" {
		h["If-None-Match"] = etag
	}
	if lm := stored.Header.Get("Last-Modified"); lm != "" && req.Header.Get("If-Modified-Since") == "" {
		h["If-Modified-Since"] = lm
	}
	return h
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

// readFromCache parses the cached response for req, if any. The bool reports
// whether the entry is still fresh (within the TTL). A stale-but-parseable
// entry is still returned (fresh=false) so the caller can revalidate it with
// a conditional request rather than refetching the full body.
func (t *CachedTransport) readFromCache(path string, req *http.Request) (*http.Response, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, false
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	// Parse the cached response
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), req)
	if err != nil {
		return nil, false
	}

	// Don't os.Remove stale entries here: another process may be concurrently
	// renaming a fresh entry into place, and removing would delete it. A stale
	// file is simply overwritten by the next save (or touched on a 304).
	age := time.Since(info.ModTime())
	fresh := age <= cacheTTL
	if !fresh && age <= cacheTTLImmutable && isImmutableCachedResponse(req, resp) {
		// Completed Actions resources are effectively write-once: serve without
		// revalidation until the immutable window ends (or cacheVersion bumps).
		fresh = true
	}
	return resp, fresh
}

// Single-run / single-job JSON paths (not list or nested collections).
var (
	reActionsRun  = regexp.MustCompile(`^/repos/[^/]+/[^/]+/actions/runs/\d+$`)
	reActionsJob  = regexp.MustCompile(`^/repos/[^/]+/[^/]+/actions/jobs/\d+$`)
	reActionsLogs = regexp.MustCompile(`^/repos/[^/]+/[^/]+/actions/jobs/\d+/logs$`)
)

// isImmutableCachedResponse reports whether this cached GET is safe to treat as
// long-lived. Only GitHub API completed workflow runs, completed jobs, and job
// logs qualify. List endpoints and in-progress resources stay on short TTL.
func isImmutableCachedResponse(req *http.Request, resp *http.Response) bool {
	if req == nil || resp == nil || resp.Body == nil {
		return false
	}
	host := req.URL.Host
	if host != "api.github.com" && !strings.HasSuffix(host, ".api.github.com") {
		return false
	}
	path := req.URL.Path
	switch {
	case reActionsLogs.MatchString(path):
		// Logs are only available after completion and do not change.
		return true
	case reActionsRun.MatchString(path), reActionsJob.MatchString(path):
		return bodyStatusCompleted(resp)
	default:
		return false
	}
}

// bodyStatusCompleted peeks at a JSON body for status=="completed" and restores
// resp.Body so the caller can still read it.
func bodyStatusCompleted(resp *http.Response) bool {
	raw, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	if err != nil || len(raw) == 0 {
		return false
	}
	var meta struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return false
	}
	return meta.Status == "completed"
}

// touch resets a cache entry's TTL by bumping its modification time to now,
// used after a 304 confirms the stored body is still current. Best-effort:
// a concurrent rename or removal racing with this is harmless.
func (t *CachedTransport) touch(path string) {
	now := time.Now()
	_ = os.Chtimes(path, now, now)
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
