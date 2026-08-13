package git

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Handle dispatches a Git smart HTTP request to the appropriate cache or
// upstream path.
//
// Decision tree:
//   - /info/refs?service=git-upload-pack  → handleInfoRefs (cacheable)
//   - /info/refs?service=git-receive-pack → forwardDirect (push refs never cached)
//   - /git-upload-pack POST              → handleUploadPack (potentially cacheable)
//   - /git-receive-pack POST             → handleReceivePack (forward + invalidate)
func (h *Handler) Handle(w http.ResponseWriter, r *http.Request) {
	host := extractHost(r)
	path := r.URL.Path
	log := h.log.WithPrefix("[git]")

	log.Info("git handler: %s %s host=%s", r.Method, path, host)

	switch {
	case strings.HasSuffix(path, "/info/refs"):
		svc := r.URL.Query().Get("service")
		switch svc {
		case "git-upload-pack":
			log.Debug("git dispatch → handleInfoRefs (upload-pack) host=%s", host)
			h.handleInfoRefs(w, r)
		case "git-receive-pack":
			log.Debug("git dispatch → forwardDirect (receive-pack refs) host=%s", host)
			h.forwardDirect(w, r, nil)
		default:
			log.Debug("git dispatch → forwardDirect (unknown service=%q) host=%s", svc, host)
			h.forwardDirect(w, r, nil)
		}

	case strings.HasSuffix(path, "/git-upload-pack"):
		log.Debug("git dispatch → handleUploadPack host=%s", host)
		h.handleUploadPack(w, r)

	case strings.HasSuffix(path, "/git-receive-pack"):
		log.Debug("git dispatch → handleReceivePack host=%s", host)
		h.handleReceivePack(w, r)

	default:
		log.Debug("git dispatch → forwardDirect (no match) host=%s path=%s", host, path)
		h.forwardDirect(w, r, nil)
	}
}

// ServeHTTP lets Handler act as an HTTP proxy endpoint.
//
// If the client sent an absolute-form proxy request, normalize it to origin-form
// before dispatching so the rest of the handler can operate on a regular request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if u, err := url.Parse(r.RequestURI); err == nil && u.Scheme != "" && u.Host != "" {
		r = r.Clone(r.Context())
		r.URL = u
		r.Host = u.Host
		r.RequestURI = u.RequestURI()
	}
	h.Handle(w, r)
}

// handleInfoRefs handles GET /info/refs?service=git-upload-pack.
// Caches the refs/capability response (L1) with the configured TTL.
// Works for both protocol v1 (refs advertisement) and v2 (capability advertisement).
func (h *Handler) handleInfoRefs(w http.ResponseWriter, r *http.Request) {
	host := extractHost(r)
	repoPath := parseRepoPath(r.URL.Path)
	v2 := isProtocolV2(r)
	log := h.log.WithPrefix("[git]")

	if v2 {
		log.Debug("git v2 info/refs (capability advertisement) host=%s path=%s", host, repoPath)
	}

	// Check if caching is enabled for this host.
	if !h.isAllowedHost(host) {
		h.forwardDirect(w, r, nil)
		return
	}

	authScope := authScopeFromRequest(r)
	key := refsCacheKey(host, repoPath, authScope)

	// Try cache first (GetOrFetch handles singleflight coalescing).
	refsCacheMiss := false
	rc, err := h.refsCache.GetOrFetch(key, func() (io.ReadCloser, time.Duration, error) {
		refsCacheMiss = true
		log.Info("git refs L1 MISS host=%s path=%s → fetching from upstream", host, repoPath)

		resp, fetchErr := h.doUpstreamRequest(r)
		if fetchErr != nil {
			return nil, 0, fetchErr
		}

		if resp.StatusCode != http.StatusOK {
			// Don't cache error responses.
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Error("git upstream refs fetch failed host=%s path=%s status=%d", host, repoPath, resp.StatusCode)
			return nil, 0, &upstreamError{status: resp.StatusCode, body: body, header: resp.Header}
		}

		return resp.Body, 0, nil // TTL = 0 means use cache default (RefsTTL)
	})

	if err != nil {
		if ue, ok := err.(*upstreamError); ok {
			h.serveUpstreamError(w, ue)
			return
		}
		log.Error("git refs cache error host=%s path=%s: %v", host, repoPath, err)
		h.writeError(w, http.StatusBadGateway, "git proxy: upstream fetch failed")
		return
	}
	defer rc.Close()

	if !refsCacheMiss {
		log.Info("git refs L1 HIT host=%s path=%s → serving from cache", host, repoPath)
	}

	// Serve from cache.
	w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// handleUploadPack handles POST /git-upload-pack.
// Supports both protocol v1 and v2:
//   - v1: parse want/have/done lines directly
//   - v2 ls-refs: cache the response as L1 refs
//   - v2 fetch: parse want/have/done after delimiter, same caching as v1
//
// For fresh clones (no "have" lines), attempts to serve from L2 pack cache.
// Incremental fetches, shallow clones, and partial clones are forwarded directly.
func (h *Handler) handleUploadPack(w http.ResponseWriter, r *http.Request) {
	host := extractHost(r)
	repoPath := parseRepoPath(r.URL.Path)
	v2 := isProtocolV2(r)
	log := h.log.WithPrefix("[git]")

	// Buffer the request body for parsing.
	body, err := io.ReadAll(io.LimitReader(r.Body, h.cfg.MaxUploadPackRequestSize+1))
	if err != nil {
		log.Debug("git upload-pack request body read error, forwarding direct host=%s path=%s: %v", host, repoPath, err)
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		h.forwardDirect(w, r, nil)
		return
	}
	if int64(len(body)) > h.cfg.MaxUploadPackRequestSize {
		log.Debug("git upload-pack request too large for caching, forwarding direct host=%s path=%s", host, repoPath)
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		h.forwardDirect(w, r, nil)
		return
	}

	// The request body may be gzip-compressed (Content-Encoding: gzip).
	// We need the raw body for forwarding upstream, but decompressed for parsing.
	parseBody := body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, gzErr := gzip.NewReader(bytes.NewReader(body))
		if gzErr == nil {
			decompressed, readErr := io.ReadAll(io.LimitReader(gz, h.cfg.MaxUploadPackRequestSize))
			gz.Close()
			if readErr == nil {
				parseBody = decompressed
			}
		}
	}

	// Parse the request — different parsers for v1 vs v2.
	var req uploadPackRequest
	var parseErr error
	if v2 {
		req, parseErr = parseV2UploadPackRequest(parseBody)
	} else {
		req, parseErr = parseUploadPackRequest(parseBody)
	}
	if parseErr != nil {
		log.Debug("git upload-pack parse error, forwarding direct host=%s path=%s: %v", host, repoPath, parseErr)
		h.forwardDirect(w, r, body)
		return
	}

	// v2 ls-refs command: cache the response as L1 refs (equivalent to v1 info/refs).
	if v2 && req.v2Command == "ls-refs" {
		h.handleV2LsRefs(w, r, body, host, repoPath)
		return
	}

	// For v2 fetch with filters (partial clone), forward directly.
	if req.hasFilter {
		log.Debug("git partial clone (filter) detected, forwarding direct host=%s path=%s", host, repoPath)
		h.forwardDirect(w, r, body)
		return
	}

	if !req.isFreshClone {
		log.Info("git incremental fetch, forwarding direct host=%s path=%s v2=%v", host, repoPath, v2)
		h.forwardDirect(w, r, body)
		return
	}

	if req.isShallow {
		log.Debug("git shallow clone detected, forwarding direct host=%s path=%s", host, repoPath)
		h.forwardDirect(w, r, body)
		return
	}

	// Check if caching is enabled for this host.
	if !h.isAllowedHost(host) {
		h.forwardDirect(w, r, body)
		return
	}

	log.Info("git fresh clone detected host=%s path=%s wants=%d v2=%v → checking pack cache", host, repoPath, len(req.wants), v2)

	// Get the fingerprint from the L1 refs cache.
	// For v1, the fingerprint comes from the info/refs response.
	// For v2, it comes from the ls-refs response (which has the actual ref data).
	authScope := authScopeFromRequest(r)
	refsPath, refsOK := h.refsCache.Get(refsCacheKey(host, repoPath, authScope))
	if !refsOK {
		// Try the v2 ls-refs cache key — v2 stores ref data there.
		refsPath, refsOK = h.refsCache.Get(lsRefsCacheKey(host, repoPath, authScope))
	}
	if !refsOK {
		// No L1 entry: we can't compute a fingerprint. Forward directly.
		// This happens when the upload-pack arrives before info/refs was cached
		// (e.g., the refs cache entry expired between the two requests).
		log.Info("git pack L2 SKIP host=%s path=%s → no L1 refs entry for fingerprint (refs expired?), forwarding to upstream", host, repoPath)
		h.forwardDirect(w, r, body)
		return
	}

	fingerprint, fpErr := fingerprintFromFile(refsPath)
	if fpErr != nil {
		log.Warn("git fingerprint error, forwarding direct host=%s path=%s: %v", host, repoPath, fpErr)
		h.forwardDirect(w, r, body)
		return
	}

	// Check L2 pack cache.
	pKey := packCacheKey(host, repoPath, authScope, fingerprint)

	packPath, packOK := h.packsCache.Get(pKey)
	if packOK {
		fi, _ := os.Stat(packPath)
		var sizeStr string
		if fi != nil {
			sizeStr = fmt.Sprintf(" size=%d", fi.Size())
		}
		log.Info("git pack L2 HIT host=%s path=%s fingerprint=%s%s → serving from disk cache", host, repoPath, fingerprint[:12], sizeStr)
		h.servePackFromDisk(w, packPath)
		return
	}

	// Singleflight: if another goroutine is already fetching this pack,
	// wait for it to finish and then serve from cache instead of making
	// a duplicate upstream request.
	flight := &packFlight{done: make(chan struct{})}
	if existing, loaded := h.packFlights.LoadOrStore(pKey, flight); loaded {
		// Another fetch is in progress — wait for it.
		existingFlight := existing.(*packFlight)
		log.Debug("git pack L2 WAIT host=%s path=%s fingerprint=%s → another fetch in progress", host, repoPath, fingerprint[:12])
		<-existingFlight.done
		// Re-check cache — the other goroutine should have cached it.
		if packPath, ok := h.packsCache.Get(pKey); ok {
			log.Info("git pack L2 HIT (after wait) host=%s path=%s fingerprint=%s → serving from disk cache", host, repoPath, fingerprint[:12])
			h.servePackFromDisk(w, packPath)
			return
		}
		// Other goroutine failed — fall through to fetch ourselves.
		log.Debug("git pack L2 MISS (after wait) host=%s path=%s → fetching from upstream", host, repoPath)
	}
	// We are the leader. Clean up the flight when done.
	defer func() {
		close(flight.done)
		h.packFlights.Delete(pKey)
	}()

	log.Info("git pack L2 MISS host=%s path=%s fingerprint=%s → fetching from upstream", host, repoPath, fingerprint[:12])

	resp, upErr := h.doUpstreamRequestWithBody(r, body)
	if upErr != nil {
		log.Error("git upstream upload-pack failed host=%s path=%s: %v", host, repoPath, upErr)
		h.writeError(w, http.StatusBadGateway, "git proxy: upstream fetch failed")
		return
	}

	if resp.StatusCode != http.StatusOK {
		h.serveUpstreamResponse(w, resp)
		return
	}

	// Stream from upstream while caching to disk (TeeReader pattern).
	h.streamAndCache(w, resp, pKey, host, repoPath)
}

// handleV2LsRefs handles a protocol v2 "ls-refs" command.
// The response is cached as L1 refs (equivalent to v1 info/refs) — it contains
// the ref list that determines the fingerprint for L2 pack caching.
func (h *Handler) handleV2LsRefs(w http.ResponseWriter, r *http.Request, body []byte, host, repoPath string) {
	if !h.isAllowedHost(host) {
		h.forwardDirect(w, r, body)
		return
	}
	log := h.log.WithPrefix("[git]")

	authScope := authScopeFromRequest(r)
	key := lsRefsCacheKey(host, repoPath, authScope)

	refsCacheMiss := false
	rc, err := h.refsCache.GetOrFetch(key, func() (io.ReadCloser, time.Duration, error) {
		refsCacheMiss = true
		log.Info("git v2 ls-refs L1 MISS host=%s path=%s → fetching from upstream", host, repoPath)

		resp, fetchErr := h.doUpstreamRequestWithBody(r, body)
		if fetchErr != nil {
			return nil, 0, fetchErr
		}

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Error("git upstream v2 ls-refs fetch failed host=%s path=%s status=%d", host, repoPath, resp.StatusCode)
			return nil, 0, &upstreamError{status: resp.StatusCode, body: respBody, header: resp.Header}
		}

		return resp.Body, 0, nil
	})

	if err != nil {
		if ue, ok := err.(*upstreamError); ok {
			h.serveUpstreamError(w, ue)
			return
		}
		log.Error("git v2 ls-refs cache error host=%s path=%s: %v", host, repoPath, err)
		h.writeError(w, http.StatusBadGateway, "git proxy: upstream fetch failed")
		return
	}
	defer rc.Close()

	if !refsCacheMiss {
		log.Info("git v2 ls-refs L1 HIT host=%s path=%s → serving from cache", host, repoPath)
	}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// handleReceivePack handles POST /git-receive-pack.
// Push is never cached. On success (HTTP 200), invalidates the L1 refs cache.
func (h *Handler) handleReceivePack(w http.ResponseWriter, r *http.Request) {
	host := extractHost(r)
	repoPath := parseRepoPath(r.URL.Path)
	log := h.log.WithPrefix("[git]")

	log.Info("git push forwarding direct host=%s path=%s", host, repoPath)

	resp, err := h.doUpstreamRequest(r)
	if err != nil {
		log.Error("git upstream receive-pack failed host=%s path=%s: %v", host, repoPath, err)
		h.writeError(w, http.StatusBadGateway, "git proxy: upstream push failed")
		return
	}
	defer resp.Body.Close()

	// On successful push, invalidate ALL L1 refs entries for this repo
	// across all auth scopes. Different credentials may see the same refs,
	// so we must invalidate broadly.
	if resp.StatusCode == http.StatusOK {
		refsRemoved := h.refsCache.RemoveByPrefix(repoRefsPrefix(host, repoPath))
		lsRefsRemoved := h.refsCache.RemoveByPrefix(repoLsRefsPrefix(host, repoPath))
		h.log.Info("git refs cache invalidated after push host=%s path=%s removed=%d", host, repoPath, refsRemoved+lsRefsRemoved)
	}

	// Forward the response to the container.
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// servePackFromDisk serves a cached pack file to the client.
func (h *Handler) servePackFromDisk(w http.ResponseWriter, path string) {
	f, err := os.Open(path)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "git proxy: failed to open cached pack")
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
}

// streamAndCache fetches from upstream, serves to w, and caches to disk simultaneously.
// Uses TeeReader: upstream response flows to client AND to disk.
func (h *Handler) streamAndCache(
	w http.ResponseWriter,
	upstreamResp *http.Response,
	packKey, host, repoPath string,
) {
	defer upstreamResp.Body.Close()
	log := h.log.WithPrefix("[git]")

	// Prepare temp file for the cache.
	hash := cacheKeyHash(packKey)
	subDir := filepath.Join(h.packsCache.Dir(), hash[:2])
	finalPath := filepath.Join(subDir, hash)
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		// Cache is unavailable; just stream to client.
		log.Warn("git pack cache unavailable, streaming direct host=%s path=%s: %v", host, repoPath, err)
		copyResponseHeaders(w.Header(), upstreamResp.Header)
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, upstreamResp.Body)
		return
	}

	// C2 fix: Use os.CreateTemp with a unique suffix so concurrent goroutines
	// writing to the same pack key each get their own temp file. The last
	// os.Rename wins atomically — no interleaved writes, no corruption.
	tmpFile, err := os.CreateTemp(subDir, ".tmp-"+hash[:16]+"-*")
	if err != nil {
		log.Warn("git pack cache write failed, streaming direct host=%s path=%s: %v", host, repoPath, err)
		copyResponseHeaders(w.Header(), upstreamResp.Header)
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, upstreamResp.Body)
		return
	}
	tmpPath := tmpFile.Name()

	// TeeReader: reads from upstream, writes to tmpFile (or countingWriter), returns data for client.
	// Only one TeeReader is created to avoid double-writing to tmpFile.
	var teeReader io.Reader
	var countingWriter *countWriter
	if h.cfg.MaxPackCacheEntrySize > 0 {
		countingWriter = &countWriter{w: tmpFile, limit: h.cfg.MaxPackCacheEntrySize}
		teeReader = io.TeeReader(upstreamResp.Body, countingWriter)
	} else {
		teeReader = io.TeeReader(upstreamResp.Body, tmpFile)
	}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.WriteHeader(http.StatusOK)

	written, copyErr := io.Copy(w, teeReader)
	closeErr := tmpFile.Close()

	// Check if the pack exceeded the size limit.
	if countingWriter != nil && countingWriter.exceeded {
		_ = os.Remove(tmpPath)
		// Safety drain: when the countWriter stopped writing to disk (limit
		// exceeded), the TeeReader's io.Copy may have returned early if the
		// countWriter's discard caused the upstream body to be only partially
		// consumed. This io.Copy ensures the client receives ALL remaining
		// upstream bytes even though we're not caching them. We only attempt
		// this when the prior copy succeeded (copyErr == nil) — if the client
		// already disconnected, draining would be pointless.
		if copyErr == nil {
			_, _ = io.Copy(w, upstreamResp.Body)
		}
		log.Info("git pack too large for cache, skipped caching host=%s path=%s written=%d max=%d",
			host, repoPath, written, h.cfg.MaxPackCacheEntrySize)
		return
	}

	if copyErr != nil || closeErr != nil {
		// Client disconnected or disk error: remove partial file.
		_ = os.Remove(tmpPath)
		log.Warn("git pack cache write failed host=%s path=%s written=%d copyErr=%v closeErr=%v",
			host, repoPath, written, copyErr, closeErr)
		return
	}

	// Atomically rename temp → final path and register with cache metadata.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		log.Warn("git pack cache rename failed host=%s path=%s: %v", host, repoPath, err)
		return
	}

	fpSlice := packKey[strings.LastIndex(packKey, ":")+1:]
	if len(fpSlice) > 12 {
		fpSlice = fpSlice[:12]
	}
	h.packsCache.RegisterAfterStream(packKey, finalPath, written)
	log.Info("git pack L2 CACHED host=%s path=%s fingerprint=%s size=%d → stored to disk, next clone will be a HIT", host, repoPath, fpSlice, written)
}

// forwardDirect forwards a request to upstream without any caching.
// If body is non-nil, it is used as the request body (for pre-buffered POST bodies).
func (h *Handler) forwardDirect(w http.ResponseWriter, r *http.Request, body []byte) {
	var resp *http.Response
	var err error

	if body != nil {
		resp, err = h.doUpstreamRequestWithBody(r, body)
	} else {
		resp, err = h.doUpstreamRequest(r)
	}
	if err != nil {
		// M2 fix: Log the upstream error for debugging.
		log := h.log.WithPrefix("[git]")
		log.Error("git upstream request failed: %v", err)
		h.writeError(w, http.StatusBadGateway, "git proxy: upstream request failed")
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// doUpstreamRequest creates and sends an upstream request using the original
// request's headers (which already have real credentials injected by InjectSecrets).
func (h *Handler) doUpstreamRequest(r *http.Request) (*http.Response, error) {
	scheme := "https"
	if r.TLS == nil {
		scheme = "http"
	}
	targetURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.RequestURI)

	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	if err != nil {
		return nil, fmt.Errorf("create upstream request: %w", err)
	}
	copyRequestHeaders(upstreamReq.Header, r.Header)
	upstreamReq.Host = r.Host

	return h.client.Do(upstreamReq)
}

// doUpstreamRequestWithBody creates and sends an upstream request using a
// pre-buffered body (for upload-pack requests that were buffered for parsing).
func (h *Handler) doUpstreamRequestWithBody(r *http.Request, body []byte) (*http.Response, error) {
	scheme := "https"
	if r.TLS == nil {
		scheme = "http"
	}
	targetURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.RequestURI)

	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create upstream request: %w", err)
	}
	upstreamReq.ContentLength = int64(len(body))
	copyRequestHeaders(upstreamReq.Header, r.Header)
	upstreamReq.Host = r.Host

	return h.client.Do(upstreamReq)
}

// serveUpstreamResponse forwards an upstream response to the client unchanged.
func (h *Handler) serveUpstreamResponse(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// serveUpstreamError writes a cached upstream error response to the client.
func (h *Handler) serveUpstreamError(w http.ResponseWriter, ue *upstreamError) {
	copyResponseHeaders(w.Header(), ue.header)
	w.WriteHeader(ue.status)
	_, _ = w.Write(ue.body)
}

// writeError writes a JSON error response.
func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error": %q}`, message)
}

// upstreamError wraps a non-200 upstream response for returning through GetOrFetch.
type upstreamError struct {
	status int
	body   []byte
	header http.Header
}

func (e *upstreamError) Error() string {
	return fmt.Sprintf("upstream returned status %d", e.status)
}

// countWriter wraps an io.Writer and tracks bytes written.
// When the limit is exceeded, it stops writing but does not return an error
// (so the TeeReader continues flowing to the client).
type countWriter struct {
	w        io.Writer
	written  int64
	limit    int64
	exceeded bool
}

func (cw *countWriter) Write(p []byte) (int, error) {
	if cw.exceeded {
		// Discard but report success so TeeReader keeps flowing to client.
		return len(p), nil
	}
	cw.written += int64(len(p))
	if cw.limit > 0 && cw.written > cw.limit {
		cw.exceeded = true
		return len(p), nil // discard, don't write to disk anymore
	}
	return cw.w.Write(p)
}

// extractHost extracts the hostname from the request (without port).
func extractHost(r *http.Request) string {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return host
}

// I5 fix: copyFilteredHeaders copies HTTP headers, skipping static hop-by-hop headers
// AND any additional headers nominated by the Connection header (RFC 7230 §6.1).
// This matches the filtering logic in proxy.go's copyHeaders.
func copyFilteredHeaders(dst, src http.Header) {
	if dst == nil || src == nil {
		return
	}

	// Build the skip set from static hop-by-hop headers.
	skip := make(map[string]struct{}, len(staticHopByHop)+4)
	for _, h := range staticHopByHop {
		skip[h] = struct{}{}
	}

	// Also skip any headers nominated by the Connection header.
	if connection := src.Get("Connection"); connection != "" {
		parts := strings.SplitSeq(connection, ",")
		for part := range parts {
			name := strings.ToLower(strings.TrimSpace(part))
			if name != "" {
				skip[name] = struct{}{}
			}
		}
	}

	for key, values := range src {
		if _, ok := skip[strings.ToLower(key)]; ok {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// staticHopByHop is the fixed list of hop-by-hop headers per RFC 7230 §6.1.
var staticHopByHop = []string{
	"connection",
	"keep-alive",
	"proxy-authenticate",
	"proxy-authorization",
	"te",
	"trailer",
	"transfer-encoding",
	"upgrade",
}

// copyRequestHeaders copies request headers, skipping hop-by-hop headers.
func copyRequestHeaders(dst, src http.Header) {
	copyFilteredHeaders(dst, src)
}

// copyResponseHeaders copies response headers, skipping hop-by-hop headers.
func copyResponseHeaders(dst, src http.Header) {
	copyFilteredHeaders(dst, src)
}
