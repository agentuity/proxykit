// Package npm implements a caching HTTP proxy for the npm registry.
// It caches package metadata and tarballs on disk using the shared
// cache engine, rewriting tarball URLs in metadata responses so that
// npm clients fetch tarballs through the proxy as well.
package npm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/agentuity/go-common/logger"
	"github.com/agentuity/proxykit/cache"
)

const (
	// DefaultUpstreamURL is the public npm registry used when no upstream is set.
	DefaultUpstreamURL = "https://registry.npmjs.org"
	// DefaultMetadataTTL balances registry freshness with metadata cache reuse.
	DefaultMetadataTTL = 1 * time.Minute
	// DefaultMaxCacheDiskPercent is the fraction of the cache filesystem used
	// by DefaultConfig when the filesystem size can be determined.
	DefaultMaxCacheDiskPercent = 0.10

	evictionInterval = 1 * time.Minute
)

// Config configures the NPM proxy server.
type Config struct {
	// UpstreamURL is the default upstream npm registry (default: https://registry.npmjs.org).
	UpstreamURL string
	// AllowedUpstreamRegistryPatterns restricts which additional registry hosts
	// may be fetched via /_upstream/<host>/ routing or forward-proxy requests.
	// When empty, DefaultAllowedUpstreamRegistryPatterns is used.
	AllowedUpstreamRegistryPatterns []string
	// CacheDir is the directory for cached packages.
	CacheDir string
	// MaxCacheSize is the maximum cache size in bytes (0 = unlimited).
	MaxCacheSize int64
	// MetadataTTL is how long to cache package metadata (default: 1 minute).
	MetadataTTL time.Duration
	// Logger is optional.
	Logger logger.Logger
}

// DefaultConfig returns a production-oriented npm proxy configuration rooted
// at cacheDir. Callers may modify the returned value before passing it to New.
func DefaultConfig(cacheDir string) Config {
	return Config{
		UpstreamURL:                     DefaultUpstreamURL,
		AllowedUpstreamRegistryPatterns: slices.Clone(DefaultAllowedUpstreamRegistryPatterns),
		CacheDir:                        cacheDir,
		MaxCacheSize:                    cache.DefaultMaxSize(cacheDir, DefaultMaxCacheDiskPercent),
		MetadataTTL:                     DefaultMetadataTTL,
	}
}

// Server is an HTTP server that proxies and caches npm registry requests.
type Server struct {
	upstreamURL string
	allowlist   *upstreamRegistryAllowlist
	metadataTTL time.Duration
	cache       *cache.Cache
	client      *http.Client
	log         logger.Logger
	server      *http.Server
	listener    net.Listener

	mu   sync.Mutex
	addr net.Addr
}

// New creates a new NPM proxy server.
func New(cfg Config) (*Server, error) {
	if cfg.CacheDir == "" {
		return nil, fmt.Errorf("cache directory is required")
	}

	upstreamURL := cfg.UpstreamURL
	if upstreamURL == "" {
		upstreamURL = DefaultUpstreamURL
	}
	upstreamURL = strings.TrimRight(upstreamURL, "/")

	metadataTTL := cfg.MetadataTTL
	if metadataTTL == 0 {
		metadataTTL = DefaultMetadataTTL
	}

	log := cfg.Logger
	if log == nil {
		log = logger.NewConsoleLogger()
	}

	c, err := cache.New(cache.Config{
		Dir:     cfg.CacheDir,
		MaxSize: cfg.MaxCacheSize,
		Logger:  log.WithPrefix("[npm]"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	allowlist, err := newUpstreamRegistryAllowlist(cfg.AllowedUpstreamRegistryPatterns)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: 5 * time.Minute,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConnsPerHost:   10,
		},
	}

	return &Server{
		upstreamURL: upstreamURL,
		allowlist:   allowlist,
		metadataTTL: metadataTTL,
		cache:       c,
		client:      client,
		log:         log,
	}, nil
}

// Start starts the server on the given address (e.g. "[::1]:14780" or ":0" for random port).
// The ready channel is closed when the server is ready to accept connections.
// Returns the actual address the server is listening on.
func (s *Server) Start(ctx context.Context, addr string, ready chan<- struct{}) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	s.mu.Lock()
	s.listener = ln
	s.addr = ln.Addr()
	s.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRequest)

	s.server = &http.Server{
		Handler: mux,
	}

	s.cache.StartEviction(ctx, evictionInterval)

	if ready != nil {
		close(ready)
	}

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.log.Error("server error: %v", err)
		}
	}()

	return ln.Addr(), nil
}

// Addr returns the server's listener address (available after Start).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// CacheSize returns the current cache size in bytes.
func (s *Server) CacheSize() int64 { return s.cache.Size() }

// CacheMaxSize returns the maximum configured cache size in bytes.
func (s *Server) CacheMaxSize() int64 { return s.cache.MaxSize() }

// CacheLen returns the number of cached entries.
func (s *Server) CacheLen() int { return s.cache.Len() }

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	s.cache.Stop()

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

// handleRequest routes incoming HTTP requests to the appropriate handler.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if r.URL.Path == "/healthz" && !isForwardProxyRequest(r) {
		s.handleHealthz(w, r)
		return
	}

	if isForwardProxyRequest(r) {
		s.handleForwardProxy(w, r)
		return
	}

	path := r.URL.Path

	// Decode any percent-encoded slashes for scoped packages.
	// e.g. /@scope%2fpkg -> /@scope/pkg
	decoded, err := url.PathUnescape(path)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	upstream, fetchPath, upstreamHost, err := s.resolveRegistryRequest(decoded)
	if err != nil {
		s.log.Warn("blocked npm proxy request path=%s: %v", decoded, err)
		http.Error(w, "forbidden: "+err.Error(), http.StatusForbidden)
		return
	}

	// Determine if this is a tarball or metadata request.
	if isTarballRequest(fetchPath) {
		s.handleTarball(w, r, upstream, fetchPath, upstreamHost)
	} else {
		s.handleMetadata(w, r, upstream, fetchPath, upstreamHost)
	}
}

func (s *Server) resolveRegistryRequest(path string) (upstreamBase string, fetchPath string, upstreamHost string, err error) {
	if host, remainder, ok := parseUpstreamPath(path); ok {
		if !s.allowlist.allowed(host) {
			return "", "", "", fmt.Errorf("host %q is not an allowed upstream fetch host", host)
		}
		return upstreamRegistryBaseURL(host), remainder, host, nil
	}
	parsed, err := url.Parse(s.upstreamURL)
	if err != nil || parsed.Host == "" {
		return "", "", "", fmt.Errorf("invalid default upstream URL %q", s.upstreamURL)
	}
	return s.upstreamURL, path, parsed.Host, nil
}

func (s *Server) handleForwardProxy(w http.ResponseWriter, r *http.Request) {
	parsed, err := validateUpstreamFetchURL(r.RequestURI, s.allowlist)
	if err != nil {
		s.log.Warn("blocked npm forward proxy request: %v", err)
		http.Error(w, "forbidden: "+err.Error(), http.StatusForbidden)
		return
	}

	upstreamHost := parsed.Host
	fetchPath := parsed.Path
	if parsed.RawQuery != "" {
		fetchPath += "?" + parsed.RawQuery
	}
	upstreamBase := parsed.Scheme + "://" + parsed.Host

	if isTarballPath(fetchPath) {
		s.handleTarballURL(w, r.RequestURI, upstreamHost, fetchPath)
		return
	}
	s.handleMetadata(w, r, upstreamBase, fetchPath, upstreamHost)
}

// isTarballPath returns true when the URL path is a tarball download.
func isTarballPath(path string) bool {
	return strings.HasSuffix(path, ".tgz")
}

// isTarballRequest returns true if the path matches an npm registry tarball pattern.
func isTarballRequest(path string) bool {
	return isTarballPath(path)
}

// handleHealthz returns a simple health check response.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleMetadata proxies and caches package metadata JSON.
// It caches the raw upstream response and rewrites tarball URLs at serve time
// using the request's Host header. This ensures each client gets tarball URLs
// matching the address it connected through (bridge IPv4 for Docker, hostname
// for containerd), avoiding DNS resolution issues across runtimes.
func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request, upstreamBase, path, upstreamHost string) {
	pkgName := extractPackageName(path)
	if pkgName == "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	cacheKey := metadataCacheKey(upstreamHost, path)

	rc, err := s.cache.GetOrFetch(cacheKey, func() (io.ReadCloser, time.Duration, error) {
		body, statusCode, fetchErr := s.fetchFromUpstream(upstreamBase, path)
		if fetchErr != nil {
			return nil, 0, fetchErr
		}
		defer body.Close()

		if statusCode == http.StatusNotFound {
			return nil, 0, &upstreamError{statusCode: http.StatusNotFound}
		}
		if statusCode >= 400 {
			return nil, 0, &upstreamError{statusCode: statusCode}
		}

		// Cache raw upstream metadata — tarball URL rewriting happens at
		// serve time so each client gets URLs matching its connection address.
		data, readErr := io.ReadAll(body)
		if readErr != nil {
			return nil, 0, fmt.Errorf("failed to read upstream metadata: %w", readErr)
		}

		return io.NopCloser(bytes.NewReader(data)), s.metadataTTL, nil
	})

	if err != nil {
		s.handleFetchError(w, err, path)
		return
	}
	defer rc.Close()

	// Read cached metadata and rewrite tarball URLs per-request.
	// Docker containers connect via bridge IPv4, containerd via hostname —
	// r.Host automatically has the right address for each.
	data, readErr := io.ReadAll(rc)
	if readErr != nil {
		s.log.Error("failed to read cached metadata path=%s: %v", path, readErr)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if r.Host == "" {
		http.Error(w, "missing Host header", http.StatusBadRequest)
		return
	}
	rewritten := rewriteRegistryTarballURLs(data, upstreamBase, upstreamHost, r.Host)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, writeErr := w.Write(rewritten); writeErr != nil {
		s.log.Error("failed to write metadata response path=%s: %v", path, writeErr)
	}
}

// handleTarball proxies and caches package tarballs.
// Tarballs are streamed and cached permanently (TTL = -1).
func (s *Server) handleTarball(w http.ResponseWriter, _ *http.Request, upstreamBase, path, upstreamHost string) {
	s.handleTarballURL(w, "", upstreamHost, path, upstreamBase)
}

func (s *Server) handleTarballURL(w http.ResponseWriter, fetchURL, upstreamHost, path string, upstreamBase ...string) {
	var cacheKey string
	var fetch func() (io.ReadCloser, int, error)
	switch {
	case fetchURL != "":
		cacheKey = "tar:url:" + fetchURL
		fetch = func() (io.ReadCloser, int, error) {
			return s.fetchFromUpstreamURL(fetchURL)
		}
	default:
		base := ""
		if len(upstreamBase) > 0 {
			base = upstreamBase[0]
		}
		cacheKey = tarballCacheKey(upstreamHost, path)
		fetch = func() (io.ReadCloser, int, error) {
			return s.fetchFromUpstream(base, path)
		}
	}

	rc, err := s.cache.GetOrFetch(cacheKey, func() (io.ReadCloser, time.Duration, error) {
		body, statusCode, fetchErr := fetch()
		if fetchErr != nil {
			return nil, 0, fetchErr
		}

		if statusCode == http.StatusNotFound {
			body.Close()
			return nil, 0, &upstreamError{statusCode: http.StatusNotFound}
		}
		if statusCode >= 400 {
			body.Close()
			return nil, 0, &upstreamError{statusCode: statusCode}
		}

		// TTL of -1 means cache forever.
		return body, -1, nil
	})

	if err != nil {
		s.handleFetchError(w, err, path)
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, copyErr := io.Copy(w, rc); copyErr != nil {
		s.log.Error("failed to write tarball response path=%s: %v", path, copyErr)
	}
}

// fetchFromUpstream makes an HTTP GET request to the upstream registry.
// Returns the response body, status code, and any error.
// The caller is responsible for closing the body on success.
func (s *Server) fetchFromUpstream(upstreamBase, path string) (io.ReadCloser, int, error) {
	upstreamURL := strings.TrimRight(upstreamBase, "/") + path

	req, err := http.NewRequest(http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create upstream request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	s.log.Debug("fetching from upstream url=%s", upstreamURL)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("upstream request failed: %w", err)
	}

	return resp.Body, resp.StatusCode, nil
}

// fetchFromUpstreamURL makes an HTTP GET request to a fully qualified upstream URL.
func (s *Server) fetchFromUpstreamURL(rawURL string) (io.ReadCloser, int, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create upstream request: %w", err)
	}

	s.log.Debug("fetching from upstream url=%s", rawURL)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("upstream request failed: %w", err)
	}

	return resp.Body, resp.StatusCode, nil
}

// handleFetchError translates fetch errors into appropriate HTTP responses.
func (s *Server) handleFetchError(w http.ResponseWriter, err error, path string) {
	if ue, ok := err.(*upstreamError); ok {
		switch {
		case ue.statusCode == http.StatusNotFound:
			http.Error(w, "not found", http.StatusNotFound)
		default:
			s.log.Error("upstream error path=%s status=%d", path, ue.statusCode)
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}
		return
	}

	s.log.Error("fetch error path=%s: %v", path, err)
	http.Error(w, "bad gateway", http.StatusBadGateway)
}

// extractPackageName extracts the npm package name from a URL path.
// Examples:
//
//	/lodash -> lodash
//	/@angular/core -> @angular/core
//	/@scope/pkg -> @scope/pkg
func extractPackageName(path string) string {
	// Remove leading slash.
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return ""
	}

	// Scoped packages: @scope/name
	if strings.HasPrefix(path, "@") {
		parts := strings.SplitN(path, "/", 3)
		if len(parts) >= 2 {
			return parts[0] + "/" + parts[1]
		}
		return ""
	}

	// Unscoped packages: just the first path segment.
	parts := strings.SplitN(path, "/", 2)
	return parts[0]
}

// metadataCacheKey generates a cache key for package metadata.
func metadataCacheKey(upstreamHost, path string) string {
	return "meta:" + upstreamHost + ":" + path
}

// tarballCacheKey generates a cache key for a tarball from its URL path.
// Examples:
//
//	/lodash/-/lodash-4.17.21.tgz -> tar:registry.npmjs.org:lodash/lodash-4.17.21.tgz
//	/@angular/core/-/core-15.0.0.tgz -> tar:npm.pkg.github.com:@angular/core/core-15.0.0.tgz
func tarballCacheKey(upstreamHost, path string) string {
	pkgName := extractPackageName(path)

	// Extract the filename after /-/
	_, after, ok := strings.Cut(path, "/-/")
	if !ok {
		return "tar:" + upstreamHost + ":" + path
	}
	filename := after

	return "tar:" + upstreamHost + ":" + pkgName + "/" + filename
}

// rewriteRegistryTarballURLs replaces the upstream registry URL with the proxy URL
// for the registry that served the metadata response.
func rewriteRegistryTarballURLs(data []byte, upstreamBase, upstreamHost, proxyHost string) []byte {
	proxyURL := proxyUpstreamBaseURL(proxyHost, upstreamHost)
	return rewriteTarballURLs(data, strings.TrimRight(upstreamBase, "/"), proxyURL)
}

// rewriteTarballURLs replaces the upstream registry URL with the proxy URL
// in the metadata JSON body. This ensures npm fetches tarballs through the proxy.
func rewriteTarballURLs(data []byte, upstreamURL, proxyURL string) []byte {
	return bytes.ReplaceAll(data, []byte(upstreamURL), []byte(proxyURL))
}

// upstreamError represents an HTTP error from the upstream registry.
type upstreamError struct {
	statusCode int
}

func (e *upstreamError) Error() string {
	return fmt.Sprintf("upstream returned status %d", e.statusCode)
}
