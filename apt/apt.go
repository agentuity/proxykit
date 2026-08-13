// Package apt implements a caching HTTP forward proxy for APT package managers.
// When configured as APT's HTTP proxy (Acquire::http::Proxy), it intercepts
// package requests and caches them on disk using the shared cache engine.
// Package files (.deb/.udeb) are cached permanently since they are immutable
// per version, while metadata (InRelease, Packages indices) uses a configurable TTL.
package apt

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/agentuity/go-common/logger"
	"github.com/agentuity/proxykit/cache"
)

const (
	// DefaultMetadataTTL is the freshness lifetime for APT repository metadata.
	DefaultMetadataTTL = 1 * time.Hour
	// DefaultMaxCacheDiskPercent is the fraction of the cache filesystem used
	// by DefaultConfig when the filesystem size can be determined.
	DefaultMaxCacheDiskPercent = 0.10

	evictionInterval = 1 * time.Minute
)

// defaultAllowedHostPatterns lists known APT repository hosts that sandboxes
// legitimately need. Each entry is matched case-insensitively against the
// hostname in the client-supplied URL. An entry starting with "*." matches
// the suffix and any sub-domain (e.g. "*.ubuntu.com" matches
// "archive.ubuntu.com" and "security.ubuntu.com").
var defaultAllowedHostPatterns = []string{
	// Ubuntu official mirrors
	"*.ubuntu.com",
	"*.canonical.com",

	// Debian official mirrors
	"deb.debian.org",
	"*.debian.org",

	// Common third-party APT repositories
	"packages.microsoft.com",
	"download.docker.com",
	"deb.nodesource.com",
	"dl.google.com",
	"apt.kubernetes.io",
	"pkgs.k8s.io",
	"ppa.launchpad.net",
	"ppa.launchpadcontent.net",

	// Agentuity APT repository
	"apt.agentuity.sh",
}

// Config configures the APT proxy server.
type Config struct {
	// CacheDir is the directory for cached packages.
	CacheDir string
	// MaxCacheSize is the maximum cache size in bytes (0 = unlimited).
	MaxCacheSize int64
	// MetadataTTL is how long to cache release metadata and package indices (default: 1 hour).
	MetadataTTL time.Duration
	// AllowedHostPatterns restricts upstream fetches to these hosts.
	// Entries starting with "*." match any sub-domain of the suffix.
	// If empty, defaultAllowedHostPatterns is used.
	AllowedHostPatterns []string
	// Logger is optional.
	Logger logger.Logger
}

// DefaultConfig returns a production-oriented APT proxy configuration rooted
// at cacheDir. Callers may modify the returned value before passing it to New.
func DefaultConfig(cacheDir string) Config {
	return Config{
		CacheDir:            cacheDir,
		MaxCacheSize:        cache.DefaultMaxSize(cacheDir, DefaultMaxCacheDiskPercent),
		MetadataTTL:         DefaultMetadataTTL,
		AllowedHostPatterns: slices.Clone(defaultAllowedHostPatterns),
	}
}

// Server is an HTTP forward proxy that caches APT repository requests.
type Server struct {
	metadataTTL  time.Duration
	allowedHosts []string
	cache        *cache.Cache
	client       *http.Client
	log          logger.Logger
	server       *http.Server
	listener     net.Listener

	mu   sync.Mutex
	addr net.Addr
}

// New creates a new APT proxy server.
func New(cfg Config) (*Server, error) {
	if cfg.CacheDir == "" {
		return nil, fmt.Errorf("cache directory is required")
	}

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
		Logger:  log.WithPrefix("[apt]"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	allowedHosts := cfg.AllowedHostPatterns
	if len(allowedHosts) == 0 {
		allowedHosts = defaultAllowedHostPatterns
	}

	// HTTP client with no proxy set (direct connections to upstream).
	// Timeouts: 30s dial, 10min overall for large .deb files.
	// The DialContext uses a Control hook that rejects connections to
	// private, loopback, link-local, and metadata IP addresses to
	// prevent SSRF even if an allowed hostname resolves to an internal IP.
	client := &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: (&net.Dialer{
				Timeout: 30 * time.Second,
				Control: denyPrivateIPs,
			}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConnsPerHost:   10,
		},
	}

	return &Server{
		metadataTTL:  metadataTTL,
		allowedHosts: allowedHosts,
		cache:        c,
		client:       client,
		log:          log,
	}, nil
}

// Start starts the server on the given address (e.g. "[::1]:14781" or ":0" for random port).
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

// handleRequest routes incoming HTTP requests.
// Forward proxy requests have a full URL in RequestURI (e.g. "http://archive.ubuntu.com/...").
// Regular requests (like /healthz) have just a path.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Health check is the only non-proxied endpoint.
	if r.URL.Path == "/healthz" && !isForwardProxy(r) {
		s.handleHealthz(w)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !isForwardProxy(r) {
		http.Error(w, "this is an APT forward proxy; configure with Acquire::http::Proxy", http.StatusBadRequest)
		return
	}

	s.handleProxy(w, r)
}

// isForwardProxy returns true if the request is a forward proxy request.
// APT sends the full URL in the request line, so RequestURI starts with "http://".
func isForwardProxy(r *http.Request) bool {
	return strings.HasPrefix(r.RequestURI, "http://") || strings.HasPrefix(r.RequestURI, "https://")
}

// handleHealthz returns a simple health check response.
func (s *Server) handleHealthz(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleProxy handles a forward proxy request by fetching from upstream
// (or returning from cache) and streaming the response back.
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	upstreamURL := r.RequestURI

	if err := s.validateUpstreamURL(upstreamURL); err != nil {
		s.log.Warn("blocked proxy request: %v", err)
		http.Error(w, "forbidden: "+err.Error(), http.StatusForbidden)
		return
	}
	urlPath := r.URL.Path
	ttl := classifyTTL(urlPath, s.metadataTTL)

	s.log.Debug("proxy request url=%s path=%s ttl=%v", upstreamURL, urlPath, ttl)

	// On cache miss, capture upstream response headers so we can forward them.
	// Each handler goroutine has its own captured headers (safe for coalescing).
	var capturedHeaders http.Header
	var capturedStatus int

	rc, err := s.cache.GetOrFetch(upstreamURL, func() (io.ReadCloser, time.Duration, error) {
		resp, fetchErr := s.fetchUpstream(upstreamURL, r)
		if fetchErr != nil {
			return nil, 0, fetchErr
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, 0, &upstreamError{statusCode: http.StatusNotFound}
		}
		if resp.StatusCode >= 400 {
			resp.Body.Close()
			return nil, 0, &upstreamError{statusCode: resp.StatusCode}
		}

		// Capture upstream headers for forwarding to the client.
		capturedHeaders = resp.Header.Clone()
		capturedStatus = resp.StatusCode

		return resp.Body, ttl, nil
	})

	if err != nil {
		s.handleFetchError(w, err, upstreamURL)
		return
	}
	defer rc.Close()

	// Write response headers.
	if capturedHeaders != nil {
		// Cache miss: forward preserved upstream headers.
		for _, hdr := range []string{"Content-Type", "Content-Length", "Last-Modified", "ETag"} {
			if v := capturedHeaders.Get(hdr); v != "" {
				w.Header().Set(hdr, v)
			}
		}
		if capturedStatus == 0 {
			capturedStatus = http.StatusOK
		}
		w.WriteHeader(capturedStatus)
	} else {
		// Cache hit (or coalesced request): infer Content-Type from path.
		w.Header().Set("Content-Type", contentTypeForPath(urlPath))
		w.WriteHeader(http.StatusOK)
	}

	if _, copyErr := io.Copy(w, rc); copyErr != nil {
		s.log.Error("failed to write response url=%s: %v", upstreamURL, copyErr)
	}
}

// classifyTTL determines the cache TTL based on the URL path pattern.
//
//	*.deb, *.udeb in /pool/  → -1 (cache forever, immutable per version)
//	Everything else          → metadataTTL (release metadata, package indices, etc.)
func classifyTTL(path string, metadataTTL time.Duration) time.Duration {
	if isPackageFile(path) {
		return -1 // cache forever
	}
	return metadataTTL
}

// isPackageFile returns true if the URL path is for an immutable package file
// (i.e. .deb or .udeb under /pool/).
func isPackageFile(path string) bool {
	if !strings.Contains(path, "/pool/") {
		return false
	}
	return strings.HasSuffix(path, ".deb") || strings.HasSuffix(path, ".udeb")
}

// contentTypeForPath infers a Content-Type from the URL path.
// Used for cache-hit responses where upstream headers are unavailable.
func contentTypeForPath(path string) string {
	switch {
	case strings.HasSuffix(path, ".deb"), strings.HasSuffix(path, ".udeb"):
		return "application/vnd.debian.binary-package"
	case strings.HasSuffix(path, ".gz"):
		return "application/gzip"
	case strings.HasSuffix(path, ".xz"):
		return "application/x-xz"
	case strings.HasSuffix(path, ".bz2"):
		return "application/x-bzip2"
	case strings.HasSuffix(path, ".gpg"):
		return "application/pgp-signature"
	case strings.HasSuffix(path, "InRelease"),
		strings.HasSuffix(path, "Release"):
		return "text/plain; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}

// validateUpstreamURL checks that the URL scheme is http/https and the host
// matches one of the configured allowed host patterns.
func (s *Server) validateUpstreamURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	switch parsed.Scheme {
	case "http", "https":
		// allowed
	default:
		return fmt.Errorf("unsupported scheme %q", parsed.Scheme)
	}

	host := parsed.Hostname() // strips port if present
	if host == "" {
		return fmt.Errorf("empty host")
	}

	if !s.isAllowedHost(host) {
		return fmt.Errorf("host %q is not in the allowed list", host)
	}

	return nil
}

// isAllowedHost returns true if host matches any of the configured patterns.
func (s *Server) isAllowedHost(host string) bool {
	host = strings.ToLower(host)
	for _, pattern := range s.allowedHosts {
		pattern = strings.ToLower(pattern)
		if strings.HasPrefix(pattern, "*.") {
			suffix := pattern[1:] // e.g. ".ubuntu.com"
			if strings.HasSuffix(host, suffix) || host == pattern[2:] {
				return true
			}
		} else if host == pattern {
			return true
		}
	}
	return false
}

// denyPrivateIPs is a net.Dialer Control hook that rejects connections to
// private, loopback, link-local, and metadata IP addresses. This prevents
// DNS-rebinding attacks where an allowed hostname resolves to an internal IP.
// The address parameter is the resolved "ip:port" after DNS lookup.
func denyPrivateIPs(_ string, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("failed to parse resolved address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if isBlockedIP(ip) {
		return fmt.Errorf("connection to %s is blocked (private/reserved address)", host)
	}
	return nil
}

// isBlockedIP returns true for IPs that must not be reached through the proxy.
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	// Block well-known cloud metadata endpoints.
	if ip.Equal(net.ParseIP("169.254.169.254")) {
		return true
	}
	return false
}

// fetchUpstream makes an HTTP GET request to the upstream URL.
func (s *Server) fetchUpstream(upstreamURL string, origReq *http.Request) (*http.Response, error) {
	req, err := http.NewRequestWithContext(origReq.Context(), http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	// Forward relevant headers from the original request.
	for _, hdr := range []string{"Accept", "Accept-Encoding", "If-Modified-Since", "If-None-Match"} {
		if v := origReq.Header.Get(hdr); v != "" {
			req.Header.Set(hdr, v)
		}
	}

	s.log.Debug("fetching from upstream url=%s", upstreamURL)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upstream request failed: %w", err)
	}

	return resp, nil
}

// handleFetchError translates fetch errors into appropriate HTTP responses.
func (s *Server) handleFetchError(w http.ResponseWriter, err error, url string) {
	if ue, ok := err.(*upstreamError); ok {
		if ue.statusCode == http.StatusNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s.log.Error("upstream error url=%s status=%d", url, ue.statusCode)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}

	s.log.Error("fetch error url=%s: %v", url, err)
	http.Error(w, "bad gateway", http.StatusBadGateway)
}

// upstreamError represents an HTTP error from the upstream server.
type upstreamError struct {
	statusCode int
}

func (e *upstreamError) Error() string {
	return fmt.Sprintf("upstream returned status %d", e.statusCode)
}
