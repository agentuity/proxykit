package git

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/agentuity/go-common/logger"
	"github.com/agentuity/proxykit/cache"
)

const (
	// DefaultRefsTTL bounds how stale branch and tag advertisements may be.
	DefaultRefsTTL = 15 * time.Second
	// DefaultPacksTTL is the hard lifetime for cached fresh-clone pack responses.
	DefaultPacksTTL = 72 * time.Hour
	// DefaultMaxUploadPackRequestSize is the largest fetch request buffered for
	// cacheability analysis.
	DefaultMaxUploadPackRequestSize = 4 << 20
	// DefaultMaxReceivePackRequestSize is the largest push request buffered
	// before it is streamed directly.
	DefaultMaxReceivePackRequestSize = 500 << 20
	// DefaultRefsCacheDiskPercent is the fraction of the cache filesystem
	// reserved for refs advertisements.
	DefaultRefsCacheDiskPercent = 0.02
	// DefaultPacksCacheDiskPercent is the fraction of the cache filesystem
	// reserved for pack responses.
	DefaultPacksCacheDiskPercent = 0.20
)

// Config configures the Git smart HTTP proxy handler.
type Config struct {
	// RefsCache configures the Level 1 ref advertisement cache.
	RefsCache CacheConfig

	// PacksCache configures the Level 2 pack data cache.
	PacksCache CacheConfig

	// RefsTTL is how long to cache info/refs responses.
	// After expiry, the next request fetches from upstream.
	// Default: 15 seconds.
	RefsTTL time.Duration

	// PacksTTL is the hard maximum age for cached pack files. Packs older
	// than this are always evicted regardless of cache space. This is the
	// safety backstop — in practice, most stale packs are cleaned up earlier
	// by the adaptive soft-eviction at 80% capacity (oldest-created first).
	// Default: 72 hours (covers overnight + weekend staleness).
	// Set to -1 for no hard expiry (soft-eviction + LRU only).
	PacksTTL time.Duration

	// MaxUploadPackRequestSize is the maximum size of a git-upload-pack
	// POST request body that will be buffered for parse-and-cache logic.
	// Requests larger than this are forwarded directly without caching.
	// Default: 4 MB.
	MaxUploadPackRequestSize int64

	// MaxReceivePackRequestSize is the maximum push body size to buffer.
	// Push bodies larger than this are streamed directly to upstream.
	// Default: 500 MB.
	MaxReceivePackRequestSize int64

	// MaxPackCacheEntrySize is the maximum size of a single pack file
	// to store in the cache. Packs larger than this are served directly
	// without caching. 0 = unlimited.
	// Default: 0 (unlimited).
	MaxPackCacheEntrySize int64

	// AllowedHosts, if non-empty, restricts Git caching to these hostnames.
	// Requests to other hosts use credential injection only (no caching).
	// Default: empty (cache all detected Git hosts).
	AllowedHosts []string

	// UpstreamScheme overrides the scheme used for upstream Git requests.
	// Valid values are "http", "https", and empty. When empty, the handler
	// uses "https" for TLS requests and "http" for plain HTTP requests.
	UpstreamScheme string

	// Logger receives cache and proxy diagnostics. A console logger is used when nil.
	Logger logger.Logger
}

// CacheConfig configures one cache tier.
type CacheConfig struct {
	// Dir is the cache root directory on the host filesystem.
	// It must be writable by the process running the proxy.
	Dir string

	// MaxSize is the maximum total cache size in bytes.
	// 0 = unlimited (not recommended for production).
	MaxSize int64
}

// DefaultConfig returns a production-oriented Git proxy configuration rooted
// at cacheDir. Refs and packs are stored in separate subdirectories. Callers
// may modify the returned value before passing it to New.
func DefaultConfig(cacheDir string) Config {
	refsDir := filepath.Join(cacheDir, "refs")
	packsDir := filepath.Join(cacheDir, "packs")
	return Config{
		RefsCache: CacheConfig{
			Dir:     refsDir,
			MaxSize: cache.DefaultMaxSize(refsDir, DefaultRefsCacheDiskPercent),
		},
		PacksCache: CacheConfig{
			Dir:     packsDir,
			MaxSize: cache.DefaultMaxSize(packsDir, DefaultPacksCacheDiskPercent),
		},
		RefsTTL:                   DefaultRefsTTL,
		PacksTTL:                  DefaultPacksTTL,
		MaxUploadPackRequestSize:  DefaultMaxUploadPackRequestSize,
		MaxReceivePackRequestSize: DefaultMaxReceivePackRequestSize,
	}
}

// Handler is an HTTP handler for Git smart HTTP requests.
//
// Thread safety: Handler is safe for concurrent use.
type Handler struct {
	refsCache   *cache.Cache
	packsCache  *cache.Cache
	cfg         Config
	log         logger.Logger
	client      *http.Client
	packFlights sync.Map // singleflight for pack fetches: key → *packFlight
}

// packFlight coordinates concurrent fetches for the same pack cache key.
// The first requester fetches from upstream and caches; others wait and
// retry the cache.
type packFlight struct {
	done chan struct{}
}

// reInjectAuthOnRedirect preserves Authorization headers across same-origin redirects.
// Go's default redirect handler strips Authorization when crossing hosts.
// Git servers commonly redirect within the same origin (e.g., /repo → /repo.git).
// Only re-inject when scheme, hostname, AND port all match — never forward
// credentials to a different origin.
func reInjectAuthOnRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > 5 {
		return http.ErrUseLastResponse
	}
	if len(via) > 0 && via[0].Header.Get("Authorization") != "" &&
		sameOrigin(req.URL, via[0].URL) {
		req.Header.Set("Authorization", via[0].Header.Get("Authorization"))
	}
	return nil
}

// sameOrigin reports whether two URLs share the same scheme, host, and port.
// An empty port is treated as the default for the scheme (80/http, 443/https).
func sameOrigin(a, b *url.URL) bool {
	if !strings.EqualFold(a.Scheme, b.Scheme) {
		return false
	}
	if !strings.EqualFold(a.Hostname(), b.Hostname()) {
		return false
	}
	return effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}

// New creates a new Git proxy Handler. Creates cache directories if they don't exist.
// Returns an error if cache initialization fails.
func New(cfg Config) (*Handler, error) {
	if err := validateUpstreamScheme(cfg.UpstreamScheme); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewConsoleLogger()
	}
	if cfg.RefsTTL == 0 {
		cfg.RefsTTL = DefaultRefsTTL
	}
	if cfg.MaxUploadPackRequestSize == 0 {
		cfg.MaxUploadPackRequestSize = DefaultMaxUploadPackRequestSize
	}
	if cfg.MaxReceivePackRequestSize == 0 {
		cfg.MaxReceivePackRequestSize = DefaultMaxReceivePackRequestSize
	}
	if cfg.PacksTTL == 0 {
		cfg.PacksTTL = DefaultPacksTTL
	}

	refsCache, err := cache.New(cache.Config{
		Dir:     cfg.RefsCache.Dir,
		MaxSize: cfg.RefsCache.MaxSize,
		TTL:     cfg.RefsTTL,
		Logger:  cfg.Logger.WithPrefix("[git/refs]"),
	})
	if err != nil {
		return nil, fmt.Errorf("git refs cache: %w", err)
	}

	// PacksTTL of -1 means no expiry (LRU-only), which translates to TTL=0
	// in cache.Config (where 0 means "never expires").
	packsCacheTTL := max(cfg.PacksTTL, 0)

	packsCache, err := cache.New(cache.Config{
		Dir:              cfg.PacksCache.Dir,
		MaxSize:          cfg.PacksCache.MaxSize,
		TTL:              packsCacheTTL,
		SoftEvictPercent: 0.8, // start age-ordered eviction at 80% capacity
		Logger:           cfg.Logger.WithPrefix("[git/packs]"),
	})
	if err != nil {
		refsCache.Stop()
		return nil, fmt.Errorf("git packs cache: %w", err)
	}

	h := &Handler{
		refsCache:  refsCache,
		packsCache: packsCache,
		cfg:        cfg,
		log:        cfg.Logger,
		client: &http.Client{
			// No global Timeout — streamed Git push/fetch bodies can be very large
			// and would be prematurely killed. Per-request deadlines are controlled
			// via request contexts from the caller.
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          20,
				MaxIdleConnsPerHost:   5,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 60 * time.Second,
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
			CheckRedirect: reInjectAuthOnRedirect,
		},
	}
	return h, nil
}

// StartEviction starts background cache eviction goroutines.
// Must be called after New and before handling requests.
func (h *Handler) StartEviction(ctx context.Context, interval time.Duration) {
	h.refsCache.StartEviction(ctx, interval)
	h.packsCache.StartEviction(ctx, interval)
}

// Stop shuts down background goroutines.
func (h *Handler) Stop() {
	h.refsCache.Stop()
	h.packsCache.Stop()
}

// RefsCacheSize returns the current refs cache (L1) usage in bytes.
func (h *Handler) RefsCacheSize() int64 { return h.refsCache.Size() }

// RefsCacheMaxSize returns the maximum configured refs cache size in bytes.
func (h *Handler) RefsCacheMaxSize() int64 { return h.refsCache.MaxSize() }

// RefsCacheLen returns the number of cached refs entries.
func (h *Handler) RefsCacheLen() int { return h.refsCache.Len() }

// PacksCacheSize returns the current packs cache (L2) usage in bytes.
func (h *Handler) PacksCacheSize() int64 { return h.packsCache.Size() }

// PacksCacheMaxSize returns the maximum configured packs cache size in bytes.
func (h *Handler) PacksCacheMaxSize() int64 { return h.packsCache.MaxSize() }

// PacksCacheLen returns the number of cached pack entries.
func (h *Handler) PacksCacheLen() int { return h.packsCache.Len() }

// ClearCaches removes all entries from both the refs and packs caches.
// Used for disk pressure relief. Returns total entries removed and bytes freed.
func (h *Handler) ClearCaches() (removed int, freed int64) {
	refsRemoved, refsFreed := h.refsCache.Clear()
	packsRemoved, packsFreed := h.packsCache.Clear()
	return refsRemoved + packsRemoved, refsFreed + packsFreed
}

// ClearPacksCache removes all entries from the packs cache only (the large one).
// Used for moderate disk pressure where preserving the small refs cache is preferred.
// Returns entries removed and bytes freed.
func (h *Handler) ClearPacksCache() (removed int, freed int64) {
	return h.packsCache.Clear()
}

// isAllowedHost checks whether caching is enabled for the given host.
// Returns true if AllowedHosts is empty (all hosts allowed) or if the host is in the list.
func (h *Handler) isAllowedHost(host string) bool {
	if len(h.cfg.AllowedHosts) == 0 {
		return true
	}
	for _, allowed := range h.cfg.AllowedHosts {
		if strings.EqualFold(allowed, host) {
			return true
		}
	}
	return false
}

// Validate checks all configuration fields and verifies that cache directories
// can be created and written.
func (cfg *Config) Validate() error {
	if cfg.RefsCache.Dir == "" {
		return errors.New("git.RefsCache.Dir must not be empty")
	}
	if cfg.PacksCache.Dir == "" {
		return errors.New("git.PacksCache.Dir must not be empty")
	}
	if canonicalizePath(cfg.RefsCache.Dir) == canonicalizePath(cfg.PacksCache.Dir) {
		return errors.New("git.RefsCache.Dir and git.PacksCache.Dir must be different directories")
	}

	// Verify directories are writable.
	for _, dir := range []string{cfg.RefsCache.Dir, cfg.PacksCache.Dir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("git cache dir %s: %w", dir, err)
		}
		testFile := filepath.Join(dir, ".hadron-write-test")
		if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
			return fmt.Errorf("git cache dir %s not writable: %w", dir, err)
		}
		_ = os.Remove(testFile)
	}

	if cfg.RefsTTL < 0 {
		return errors.New("git.RefsTTL must not be negative")
	}
	if cfg.RefsTTL > 24*time.Hour {
		return errors.New("git.RefsTTL must not exceed 24 hours")
	}
	if cfg.MaxUploadPackRequestSize < 0 {
		return errors.New("git.MaxUploadPackRequestSize must not be negative")
	}
	if cfg.MaxReceivePackRequestSize < 0 {
		return errors.New("git.MaxReceivePackRequestSize must not be negative")
	}
	if cfg.MaxPackCacheEntrySize < 0 {
		return errors.New("git.MaxPackCacheEntrySize must not be negative")
	}
	if err := validateUpstreamScheme(cfg.UpstreamScheme); err != nil {
		return err
	}

	for i, host := range cfg.AllowedHosts {
		if strings.TrimSpace(host) == "" {
			return fmt.Errorf("git.AllowedHosts[%d] must not be empty", i)
		}
		if strings.ContainsAny(host, "/:") {
			return fmt.Errorf("git.AllowedHosts[%d] must be a hostname only (no scheme or path): %q", i, host)
		}
	}

	return nil
}

func validateUpstreamScheme(scheme string) error {
	if scheme != "" && scheme != "http" && scheme != "https" {
		return fmt.Errorf("git.UpstreamScheme must be empty, http, or https: %q", scheme)
	}
	return nil
}

// envVarNameRegexp validates environment variable names.
var envVarNameRegexp = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Validate validates a GitCredentialConfig.
func (cfg *GitCredentialConfig) Validate() error {
	if cfg == nil {
		return nil
	}
	seen := make(map[string]int, len(cfg.Hosts))
	for i, cred := range cfg.Hosts {
		if strings.TrimSpace(cred.Host) == "" {
			return fmt.Errorf("gitCredentials.hosts[%d].host must not be empty", i)
		}
		if strings.ContainsAny(cred.Host, "/:") {
			return fmt.Errorf("gitCredentials.hosts[%d].host must be a hostname only: %q", i, cred.Host)
		}
		lower := strings.ToLower(cred.Host)
		if prev, dup := seen[lower]; dup {
			return fmt.Errorf("gitCredentials.hosts[%d].host is duplicate of hosts[%d]: %q", i, prev, cred.Host)
		}
		seen[lower] = i
		if strings.TrimSpace(cred.EnvVar) == "" {
			return fmt.Errorf("gitCredentials.hosts[%d].env must not be empty", i)
		}
		if !envVarNameRegexp.MatchString(cred.EnvVar) {
			return fmt.Errorf("gitCredentials.hosts[%d].env is not a valid env var name: %q", i, cred.EnvVar)
		}
	}
	return nil
}

// canonicalizePath resolves a path to its absolute, symlink-resolved form.
// Falls back to filepath.Abs + filepath.Clean if EvalSymlinks fails.
func canonicalizePath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return filepath.Clean(abs)
	}
	return resolved
}
