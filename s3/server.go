package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agentuity/go-common/logger"
	"github.com/agentuity/proxykit/cache"
)

type requestSecurity struct {
	identity  Identity
	authScope string
	signer    RequestSigner
	cacheable bool
}

type cacheFlight struct {
	done chan struct{}
}

// Stats is a point-in-time snapshot of S3 acceleration activity.
type Stats struct {
	// Hits is the number of requests served from a fresh or revalidated entry.
	Hits uint64
	// Misses is the number of cacheable requests sent upstream.
	Misses uint64
	// Stores is the number of object or metadata entries stored successfully.
	Stores uint64
	// Revalidations is the number of upstream 304 responses served from cache.
	Revalidations uint64
	// Bypasses is the number of requests excluded from caching by policy or scope.
	Bypasses uint64
}

type metrics struct {
	hits          atomic.Uint64
	misses        atomic.Uint64
	stores        atomic.Uint64
	revalidations atomic.Uint64
	bypasses      atomic.Uint64
}

// Server is an HTTP handler and optional standalone server for S3-compatible
// object acceleration. It is safe for concurrent use.
type Server struct {
	cfg      Config
	upstream *url.URL
	disk     *cache.Cache
	client   *http.Client
	log      logger.Logger
	flights  sync.Map
	metrics  metrics

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
}

// New creates an S3 acceleration server.
func New(cfg Config) (*Server, error) {
	applyConfigDefaults(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var upstream *url.URL
	if cfg.UpstreamURL != "" {
		var err error
		upstream, err = url.Parse(cfg.UpstreamURL)
		if err != nil {
			return nil, err
		}
	}
	log := cfg.Logger
	if log == nil {
		log = logger.NewConsoleLogger()
	}
	disk, err := cache.New(cache.Config{
		Dir:              cfg.CacheDir,
		MaxSize:          cfg.MaxCacheSize,
		SoftEvictPercent: cfg.SoftEvictPercent,
		Logger:           log.WithPrefix("[s3/cache]"),
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 cache: %w", err)
	}
	client := cfg.HTTPClient
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		if cfg.TLSConfig != nil {
			transport.TLSClientConfig = cfg.TLSConfig.Clone()
		}
		client = &http.Client{
			Transport: transport,
			Timeout:   30 * time.Minute,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &Server{
		cfg:      cfg,
		upstream: upstream,
		disk:     disk,
		client:   client,
		log:      log.WithPrefix("[s3]"),
	}, nil
}

func applyConfigDefaults(cfg *Config) {
	if cfg.UpstreamScheme == "" {
		cfg.UpstreamScheme = "https"
	}
	if cfg.FreshTTL == 0 {
		cfg.FreshTTL = DefaultFreshTTL
	}
	if cfg.VersionedTTL == 0 {
		cfg.VersionedTTL = DefaultVersionedTTL
	}
	if cfg.MaxTTL == 0 {
		cfg.MaxTTL = DefaultMaxTTL
	}
	if cfg.EvictionInterval == 0 {
		cfg.EvictionInterval = DefaultEvictionInterval
	}
}

// Start starts a standalone HTTP endpoint. Clients should configure it as a
// custom path-style S3 endpoint rather than as an HTTPS CONNECT proxy.
func (s *Server) Start(ctx context.Context, addr string, ready chan<- struct{}) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	s.mu.Lock()
	s.listener = ln
	s.server = &http.Server{Handler: s}
	s.mu.Unlock()
	s.StartEviction(ctx)
	if ready != nil {
		close(ready)
	}
	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("server error: %v", err)
		}
	}()
	return ln.Addr(), nil
}

// StartEviction starts background cache eviction for an embedded Server.
func (s *Server) StartEviction(ctx context.Context) {
	s.disk.StartEviction(ctx, s.cfg.EvictionInterval)
}

// Stop gracefully stops the standalone server and closes the cache.
func (s *Server) Stop() error {
	s.mu.Lock()
	srv := s.server
	s.mu.Unlock()
	var err error
	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = srv.Shutdown(ctx)
	}
	s.disk.Stop()
	return err
}

// Addr returns the standalone listener address after Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// CacheSize returns the current cache size in bytes.
func (s *Server) CacheSize() int64 { return s.disk.Size() }

// CacheLen returns the number of body and metadata cache records.
func (s *Server) CacheLen() int { return s.disk.Len() }

// Stats returns an atomic snapshot of cache activity.
func (s *Server) Stats() Stats {
	return Stats{
		Hits:          s.metrics.hits.Load(),
		Misses:        s.metrics.misses.Load(),
		Stores:        s.metrics.stores.Load(),
		Revalidations: s.metrics.revalidations.Load(),
		Bypasses:      s.metrics.bypasses.Load(),
	}
}

// ServeHTTP handles one S3-compatible request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Authorizer != nil {
		if err := s.cfg.Authorizer(r); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}
	security, err := s.resolveSecurity(r)
	if err != nil {
		s.log.Warn("resolve request security: %v", err)
		http.Error(w, "request authorization failed", http.StatusBadGateway)
		return
	}
	target, err := s.targetURL(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	method := strings.ToUpper(r.Method)
	if isMutation(method) {
		s.forwardMutation(w, r, target, security)
		return
	}
	if method != http.MethodGet && method != http.MethodHead {
		s.forward(w, r, target, security.signer, "")
		return
	}
	if !security.cacheable || !s.cachePolicy(r) {
		s.metrics.bypasses.Add(1)
		s.forward(w, r, target, security.signer, "BYPASS")
		return
	}
	key := s.cacheKey(target, security)
	s.serveCacheable(w, r, target, security, key)
}

func (s *Server) resolveSecurity(r *http.Request) (requestSecurity, error) {
	security := requestSecurity{identity: Identity{Tenant: "local"}, cacheable: true}
	if s.cfg.IdentityResolver != nil {
		identity, ok, err := s.cfg.IdentityResolver(r.Context(), r)
		if err != nil {
			return security, err
		}
		security.identity = identity
		security.cacheable = ok && strings.TrimSpace(identity.Tenant) != ""
	}

	if s.cfg.SigningResolver != nil {
		resolved, ok, err := s.cfg.SigningResolver(r.Context(), r, security.identity)
		if err != nil {
			return security, err
		}
		if ok {
			security.signer = resolved.Sign
			security.authScope = strings.TrimSpace(resolved.Scope)
			if security.authScope == "" {
				security.cacheable = false
			}
		}
	} else {
		var creds Credentials
		var ok bool
		if s.cfg.CredentialResolver != nil {
			var err error
			creds, ok, err = s.cfg.CredentialResolver(r.Context(), r, security.identity)
			if err != nil {
				return security, err
			}
		} else if s.cfg.Credentials != nil {
			creds = *s.cfg.Credentials
			ok = true
		}
		if ok {
			security.authScope = credentialScope(creds)
			security.signer = func(ctx context.Context, outgoing *http.Request) error {
				return signRequest(ctx, outgoing, creds, time.Now())
			}
		}
	}
	if security.authScope == "" {
		security.authScope = requestCredentialScope(r)
	}
	return security, nil
}

func (s *Server) targetURL(r *http.Request) (*url.URL, error) {
	var target url.URL
	if s.upstream != nil {
		target = *s.upstream
		target.Path = joinURLPath(s.upstream.Path, r.URL.Path)
		joinedRawPath := joinURLPath(s.upstream.EscapedPath(), r.URL.EscapedPath())
		if joinedRawPath != target.EscapedPath() {
			target.RawPath = joinedRawPath
		} else {
			target.RawPath = ""
		}
		query := target.Query()
		for key, values := range r.URL.Query() {
			for _, value := range values {
				query.Add(key, value)
			}
		}
		target.RawQuery = query.Encode()
	} else if r.URL.IsAbs() {
		target = *r.URL
	} else {
		if r.Host == "" {
			return nil, errors.New("request has no upstream host")
		}
		target = *r.URL
		target.Scheme = s.cfg.UpstreamScheme
		target.Host = r.Host
	}
	return &target, nil
}

func joinURLPath(base, requestPath string) string {
	base = strings.TrimSuffix(base, "/")
	if requestPath == "" {
		requestPath = "/"
	}
	if !strings.HasPrefix(requestPath, "/") {
		requestPath = "/" + requestPath
	}
	return base + requestPath
}

func (s *Server) cachePolicy(r *http.Request) bool {
	if s.cfg.CachePolicy != nil {
		return s.cfg.CachePolicy(r)
	}
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		return false
	}
	if s.cfg.PathStyle && !strings.Contains(path, "/") {
		return false
	}
	for _, key := range []string{
		"list-type", "prefix", "delimiter", "continuation-token", "uploads",
		"uploadId", "acl", "tagging", "location", "versions", "object-lock",
	} {
		if _, exists := r.URL.Query()[key]; exists {
			return false
		}
	}
	return true
}

func (s *Server) cacheKey(target *url.URL, security requestSecurity) string {
	q := target.Query()
	for key := range q {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-amz-") || strings.HasPrefix(lower, "x-goog-") {
			q.Del(key)
		}
	}
	stableQuery := encodeSortedQuery(q)
	return strings.Join([]string{
		"tenant=" + security.identity.Tenant,
		"subject=" + security.identity.Subject,
		"auth=" + security.authScope,
		"host=" + strings.ToLower(target.Host),
		"path=" + target.EscapedPath(),
		"query=" + stableQuery,
	}, "|")
}

func encodeSortedQuery(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var pairs []string
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		if len(vals) == 0 {
			pairs = append(pairs, url.QueryEscape(key))
			continue
		}
		for _, value := range vals {
			pairs = append(pairs, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(pairs, "&")
}

func (s *Server) serveCacheable(w http.ResponseWriter, r *http.Request, target *url.URL, security requestSecurity, key string) {
	if meta, bodyPath, ok := loadMetadata(s.disk, key); ok && s.entryUsable(r, meta) && time.Now().Before(meta.FreshUntil) {
		s.metrics.hits.Add(1)
		if err := serveEntry(w, r, meta, bodyPath, s.cacheHeader("HIT")); err == nil {
			return
		}
		removeEntry(s.disk, key)
	}

	for {
		flight := &cacheFlight{done: make(chan struct{})}
		actual, loaded := s.flights.LoadOrStore(key, flight)
		if loaded {
			<-actual.(*cacheFlight).done
			if meta, bodyPath, ok := loadMetadata(s.disk, key); ok && s.entryUsable(r, meta) && time.Now().Before(meta.FreshUntil) {
				s.metrics.hits.Add(1)
				if err := serveEntry(w, r, meta, bodyPath, s.cacheHeader("HIT")); err == nil {
					return
				}
				removeEntry(s.disk, key)
			}
			continue
		}
		defer func() {
			close(flight.done)
			s.flights.Delete(key)
		}()
		break
	}

	meta, bodyPath, cached := loadMetadata(s.disk, key)
	if cached && s.entryUsable(r, meta) && time.Now().Before(meta.FreshUntil) {
		s.metrics.hits.Add(1)
		_ = serveEntry(w, r, meta, bodyPath, s.cacheHeader("HIT"))
		return
	}
	s.metrics.misses.Add(1)
	s.fetchAndMaybeStore(w, r, target, security.signer, key, meta, bodyPath)
}

func (s *Server) entryUsable(r *http.Request, meta *entryMetadata) bool {
	return meta != nil && (r.Method == http.MethodHead || meta.HasBody)
}

func (s *Server) fetchAndMaybeStore(w http.ResponseWriter, r *http.Request, target *url.URL, signer RequestSigner, key string, stale *entryMetadata, staleBody string) {
	outgoing, err := s.newUpstreamRequest(r, target)
	if err != nil {
		http.Error(w, "create upstream request", http.StatusBadGateway)
		return
	}
	canRevalidate := stale != nil && (r.Method == http.MethodHead || stale.HasBody)
	if canRevalidate {
		if stale.ETag != "" {
			outgoing.Header.Set("If-None-Match", stale.ETag)
		} else if stale.LastModified != "" {
			outgoing.Header.Set("If-Modified-Since", stale.LastModified)
		} else {
			canRevalidate = false
		}
	}
	if signer != nil {
		if err := signer(r.Context(), outgoing); err != nil {
			http.Error(w, "sign upstream request", http.StatusBadGateway)
			return
		}
	}
	resp, err := s.client.Do(outgoing)
	if err != nil {
		if s.canServeStale(stale) && s.entryUsable(r, stale) {
			_ = serveEntry(w, r, stale, staleBody, s.cacheHeader("STALE"))
			return
		}
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified && canRevalidate {
		s.metrics.revalidations.Add(1)
		stale.StoredAt = time.Now()
		stale.FreshUntil = stale.StoredAt.Add(s.ttlForResponse(resp, r))
		mergeValidationHeaders(stale, resp.Header)
		if err := storeMetadata(s.disk, key, stale); err != nil {
			s.log.Warn("refresh metadata: %v", err)
		}
		_ = serveEntry(w, r, stale, staleBody, s.cacheHeader("REVALIDATED"))
		return
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		removeEntry(s.disk, key)
	}
	if !s.shouldStore(r, resp) {
		s.copyUpstreamResponse(w, resp, s.cacheHeader("MISS"))
		return
	}
	if r.Method == http.MethodHead {
		s.disk.Remove(bodyKey(key))
		meta := s.metadataFromResponse(resp, r, false, 0)
		if err := storeMetadata(s.disk, key, meta); err == nil {
			s.metrics.stores.Add(1)
		}
		s.copyUpstreamResponse(w, resp, s.cacheHeader("MISS"))
		return
	}
	s.streamAndStore(w, resp, r, key)
}

func (s *Server) canServeStale(meta *entryMetadata) bool {
	return meta != nil && s.cfg.StaleIfError > 0 && time.Now().Before(meta.FreshUntil.Add(s.cfg.StaleIfError))
}

func (s *Server) shouldStore(r *http.Request, resp *http.Response) bool {
	if resp.StatusCode != http.StatusOK || r.Header.Get("Range") != "" {
		return false
	}
	cc := strings.ToLower(resp.Header.Get("Cache-Control"))
	if strings.Contains(cc, "no-store") || (s.cfg.RespectPrivate && strings.Contains(cc, "private")) {
		return false
	}
	return resp.Header.Get("Set-Cookie") == ""
}

func (s *Server) ttlForResponse(resp *http.Response, r *http.Request) time.Duration {
	if value, ok := cacheControlMaxAge(resp.Header.Get("Cache-Control")); ok {
		ttl := time.Duration(value) * time.Second
		if s.cfg.MaxTTL > 0 && ttl > s.cfg.MaxTTL {
			return s.cfg.MaxTTL
		}
		return ttl
	}
	if r.URL.Query().Get("versionId") != "" {
		return s.cfg.VersionedTTL
	}
	return s.cfg.FreshTTL
}

func cacheControlMaxAge(value string) (int64, bool) {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(strings.ToLower(part))
		if !strings.HasPrefix(part, "max-age=") {
			continue
		}
		seconds, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(part, "max-age=")), 10, 64)
		return seconds, err == nil && seconds >= 0
	}
	return 0, false
}

func (s *Server) metadataFromResponse(resp *http.Response, r *http.Request, hasBody bool, size int64) *entryMetadata {
	now := time.Now()
	return &entryMetadata{
		StatusCode:   resp.StatusCode,
		Header:       filteredResponseHeader(resp.Header),
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		StoredAt:     now,
		FreshUntil:   now.Add(s.ttlForResponse(resp, r)),
		HasBody:      hasBody,
		BodySize:     size,
	}
}

func mergeValidationHeaders(meta *entryMetadata, header http.Header) {
	if value := header.Get("ETag"); value != "" {
		meta.ETag = value
		meta.Header.Set("ETag", value)
	}
	if value := header.Get("Last-Modified"); value != "" {
		meta.LastModified = value
		meta.Header.Set("Last-Modified", value)
	}
}

func (s *Server) streamAndStore(w http.ResponseWriter, resp *http.Response, r *http.Request, key string) {
	if s.cfg.MaxEntrySize > 0 && resp.ContentLength > s.cfg.MaxEntrySize {
		s.copyUpstreamResponse(w, resp, s.cacheHeader("BYPASS"))
		return
	}
	hash := sha256.Sum256([]byte(bodyKey(key)))
	hexHash := hex.EncodeToString(hash[:])
	dir := filepath.Join(s.disk.Dir(), "streams", hexHash[:2])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.copyUpstreamResponse(w, resp, s.cacheHeader("BYPASS"))
		return
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		s.copyUpstreamResponse(w, resp, s.cacheHeader("BYPASS"))
		return
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	copyResponseHeader(w.Header(), resp.Header)
	if value := s.cacheHeader("MISS"); value != "" {
		w.Header().Set("X-Proxykit-Cache", value)
	}
	w.WriteHeader(resp.StatusCode)
	writer := &limitedCacheWriter{writer: tmp, limit: s.cfg.MaxEntrySize}
	written, copyErr := io.Copy(io.MultiWriter(w, writer), resp.Body)
	closeErr := tmp.Close()
	if copyErr != nil || closeErr != nil || writer.exceeded || writer.failed {
		return
	}
	finalPath := filepath.Join(dir, hexHash)
	s.disk.Remove(bodyKey(key))
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return
	}
	s.disk.RegisterAfterStream(bodyKey(key), finalPath, written)
	meta := s.metadataFromResponse(resp, r, true, written)
	if err := storeMetadata(s.disk, key, meta); err != nil {
		s.disk.Remove(bodyKey(key))
		return
	}
	s.metrics.stores.Add(1)
}

type limitedCacheWriter struct {
	writer   io.Writer
	limit    int64
	written  int64
	exceeded bool
	failed   bool
}

func (w *limitedCacheWriter) Write(p []byte) (int, error) {
	if w.limit > 0 && w.written+int64(len(p)) > w.limit {
		w.exceeded = true
		return len(p), nil
	}
	if w.exceeded {
		return len(p), nil
	}
	n, err := w.writer.Write(p)
	w.written += int64(n)
	if err != nil || n != len(p) {
		w.failed = true
		return len(p), nil
	}
	return len(p), nil
}

func (s *Server) forwardMutation(w http.ResponseWriter, r *http.Request, target *url.URL, security requestSecurity) {
	outgoing, err := s.newUpstreamRequest(r, target)
	if err != nil {
		http.Error(w, "create upstream request", http.StatusBadGateway)
		return
	}
	if security.signer != nil {
		if err := security.signer(r.Context(), outgoing); err != nil {
			http.Error(w, "sign upstream request", http.StatusBadGateway)
			return
		}
	}
	resp, err := s.client.Do(outgoing)
	if err != nil {
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && security.cacheable {
		s.invalidateTarget(target, security)
	}
	s.copyUpstreamResponse(w, resp, "")
}

func (s *Server) invalidateTarget(target *url.URL, security requestSecurity) {
	key := s.cacheKey(target, security)
	prefix, _, _ := strings.Cut(key, "|query=")
	s.disk.RemoveMatching(func(record string) bool {
		return strings.HasPrefix(record, "s3:meta:"+prefix+"|query=") ||
			strings.HasPrefix(record, "s3:body:"+prefix+"|query=")
	})
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, target *url.URL, signer RequestSigner, cacheStatus string) {
	outgoing, err := s.newUpstreamRequest(r, target)
	if err != nil {
		http.Error(w, "create upstream request", http.StatusBadGateway)
		return
	}
	if signer != nil {
		if err := signer(r.Context(), outgoing); err != nil {
			http.Error(w, "sign upstream request", http.StatusBadGateway)
			return
		}
	}
	resp, err := s.client.Do(outgoing)
	if err != nil {
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	s.copyUpstreamResponse(w, resp, s.cacheHeader(cacheStatus))
}

func (s *Server) newUpstreamRequest(r *http.Request, target *url.URL) (*http.Request, error) {
	outgoing := r.Clone(r.Context())
	outgoing.URL = new(url.URL)
	*outgoing.URL = *target
	outgoing.Host = target.Host
	outgoing.RequestURI = ""
	removeHopByHopHeaders(outgoing.Header)
	outgoing.Header.Del("X-Proxykit-Cache")
	return outgoing, nil
}

func (s *Server) copyUpstreamResponse(w http.ResponseWriter, resp *http.Response, cacheStatus string) {
	copyResponseHeader(w.Header(), resp.Header)
	if cacheStatus != "" {
		w.Header().Set("X-Proxykit-Cache", cacheStatus)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) cacheHeader(value string) string {
	if !s.cfg.ExposeCacheHeader {
		return ""
	}
	return value
}

func filteredResponseHeader(source http.Header) http.Header {
	destination := source.Clone()
	removeHopByHopHeaders(destination)
	destination.Del("X-Proxykit-Cache")
	return destination
}

func copyResponseHeader(destination, source http.Header) {
	for key := range destination {
		destination.Del(key)
	}
	for key, values := range filteredResponseHeader(source) {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func removeHopByHopHeaders(header http.Header) {
	for _, key := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		header.Del(key)
	}
}

func isMutation(method string) bool {
	return method == http.MethodPut || method == http.MethodPost || method == http.MethodDelete
}
