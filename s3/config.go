package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/agentuity/go-common/logger"
	"github.com/agentuity/proxykit/cache"
)

const (
	// DefaultFreshTTL is how long an unversioned object is served without
	// contacting the upstream.
	DefaultFreshTTL = 5 * time.Minute
	// DefaultVersionedTTL is the freshness lifetime for a request containing an
	// explicit S3 versionId.
	DefaultVersionedTTL = 7 * 24 * time.Hour
	// DefaultMaxTTL caps freshness supplied by an upstream Cache-Control header.
	DefaultMaxTTL = 24 * time.Hour
	// DefaultMaxCacheDiskPercent is the fraction of the cache filesystem used by
	// DefaultConfig when its capacity can be determined.
	DefaultMaxCacheDiskPercent = 0.10
	// DefaultSoftEvictPercent starts age-based eviction at 80 percent capacity.
	DefaultSoftEvictPercent = 0.8
	// DefaultMaxEntrySize is used when a disk-derived entry limit is unavailable.
	DefaultMaxEntrySize = int64(1 << 30)
	// DefaultEvictionInterval controls background expiration and LRU checks.
	DefaultEvictionInterval = time.Minute
)

// Credentials contains AWS-compatible credentials used to sign upstream
// requests. Scope identifies the cache isolation boundary; when empty, a hash
// of AccessKeyID is used.
type Credentials struct {
	// AccessKeyID is the AWS-compatible access key identifier.
	AccessKeyID string
	// SecretAccessKey is the AWS-compatible signing secret.
	SecretAccessKey string
	// SessionToken is the optional temporary credential token.
	SessionToken string
	// Region is the SigV4 signing region. Empty defaults to us-east-1.
	Region string
	// Scope is a stable, non-secret cache authorization scope.
	Scope string
}

// CredentialResolver selects upstream credentials for a request. The boolean
// result is false for an unsigned or public upstream request.
type CredentialResolver func(context.Context, *http.Request, Identity) (Credentials, bool, error)

// RequestSigner signs or otherwise authorizes one complete outgoing upstream
// request. Conditional revalidation headers are present before it is called.
type RequestSigner func(context.Context, *http.Request) error

// SigningContext binds a stable cache authorization scope to a request signer.
// Scope must identify the effective upstream permissions without containing a
// raw secret.
type SigningContext struct {
	// Scope identifies the effective upstream authorization boundary.
	Scope string
	// Sign authorizes the final outgoing upstream request.
	Sign RequestSigner
}

// Identity identifies the tenant and optional subject that originated a
// request. These values become part of every cache key.
type Identity struct {
	// Tenant is the required top-level cache isolation boundary.
	Tenant string
	// Subject optionally separates users, jobs, or service identities.
	Subject string
}

// IdentityResolver maps the original request to its trusted tenant boundary.
// It can inspect RemoteAddr, headers, TLS state, and request context. A false
// result permits forwarding but disables caching for the request.
type IdentityResolver func(context.Context, *http.Request) (Identity, bool, error)

// SigningResolver examines the original request and resolved identity, then
// dynamically selects the signer and cache authorization scope.
type SigningResolver func(context.Context, *http.Request, Identity) (SigningContext, bool, error)

// Authorizer validates whether a client may use the configured upstream
// credentials. Returning an error rejects the request.
type Authorizer func(*http.Request) error

// CachePolicy decides whether a GET or HEAD request represents a cacheable S3
// object read. The default policy excludes common bucket-listing and control
// operations.
type CachePolicy func(*http.Request) bool

// Config configures an S3 acceleration proxy.
type Config struct {
	// UpstreamURL is an optional fixed S3-compatible endpoint, such as
	// https://s3.us-east-1.amazonaws.com or http://127.0.0.1:9000. When empty,
	// each request retains its original absolute URL or Host.
	UpstreamURL string
	// UpstreamScheme is used in transparent mode when a request has no URL
	// scheme. It defaults to https.
	UpstreamScheme string
	// CacheDir is the directory containing cached objects and metadata.
	CacheDir string
	// MaxCacheSize is the maximum total cache size in bytes. Zero is unlimited.
	MaxCacheSize int64
	// MaxEntrySize is the largest object stored in the cache. Zero is unlimited.
	MaxEntrySize int64
	// FreshTTL controls freshness for unversioned objects.
	FreshTTL time.Duration
	// VersionedTTL controls freshness for requests with a versionId.
	VersionedTTL time.Duration
	// MaxTTL caps an upstream Cache-Control max-age value.
	MaxTTL time.Duration
	// StaleIfError permits serving a stale object for this duration after its
	// freshness lifetime when upstream revalidation fails. Zero disables it.
	StaleIfError time.Duration
	// SoftEvictPercent begins age-based eviction before MaxCacheSize is reached.
	SoftEvictPercent float64
	// EvictionInterval controls background cache eviction.
	EvictionInterval time.Duration
	// RespectPrivate prevents storage of Cache-Control: private responses.
	RespectPrivate bool
	// ExposeCacheHeader emits X-Proxykit-Cache diagnostics.
	ExposeCacheHeader bool
	// PathStyle requires paths to contain both a bucket and object key before
	// they are cached. Disable it for virtual-hosted bucket endpoints.
	PathStyle bool

	// Credentials signs every upstream request when non-nil. When no credentials
	// resolve, the original Authorization header or presigned query is preserved.
	Credentials *Credentials
	// CredentialResolver overrides Credentials and may select credentials per request.
	CredentialResolver CredentialResolver
	// IdentityResolver maps requests to trusted tenant and subject identities.
	// When nil, all requests use one local default tenant.
	IdentityResolver IdentityResolver
	// SigningResolver overrides CredentialResolver and Credentials. It supports
	// request-dependent or externally implemented signing without exposing keys
	// to proxykit.
	SigningResolver SigningResolver
	// Authorizer can restrict access to this credential-bearing endpoint.
	Authorizer Authorizer
	// CachePolicy overrides the default object-read eligibility policy.
	CachePolicy CachePolicy
	// HTTPClient sends requests to the configured upstream.
	HTTPClient *http.Client
	// TLSConfig configures outbound TLS when HTTPClient is nil. It is cloned
	// before use. Client-facing TLS interception belongs to the outer proxy.
	TLSConfig *tls.Config
	// Logger receives proxy and cache diagnostics.
	Logger logger.Logger
}

// DefaultConfig returns a production-oriented S3 acceleration configuration.
// Callers should configure identity, signing, and authorization callbacks as
// needed before passing the result to New.
func DefaultConfig(cacheDir, upstreamURL string) Config {
	maxSize := cache.DefaultMaxSize(cacheDir, DefaultMaxCacheDiskPercent)
	maxEntry := DefaultMaxEntrySize
	if maxSize > 0 && maxSize/10 < maxEntry {
		maxEntry = maxSize / 10
	}
	return Config{
		UpstreamURL:       upstreamURL,
		UpstreamScheme:    "https",
		CacheDir:          cacheDir,
		MaxCacheSize:      maxSize,
		MaxEntrySize:      maxEntry,
		FreshTTL:          DefaultFreshTTL,
		VersionedTTL:      DefaultVersionedTTL,
		MaxTTL:            DefaultMaxTTL,
		SoftEvictPercent:  DefaultSoftEvictPercent,
		EvictionInterval:  DefaultEvictionInterval,
		RespectPrivate:    true,
		ExposeCacheHeader: true,
		PathStyle:         true,
	}
}

// Validate checks the S3 proxy configuration without creating resources.
func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.CacheDir) == "" {
		return errors.New("cache directory is required")
	}
	if cfg.UpstreamURL != "" {
		u, err := url.Parse(cfg.UpstreamURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid upstream URL %q", cfg.UpstreamURL)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("unsupported upstream URL scheme %q", u.Scheme)
		}
	}
	scheme := strings.TrimSpace(cfg.UpstreamScheme)
	if scheme != "" && scheme != "http" && scheme != "https" {
		return fmt.Errorf("unsupported upstream scheme %q", scheme)
	}
	if cfg.MaxCacheSize < 0 || cfg.MaxEntrySize < 0 {
		return errors.New("cache sizes must not be negative")
	}
	if cfg.FreshTTL < 0 || cfg.VersionedTTL < 0 || cfg.MaxTTL < 0 || cfg.StaleIfError < 0 {
		return errors.New("cache durations must not be negative")
	}
	if cfg.SoftEvictPercent < 0 || cfg.SoftEvictPercent > 1 {
		return errors.New("soft eviction percent must be between 0 and 1")
	}
	if cfg.EvictionInterval < 0 {
		return errors.New("eviction interval must not be negative")
	}
	if cfg.Credentials != nil {
		if strings.TrimSpace(cfg.Credentials.AccessKeyID) == "" || strings.TrimSpace(cfg.Credentials.SecretAccessKey) == "" {
			return errors.New("both access key ID and secret access key are required")
		}
	}
	return nil
}
