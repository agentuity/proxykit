package proxykit

import (
	"github.com/agentuity/proxykit/apt"
	"github.com/agentuity/proxykit/cache"
	"github.com/agentuity/proxykit/git"
	"github.com/agentuity/proxykit/internal/glob"
	"github.com/agentuity/proxykit/npm"
	"github.com/agentuity/proxykit/s3"
)

// GlobMatcher is a compiled glob pattern for hostname matching.
type GlobMatcher = glob.GlobMatcher

// CompileGlob compiles a hostname glob pattern into a GlobMatcher.
func CompileGlob(pattern string) (*GlobMatcher, error) {
	return glob.CompileGlob(pattern)
}

// CacheConfig is the disk cache configuration shared by the proxy packages.
type CacheConfig = cache.Config

// Cache is the shared disk-backed cache implementation.
type Cache = cache.Cache

// DefaultCacheConfig returns the recommended shared cache configuration.
func DefaultCacheConfig(cacheDir string) CacheConfig { return cache.DefaultConfig(cacheDir) }

// NPMConfig configures the npm proxy server.
type NPMConfig = npm.Config

// NPMServer is the npm caching proxy server.
type NPMServer = npm.Server

// DefaultNPMConfig returns the recommended npm proxy configuration.
func DefaultNPMConfig(cacheDir string) NPMConfig { return npm.DefaultConfig(cacheDir) }

// NewNPM creates a new npm proxy server.
func NewNPM(cfg NPMConfig) (*NPMServer, error) { return npm.New(cfg) }

// APTConfig configures the apt proxy server.
type APTConfig = apt.Config

// APTServer is the apt caching proxy server.
type APTServer = apt.Server

// DefaultAPTConfig returns the recommended APT proxy configuration.
func DefaultAPTConfig(cacheDir string) APTConfig { return apt.DefaultConfig(cacheDir) }

// NewAPT creates a new apt proxy server.
func NewAPT(cfg APTConfig) (*APTServer, error) { return apt.New(cfg) }

// GitConfig configures the Git smart HTTP proxy handler.
type GitConfig = git.Config

// GitCacheConfig configures one Git cache tier.
type GitCacheConfig = git.CacheConfig

// GitHandler is the Git smart HTTP proxy handler.
type GitHandler = git.Handler

// DefaultGitConfig returns the recommended Git proxy configuration.
func DefaultGitConfig(cacheDir string) GitConfig { return git.DefaultConfig(cacheDir) }

// NewGit creates a new Git proxy handler.
func NewGit(cfg GitConfig) (*GitHandler, error) { return git.New(cfg) }

// S3Config configures an S3 acceleration proxy.
type S3Config = s3.Config

// S3Server is the S3 acceleration HTTP handler and standalone server.
type S3Server = s3.Server

// S3Credentials contains optional AWS-compatible upstream credentials.
type S3Credentials = s3.Credentials

// S3Identity identifies the tenant and subject for an S3 request.
type S3Identity = s3.Identity

// S3SigningContext contains a dynamic signer and its stable cache scope.
type S3SigningContext = s3.SigningContext

// DefaultS3Config returns the recommended S3 acceleration configuration.
func DefaultS3Config(cacheDir, upstreamURL string) S3Config {
	return s3.DefaultConfig(cacheDir, upstreamURL)
}

// NewS3 creates a new S3 acceleration server.
func NewS3(cfg S3Config) (*S3Server, error) { return s3.New(cfg) }
