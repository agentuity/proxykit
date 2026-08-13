package npm

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/agentuity/proxykit/internal/glob"
)

const upstreamPathPrefix = "/_upstream/"

// DefaultAllowedUpstreamRegistryPatterns lists npm-compatible registry hosts
// that the caching proxy may fetch from. The default upstream
// (registry.npmjs.org) is always permitted in addition to these patterns.
//
// Opt-in patterns cover common private and alternate registries:
//   - Yarn (registry.yarnpkg.com)
//   - GitHub Packages (npm.pkg.github.com)
//   - GitLab (gitlab.com and self-hosted *.gitlab.com)
//   - Google Artifact Registry (*-npm.pkg.dev)
//   - AWS CodeArtifact (*.d.codeartifact.*.amazonaws.com)
//   - Azure Artifacts (pkgs.dev.azure.com, pkgs.visualstudio.com)
//   - Authoritative package CDNs used by direct tarball dependencies (cdn.sheetjs.com)
var DefaultAllowedUpstreamRegistryPatterns = []string{
	"registry.npmjs.org",
	"registry.yarnpkg.com",
	"yarnpkg.com",
	"*.yarnpkg.com",
	"npm.pkg.github.com",
	"gitlab.com",
	"*.gitlab.com",
	"*-npm.pkg.dev",
	"*.d.codeartifact.*.amazonaws.com",
	"pkgs.dev.azure.com",
	"pkgs.visualstudio.com",
	"*.visualstudio.com",
	"cdn.sheetjs.com",
	"*.sheetjs.com",
}

type upstreamRegistryAllowlist struct {
	matchers []*glob.GlobMatcher
}

func newUpstreamRegistryAllowlist(patterns []string) (*upstreamRegistryAllowlist, error) {
	if len(patterns) == 0 {
		patterns = DefaultAllowedUpstreamRegistryPatterns
	}
	matchers := make([]*glob.GlobMatcher, 0, len(patterns))
	seen := make(map[string]struct{}, len(patterns))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		matcher, err := glob.CompileGlob(pattern)
		if err != nil {
			return nil, fmt.Errorf("compile allowed upstream registry pattern %q: %w", pattern, err)
		}
		matchers = append(matchers, matcher)
	}
	return &upstreamRegistryAllowlist{matchers: matchers}, nil
}

func (a *upstreamRegistryAllowlist) allowed(host string) bool {
	if a == nil {
		return false
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	for _, matcher := range a.matchers {
		if matcher.Match(host) {
			return true
		}
	}
	return false
}

func upstreamRegistryBaseURL(host string) string {
	host = strings.TrimSuffix(host, "/")
	if usesHTTPUpstream(host) {
		return "http://" + host
	}
	return "https://" + host
}

func usesHTTPUpstream(host string) bool {
	hostname, _, err := net.SplitHostPort(host)
	if err != nil {
		hostname = host
	}
	if strings.EqualFold(hostname, "localhost") {
		return true
	}
	return net.ParseIP(hostname) != nil
}

func proxyUpstreamBaseURL(proxyHost, upstreamHost string) string {
	return "http://" + strings.TrimSuffix(proxyHost, "/") + upstreamPathPrefix + upstreamHost
}

// parseUpstreamPath extracts an opt-in upstream registry host from a
// /_upstream/<host>/... request path. Returns ok=false when the path does
// not use upstream routing.
func parseUpstreamPath(path string) (host string, remainder string, ok bool) {
	if !strings.HasPrefix(path, upstreamPathPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(path, upstreamPathPrefix)
	var cutOK bool
	host, remainder, cutOK = strings.Cut(rest, "/")
	ok = cutOK
	if !ok {
		remainder = "/"
	} else {
		remainder = "/" + remainder
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "", "", false
	}
	return host, remainder, true
}

func validateUpstreamFetchURL(rawURL string, allowlist *upstreamRegistryAllowlist) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	switch parsed.Scheme {
	case "http", "https":
	default:
		return nil, fmt.Errorf("unsupported scheme %q", parsed.Scheme)
	}
	host := parsed.Hostname()
	if host == "" {
		return nil, fmt.Errorf("empty host")
	}
	if allowlist == nil || !allowlist.allowed(host) {
		return nil, fmt.Errorf("host %q is not an allowed upstream fetch host", host)
	}
	return parsed, nil
}

func isForwardProxyRequest(r *http.Request) bool {
	return strings.HasPrefix(r.RequestURI, "http://") || strings.HasPrefix(r.RequestURI, "https://")
}
