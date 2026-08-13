package npm

import (
	"net/url"
	"regexp"
	"strings"
)

var directTarballURLPattern = regexp.MustCompile(`https://[a-zA-Z0-9][a-zA-Z0-9._-]*(?::[0-9]+)?/[^\s"'\\]+\.tgz(?:\?[^\s"'\\]*)?`)

// rewriteDirectTarballURLs replaces allowed absolute HTTPS tarball URLs with
// npm proxy /_upstream/<host>/ routes.
func rewriteDirectTarballURLs(content []byte, proxyBase string, allowlist *upstreamRegistryAllowlist) []byte {
	if len(content) == 0 || allowlist == nil {
		return content
	}
	proxyBase = strings.TrimRight(proxyBase, "/")
	return directTarballURLPattern.ReplaceAllFunc(content, func(match []byte) []byte {
		parsed, err := url.Parse(string(match))
		if err != nil || parsed.Host == "" {
			return match
		}
		if !allowlist.allowed(parsed.Hostname()) {
			return match
		}
		path := parsed.EscapedPath()
		if path == "" {
			path = "/"
		}
		if parsed.RawQuery != "" {
			path += "?" + parsed.RawQuery
		}
		return []byte(proxyBase + upstreamPathPrefix + parsed.Host + path)
	})
}

// RewriteDirectTarballURLsWithDefaults rewrites allowed absolute HTTPS tarball
// URLs using DefaultAllowedUpstreamRegistryPatterns.
func RewriteDirectTarballURLsWithDefaults(content []byte, proxyBase string) ([]byte, error) {
	allowlist, err := newUpstreamRegistryAllowlist(DefaultAllowedUpstreamRegistryPatterns)
	if err != nil {
		return content, err
	}
	return rewriteDirectTarballURLs(content, proxyBase, allowlist), nil
}
