package glob

import (
	"regexp"
	"strings"
)

// GlobMatcher is a compiled glob pattern for hostname matching.
type GlobMatcher struct {
	pattern string         // Original glob pattern
	regex   *regexp.Regexp // Compiled regex
}

// CompileGlob compiles a glob pattern into a GlobMatcher.
func CompileGlob(pattern string) (*GlobMatcher, error) {
	escaped := regexp.QuoteMeta(pattern)

	// Replace "**" before "*" to avoid double-processing.
	escaped = strings.ReplaceAll(escaped, "\\*\\*", ".+")
	escaped = strings.ReplaceAll(escaped, "\\*", "[^.]+")

	expr := "(?i)^" + escaped + "$"
	compiled, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}

	return &GlobMatcher{
		pattern: pattern,
		regex:   compiled,
	}, nil
}

// Match tests whether a hostname matches the glob pattern.
func (g *GlobMatcher) Match(hostname string) bool {
	if g == nil || g.regex == nil {
		return false
	}
	return g.regex.MatchString(hostname)
}

// String returns the original glob pattern.
func (g *GlobMatcher) String() string {
	if g == nil {
		return ""
	}
	return g.pattern
}
