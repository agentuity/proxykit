package git

import (
	"net/http"
	"strings"
)

// IsGitRequest reports whether r is a Git smart HTTP protocol request.
// Checks URL path suffixes and Content-Type.
// Must be called after TLS termination (r.URL.Path is populated).
func IsGitRequest(r *http.Request) bool {
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/info/refs"):
		svc := r.URL.Query().Get("service")
		return svc == "git-upload-pack" || svc == "git-receive-pack"
	case strings.HasSuffix(path, "/git-upload-pack"),
		strings.HasSuffix(path, "/git-receive-pack"):
		return true
	}
	// Also detect by Content-Type for POST requests where path matching fails.
	ct := r.Header.Get("Content-Type")
	return ct == "application/x-git-upload-pack-request" ||
		ct == "application/x-git-receive-pack-request"
}

// parseRepoPath extracts the logical repository path from a Git HTTP URL.
// Input:  "/org/repo.git/info/refs"
// Output: "/org/repo.git"
func parseRepoPath(urlPath string) string {
	suffixes := []string{
		"/info/refs",
		"/git-upload-pack",
		"/git-receive-pack",
	}
	for _, suffix := range suffixes {
		if idx := strings.LastIndex(urlPath, suffix); idx >= 0 {
			return urlPath[:idx]
		}
	}
	return urlPath
}

// isProtocolV2 reports whether the request uses Git protocol version 2.
func isProtocolV2(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Git-Protocol"), "version=2")
}

// IsLFSRequest reports whether the request is a Git LFS API request.
// LFS requests are forwarded directly with credential injection (no caching).
// This is exported for potential use by the proxy dispatch layer; the git
// Handler itself does not call it (LFS requests fall through to forwardDirect
// via the default case in Handle).
func IsLFSRequest(r *http.Request) bool {
	return strings.Contains(r.URL.Path, "/info/lfs/")
}
