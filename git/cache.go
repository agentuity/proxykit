package git

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
)

// refsFingerprint computes the fingerprint of an info/refs response body.
// The fingerprint is used as part of the L2 pack cache key.
// Returns 40 hex characters (first 20 bytes of SHA256).
func refsFingerprint(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:20])
}

// refsCacheKey returns the L1 cache key for a refs/capability advertisement.
// The authScope parameter is a stable hash of the effective credentials,
// ensuring that different sandboxes with different authorization levels
// see separate cache entries and cannot leak data cross-tenant.
func refsCacheKey(host, repoPath, authScope string) string {
	return "refs:" + host + ":" + repoPath + ":" + authScope + ":upload-pack"
}

// lsRefsCacheKey returns the L1 cache key for a v2 ls-refs response.
// Separate from refsCacheKey because v2 info/refs (capability advertisement)
// and ls-refs (actual ref list) are different responses for the same repo.
func lsRefsCacheKey(host, repoPath, authScope string) string {
	return "lsrefs:" + host + ":" + repoPath + ":" + authScope
}

// packCacheKey returns the L2 cache key for a fresh-clone pack.
// authScope partitions entries by authorization context (see refsCacheKey).
func packCacheKey(host, repoPath, authScope, fingerprint string) string {
	return "pack:" + host + ":" + repoPath + ":" + authScope + ":" + fingerprint
}

// authScopeFromRequest derives a stable auth scope string from the request's
// Authorization header. This is a SHA256 hash of the header value (which
// already contains the real token after InjectSecrets). Using a hash ensures
// the cache key doesn't contain the raw credential.
func authScopeFromRequest(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "anon"
	}
	h := sha256.Sum256([]byte(auth))
	return hex.EncodeToString(h[:10]) // 20 hex chars, sufficient for partitioning
}

// repoRefsPrefix returns the key prefix shared by all refs cache entries
// (across all auth scopes) for a given repo. Used for push invalidation.
func repoRefsPrefix(host, repoPath string) string {
	return "refs:" + host + ":" + repoPath + ":"
}

// repoLsRefsPrefix returns the key prefix shared by all ls-refs cache entries.
func repoLsRefsPrefix(host, repoPath string) string {
	return "lsrefs:" + host + ":" + repoPath + ":"
}

// cacheKeyHash produces a hex-encoded SHA256 hash of a cache key.
// Used for filesystem path computation.
func cacheKeyHash(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// fingerprintFromFile reads a cached refs file and computes its fingerprint.
func fingerprintFromFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read refs file: %w", err)
	}
	return refsFingerprint(data), nil
}
