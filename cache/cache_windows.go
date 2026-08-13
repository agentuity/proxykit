//go:build windows

package cache

// DefaultMaxSize returns 0 on Windows where syscall.Statfs is unavailable.
// Callers should treat 0 as "unlimited" or apply their own fallback.
func DefaultMaxSize(_ string, _ float64) int64 {
	return 0
}
