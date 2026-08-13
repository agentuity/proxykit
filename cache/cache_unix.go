//go:build !windows

package cache

import (
	"os"
	"path/filepath"
	"syscall"
)

// DefaultMaxSize computes a default cache max size as a percentage of the total
// disk space at the given directory path. If the directory doesn't exist yet, it
// walks up to find an existing parent for the stat call. Returns 0 if the disk
// size cannot be determined.
func DefaultMaxSize(dir string, percent float64) int64 {
	if percent <= 0 || percent > 1 {
		return 0
	}
	// Walk up to find an existing directory for Statfs.
	statDir := dir
	for {
		if _, err := os.Stat(statDir); err == nil {
			break
		}
		parent := filepath.Dir(statDir)
		if parent == statDir {
			return 0 // reached filesystem root without finding an existing dir
		}
		statDir = parent
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(statDir, &stat); err != nil {
		return 0
	}
	totalDisk := int64(stat.Blocks * uint64(stat.Bsize))
	return int64(float64(totalDisk) * percent)
}
