package apt

import (
	"testing"

	"github.com/agentuity/proxykit/cache"
)

func TestDefaultConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)

	if cfg.CacheDir != dir {
		t.Fatalf("CacheDir = %q, want %q", cfg.CacheDir, dir)
	}
	if cfg.MetadataTTL != DefaultMetadataTTL {
		t.Fatalf("MetadataTTL = %v, want %v", cfg.MetadataTTL, DefaultMetadataTTL)
	}
	if want := cache.DefaultMaxSize(dir, DefaultMaxCacheDiskPercent); cfg.MaxCacheSize != want {
		t.Fatalf("MaxCacheSize = %d, want %d", cfg.MaxCacheSize, want)
	}
	if len(cfg.AllowedHostPatterns) == 0 {
		t.Fatal("AllowedHostPatterns is empty")
	}

	original := defaultAllowedHostPatterns[0]
	cfg.AllowedHostPatterns[0] = "changed.example"
	if defaultAllowedHostPatterns[0] != original {
		t.Fatal("DefaultConfig returned the package default slice without cloning it")
	}
}
