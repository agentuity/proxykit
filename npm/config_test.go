package npm

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
	if cfg.UpstreamURL != DefaultUpstreamURL {
		t.Fatalf("UpstreamURL = %q, want %q", cfg.UpstreamURL, DefaultUpstreamURL)
	}
	if cfg.MetadataTTL != DefaultMetadataTTL {
		t.Fatalf("MetadataTTL = %v, want %v", cfg.MetadataTTL, DefaultMetadataTTL)
	}
	if want := cache.DefaultMaxSize(dir, DefaultMaxCacheDiskPercent); cfg.MaxCacheSize != want {
		t.Fatalf("MaxCacheSize = %d, want %d", cfg.MaxCacheSize, want)
	}
	if len(cfg.AllowedUpstreamRegistryPatterns) == 0 {
		t.Fatal("AllowedUpstreamRegistryPatterns is empty")
	}

	original := DefaultAllowedUpstreamRegistryPatterns[0]
	cfg.AllowedUpstreamRegistryPatterns[0] = "changed.example"
	if DefaultAllowedUpstreamRegistryPatterns[0] != original {
		t.Fatal("DefaultConfig returned the package default slice without cloning it")
	}
}
