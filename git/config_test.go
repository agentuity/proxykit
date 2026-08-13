package git

import (
	"path/filepath"
	"testing"

	"github.com/agentuity/proxykit/cache"
)

func TestDefaultConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)

	if cfg.RefsCache.Dir != filepath.Join(dir, "refs") {
		t.Fatalf("RefsCache.Dir = %q", cfg.RefsCache.Dir)
	}
	if cfg.PacksCache.Dir != filepath.Join(dir, "packs") {
		t.Fatalf("PacksCache.Dir = %q", cfg.PacksCache.Dir)
	}
	if want := cache.DefaultMaxSize(cfg.RefsCache.Dir, DefaultRefsCacheDiskPercent); cfg.RefsCache.MaxSize != want {
		t.Fatalf("RefsCache.MaxSize = %d, want %d", cfg.RefsCache.MaxSize, want)
	}
	if want := cache.DefaultMaxSize(cfg.PacksCache.Dir, DefaultPacksCacheDiskPercent); cfg.PacksCache.MaxSize != want {
		t.Fatalf("PacksCache.MaxSize = %d, want %d", cfg.PacksCache.MaxSize, want)
	}
	if cfg.RefsTTL != DefaultRefsTTL {
		t.Fatalf("RefsTTL = %v, want %v", cfg.RefsTTL, DefaultRefsTTL)
	}
	if cfg.PacksTTL != DefaultPacksTTL {
		t.Fatalf("PacksTTL = %v, want %v", cfg.PacksTTL, DefaultPacksTTL)
	}
	if cfg.MaxUploadPackRequestSize != DefaultMaxUploadPackRequestSize {
		t.Fatalf("MaxUploadPackRequestSize = %d", cfg.MaxUploadPackRequestSize)
	}
	if cfg.MaxReceivePackRequestSize != DefaultMaxReceivePackRequestSize {
		t.Fatalf("MaxReceivePackRequestSize = %d", cfg.MaxReceivePackRequestSize)
	}
}
