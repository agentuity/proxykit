package cache

import "testing"

func TestDefaultConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)

	if cfg.Dir != dir {
		t.Fatalf("Dir = %q, want %q", cfg.Dir, dir)
	}
	if want := DefaultMaxSize(dir, DefaultMaxDiskPercent); cfg.MaxSize != want {
		t.Fatalf("MaxSize = %d, want %d", cfg.MaxSize, want)
	}
	if cfg.TTL != 0 {
		t.Fatalf("TTL = %v, want no expiration", cfg.TTL)
	}
	if cfg.SoftEvictPercent != 0.8 {
		t.Fatalf("SoftEvictPercent = %v, want 0.8", cfg.SoftEvictPercent)
	}
}
