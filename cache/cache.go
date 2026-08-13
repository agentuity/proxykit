package cache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agentuity/go-common/logger"
	_ "modernc.org/sqlite"
)

// DefaultMaxDiskPercent is the fraction of the cache filesystem used by
// DefaultConfig when a maximum size can be determined.
const DefaultMaxDiskPercent = 0.10

// Config configures a disk cache instance.
type Config struct {
	Dir     string        // Root directory for cached files
	MaxSize int64         // Maximum total size in bytes (0 = unlimited)
	TTL     time.Duration // Default TTL for entries (0 = no expiration)
	Logger  logger.Logger // Optional logger

	// SoftEvictPercent, when > 0, triggers age-ordered eviction when cache
	// size exceeds SoftEvictPercent * MaxSize. Entries are evicted oldest-first
	// (by createdAt) until size drops below the threshold. This provides
	// adaptive pressure: entries stay cached as long as there's room, but
	// the oldest are cleaned up first when space gets tight — before the
	// hard LRU eviction at 100% kicks in.
	// Range: 0.0-1.0. Default: 0 (disabled, only hard LRU at MaxSize).
	SoftEvictPercent float64
}

// DefaultConfig returns a cache configuration with a disk-derived size limit
// and soft eviction beginning at 80 percent of that limit. The cache does not
// expire entries by default.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:              dir,
		MaxSize:          DefaultMaxSize(dir, DefaultMaxDiskPercent),
		SoftEvictPercent: 0.8,
	}
}

// entry holds in-memory metadata for a cached file.
type entry struct {
	key        string
	path       string // filesystem path
	size       int64
	createdAt  time.Time
	accessedAt time.Time // updated on Get, used for LRU
	expiresAt  time.Time // zero means never expires
}

// call represents an in-flight fetch for request coalescing.
type call struct {
	wg  sync.WaitGroup
	val string // cached file path on success
	err error
}

// Cache is a disk-backed file cache with LRU + TTL eviction and request coalescing.
type Cache struct {
	dir              string
	maxSize          int64
	ttl              time.Duration
	softEvictPercent float64 // 0 = disabled
	log              logger.Logger
	db               *sql.DB

	mu        sync.RWMutex
	entries   map[string]*entry
	totalSize int64

	// request coalescing
	calls   sync.Map // map[string]*call
	cancel  context.CancelFunc
	stopped chan struct{}
}

// New creates a new cache. It creates the directory if needed and runs
// corruption recovery (removes orphaned .tmp-* files).
func New(cfg Config) (*Cache, error) {
	if cfg.Dir == "" {
		return nil, errors.New("cache directory is required")
	}

	log := cfg.Logger
	if log == nil {
		log = logger.NewConsoleLogger()
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	c := &Cache{
		dir:              cfg.Dir,
		maxSize:          cfg.MaxSize,
		ttl:              cfg.TTL,
		softEvictPercent: cfg.SoftEvictPercent,
		log:              log,
		entries:          make(map[string]*entry),
		stopped:          make(chan struct{}),
	}
	c.openMetadataDB()

	if err := c.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover cache: %w", err)
	}

	return c, nil
}

// Get retrieves a cached entry. Returns the file path and true if found and not expired.
// Updates the last-access time for LRU tracking.
func (c *Cache) Get(key string) (path string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, exists := c.entries[key]
	if !exists {
		return "", false
	}

	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		c.removeLocked(e)
		return "", false
	}

	now := time.Now()
	updateAccessInDB := now.Sub(e.accessedAt) > 60*time.Second
	e.accessedAt = now
	if updateAccessInDB && c.db != nil {
		if _, err := c.db.Exec("UPDATE cache_entries SET accessed_at = ? WHERE key = ?", now.UnixNano(), key); err != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite update accessed_at failed key=%s: %v", key, err)
			}
		}
	}
	return e.path, true
}

// Put stores data in the cache with an optional TTL override.
// If ttl is 0, uses the cache's default TTL. If ttl is -1, entry never expires.
// Returns the file path where data was stored.
// Data is written atomically: temp file -> rename.
func (c *Cache) Put(key string, data io.Reader, ttl time.Duration) (path string, err error) {
	hash := hashKey(key)
	subDir := filepath.Join(c.dir, hash[:2])
	finalPath := filepath.Join(subDir, hash)

	if err := os.MkdirAll(subDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create cache subdirectory: %w", err)
	}

	tmpFile, err := os.CreateTemp(subDir, ".tmp-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Ensure cleanup on any failure path.
	success := false
	defer func() {
		if !success {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	n, err := io.Copy(tmpFile, data)
	if err != nil {
		return "", fmt.Errorf("failed to write cache data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", fmt.Errorf("failed to rename temp file: %w", err)
	}
	success = true

	now := time.Now()
	var expiresAt time.Time
	effectiveTTL := ttl
	if effectiveTTL == 0 {
		effectiveTTL = c.ttl
	}
	if effectiveTTL > 0 {
		expiresAt = now.Add(effectiveTTL)
	}
	// ttl == -1 means never expires (expiresAt stays zero)

	c.mu.Lock()
	// Remove old entry if exists (update size tracking).
	if old, exists := c.entries[key]; exists {
		c.totalSize -= old.size
	}
	c.entries[key] = &entry{
		key:        key,
		path:       finalPath,
		size:       n,
		createdAt:  now,
		accessedAt: now,
		expiresAt:  expiresAt,
	}
	c.totalSize += n
	if c.db != nil {
		e := c.entries[key]
		if _, dbErr := c.db.Exec(
			"INSERT OR REPLACE INTO cache_entries (key, path, size, created_at, accessed_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
			e.key,
			e.path,
			e.size,
			e.createdAt.UnixNano(),
			e.accessedAt.UnixNano(),
			timeToUnixNano(e.expiresAt),
		); dbErr != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite upsert failed key=%s: %v", key, dbErr)
			}
		}
	}
	c.mu.Unlock()

	// Run inline eviction if over max size.
	if c.maxSize > 0 {
		c.evictLRU()
	}

	return finalPath, nil
}

// GetOrFetch retrieves from cache or fetches via the provided function.
// Uses request coalescing: concurrent calls for the same key share a single fetch.
// The fetch function should return a reader and the TTL for this entry.
// This is the primary method callers should use.
func (c *Cache) GetOrFetch(key string, fetch func() (io.ReadCloser, time.Duration, error)) (io.ReadCloser, error) {
	// Fast path: check cache first.
	if path, ok := c.Get(key); ok {
		f, err := os.Open(path)
		if err == nil {
			c.log.Debug("cache hit key=%s", key)
			return f, nil
		}
		// File disappeared from disk; remove stale entry and proceed to fetch.
		c.Remove(key)
	}

	c.log.Debug("cache miss key=%s", key)

	// Request coalescing: only one goroutine fetches per key.
	cl := &call{}
	cl.wg.Add(1)

	if existing, loaded := c.calls.LoadOrStore(key, cl); loaded {
		// Another goroutine is already fetching; wait for it.
		existingCall := existing.(*call)
		existingCall.wg.Wait()
		if existingCall.err != nil {
			return nil, existingCall.err
		}
		f, err := os.Open(existingCall.val)
		if err != nil {
			return nil, fmt.Errorf("failed to open cached file: %w", err)
		}
		return f, nil
	}

	// We are the first caller: do the fetch.
	defer func() {
		cl.wg.Done()
		c.calls.Delete(key)
	}()

	cl.val, cl.err = c.doFetch(key, fetch)
	if cl.err != nil {
		return nil, cl.err
	}

	f, err := os.Open(cl.val)
	if err != nil {
		return nil, fmt.Errorf("failed to open cached file: %w", err)
	}
	return f, nil
}

// doFetch executes the fetch function, recovers from panics, and stores the result.
func (c *Cache) doFetch(key string, fetch func() (io.ReadCloser, time.Duration, error)) (path string, err error) {
	// Recover from panics in the fetch function.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fetch panicked: %v", r)
		}
	}()

	reader, ttl, fetchErr := fetch()
	if fetchErr != nil {
		return "", fetchErr
	}
	defer reader.Close()

	return c.Put(key, reader, ttl)
}

// Remove removes a cached entry.
func (c *Cache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, exists := c.entries[key]; exists {
		c.removeLocked(e)
	}
}

// removeLocked removes an entry while holding the write lock.
func (c *Cache) removeLocked(e *entry) {
	_ = os.Remove(e.path)
	c.totalSize -= e.size
	delete(c.entries, e.key)
	if c.db != nil {
		if _, err := c.db.Exec("DELETE FROM cache_entries WHERE key = ?", e.key); err != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite delete failed key=%s: %v", e.key, err)
			}
		}
	}
}

// RemoveByPrefix removes all cache entries whose key starts with prefix.
// Returns the number of entries removed. Used for repo-wide invalidation
// when a push may affect entries across multiple auth scopes.
func (c *Cache) RemoveByPrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for key, e := range c.entries {
		if strings.HasPrefix(key, prefix) {
			c.removeLocked(e)
			removed++
		}
	}
	if c.db != nil {
		if _, err := c.db.Exec("DELETE FROM cache_entries WHERE key LIKE ?", prefix+"%"); err != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite prefix delete failed prefix=%s: %v", prefix, err)
			}
		}
	}
	return removed
}

// RemoveMatching removes cache entries for which match returns true.
func (c *Cache) RemoveMatching(match func(key string) bool) int {
	if c == nil || match == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for key, e := range c.entries {
		if match(key) {
			c.removeLocked(e)
			removed++
		}
	}
	return removed
}

// Clear removes all entries from the cache, deleting their backing files and
// any untracked files on disk (which recover() would otherwise re-count).
// Returns the number of tracked entries removed and bytes freed.
//
// Note: Clear() can race with concurrent streaming writers that use Dir()
// and RegisterAfterStream() outside the cache mutex (the TeeReader pattern).
// In practice this is acceptable because Clear() is only called from disk
// pressure handlers (emergency cleanup) and a partially-written temp file
// will simply be orphaned and cleaned up on the next Clear() or restart.
func (c *Cache) Clear() (removed int, freed int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	removed = len(c.entries)
	freed = c.totalSize

	// Wipe tracked entries first (fast path for accounting).
	c.entries = make(map[string]*entry)
	c.totalSize = 0

	if c.db != nil {
		if err := c.db.Close(); err != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite close during clear failed: %v", err)
			}
		}
		c.db = nil
	}

	// Remove all files under the cache directory, then recreate the empty dir.
	// This catches both tracked entries and any orphaned/untracked files that
	// recover() would otherwise pick up on restart.
	if err := os.RemoveAll(c.dir); err != nil {
		// Best effort — log but don't fail.
		if c.log != nil {
			c.log.Warn("cache clear: failed to remove directory %s: %v", c.dir, err)
		}
	}
	_ = os.MkdirAll(c.dir, 0o755)
	c.openMetadataDB()

	return removed, freed
}

// Size returns the current total cache size in bytes.
func (c *Cache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalSize
}

// Len returns the number of cached entries.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// StartEviction starts the background eviction goroutine.
// It runs every interval, evicting expired entries and LRU entries if over max size.
// Call Stop() to terminate.
//
// StartEviction resets the internal stopped channel so it is safe to call again
// after a previous Stop() — for example, during testing or if the caller needs
// to restart eviction with a different interval. Each call creates a fresh
// lifecycle: the goroutine started here will be the one that Stop() waits on.
func (c *Cache) StartEviction(ctx context.Context, interval time.Duration) {
	c.stopped = make(chan struct{}) // reset for this eviction lifecycle
	ctx, c.cancel = context.WithCancel(ctx)
	go c.evictionLoop(ctx, interval)
}

// Stop stops the background eviction goroutine and cleans up.
func (c *Cache) Stop() {
	if c.cancel != nil {
		c.cancel()
		<-c.stopped
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite close failed: %v", err)
			}
		}
		c.db = nil
	}
}

// evictionLoop runs periodic eviction until the context is cancelled.
func (c *Cache) evictionLoop(ctx context.Context, interval time.Duration) {
	defer close(c.stopped)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.evictExpired()
			if c.maxSize > 0 {
				c.softEvictByAge()
				c.evictLRU()
			}
		}
	}
}

// evictExpired removes all entries that have passed their TTL.
func (c *Cache) evictExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range c.entries {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			c.log.Debug("evicting expired entry key=%s", e.key)
			c.removeLocked(e)
		}
	}
}

// softEvictByAge removes the oldest entries (by createdAt) when the cache exceeds
// the soft eviction threshold. This provides adaptive pressure: entries stay cached
// as long as there's room, but the oldest are cleaned up first when space gets tight.
// Does nothing if SoftEvictPercent is 0 or MaxSize is 0.
func (c *Cache) softEvictByAge() {
	if c.softEvictPercent <= 0 || c.maxSize <= 0 {
		return
	}

	threshold := int64(float64(c.maxSize) * c.softEvictPercent)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.totalSize <= threshold {
		return
	}

	// Sort entries by createdAt ascending (oldest-created first).
	sorted := make([]*entry, 0, len(c.entries))
	for _, e := range c.entries {
		sorted = append(sorted, e)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].createdAt.Before(sorted[j].createdAt)
	})

	for _, e := range sorted {
		if c.totalSize <= threshold {
			break
		}
		c.log.Debug("soft-evicting oldest entry key=%s age=%s size=%d",
			e.key, time.Since(e.createdAt).Truncate(time.Second), e.size)
		c.removeLocked(e)
	}
}

// evictLRU removes least-recently-accessed entries until total size is within maxSize.
func (c *Cache) evictLRU() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.maxSize <= 0 || c.totalSize <= c.maxSize {
		return
	}

	// Collect entries and sort by accessedAt ascending (oldest first).
	sorted := make([]*entry, 0, len(c.entries))
	for _, e := range c.entries {
		sorted = append(sorted, e)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].accessedAt.Before(sorted[j].accessedAt)
	})

	for _, e := range sorted {
		if c.totalSize <= c.maxSize {
			break
		}
		c.log.Debug("evicting LRU entry key=%s size=%d", e.key, e.size)
		c.removeLocked(e)
	}
}

// recover cleans up orphaned temp files and rebuilds metadata from SQLite,
// with disk scanning fallback for orphaned/legacy files.
func (c *Cache) recover() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	var tmpCleaned int
	var filesRecovered int
	knownPaths := make(map[string]struct{})

	if c.db != nil {
		rows, queryErr := c.db.Query("SELECT key, path, size, created_at, accessed_at, expires_at FROM cache_entries")
		if queryErr != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite query failed, falling back to disk-only recovery: %v", queryErr)
			}
			_ = c.db.Close()
			c.db = nil
		} else {
			staleKeys := make([]string, 0)
			for rows.Next() {
				var key, path string
				var size, createdAtNS, accessedAtNS, expiresAtNS int64
				if scanErr := rows.Scan(&key, &path, &size, &createdAtNS, &accessedAtNS, &expiresAtNS); scanErr != nil {
					if c.log != nil {
						c.log.Warn("cache sqlite row scan failed: %v", scanErr)
					}
					continue
				}

				// Skip entries that have already expired — remove from
				// SQLite and delete the backing file so we don't carry
				// stale data across restarts.
				if expiresAtNS != 0 && time.Now().After(time.Unix(0, expiresAtNS)) {
					staleKeys = append(staleKeys, key)
					_ = os.Remove(path)
					continue
				}

				info, statErr := os.Stat(path)
				if statErr != nil || info.IsDir() {
					staleKeys = append(staleKeys, key)
					continue
				}

				e := &entry{
					key:        key,
					path:       path,
					size:       info.Size(),
					createdAt:  time.Unix(0, createdAtNS),
					accessedAt: time.Unix(0, accessedAtNS),
				}
				if expiresAtNS != 0 {
					e.expiresAt = time.Unix(0, expiresAtNS)
				}

				c.entries[key] = e
				c.totalSize += e.size
				knownPaths[path] = struct{}{}
				filesRecovered++
			}

			if err := rows.Close(); err != nil {
				if c.log != nil {
					c.log.Warn("cache sqlite rows close failed: %v", err)
				}
			}
			if err := rows.Err(); err != nil {
				if c.log != nil {
					c.log.Warn("cache sqlite rows iteration failed: %v", err)
				}
			}
			for _, key := range staleKeys {
				if _, delErr := c.db.Exec("DELETE FROM cache_entries WHERE key = ?", key); delErr != nil {
					if c.log != nil {
						c.log.Warn("cache sqlite stale row delete failed key=%s: %v", key, delErr)
					}
				}
			}

			if c.log != nil && (filesRecovered > 0 || len(staleKeys) > 0) {
				c.log.Info("cache restored from metadata db: entries=%d stale=%d totalSize=%d", filesRecovered, len(staleKeys), c.totalSize)
			}
		}
	}

	for _, dirEntry := range entries {
		if !dirEntry.IsDir() {
			continue
		}
		subPath := filepath.Join(c.dir, dirEntry.Name())
		subEntries, err := os.ReadDir(subPath)
		if err != nil {
			c.log.Warn("failed to read cache subdirectory path=%s: %v", subPath, err)
			continue
		}

		for _, fileEntry := range subEntries {
			filePath := filepath.Join(subPath, fileEntry.Name())

			if strings.HasPrefix(fileEntry.Name(), ".tmp-") {
				if err := os.Remove(filePath); err != nil {
					c.log.Warn("failed to remove orphaned temp file path=%s: %v", filePath, err)
				} else {
					tmpCleaned++
				}
				continue
			}

			info, err := fileEntry.Info()
			if err != nil {
				c.log.Warn("failed to stat cached file path=%s: %v", filePath, err)
				continue
			}

			if _, known := knownPaths[filePath]; known {
				continue
			}

			// Track total disk usage for unknown files (legacy/orphaned).
			// These files are intentionally not added to the entries map because
			// they cannot be keyed without metadata.
			c.totalSize += info.Size()
		}
	}

	if tmpCleaned > 0 || filesRecovered > 0 {
		c.log.Info("cache recovery complete tmpCleaned=%d filesRecovered=%d totalSize=%d", tmpCleaned, filesRecovered, c.totalSize)
	}

	return nil
}

func (c *Cache) openMetadataDB() {
	dbPath := filepath.Join(c.dir, "cache.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		if c.log != nil {
			c.log.Warn("cache sqlite open failed path=%s: %v", dbPath, err)
		}
		c.db = nil
		return
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		if c.log != nil {
			c.log.Warn("cache sqlite pragma journal_mode=WAL failed: %v", err)
		}
	}
	if _, err := db.Exec("PRAGMA journal_size_limit=1048576"); err != nil {
		if c.log != nil {
			c.log.Warn("cache sqlite pragma journal_size_limit failed: %v", err)
		}
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS cache_entries (
		key TEXT PRIMARY KEY,
		path TEXT NOT NULL,
		size INTEGER NOT NULL,
		created_at INTEGER NOT NULL,
		accessed_at INTEGER NOT NULL,
		expires_at INTEGER NOT NULL
	)`); err != nil {
		if c.log != nil {
			c.log.Warn("cache sqlite create table failed: %v", err)
		}
		_ = db.Close()
		c.db = nil
		return
	}

	c.db = db
}

// hashKey produces a hex-encoded SHA256 hash of the cache key.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// MaxSize returns the maximum configured cache size in bytes (0 = unlimited).
func (c *Cache) MaxSize() int64 {
	return c.maxSize
}

// Dir returns the root directory of the cache.
func (c *Cache) Dir() string {
	return c.dir
}

// RegisterAfterStream registers a manually-written cache file with metadata.
// This is used by the streaming cache write pattern where the caller writes
// the file directly (e.g., using TeeReader) instead of going through Put.
// After calling this, the entry is tracked for LRU eviction and size accounting.
func (c *Cache) RegisterAfterStream(key, finalPath string, size int64) {
	now := time.Now()
	var expiresAt time.Time
	if c.ttl > 0 {
		expiresAt = now.Add(c.ttl)
	}
	c.mu.Lock()
	// Remove old entry if exists (update size tracking).
	if old, exists := c.entries[key]; exists {
		c.totalSize -= old.size
	}
	c.entries[key] = &entry{
		key:        key,
		path:       finalPath,
		size:       size,
		createdAt:  now,
		accessedAt: now,
		expiresAt:  expiresAt,
	}
	c.totalSize += size
	if c.db != nil {
		e := c.entries[key]
		if _, err := c.db.Exec(
			"INSERT OR REPLACE INTO cache_entries (key, path, size, created_at, accessed_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
			e.key,
			e.path,
			e.size,
			e.createdAt.UnixNano(),
			e.accessedAt.UnixNano(),
			timeToUnixNano(e.expiresAt),
		); err != nil {
			if c.log != nil {
				c.log.Warn("cache sqlite upsert failed key=%s: %v", key, err)
			}
		}
	}
	c.mu.Unlock()

	// Run inline eviction if over max size.
	if c.maxSize > 0 {
		c.evictLRU()
	}
}

// ParseSize parses a human-readable size string into bytes.
// Supported formats: "10GB", "500MB", "1TB", "1024" (raw bytes), "0" (unlimited).
// Case-insensitive.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size string")
	}

	s = strings.ToUpper(s)

	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1 << 40},
		{"GB", 1 << 30},
		{"MB", 1 << 20},
		{"KB", 1 << 10},
	}

	for _, m := range multipliers {
		if before, ok := strings.CutSuffix(s, m.suffix); ok {
			numStr := before
			numStr = strings.TrimSpace(numStr)
			n, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size: %q", s)
			}
			if n < 0 {
				return 0, fmt.Errorf("negative size: %q", s)
			}
			return int64(n * float64(m.mult)), nil
		}
	}

	// No suffix: treat as raw bytes.
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size: %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative size: %q", s)
	}
	return n, nil
}

func timeToUnixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
