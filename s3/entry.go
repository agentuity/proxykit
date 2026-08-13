package s3

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agentuity/proxykit/cache"
)

type entryMetadata struct {
	StatusCode   int         `json:"statusCode"`
	Header       http.Header `json:"header"`
	ETag         string      `json:"etag,omitempty"`
	LastModified string      `json:"lastModified,omitempty"`
	StoredAt     time.Time   `json:"storedAt"`
	FreshUntil   time.Time   `json:"freshUntil"`
	HasBody      bool        `json:"hasBody"`
	BodySize     int64       `json:"bodySize,omitempty"`
}

func metadataKey(key string) string { return "s3:meta:" + key }
func bodyKey(key string) string     { return "s3:body:" + key }

func loadMetadata(disk *cache.Cache, key string) (*entryMetadata, string, bool) {
	path, ok := disk.Get(metadataKey(key))
	if !ok {
		return nil, "", false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		disk.Remove(metadataKey(key))
		return nil, "", false
	}
	var meta entryMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		disk.Remove(metadataKey(key))
		return nil, "", false
	}
	bodyPath := ""
	if meta.HasBody {
		bodyPath, ok = disk.Get(bodyKey(key))
		if !ok {
			disk.Remove(metadataKey(key))
			return nil, "", false
		}
	}
	return &meta, bodyPath, true
}

func storeMetadata(disk *cache.Cache, key string, meta *entryMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = disk.Put(metadataKey(key), strings.NewReader(string(data)), -1)
	return err
}

func removeEntry(disk *cache.Cache, key string) {
	disk.Remove(metadataKey(key))
	disk.Remove(bodyKey(key))
}

func serveEntry(w http.ResponseWriter, r *http.Request, meta *entryMetadata, bodyPath, cacheStatus string) error {
	copyResponseHeader(w.Header(), meta.Header)
	if cacheStatus != "" {
		w.Header().Set("X-Proxykit-Cache", cacheStatus)
	}
	if clientNotModified(r, meta) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(meta.StatusCode)
		return nil
	}
	if !meta.HasBody || bodyPath == "" {
		return errors.New("cached object body is unavailable")
	}
	f, err := os.Open(bodyPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if value := r.Header.Get("Range"); value != "" {
		start, end, ok := parseRange(value, meta.BodySize)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.BodySize))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return nil
		}
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			return err
		}
		length := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.BodySize))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, err = io.CopyN(w, f, length)
		return err
	}

	w.Header().Set("Content-Length", strconv.FormatInt(meta.BodySize, 10))
	w.WriteHeader(meta.StatusCode)
	_, err = io.Copy(w, f)
	return err
}

func clientNotModified(r *http.Request, meta *entryMetadata) bool {
	if r == nil || meta == nil {
		return false
	}
	if value := r.Header.Get("If-None-Match"); value != "" && meta.ETag != "" {
		for _, candidate := range strings.Split(value, ",") {
			if strings.TrimSpace(candidate) == "*" || normalizeETag(candidate) == normalizeETag(meta.ETag) {
				return true
			}
		}
	}
	return false
}

func normalizeETag(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(value), "W/"))
	return strings.Trim(value, "\"")
}

func parseRange(value string, total int64) (int64, int64, bool) {
	if total <= 0 || !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		if suffix > total {
			suffix = total
		}
		return total - suffix, total - 1, true
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= total {
		return 0, 0, false
	}
	end := total - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, false
		}
		if end >= total {
			end = total - 1
		}
	}
	return start, end, true
}
