package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestServer(t *testing.T, upstream http.Handler, mutate func(*Config)) (*Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)
	cfg := DefaultConfig(t.TempDir(), backend.URL)
	cfg.MaxCacheSize = 10 << 20
	cfg.MaxEntrySize = 2 << 20
	if mutate != nil {
		mutate(&cfg)
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	return server, &calls
}

func proxyRequest(t *testing.T, server *Server, method, target string, body io.Reader, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	if header != nil {
		req.Header = header.Clone()
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	return recorder
}

func TestRepeatedGETUsesCache(t *testing.T) {
	server, calls := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"artifact-v1"`)
		_, _ = io.WriteString(w, "artifact bytes")
	}), nil)

	first := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/artifact.tar", nil, nil)
	second := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/artifact.tar", nil, nil)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d, %d", first.Code, second.Code)
	}
	if first.Body.String() != "artifact bytes" || second.Body.String() != "artifact bytes" {
		t.Fatalf("unexpected bodies: %q, %q", first.Body.String(), second.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls.Load())
	}
	if second.Header().Get("X-Proxykit-Cache") != "HIT" {
		t.Fatalf("cache header = %q, want HIT", second.Header().Get("X-Proxykit-Cache"))
	}
}

func TestExpiredEntryRevalidates(t *testing.T) {
	server, calls := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"artifact-v1"`)
		if r.Header.Get("If-None-Match") == `"artifact-v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, "artifact bytes")
	}), func(cfg *Config) {
		cfg.FreshTTL = 20 * time.Millisecond
	})

	proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/artifact.tar", nil, nil)
	time.Sleep(30 * time.Millisecond)
	revalidated := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/artifact.tar", nil, nil)
	hit := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/artifact.tar", nil, nil)

	if revalidated.Body.String() != "artifact bytes" || hit.Body.String() != "artifact bytes" {
		t.Fatal("revalidation did not preserve the cached body")
	}
	if revalidated.Header().Get("X-Proxykit-Cache") != "REVALIDATED" {
		t.Fatalf("cache header = %q", revalidated.Header().Get("X-Proxykit-Cache"))
	}
	if calls.Load() != 2 {
		t.Fatalf("upstream calls = %d, want 2", calls.Load())
	}
}

func TestRangeServedFromFullObject(t *testing.T) {
	server, calls := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "0123456789")
	}), nil)
	proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, nil)
	ranged := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, http.Header{"Range": []string{"bytes=2-5"}})

	if ranged.Code != http.StatusPartialContent || ranged.Body.String() != "2345" {
		t.Fatalf("range response status=%d body=%q", ranged.Code, ranged.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls.Load())
	}
}

func TestTenantScopesDoNotShareEntries(t *testing.T) {
	server, calls := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "body for "+r.Header.Get("X-Tenant"))
	}), func(cfg *Config) {
		cfg.IdentityResolver = func(_ context.Context, r *http.Request) (Identity, bool, error) {
			return Identity{Tenant: r.Header.Get("X-Tenant")}, true, nil
		}
	})

	a := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, http.Header{"X-Tenant": []string{"a"}})
	b := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, http.Header{"X-Tenant": []string{"b"}})
	a2 := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, http.Header{"X-Tenant": []string{"a"}})
	if a.Body.String() != "body for a" || b.Body.String() != "body for b" || a2.Body.String() != "body for a" {
		t.Fatalf("tenant bodies = %q, %q, %q", a.Body.String(), b.Body.String(), a2.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("upstream calls = %d, want 2", calls.Load())
	}
}

func TestMutationInvalidatesAllObjectVariants(t *testing.T) {
	var mu sync.Mutex
	body := "v1"
	server, _ := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPut {
			data, _ := io.ReadAll(r.Body)
			body = string(data)
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, body)
	}), nil)

	first := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object?response-content-type=text/plain", nil, nil)
	proxyRequest(t, server, http.MethodPut, "http://proxy/bucket/object?uploadId=completed", strings.NewReader("v2"), nil)
	second := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object?response-content-type=text/plain", nil, nil)
	if first.Body.String() != "v1" || second.Body.String() != "v2" {
		t.Fatalf("bodies before/after mutation = %q, %q", first.Body.String(), second.Body.String())
	}
}

func TestDynamicSignerReceivesIdentityAndConditionalRequest(t *testing.T) {
	var signedConditional atomic.Bool
	server, _ := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Signed-Tenant") != "tenant-a" {
			t.Errorf("missing dynamic signature header")
		}
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, "signed body")
	}), func(cfg *Config) {
		cfg.FreshTTL = 20 * time.Millisecond
		cfg.IdentityResolver = func(_ context.Context, _ *http.Request) (Identity, bool, error) {
			return Identity{Tenant: "tenant-a", Subject: "job-1"}, true, nil
		}
		cfg.SigningResolver = func(_ context.Context, _ *http.Request, identity Identity) (SigningContext, bool, error) {
			if identity.Tenant != "tenant-a" {
				t.Fatalf("signing identity = %+v", identity)
			}
			return SigningContext{
				Scope: "role/build-reader",
				Sign: func(_ context.Context, outgoing *http.Request) error {
					outgoing.Header.Set("X-Signed-Tenant", identity.Tenant)
					if outgoing.Header.Get("If-None-Match") != "" {
						signedConditional.Store(true)
					}
					return nil
				},
			}, true, nil
		}
	})

	proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, nil)
	time.Sleep(30 * time.Millisecond)
	proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, nil)
	if !signedConditional.Load() {
		t.Fatal("dynamic signer did not receive the conditional upstream request")
	}
}

func TestEscapedObjectPathIsPreserved(t *testing.T) {
	var escapedPath atomic.Value
	server, _ := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapedPath.Store(r.URL.EscapedPath())
		_, _ = io.WriteString(w, "encoded object")
	}), nil)

	response := proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/folder%2Fname.txt", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if got := escapedPath.Load(); got != "/bucket/folder%2Fname.txt" {
		t.Fatalf("upstream escaped path = %q", got)
	}
}

func TestDynamicSignerWithoutScopeBypassesCache(t *testing.T) {
	server, calls := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "uncached")
	}), func(cfg *Config) {
		cfg.SigningResolver = func(_ context.Context, _ *http.Request, _ Identity) (SigningContext, bool, error) {
			return SigningContext{Sign: func(context.Context, *http.Request) error { return nil }}, true, nil
		}
	})

	proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, nil)
	proxyRequest(t, server, http.MethodGet, "http://proxy/bucket/object", nil, nil)
	if calls.Load() != 2 {
		t.Fatalf("upstream calls = %d, want 2", calls.Load())
	}
}
