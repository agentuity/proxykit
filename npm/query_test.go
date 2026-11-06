package npm

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueryVariants(t *testing.T) {
	for _, route := range []string{"default", "explicit", "forward-http", "forward-https"} {
		for _, path := range []string{"/example", "/@scope/pkg", "/@scope%2fpkg"} {
			for _, tarball := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/tarball=%t", route, path, tarball)
				t.Run(name, func(t *testing.T) {
					if tarball {
						path += "/-/pkg-1.0.0.tgz"
					}
					var requests atomic.Int32
					upstream := newQueryUpstream(t, route, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests.Add(1)
						// Echo the decoded path and untouched query independently of proxy routing.
						fmt.Fprint(w, r.URL.Path+"?"+r.URL.RawQuery)
					}))
					_, addr := startQueryProxy(t, upstream, time.Minute)
					const first = "variant=alpha%2Bbeta&dup=x&dup=y&empty=&space=+&slash=%2f"
					for _, query := range []string{first, "variant=second&path=/-/other.tgz", first, "", ""} {
						target := queryTarget(route, upstream.URL, path)
						if query != "" {
							target += "?" + query
						}
						status, contentType, body := queryRequest(t, addr, target)
						if status != http.StatusOK {
							t.Fatalf("status = %d, body = %q", status, body)
						}
						decodedPath := strings.ReplaceAll(path, "%2f", "/")
						if want := decodedPath + "?" + query; body != want {
							t.Errorf("body = %q, want %q", body, want)
						}
						wantType := "application/json"
						if tarball {
							wantType = "application/octet-stream"
						}
						if contentType != wantType {
							t.Errorf("Content-Type = %q, want %q", contentType, wantType)
						}
					}
					if got := requests.Load(); got != 3 {
						t.Errorf("upstream requests = %d, want 3 independent variants with repeated cache hits", got)
					}
				})
			}
		}
	}
}

func TestQueryTarballOutlivesMetadataTTL(t *testing.T) {
	for _, route := range []string{"default", "explicit", "forward-http", "forward-https"} {
		t.Run(route, func(t *testing.T) {
			var requests atomic.Int32
			upstream := newQueryUpstream(t, route, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Write([]byte{0x1f, 0x8b, 0x00, 0xff})
			}))
			// Any metadata entry expires before the next request, without timing sleeps.
			_, addr := startQueryProxy(t, upstream, time.Nanosecond)
			for range 2 {
				_, contentType, body := queryRequest(t, addr, queryTarget(route, upstream.URL, "/example/-/example.tgz?variant=one"))
				if contentType != "application/octet-stream" || body != "\x1f\x8b\x00\xff" {
					t.Errorf("tarball type = %q, body = %q", contentType, body)
				}
			}
			if got := requests.Load(); got != 1 {
				t.Errorf("tarball upstream requests = %d, want 1 after metadata TTL", got)
			}
			before := requests.Load()
			for range 2 {
				status, _, _ := queryRequest(t, addr, queryTarget(route, upstream.URL, "/example?variant=one"))
				if status != http.StatusOK {
					t.Fatalf("metadata status = %d", status)
				}
			}
			if got := requests.Load() - before; got != 2 {
				t.Errorf("metadata upstream requests = %d, want 2 expired entries", got)
			}
		})
	}
}

func TestQueryMetadataRewritesTarballURLs(t *testing.T) {
	for _, route := range []string{"default", "explicit", "forward-http", "forward-https"} {
		t.Run(route, func(t *testing.T) {
			upstream := newQueryUpstream(t, route, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, ".tgz") {
					fmt.Fprint(w, r.URL.RawQuery)
					return
				}
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				json.NewEncoder(w).Encode(map[string]string{"tarball": scheme + "://" + r.Host + "/@scope/pkg/-/pkg.tgz?variant=alpha%2Bbeta"})
			}))
			_, addr := startQueryProxy(t, upstream, time.Minute)
			_, _, body := queryRequest(t, addr, queryTarget(route, upstream.URL, "/@scope%2fpkg?variant=metadata"))
			var metadata map[string]string
			if err := json.Unmarshal([]byte(body), &metadata); err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			proxyHost := addr
			if strings.HasPrefix(route, "forward-") {
				// Preserve rewriting against the incoming Host, including absolute-form requests.
				proxyHost = u.Host
			}
			want := "http://" + proxyHost + "/_upstream/" + u.Host + "/@scope/pkg/-/pkg.tgz?variant=alpha%2Bbeta"
			if metadata["tarball"] != want {
				t.Fatalf("tarball URL = %q, want %q", metadata["tarball"], want)
			}
			// IP-based explicit routes intentionally use HTTP, so follow only HTTP upstream links.
			if route != "forward-https" {
				status, contentType, body := queryRequest(t, addr, strings.TrimPrefix(want, "http://"+proxyHost))
				if status != http.StatusOK || contentType != "application/octet-stream" || body != "variant=alpha%2Bbeta" {
					t.Errorf("rewritten tarball status = %d, type = %q, body = %q", status, contentType, body)
				}
			}
		})
	}
}

func TestQueryRoutingPreservesAllowlist(t *testing.T) {
	var requests atomic.Int32
	upstream := newQueryUpstream(t, "default", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	_, addr := startQueryProxy(t, upstream, time.Minute, "allowed.invalid")
	for _, route := range []string{"explicit", "forward-http"} {
		status, _, _ := queryRequest(t, addr, queryTarget(route, upstream.URL, "/example/-/example.tgz?variant=one"))
		if status != http.StatusForbidden {
			t.Errorf("%s status = %d, want 403", route, status)
		}
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("blocked upstream requests = %d, want 0", got)
	}
}

func TestQueryRoutingReusesUnqueriedCacheKeys(t *testing.T) {
	var requests atomic.Int32
	upstream := newQueryUpstream(t, "default", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	s, addr := startQueryProxy(t, upstream, time.Minute)
	u, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		key  string
	}{
		{"/example", "meta:" + u.Host + ":/example"},
		{"/example/-/example.tgz", "tar:" + u.Host + ":example/example.tgz"},
		{"/@scope/pkg/-/pkg.tgz", "tar:" + u.Host + ":@scope/pkg/pkg.tgz"},
		{upstream.URL + "/example/-/example.tgz", "tar:url:" + upstream.URL + "/example/-/example.tgz"},
	} {
		// Seed the existing key format to model cache entries from before an upgrade.
		if _, err := s.cache.Put(tc.key, strings.NewReader("cached control"), -1); err != nil {
			t.Fatal(err)
		}
		status, _, body := queryRequest(t, addr, tc.path)
		if status != http.StatusOK || body != "cached control" {
			t.Errorf("%s status = %d, body = %q", tc.path, status, body)
		}
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("upstream requests = %d, want 0 for existing cache entries", got)
	}
}

// --- Test helpers: local HTTP fixtures and wire requests. ---

func newQueryUpstream(t *testing.T, route string, handler http.Handler) *httptest.Server {
	t.Helper()
	var upstream *httptest.Server
	if route == "forward-https" {
		upstream = httptest.NewTLSServer(handler)
	} else {
		upstream = httptest.NewServer(handler)
	}
	t.Cleanup(upstream.Close)
	return upstream
}

func startQueryProxy(t *testing.T, upstream *httptest.Server, ttl time.Duration, allowed ...string) (*Server, string) {
	t.Helper()
	u, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowed) == 0 {
		allowed = []string{u.Host, u.Hostname()}
	}
	s, err := New(Config{
		CacheDir: t.TempDir(), UpstreamURL: upstream.URL, MetadataTTL: ttl,
		AllowedUpstreamRegistryPatterns: allowed,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Trust only this local TLS fixture, without changing global transport settings.
	s.client = upstream.Client()
	addr, err := s.Start(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Stop(); err != nil {
			t.Error(err)
		}
	})
	return s, addr.String()
}

func queryTarget(route, upstreamURL, path string) string {
	switch route {
	case "explicit":
		return "/_upstream/" + strings.TrimPrefix(upstreamURL, "http://") + path
	case "forward-http", "forward-https":
		return upstreamURL + path
	default:
		return path
	}
}

// queryRequest sends absolute HTTPS request targets without invoking CONNECT.
// This is the npm server's forward-request contract behind an outer proxy.
func queryRequest(t *testing.T, addr, target string) (int, string, string) {
	t.Helper()
	forward := strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://")
	if !forward {
		target = "http://" + addr + target
	}
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if forward {
		err = req.WriteProxy(conn)
	} else {
		err = req.Write(conn)
	}
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
}

// --- End test helpers. ---
