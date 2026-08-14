package git

import (
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	if cfg.UpstreamScheme != "" {
		t.Fatalf("UpstreamScheme = %q, want empty", cfg.UpstreamScheme)
	}
}

func TestConfigValidateUpstreamScheme(t *testing.T) {
	for _, scheme := range []string{"", "http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			cfg := DefaultConfig(t.TempDir())
			cfg.UpstreamScheme = scheme
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	for _, scheme := range []string{"HTTP", "ftp", "https://"} {
		t.Run("invalid_"+scheme, func(t *testing.T) {
			cfg := DefaultConfig(t.TempDir())
			cfg.UpstreamScheme = scheme
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil")
			}
			if !strings.Contains(err.Error(), "git.UpstreamScheme") {
				t.Fatalf("Validate() error = %q", err)
			}
		})
	}
}

func TestNewRejectsInvalidUpstreamScheme(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.UpstreamScheme = "ftp"

	if _, err := New(cfg); err == nil {
		t.Fatal("New() error = nil")
	} else if !strings.Contains(err.Error(), "git.UpstreamScheme") {
		t.Fatalf("New() error = %q", err)
	}
}

func TestUpstreamSchemeForStreamingAndBufferedRequests(t *testing.T) {
	tests := []struct {
		name           string
		configured     string
		requestUsesTLS bool
		want           string
	}{
		{name: "default plain HTTP", want: "http"},
		{name: "default TLS", requestUsesTLS: true, want: "https"},
		{name: "force HTTP for plain HTTP", configured: "http", want: "http"},
		{name: "force HTTP for TLS", configured: "http", requestUsesTLS: true, want: "http"},
		{name: "force HTTPS for plain HTTP", configured: "https", want: "https"},
		{name: "force HTTPS for TLS", configured: "https", requestUsesTLS: true, want: "https"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, buffered := range []bool{false, true} {
				path := "streaming"
				if buffered {
					path = "buffered"
				}
				t.Run(path, func(t *testing.T) {
					const body = "request body"
					req := httptest.NewRequest(http.MethodPost, "http://git.example/repo.git/git-upload-pack?service=git-upload-pack", bytes.NewBufferString(body))
					req.RequestURI = req.URL.RequestURI()
					if tt.requestUsesTLS {
						req.TLS = &tls.ConnectionState{}
					}

					var gotURL string
					var gotBody string
					h := &Handler{
						cfg: Config{UpstreamScheme: tt.configured},
						client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
							gotURL = r.URL.String()
							requestBody, err := io.ReadAll(r.Body)
							if err != nil {
								t.Fatalf("read upstream request body: %v", err)
							}
							gotBody = string(requestBody)
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     make(http.Header),
								Body:       io.NopCloser(strings.NewReader("ok")),
							}, nil
						})},
					}

					var resp *http.Response
					var err error
					if buffered {
						resp, err = h.doUpstreamRequestWithBody(req, []byte(body))
					} else {
						resp, err = h.doUpstreamRequest(req)
					}
					if err != nil {
						t.Fatalf("upstream request error = %v", err)
					}
					resp.Body.Close()

					wantURL := tt.want + "://git.example/repo.git/git-upload-pack?service=git-upload-pack"
					if gotURL != wantURL {
						t.Fatalf("upstream URL = %q, want %q", gotURL, wantURL)
					}
					if gotBody != body {
						t.Fatalf("upstream body = %q, want %q", gotBody, body)
					}
				})
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
