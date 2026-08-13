package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/agentuity/proxykit/apt"
	"github.com/agentuity/proxykit/git"
	"github.com/agentuity/proxykit/npm"
	"github.com/agentuity/proxykit/s3"
)

func main() {
	var (
		npmAddr    = flag.String("npm-addr", ":0", "listen address for the npm proxy")
		aptAddr    = flag.String("apt-addr", ":0", "listen address for the apt proxy")
		gitAddr    = flag.String("git-addr", ":0", "listen address for the git proxy wrapper")
		serveNPM   = flag.Bool("npm", true, "start the npm proxy")
		serveAPT   = flag.Bool("apt", true, "start the apt proxy")
		serveGit   = flag.Bool("git", true, "start the git proxy wrapper")
		serveS3    = flag.Bool("s3", false, "start the S3 acceleration endpoint")
		s3Addr     = flag.String("s3-addr", ":0", "listen address for the S3 acceleration endpoint")
		s3Upstream = flag.String("s3-upstream", "", "fixed S3-compatible upstream URL")
		cacheRoot  = flag.String("cache-root", "", "optional cache root directory; defaults to a temp dir")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := *cacheRoot
	var err error
	if root == "" {
		root, err = os.MkdirTemp("", "accelerator-proxy-")
		if err != nil {
			fatalf("create temp cache root: %v", err)
		}
		defer os.RemoveAll(root)
	}

	type closer interface{ Stop() error }
	var closers []closer

	if *serveNPM {
		srv, err := npm.New(npm.DefaultConfig(filepath.Join(root, "npm")))
		if err != nil {
			fatalf("create npm proxy: %v", err)
		}
		closers = append(closers, srv)
		addr, err := srv.Start(ctx, *npmAddr, make(chan struct{}))
		if err != nil {
			fatalf("start npm proxy: %v", err)
		}
		fmt.Printf("npm=%s\n", addr.String())
	}

	if *serveAPT {
		srv, err := apt.New(apt.DefaultConfig(filepath.Join(root, "apt")))
		if err != nil {
			fatalf("create apt proxy: %v", err)
		}
		closers = append(closers, srv)
		addr, err := srv.Start(ctx, *aptAddr, make(chan struct{}))
		if err != nil {
			fatalf("start apt proxy: %v", err)
		}
		fmt.Printf("apt=%s\n", addr.String())
	}

	if *serveGit {
		handler, err := git.New(git.DefaultConfig(filepath.Join(root, "git")))
		if err != nil {
			fatalf("create git handler: %v", err)
		}
		handler.StartEviction(ctx, time.Minute)
		closers = append(closers, closerFunc(func() error {
			handler.Stop()
			return nil
		}))

		mux := http.NewServeMux()
		mux.HandleFunc("/", handler.ServeHTTP)
		srv := &http.Server{Handler: mux}
		ln, err := net.Listen("tcp", *gitAddr)
		if err != nil {
			fatalf("start git proxy wrapper: %v", err)
		}
		closers = append(closers, closerFunc(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(ctx)
		}))
		closers = append(closers, closerFunc(func() error { return ln.Close() }))
		go func() {
			if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
				fatalf("git proxy wrapper: %v", serveErr)
			}
		}()
		fmt.Printf("git=%s\n", ln.Addr().String())
	}

	if *serveS3 {
		if *s3Upstream == "" {
			fatalf("-s3-upstream is required when -s3 is enabled")
		}
		cfg := s3.DefaultConfig(filepath.Join(root, "s3"), *s3Upstream)
		if accessKey := os.Getenv("PROXYKIT_S3_ACCESS_KEY_ID"); accessKey != "" {
			cfg.Credentials = &s3.Credentials{
				AccessKeyID:     accessKey,
				SecretAccessKey: os.Getenv("PROXYKIT_S3_SECRET_ACCESS_KEY"),
				SessionToken:    os.Getenv("PROXYKIT_S3_SESSION_TOKEN"),
				Region:          os.Getenv("PROXYKIT_S3_REGION"),
			}
		}
		srv, err := s3.New(cfg)
		if err != nil {
			fatalf("create S3 acceleration endpoint: %v", err)
		}
		closers = append(closers, srv)
		addr, err := srv.Start(ctx, *s3Addr, make(chan struct{}))
		if err != nil {
			fatalf("start S3 acceleration endpoint: %v", err)
		}
		fmt.Printf("s3=%s\n", addr.String())
	}

	<-ctx.Done()
	for i := len(closers) - 1; i >= 0; i-- {
		_ = closers[i].Stop()
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

type closerFunc func() error

func (f closerFunc) Stop() error { return f() }
