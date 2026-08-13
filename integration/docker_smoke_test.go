//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDockerProxySmoke(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	backendURL, stopGitBackend := startGitBackend(t, root)
	t.Cleanup(stopGitBackend)

	proxyCmd, proxyPorts, stopProxy := startProxyBinary(t, root)
	t.Cleanup(stopProxy)

	t.Run("npm", func(t *testing.T) {
		runDocker(t, "node:20-bookworm-slim", []string{
			"--add-host=host.docker.internal:host-gateway",
			"-e", "npm_config_registry=http://host.docker.internal:" + proxyPorts.npm,
		}, "sh", "-lc", `
			set -e
			version="$(npm view left-pad version)"
			test -n "$version"
			printf '%s\n' "$version" | grep -Eq '^[0-9]'
		`)
	})

	t.Run("pnpm", func(t *testing.T) {
		runDocker(t, "node:22-bookworm-slim", []string{
			"--add-host=host.docker.internal:host-gateway",
		}, "sh", "-lc", `
			set -e
			corepack enable pnpm
			pnpm config set registry http://host.docker.internal:`+proxyPorts.npm+`
			version="$(pnpm view left-pad version)"
			test -n "$version"
			printf '%s\n' "$version" | grep -Eq '^[0-9]'
		`)
	})

	t.Run("apt", func(t *testing.T) {
		runDocker(t, "debian:bookworm-slim", []string{
			"--add-host=host.docker.internal:host-gateway",
		}, "sh", "-lc", `
			set -e
			apt-get -o Acquire::http::Proxy=http://host.docker.internal:`+proxyPorts.apt+` update -qq
		`)
	})

	t.Run("git", func(t *testing.T) {
		runDocker(t, "alpine/git:latest", []string{
			"--add-host=host.docker.internal:host-gateway",
			"--entrypoint", "sh",
		}, "-lc", `
			set -e
			export HOME="$(mktemp -d)"
			git config --global http.proxy http://host.docker.internal:`+proxyPorts.git+`
			git ls-remote `+backendURL+` | grep -q 'refs/heads/main'
		`)
	})

	_ = proxyCmd
}

type proxyPorts struct {
	npm string
	apt string
	git string
}

func startProxyBinary(t *testing.T, root string) (*exec.Cmd, proxyPorts, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cacheRoot := t.TempDir()

	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/proxytest", "-cache-root", cacheRoot)
	cmd.Dir = root

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("proxy stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("proxy stderr pipe: %v", err)
	}

	var logs bytes.Buffer
	var logMu sync.Mutex
	drain := func(r io.Reader) {
		_, _ = io.Copy(&lockedWriter{mu: &logMu, buf: &logs}, r)
	}
	go drain(stderr)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start proxy binary: %v", err)
	}

	portsCh := make(chan proxyPorts, 1)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		var ports proxyPorts
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "npm="):
				ports.npm = portOnly(strings.TrimPrefix(line, "npm="))
			case strings.HasPrefix(line, "apt="):
				ports.apt = portOnly(strings.TrimPrefix(line, "apt="))
			case strings.HasPrefix(line, "git="):
				ports.git = portOnly(strings.TrimPrefix(line, "git="))
			}
			if ports.npm != "" && ports.apt != "" && ports.git != "" {
				portsCh <- ports
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errCh <- err
			return
		}
		errCh <- fmt.Errorf("proxy binary exited before printing all ports; logs:\n%s", logs.String())
	}()

	var ports proxyPorts
	select {
	case ports = <-portsCh:
	case err := <-errCh:
		t.Fatalf("proxy startup failed: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting for proxy ports; logs:\n%s", logs.String())
	}

	stop := func() {
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		}
	}

	return cmd, ports, stop
}

func startGitBackend(t *testing.T, root string) (string, func()) {
	t.Helper()

	tmp := t.TempDir()
	work := filepath.Join(tmp, "work")
	bare := filepath.Join(tmp, "repo.git")

	runGit(t, tmp, "init", "--initial-branch=main", "work")
	runGit(t, work, "config", "user.email", "smoke@example.com")
	runGit(t, work, "config", "user.name", "Smoke Test")

	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("smoke test\n"), 0o644); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	runGit(t, work, "add", "README.md")
	runGit(t, work, "commit", "-m", "initial commit")
	runGit(t, tmp, "clone", "--bare", work, bare)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveGitHTTP(t, w, r, tmp)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("git backend listen: %v", err)
	}

	srv := &http.Server{Handler: mux}
	go func() {
		_ = srv.Serve(ln)
	}()

	return "http://" + ln.Addr().String() + "/repo.git", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

func serveGitHTTP(t *testing.T, w http.ResponseWriter, r *http.Request, projectRoot string) {
	t.Helper()
	defer r.Body.Close()

	cmd := exec.Command("git", "http-backend")
	cmd.Env = append(os.Environ(),
		"GIT_PROJECT_ROOT="+projectRoot,
		"GIT_HTTP_EXPORT_ALL=1",
		"GATEWAY_INTERFACE=CGI/1.1",
		"PATH_INFO="+r.URL.Path,
		"QUERY_STRING="+r.URL.RawQuery,
		"REQUEST_METHOD="+r.Method,
		"REQUEST_URI="+r.RequestURI,
		"SERVER_PROTOCOL="+r.Proto,
		"SERVER_NAME="+serverName(r.Host),
		"SERVER_PORT="+serverPort(r.Host),
		"REMOTE_ADDR="+r.RemoteAddr,
	)
	if ct := r.Header.Get("Content-Type"); ct != "" {
		cmd.Env = append(cmd.Env, "CONTENT_TYPE="+ct)
	}
	if r.ContentLength > 0 {
		cmd.Env = append(cmd.Env, fmt.Sprintf("CONTENT_LENGTH=%d", r.ContentLength))
	}
	cmd.Stdin = r.Body

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("git backend stdout: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("git backend start: %v", err)
	}

	reader := bufio.NewReader(stdout)
	status := http.StatusOK
	respHeaders := http.Header{}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("git backend read headers: %v (stderr=%s)", err, stderr.String())
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.EqualFold(key, "Status") {
			var code int
			if _, scanErr := fmt.Sscanf(strings.TrimSpace(value), "%d", &code); scanErr == nil && code > 0 {
				status = code
			}
			continue
		}
		respHeaders.Add(strings.TrimSpace(key), strings.TrimSpace(value))
	}

	for key, values := range respHeaders {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(status)
	if _, err := io.Copy(w, reader); err != nil {
		t.Fatalf("git backend copy response: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("git backend wait: %v (stderr=%s)", err, stderr.String())
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func runDocker(t *testing.T, image string, extraArgs []string, command ...string) {
	t.Helper()

	args := append([]string{"run", "--rm"}, extraArgs...)
	args = append(args, image)
	args = append(args, command...)

	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", image, err, out)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func portOnly(addr string) string {
	_, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err == nil {
		return port
	}
	if strings.HasPrefix(addr, "[") && strings.Contains(addr, "]:") {
		if _, port, err := net.SplitHostPort(addr); err == nil {
			return port
		}
	}
	return strings.TrimSpace(addr)
}

func serverName(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err == nil {
		return host
	}
	return hostport
}

func serverPort(hostport string) string {
	_, port, err := net.SplitHostPort(hostport)
	if err == nil {
		return port
	}
	return "80"
}

type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
