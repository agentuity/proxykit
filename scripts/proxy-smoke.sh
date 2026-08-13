#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmpdir="$(mktemp -d)"
build_dir="$tmpdir/build"
cache_root="$tmpdir/cache"
proxy_stdout="$tmpdir/proxy.stdout"
proxy_stderr="$tmpdir/proxy.stderr"
git_stdout="$tmpdir/git.stdout"
git_stderr="$tmpdir/git.stderr"
s3_fixture="$tmpdir/s3-artifact.txt"
mkdir -p "$build_dir" "$cache_root"

proxy_pid=""
git_pid=""
minio_name="proxykit-minio-$$-${RANDOM}"
proxy_bin="$build_dir/proxytest"
git_backend_url=""
minio_port=""
npm_port=""
apt_port=""
git_port=""
s3_port=""

cleanup() {
	local status=$?

	if [[ -n "${git_pid}" ]] && kill -0 "$git_pid" 2>/dev/null; then
		kill "$git_pid" 2>/dev/null || true
		wait "$git_pid" 2>/dev/null || true
	fi
	if [[ -n "${proxy_pid}" ]] && kill -0 "$proxy_pid" 2>/dev/null; then
		kill "$proxy_pid" 2>/dev/null || true
		wait "$proxy_pid" 2>/dev/null || true
	fi
	if docker inspect "$minio_name" >/dev/null 2>&1; then
		docker rm -f "$minio_name" >/dev/null 2>&1 || true
	fi

	rm -rf "$tmpdir"
	exit "$status"
}
trap cleanup EXIT INT TERM

require_cmd() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "missing required command: $1" >&2
		exit 1
	}
}

require_cmd go
require_cmd docker
require_cmd git
require_cmd python3
require_cmd curl

cd "$root"

echo "building proxy binary"
go build -o "$proxy_bin" ./cmd/proxytest

start_proxy() {
	PROXYKIT_S3_ACCESS_KEY_ID=minioadmin \
		PROXYKIT_S3_SECRET_ACCESS_KEY=minioadmin \
		PROXYKIT_S3_REGION=us-east-1 \
		"$proxy_bin" -cache-root "$cache_root" -s3 \
		-s3-upstream "http://127.0.0.1:${minio_port}" \
		>"$proxy_stdout" 2>"$proxy_stderr" &
	proxy_pid=$!
}

start_minio() {
	docker run -d --rm \
		--name "$minio_name" \
		-p 127.0.0.1::9000 \
		-e MINIO_ROOT_USER=minioadmin \
		-e MINIO_ROOT_PASSWORD=minioadmin \
		minio/minio:latest server /data >/dev/null

	local published
	published="$(docker port "$minio_name" 9000/tcp | head -n1)"
	minio_port="${published##*:}"
	if [[ -z "$minio_port" ]]; then
		echo "failed to determine MinIO port" >&2
		return 1
	fi
}

start_git_backend() {
	local repo_root="$tmpdir/git-repo"
	local workdir="$repo_root/work"
	local baredir="$repo_root/repo.git"
	mkdir -p "$repo_root"

	git init --initial-branch=main "$workdir" >/dev/null
	git -C "$workdir" config user.email "smoke@example.com"
	git -C "$workdir" config user.name "Smoke Test"
	printf 'smoke test\n' >"$workdir/README.md"
	git -C "$workdir" add README.md
	git -C "$workdir" commit -m "initial commit" >/dev/null
	git clone --bare "$workdir" "$baredir" >/dev/null

	python3 - "$repo_root" >"$git_stdout" 2>"$git_stderr" <<'PY' &
import http.server
import os
import socketserver
import subprocess
import sys
import urllib.parse

project_root = sys.argv[1]

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        parsed = urllib.parse.urlsplit(self.path)
        host = self.headers.get("Host", "127.0.0.1")
        if ":" in host:
            server_name, server_port = host.rsplit(":", 1)
        else:
            server_name, server_port = host, "80"

        env = os.environ.copy()
        env.update({
            "GIT_PROJECT_ROOT": project_root,
            "GIT_HTTP_EXPORT_ALL": "1",
            "GATEWAY_INTERFACE": "CGI/1.1",
            "PATH_INFO": parsed.path,
            "QUERY_STRING": parsed.query,
            "REQUEST_METHOD": self.command,
            "REQUEST_URI": self.path,
            "SERVER_PROTOCOL": self.request_version,
            "SERVER_NAME": server_name,
            "SERVER_PORT": server_port,
            "REMOTE_ADDR": self.client_address[0],
        })
        ctype = self.headers.get("Content-Type")
        if ctype:
            env["CONTENT_TYPE"] = ctype
        if length:
            env["CONTENT_LENGTH"] = str(length)

        proc = subprocess.Popen(
            ["git", "http-backend"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        out, err = proc.communicate(body)
        if proc.returncode not in (0, None) and err:
            sys.stderr.write(err.decode("utf-8", "replace"))

        header_blob, sep, resp_body = out.partition(b"\r\n\r\n")
        if not sep:
            header_blob, sep, resp_body = out.partition(b"\n\n")
        status = 200
        headers = []
        for raw in header_blob.splitlines():
            raw = raw.strip()
            if not raw or b":" not in raw:
                continue
            key, value = raw.split(b":", 1)
            key_s = key.decode("latin1").strip()
            value_s = value.decode("latin1").strip()
            if key_s.lower() == "status":
                try:
                    status = int(value_s.split()[0])
                except Exception:
                    status = 200
            else:
                headers.append((key_s, value_s))

        self.send_response(status)
        for key, value in headers:
            self.send_header(key, value)
        self.end_headers()
        if resp_body:
            self.wfile.write(resp_body)

class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as httpd:
    print(f"http://127.0.0.1:{httpd.server_address[1]}/repo.git", flush=True)
    httpd.serve_forever()
PY
	git_pid=$!
}

extract_port() {
	local value="$1"
	value="${value##*=}"
	value="${value#\[}"
	value="${value%\]}"
	if [[ "$value" == *:* ]]; then
		value="${value##*:}"
	fi
	printf '%s' "$value"
}

wait_for_proxy_ports() {
	local deadline=$((SECONDS + 60))
	while (( SECONDS < deadline )); do
		if [[ -s "$proxy_stdout" ]]; then
			npm_port="$(awk -F= '/^npm=/{print $2}' "$proxy_stdout" | tail -n1)"
			apt_port="$(awk -F= '/^apt=/{print $2}' "$proxy_stdout" | tail -n1)"
			git_port="$(awk -F= '/^git=/{print $2}' "$proxy_stdout" | tail -n1)"
			s3_port="$(awk -F= '/^s3=/{print $2}' "$proxy_stdout" | tail -n1)"
			if [[ -n "$npm_port" && -n "$apt_port" && -n "$git_port" && -n "$s3_port" ]]; then
				npm_port="$(extract_port "$npm_port")"
				apt_port="$(extract_port "$apt_port")"
				git_port="$(extract_port "$git_port")"
				s3_port="$(extract_port "$s3_port")"
				return 0
			fi
		fi
		if ! kill -0 "$proxy_pid" 2>/dev/null; then
			echo "proxy process exited early" >&2
			cat "$proxy_stdout" >&2 || true
			cat "$proxy_stderr" >&2 || true
			return 1
		fi
		sleep 0.2
	done

	echo "timed out waiting for proxy ports" >&2
	cat "$proxy_stdout" >&2 || true
	cat "$proxy_stderr" >&2 || true
	return 1
}

wait_for_git_backend() {
	local deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		if [[ -s "$git_stdout" ]]; then
			git_backend_url="$(tail -n1 "$git_stdout" | tr -d '\r\n')"
			if [[ "$git_backend_url" == http://* ]]; then
				return 0
			fi
		fi
		if ! kill -0 "$git_pid" 2>/dev/null; then
			echo "git backend exited early" >&2
			cat "$git_stdout" >&2 || true
			cat "$git_stderr" >&2 || true
			return 1
		fi
		sleep 0.2
	done

	echo "timed out waiting for git backend" >&2
	cat "$git_stdout" >&2 || true
	cat "$git_stderr" >&2 || true
	return 1
}

docker_run() {
	docker run --rm --add-host host.docker.internal:host-gateway "$@"
}

seed_minio() {
	printf 'proxykit s3 acceleration smoke test\n' >"$s3_fixture"
	local endpoint="http://minioadmin:minioadmin@host.docker.internal:${minio_port}"
	local deadline=$((SECONDS + 60))
	while (( SECONDS < deadline )); do
		if docker_run -e "MC_HOST_local=${endpoint}" minio/mc:latest ls local >/dev/null 2>&1; then
			docker_run -e "MC_HOST_local=${endpoint}" minio/mc:latest mb --ignore-existing local/smoke >/dev/null
			docker_run -e "MC_HOST_local=${endpoint}" -v "$s3_fixture:/fixture/artifact.txt:ro" \
				minio/mc:latest cp /fixture/artifact.txt local/smoke/artifact.txt >/dev/null
			return 0
		fi
		sleep 0.5
	done
	echo "timed out waiting for MinIO" >&2
	docker logs "$minio_name" >&2 || true
	return 1
}

run_s3_check() {
	local endpoint="http://minioadmin:minioadmin@host.docker.internal:${s3_port}"
	local expected="proxykit s3 acceleration smoke test"
	local first second headers
	first="$(docker_run -e "MC_HOST_accelerated=${endpoint}" minio/mc:latest cat accelerated/smoke/artifact.txt)"
	second="$(docker_run -e "MC_HOST_accelerated=${endpoint}" minio/mc:latest cat accelerated/smoke/artifact.txt)"
	[[ "$first" == "$expected" && "$second" == "$expected" ]]
	headers="$(curl -sS -D - "http://127.0.0.1:${s3_port}/smoke/artifact.txt" -o /dev/null)"
	printf '%s\n' "$headers" | grep -Eiq '^X-Proxykit-Cache:[[:space:]]*HIT'
}

run_check() {
	local name="$1"
	shift
	echo "running ${name}"
	"$@"
}

start_minio
seed_minio

start_proxy
wait_for_proxy_ports

start_git_backend
wait_for_git_backend

run_check npm docker_run \
	-e "npm_config_registry=http://host.docker.internal:${npm_port}" \
	node:22-bookworm-slim \
	sh -lc "set -e; version=\"\$(npm view left-pad version)\"; test -n \"\$version\"; printf '%s\n' \"\$version\" | grep -Eq '^[0-9]'"

run_check pnpm docker_run \
	-e "npm_config_registry=http://host.docker.internal:${npm_port}" \
	node:22-bookworm-slim \
	sh -lc "set -e; npm install -g pnpm@10 >/dev/null; pnpm config set registry http://host.docker.internal:${npm_port}; version=\"\$(pnpm view left-pad version)\"; test -n \"\$version\"; printf '%s\n' \"\$version\" | grep -Eq '^[0-9]'"

run_check apt docker_run \
	debian:bookworm-slim \
	sh -lc "set -e; apt-get -o Acquire::http::Proxy=http://host.docker.internal:${apt_port} update -qq"

run_check git docker_run \
	--entrypoint sh \
	alpine/git:latest \
	-lc "set -e; export HOME=\"\$(mktemp -d)\"; git config --global http.proxy http://host.docker.internal:${git_port}; git ls-remote ${git_backend_url} | grep -q 'refs/heads/main'"

run_check s3 run_s3_check

echo "proxy smoke test passed"
