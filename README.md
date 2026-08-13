# Proxykit

This repo extracts the npm, apt, Git, and S3 acceleration logic from Hadron
into a small standalone Go module.

The module path is `github.com/agentuity/proxykit`. Concrete proxies are
available directly from the `apt`, `git`, `npm`, and `s3` subpackages.

## Packages

- `github.com/agentuity/proxykit`
- `github.com/agentuity/proxykit/apt`
- `github.com/agentuity/proxykit/git`
- `github.com/agentuity/proxykit/npm`
- `github.com/agentuity/proxykit/s3`
- `github.com/agentuity/proxykit/cache`

The only non-stdlib dependency shared with Hadron is
`github.com/agentuity/go-common`.

## Configuration

Each proxy package provides a complete, production-oriented default
configuration. Start with that configuration and override only the settings
needed by the application:

```go
cfg := npm.DefaultConfig("/var/cache/proxykit/npm")
cfg.MetadataTTL = 30 * time.Second

server, err := npm.New(cfg)
```

The root package provides equivalent `proxykit.DefaultNPMConfig`,
`proxykit.DefaultAPTConfig`, `proxykit.DefaultGitConfig`, and
`proxykit.DefaultS3Config` helpers.

The defaults are:

| Package | Metadata/refs TTL | Disk limit |
| --- | ---: | ---: |
| npm | 1 minute | 10% |
| apt | 1 hour | 10% |
| Git refs | 15 seconds | 2% |
| Git packs | 72 hours | 20% |
| S3 objects | 5 minutes | 10% |

Disk limits are calculated from the filesystem containing the cache directory.
If its capacity cannot be determined, the limit is `0` (unlimited). Set a
maximum size to `0` after calling `DefaultConfig` to explicitly request an
unlimited cache.

## S3 acceleration

The `s3` package accelerates repeated reads from one fixed S3-compatible
upstream or from original upstream URLs supplied by an outer transparent proxy.
It streams objects to clients and disk simultaneously, serves ranges from full
cached objects, conditionally revalidates stale entries, and invalidates object
variants after successful mutations.

For a standalone path-style endpoint:

```go
cfg := s3.DefaultConfig("/var/cache/proxykit/s3", "https://s3.us-east-1.amazonaws.com")
cfg.Credentials = &s3.Credentials{
	AccessKeyID:     accessKey,
	SecretAccessKey: secretKey,
	Region:          "us-east-1",
}

server, err := s3.New(cfg)
```

Static credentials are optional. Without a resolved signer, proxykit preserves
the client's existing SigV4 Authorization header or presigned query. This is
useful when the S3 handler is embedded behind a transparent TLS-terminating
proxy and the original host is retained.

Multi-tenant deployments should resolve identity and signing separately:

```go
cfg.IdentityResolver = func(ctx context.Context, req *http.Request) (s3.Identity, bool, error) {
	return identityFromTrustedProxyContext(ctx, req)
}
cfg.SigningResolver = func(ctx context.Context, req *http.Request, identity s3.Identity) (s3.SigningContext, bool, error) {
	credentials, err := credentialsForTenant(ctx, identity.Tenant)
	if err != nil {
		return s3.SigningContext{}, false, err
	}
	return signerFor(credentials), true, nil
}
```

`IdentityResolver` receives the complete original request, including
`RemoteAddr`, TLS state, headers, and context. Tenant identity must come from a
trusted source such as authenticated proxy credentials, mTLS, or context added
while handling CONNECT. Do not trust an unauthenticated tenant header.

`SigningResolver` receives the resolved identity and returns both a stable,
non-secret authorization scope for cache isolation and a function that signs
the final outgoing request. The outgoing request includes conditional ETag or
Last-Modified headers before the signer runs.

`s3.Config.TLSConfig` configures outbound TLS to S3 or private MinIO endpoints.
Client-facing TLS interception and certificate generation belong to the outer
forward proxy and use separate TLS configuration.

## Per-session proxy identity

A multi-tenant job runner can give each job unique proxy credentials through
its standard proxy environment:

```bash
export PROXY_SESSION_ID='sess_01J...'
export PROXY_SESSION_SECRET='a-long-random-single-use-secret'
export PROXY_URL="http://${PROXY_SESSION_ID}:${PROXY_SESSION_SECRET}@127.0.0.1:9999"

export HTTP_PROXY="$PROXY_URL"
export HTTPS_PROXY="$PROXY_URL"
export http_proxy="$PROXY_URL"
export https_proxy="$PROXY_URL"
```

The proxy URL normally uses `http://` even for HTTPS destinations. In that
case, an HTTPS client asks the proxy to open a CONNECT tunnel and sends the
credentials in `Proxy-Authorization`. A proxy URL beginning with `https://`
instead requests TLS between the job and the proxy itself; support for HTTPS
proxy endpoints varies between clients.

The session ID is a lookup key, not a trusted organization ID. The outer proxy
must validate both values against a session registry and resolve an identity:

```go
type Session struct {
	TenantID string
	JobID    string
}

session, err := sessions.Authenticate(ctx, proxyUsername, proxyPassword)
if err != nil {
	return proxyAuthenticationRequired()
}

identity := s3.Identity{
	Tenant:  session.TenantID,
	Subject: session.JobID,
}
```

Missing or invalid proxy credentials should produce `407 Proxy Authentication
Required` with an appropriate `Proxy-Authenticate` challenge, not a normal
origin `401`. The outer proxy must remove `Proxy-Authorization` before
forwarding requests or passing decrypted requests to package handlers.

For a CONNECT request, authenticate before opening the tunnel and bind the
resolved identity to that tunnel. If the proxy terminates TLS, propagate the
same identity through request context to every decrypted HTTP request. Plain
HTTP forward-proxy requests must be authenticated individually.

The S3 handler can then consume only the trusted context:

```go
cfg.IdentityResolver = func(ctx context.Context, req *http.Request) (s3.Identity, bool, error) {
	identity, ok := trustedIdentityFromContext(req.Context())
	return identity, ok, nil
}

cfg.SigningResolver = func(ctx context.Context, req *http.Request, identity s3.Identity) (s3.SigningContext, bool, error) {
	return tenantS3Signer(ctx, identity.Tenant)
}
```

The resolved tenant, subject, and signing authorization scope are all included
in S3 cache keys. A signing resolver must return a stable, non-secret scope;
proxykit bypasses caching if a dynamic signer does not provide one.

Do not use a proxy path such as `https://proxy/org-id` for forward-proxy
identity. Standard HTTP proxy clients do not reliably send that path on normal
requests or CONNECT. `Proxy-Authorization`, mTLS identity, a dedicated local
socket, or another authenticated connection attribute is the appropriate
identity source.

### Client configuration

Most supported clients accept authenticated proxy URLs directly:

```bash
# npm and pnpm
export npm_config_proxy="$PROXY_URL"
export npm_config_https_proxy="$PROXY_URL"

# Git
git config --global http.proxy "$PROXY_URL"
git config --global https.proxy "$PROXY_URL"

# APT
cat >/etc/apt/apt.conf.d/99proxykit <<EOF
Acquire::http::Proxy "$PROXY_URL";
Acquire::https::Proxy "$PROXY_URL";
EOF
```

Credentials containing URL-reserved characters must be percent-encoded before
being embedded in a proxy URL. Prefer URL-safe random session identifiers and
secrets to avoid inconsistent client parsing.

### Security requirements

- Generate long, random, short-lived credentials for each job or session.
- Revoke credentials when the job ends.
- Never log proxy URLs, environment values containing them, or
  `Proxy-Authorization`.
- Remove `Proxy-Authorization` before forwarding anything upstream.
- Do not accept a bare organization ID as proof of tenant identity.
- Reject attempts to change identity within an established CONNECT tunnel.
- Use constant-time secret verification or store only a password hash.
- On localhost, an HTTP proxy endpoint is generally sufficient when jobs
  cannot inspect each other's processes or traffic.
- Across machines or an untrusted cluster network, protect the job-to-proxy
  connection with an HTTPS proxy endpoint, mTLS, or an encrypted private
  network.
- Keep job-to-proxy TLS configuration separate from `s3.Config.TLSConfig`,
  which controls only proxykit's outbound connection to object storage.

## Smoke test binary

`cmd/proxytest` starts the npm, apt, and Git proxy services on ephemeral ports
and can optionally start the S3 acceleration service.

```bash
go run ./cmd/proxytest
```

Flags:

- `-npm`, `-apt`, `-git`, `-s3` enable or disable each service
- `-npm-addr`, `-apt-addr`, `-git-addr`, `-s3-addr` choose listen addresses
- `-s3-upstream` sets the fixed S3-compatible upstream when S3 is enabled
- `-cache-root` overrides the temporary cache root

## Smoke test script

Run the full host-plus-Docker smoke test with:

```bash
./scripts/proxy-smoke.sh
```

The script builds and runs the proxy binary directly, starts a local Git HTTP
backend and a MinIO S3-compatible backend, and then validates npm, pnpm, apt,
Git, and repeated signed S3 object reads from Docker containers.

## Proxy mode

The client-to-proxy connection is HTTP. These package-specific proxies can
fetch HTTPS resources from their upstream servers, but they are not general
HTTPS `CONNECT` tunnels. Configure each client to use the matching HTTP
endpoint:

- npm sets `npm_config_registry` to the npm proxy URL
- pnpm uses the npm-compatible `registry` setting
- apt sets `Acquire::http::Proxy` to the apt proxy URL
- git sets `http.proxy` to the Git proxy URL and uses HTTP repository URLs

Supporting arbitrary HTTPS URLs through standard `npm_config_https_proxy`,
`Acquire::https::Proxy`, or Git's `https.proxy` would require `CONNECT`
tunneling or TLS interception. The current package-specific servers do not
provide that outer forward-proxy layer; an application can place them behind
one and propagate its authenticated session identity through request context.

For Git, the common configuration pattern is the one documented in the public
proxy gist by evantoli:

```bash
git config --global http.proxy http://127.0.0.1:3128
git config --global https.proxy http://127.0.0.1:3128
```

The `https.proxy` form above is the conventional Git configuration from the
referenced gist, but it requires a proxy with `CONNECT` support. Proxykit's Git
handler currently validates the equivalent `http.proxy` flow with HTTP Git
repository URLs.

## Git proxy usage

The Git client needs an HTTP proxy setting. A common pattern is:

```bash
git config --global http.proxy http://127.0.0.1:3128
git ls-remote http://example.com/repo.git
```

For a single command, you can also use:

```bash
git -c http.proxy=http://127.0.0.1:3128 ls-remote http://example.com/repo.git
```

That matches the standard Git proxy configuration pattern described in the
public Git proxy gist by evantoli.

## Container smoke test

The integration test in `integration/docker_smoke_test.go` starts the proxy on
the host, then validates these package-manager flows from Docker containers:

- npm registry access through the host proxy
- pnpm registry access through the host proxy
- apt package index access through the host proxy
- git smart HTTP access through the host proxy using `git config --global
  http.proxy ...`

The smoke test covers the supported HTTP client-to-proxy mode. HTTPS upstream
traffic still occurs where the package proxy fetches an HTTPS origin, such as
the public npm registry.

Run it with:

```bash
go test -tags integration ./integration -run TestDockerProxySmoke -v
```
