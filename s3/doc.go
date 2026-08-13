// Package s3 implements a disk-backed acceleration proxy for S3-compatible
// object storage.
//
// Clients address the proxy as a custom, path-style S3 endpoint. The proxy
// forwards requests to a fixed or request-selected upstream, optionally
// re-signs them with AWS SigV4 credentials, and caches successful object GET
// responses. Fresh cache hits avoid upstream requests; stale entries are
// conditionally revalidated with the upstream ETag or Last-Modified value.
package s3
