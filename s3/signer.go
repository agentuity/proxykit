package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func signRequest(ctx context.Context, r *http.Request, creds Credentials, now time.Time) error {
	stripSigningQuery(r)
	r.Header.Del("Authorization")
	r.Header.Del("X-Amz-Date")
	r.Header.Del("X-Amz-Security-Token")

	region := strings.TrimSpace(creds.Region)
	if region == "" {
		region = "us-east-1"
	}
	payloadHash := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256"))
	if payloadHash == "" {
		if r.Body == nil || r.ContentLength == 0 {
			payloadHash = emptyPayloadSHA256
		} else {
			payloadHash = "UNSIGNED-PAYLOAD"
			r.Header.Set("X-Amz-Content-Sha256", payloadHash)
		}
	}

	provider := credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)
	resolved, err := provider.Retrieve(ctx)
	if err != nil {
		return err
	}
	signer := v4.NewSigner()
	return signer.SignHTTP(ctx, resolved, r, payloadHash, "s3", region, now, func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
}

func stripSigningQuery(r *http.Request) {
	if r == nil || r.URL == nil {
		return
	}
	q := r.URL.Query()
	for key := range q {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-amz-") || strings.HasPrefix(lower, "x-goog-") {
			q.Del(key)
		}
	}
	r.URL.RawQuery = q.Encode()
}

func credentialScope(creds Credentials) string {
	if scope := strings.TrimSpace(creds.Scope); scope != "" {
		return scope
	}
	if creds.AccessKeyID == "" {
		return "anonymous"
	}
	sum := sha256.Sum256([]byte(creds.AccessKeyID))
	return hex.EncodeToString(sum[:16])
}

func requestCredentialScope(r *http.Request) string {
	if r == nil {
		return "anonymous"
	}
	identity := credentialIdentity(r.Header.Get("Authorization"))
	if identity == "" && r.URL != nil {
		identity = credentialIdentity(r.URL.Query().Get("X-Amz-Credential"))
		if identity == "" {
			identity = credentialIdentity(r.URL.Query().Get("X-Goog-Credential"))
		}
	}
	if identity == "" {
		return "anonymous"
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:16])
}

func credentialIdentity(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if idx := strings.Index(value, "Credential="); idx >= 0 {
		value = value[idx+len("Credential="):]
	}
	value = strings.TrimLeft(value, " \t")
	if idx := strings.IndexAny(value, "/, \t"); idx >= 0 {
		value = value[:idx]
	}
	return value
}
