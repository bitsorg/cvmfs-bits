// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cas

// s3.go — CAS backend writing into the same S3 bucket the CVMFS repository is
// served from.
//
// The object keys produced here MUST be byte-identical to what the C++ uploader
// writes, because the client resolves them from catalogs without any
// indirection:
//
//	upload_s3.cc:478
//	  final_path = repository_alias_ + "/data/" + content_hash.MakePath();
//
// i.e. "<alias>/data/<first-2-hex>/<remaining-hex><suffix>". The suffix (C for
// catalogs, P for chunk objects) is already part of the hash string handed to
// this backend, so ObjectPath() reproduces MakePath() exactly.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// Transport bounds for the object store. Deliberately generous: the object
// store is on the same site and these exist to convert "hung forever" into "one
// failed job", not to police latency.
const (
	dialTimeout           = 10 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	expectContinueTimeout = 5 * time.Second
	idleConnTimeout       = 90 * time.Second

	// responseHeaderTimeout bounds the wait between "request fully sent" and
	// "first byte of the response". A large PUT can legitimately take minutes to
	// transfer, but once it is sent the store answers promptly or not at all.
	responseHeaderTimeout = 2 * time.Minute

	// opTimeout is the per-operation ceiling applied by withTimeout. It covers
	// the whole call including the body transfer and any SDK retries, so it must
	// accommodate the largest single object at the slowest tolerable rate.
	opTimeout = 15 * time.Minute
)

// withTimeout bounds a single CAS operation.
//
// The transport timeouts above catch a store that stops answering; this catches
// everything else — a body that trickles, a retry loop that will not converge,
// a proxy holding the connection open. A caller that already has a shorter
// deadline keeps it: context.WithTimeout never extends an existing one.
func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, opTimeout)
}

// S3 is a CAS backend backed by an S3-compatible object store.
type S3 struct {
	client *s3.Client
	bucket string
	alias  string
	acl    string // canned ACL, or "" to omit the header
	// endpoint is kept for logging; the full S3Settings (which holds the
	// secret key) is deliberately NOT retained on the backend.
	endpoint      string
	peekBeforePut bool
}

// NewS3 builds an S3 CAS backend from resolved settings.
func NewS3(ctx context.Context, st S3Settings) (*S3, error) {
	if err := st.Validate(); err != nil {
		return nil, fmt.Errorf("s3 CAS settings: %w", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(st.EffectiveRegion()),
		// Bound the transport. The SDK's default client sets no response-header
		// timeout and no overall deadline, so a connection that establishes and
		// then goes quiet blocks until the OS gives up on TCP keepalive — over
		// two hours. That is not a theoretical concern: it took the service
		// down. The pipeline's upload workers hold the errgroup, the errgroup
		// holds the job's concurrency slot, and every other stage blocks behind
		// the one that never returns, at zero CPU, with nothing in the log.
		//
		// ResponseHeaderTimeout is the one that matters — it fires when the
		// request has been sent and no status line comes back, which is exactly
		// the observed failure. It does NOT bound the body transfer, so a slow
		// but progressing upload is unaffected.
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(
			func(tr *http.Transport) {
				tr.DialContext = (&net.Dialer{
					Timeout:   dialTimeout,
					KeepAlive: 30 * time.Second,
				}).DialContext
				tr.TLSHandshakeTimeout = tlsHandshakeTimeout
				tr.ResponseHeaderTimeout = responseHeaderTimeout
				tr.ExpectContinueTimeout = expectContinueTimeout
				tr.IdleConnTimeout = idleConnTimeout
			})),
		// Credentials come from the repository's own S3 config, NOT from the
		// ambient AWS chain: the prepub must authenticate as the identity that
		// owns the repository's bucket, and picking up an unrelated instance
		// role would fail confusingly (or, worse, write to the wrong place).
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(st.AccessKey, st.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("building AWS config: %w", err)
	}

	endpoint := st.Endpoint()
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// CVMFS_S3_DNS_BUCKETS (default on) selects virtual-host addressing;
		// path-style is used only when it is explicitly "false".
		o.UsePathStyle = !st.DNSBuckets
		// Send a plain signed PUT, as upload_s3.cc does.
		//
		// The SDK otherwise defaults to WhenSupported, which over HTTPS
		// rewrites every PutObject into Content-Encoding: aws-chunked with a
		// trailing checksum. An S3-compatible store that does not implement
		// the unsigned-trailer encoding (older Ceph RGW, older MinIO) then
		// stores the chunk FRAMING as the object body: Put returns success and
		// the corruption only surfaces as a hash mismatch on a CVMFS client.
		// It also makes non-seekable bodies fail outright over plain HTTP,
		// which is the CVMFS default transport.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})

	acl := st.ACL
	if acl == "-" { // explicit opt-out
		acl = ""
	}
	return &S3{
		client:        client,
		bucket:        st.Bucket,
		alias:         st.RepoAlias,
		acl:           acl,
		endpoint:      endpoint,
		peekBeforePut: st.PeekBeforePut,
	}, nil
}

// Endpoint returns the resolved S3 endpoint (for startup logging).
func (s *S3) Endpoint() string { return s.endpoint }

// Bucket returns the bucket name (for startup logging).
func (s *S3) Bucket() string { return s.bucket }

// Alias returns the repository alias used as the key prefix.
func (s *S3) Alias() string { return s.alias }

// ExistsIsNative reports false: every Exists is a network round trip, so the
// Bloom-filter pre-check in the dedup stage is worth its cost here (unlike
// LocalFS, where Exists is a single os.Stat).
func (s *S3) ExistsIsNative() bool { return false }

// key returns the full object key for a hash: "<alias>/data/<xx>/<rest>".
// Callers must have validated the hash with validHashKey first.
func (s *S3) key(hash string) string {
	return s.alias + "/" + cvmfshash.ObjectPath(hash)
}

// dataPrefix is the key prefix under which all objects live.
func (s *S3) dataPrefix() string {
	return s.alias + "/data/"
}

// isNotFound reports whether an S3 error means "no such key" — and ONLY that.
//
// It must not swallow NoSuchBucket, AccessDenied or a redirect, all of which
// can also arrive as 404/4xx: mapping those to "object absent" would classify
// an entire misconfigured repository as new (re-uploading everything), make
// Delete report success while doing nothing, and hide the real fault until a
// later, unrelated error.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	// Several S3-compatible stores return a bare 404 for HeadObject with no
	// typed body. Accept that only when the error carries no API error code,
	// or the code is explicitly key-not-found.
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		default:
			return false // NoSuchBucket, AccessDenied, PermanentRedirect, …
		}
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return true
	}
	return false
}

// validHashKey guards the one input that becomes an object key. S3 keys have no
// root to be confined to, so a hash containing "/" or ".." would escape the
// "<alias>/data/" prefix entirely — writing, for instance, the repository's
// .cvmfspublished manifest. Callers currently validate, but the Backend
// contract does not promise it and the blast radius here is the whole
// repository.
func validHashKey(hash string) error {
	if len(hash) < 40 || len(hash) > 50 {
		return fmt.Errorf("invalid CAS hash %q: implausible length %d", hash, len(hash))
	}
	for i := 0; i < len(hash); i++ {
		c := hash[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
			continue
		case i == len(hash)-1 && ((c >= 'A' && c <= 'Z') || (c >= 'g' && c <= 'z')):
			continue // single trailing content-type suffix (C, P, …)
		default:
			return fmt.Errorf("invalid CAS hash %q: illegal character %q at %d", hash, c, i)
		}
	}
	return nil
}

// Exists reports whether the object is already in the store.
func (s *S3) Exists(ctx context.Context, hash string) (bool, error) {
	if err := validHashKey(hash); err != nil {
		return false, err
	}
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(hash)),
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("s3 head %s: %w", s.key(hash), err)
}

// Size returns the stored size in bytes.
func (s *S3) Size(ctx context.Context, hash string) (int64, error) {
	if err := validHashKey(hash); err != nil {
		return 0, err
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(hash)),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 head %s: %w", s.key(hash), err)
	}
	if out.ContentLength == nil {
		return 0, fmt.Errorf("s3 head %s: no ContentLength", s.key(hash))
	}
	return *out.ContentLength, nil
}

// Put stores an object. It is idempotent and never overwrites: content is
// addressed by the hash of its own bytes, so an existing key already holds
// identical content, and re-uploading only risks disturbing an object a
// published catalog already references.
func (s *S3) Put(ctx context.Context, hash string, r io.Reader, size int64) error {
	if err := validHashKey(hash); err != nil {
		return err
	}
	key := s.key(hash)

	// CVMFS_S3_PEEK_BEFORE_PUT (default on, as in C++). The pipeline already
	// runs its own CAS.Exists before calling Put, so this is a second HEAD per
	// new object; operators can disable it to halve the round trips.
	if s.peekBeforePut {
		exists, err := s.Exists(ctx, hash)
		if err != nil {
			return err
		}
		if exists {
			return nil // already in CAS — idempotent, and we must not rewrite it
		}
	}

	// Bounded here rather than around the whole function so the peek above keeps
	// its own budget: two operations, two deadlines.
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	if size >= 0 {
		in.ContentLength = aws.Int64(size)
	}
	if s.acl != "" {
		in.ACL = types.ObjectCannedACL(s.acl)
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

// Get retrieves an object. The caller must close the returned reader.
func (s *S3) Get(ctx context.Context, hash string) (io.ReadCloser, error) {
	if err := validHashKey(hash); err != nil {
		return nil, err
	}
	// NOT `defer cancel()`. GetObject returns as soon as the headers arrive and
	// the caller streams the body afterwards, so cancelling on return would kill
	// the download the moment it started. The cancel is attached to the body's
	// Close instead, which the caller is already required to call — so the
	// deadline covers the transfer and the context is still released.
	ctx, cancel := withTimeout(ctx)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(hash)),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("s3 get %s: %w", s.key(hash), err)
	}
	return &cancelOnClose{ReadCloser: out.Body, cancel: cancel}, nil
}

// cancelOnClose releases a request context when the body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// Delete removes an object.
func (s *S3) Delete(ctx context.Context, hash string) error {
	if err := validHashKey(hash); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(hash)),
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("s3 delete %s: %w", s.key(hash), err)
	}
	return nil
}

// List returns every object hash in the store.
//
// Note this can be very large (millions of keys) on a production repository;
// callers use it for the dedup seed and pass a context with a timeout.
func (s *S3) List(ctx context.Context) ([]string, error) {
	var hashes []string
	prefix := s.dataPrefix()

	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		select {
		case <-ctx.Done():
			return hashes, ctx.Err()
		default:
		}
		page, err := p.NextPage(ctx)
		if err != nil {
			return hashes, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			rel := strings.TrimPrefix(*obj.Key, prefix)
			// "<xx>/<rest>" -> "<xx><rest>"; skip anything unexpected rather
			// than emitting a corrupt hash into the dedup set.
			if len(rel) < 3 || rel[2] != '/' {
				continue
			}
			hashes = append(hashes, rel[:2]+rel[3:])
		}
	}
	return hashes, nil
}

// Probe verifies the endpoint, credentials, addressing style and bucket access
// with a single read-only request, satisfying cas.Prober.
//
// It deliberately does NOT write: the generic write/read/delete probe would
// deposit a sentinel object in the repository's production bucket on every
// service restart, and a failed cleanup would leave an object no catalog
// references — indistinguishable, to an audit, from a real orphan.
//
// ListObjectsV2 limited to one key exercises everything a misconfiguration
// would break: DNS/endpoint reachability, SigV4 signing (wrong key or region
// fails here), path-vs-virtual-host addressing, and the bucket's existence and
// readability.
func (s *S3) Probe(ctx context.Context) error {
	_, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(s.dataPrefix()),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("s3 probe (bucket %q, endpoint %s, alias %q): %w",
			s.bucket, s.endpoint, s.alias, err)
	}
	return nil
}
