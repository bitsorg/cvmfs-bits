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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// S3 is a CAS backend backed by an S3-compatible object store.
type S3 struct {
	client   *s3.Client
	bucket   string
	alias    string
	acl      string // canned ACL, or "" to omit the header
	settings S3Settings
}

// NewS3 builds an S3 CAS backend from resolved settings.
func NewS3(ctx context.Context, st S3Settings) (*S3, error) {
	if err := st.Validate(); err != nil {
		return nil, fmt.Errorf("s3 CAS settings: %w", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(st.EffectiveRegion()),
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
		// Ceph RGW and MinIO are path-style; CVMFS_S3_DNS_BUCKETS opts into
		// virtual-host addressing explicitly.
		o.UsePathStyle = !st.DNSBuckets
	})

	acl := st.ACL
	if acl == "-" { // explicit opt-out
		acl = ""
	}
	return &S3{
		client:   client,
		bucket:   st.Bucket,
		alias:    st.RepoAlias,
		acl:      acl,
		settings: st,
	}, nil
}

// Endpoint returns the resolved S3 endpoint (for startup logging).
func (s *S3) Endpoint() string { return s.settings.Endpoint() }

// Bucket returns the bucket name (for startup logging).
func (s *S3) Bucket() string { return s.bucket }

// Alias returns the repository alias used as the key prefix.
func (s *S3) Alias() string { return s.alias }

// ExistsIsNative reports false: every Exists is a network round trip, so the
// Bloom-filter pre-check in the dedup stage is worth its cost here (unlike
// LocalFS, where Exists is a single os.Stat).
func (s *S3) ExistsIsNative() bool { return false }

// key returns the full object key for a hash: "<alias>/data/<xx>/<rest>".
func (s *S3) key(hash string) string {
	return s.alias + "/" + cvmfshash.ObjectPath(hash)
}

// dataPrefix is the key prefix under which all objects live.
func (s *S3) dataPrefix() string {
	return s.alias + "/data/"
}

// isNotFound reports whether an S3 error means "no such key".
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	// HeadObject returns a bare 404 with no typed body on several
	// S3-compatible implementations.
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return true
	}
	return false
}

// Exists reports whether the object is already in the store.
func (s *S3) Exists(ctx context.Context, hash string) (bool, error) {
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
	key := s.key(hash)

	exists, err := s.Exists(ctx, hash)
	if err != nil {
		return err
	}
	if exists {
		return nil // already in CAS — idempotent, and we must not rewrite it
	}

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
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(hash)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", s.key(hash), err)
	}
	return out.Body, nil
}

// Delete removes an object.
func (s *S3) Delete(ctx context.Context, hash string) error {
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
