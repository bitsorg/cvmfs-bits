// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// PromoteResult reports what a promotion moved.
type PromoteResult struct {
	Copied   int   // objects copied into the CAS
	Skipped  int   // objects already present, left untouched
	Rejected int   // keys that are not valid CAS objects; never copied
	Bytes    int64 // bytes of the copied objects, as reported by the listing
}

// maxPromoteWorkers bounds the fan-out. The shared transport is sized for
// maxIdleConnsPerHost; letting a caller ask for tens of thousands of workers
// would reproduce the ephemeral-port exhaustion that once failed 64 of 170 jobs
// in a 39 s window.
const maxPromoteWorkers = 256

// PromoteFrom copies every object under a staging prefix into this CAS, using
// server-side copies so no object data passes through this process.
//
// A producer writes prepared objects to <stagingAlias>/data/... in the same
// bucket; this moves them to <alias>/data/... where the repository expects
// them. Source and destination keys differ only in the alias, because both are
// derived from the same content hash.
//
// SERVER-SIDE, AND THAT IS THE POINT. Measured on the same object store:
// server-side copy ran at 2,408 MB/s with nothing crossing the publisher, while
// falling back to GET-then-PUT managed 104 MB/s and moved every byte twice.
// Streaming is not a slower version of this — it is a different design, and a
// worse one than the path it replaces.
//
// KEYS ARE VALIDATED, NOT TRUSTED. The staging prefix is written by a
// less-privileged producer, so a listed key is input, not fact. Each is parsed
// back into a hash, checked with validHashKey, and the destination is then
// built with s.key() rather than by substituting one prefix for another. Naive
// string substitution copied `stage/data/../.cvmfspublished` to
// `repo/data/../.cvmfspublished`, which any normalising proxy collapses onto the
// repository manifest — and the existence check, running on the un-normalised
// key, would not have noticed. Keys that do not parse are counted in Rejected
// and never copied; empty "directory" markers that S3 browsers leave behind land
// there too, which is why they are a count rather than an error.
//
// EXISTING OBJECTS ARE SKIPPED, and that is an optimisation plus defence in
// depth — NOT the safety property. There is a window between the existence
// check and the copy in which another publisher can write the same key, so the
// skip cannot be relied on to prevent substitution. What actually protects the
// repository is content addressing: a client verifies an object against the
// hash it fetched it by, so content that does not match its key fails
// verification rather than being served. If the object store supports
// conditional writes, adding IfNoneMatch to the copy would close the window and
// turn this into the guarantee the skip only approximates.
//
// Concurrency is per object; failures are collected rather than cancelling the
// rest, so one bad key does not strand a mostly-complete promotion. The caller
// decides what a partial result means — nothing has been deleted either way.
func (s *S3) PromoteFrom(ctx context.Context, stagingAlias string, workers int) (PromoteResult, error) {
	var res PromoteResult

	if stagingAlias == "" {
		return res, errors.New("promote: empty staging alias")
	}
	// Same alias would list and copy onto itself: every object "already exists",
	// so it would report a clean no-op while doing nothing. Refuse instead.
	if stagingAlias == s.alias {
		return res, fmt.Errorf("promote: staging alias %q is the repository alias; "+
			"a promotion must move objects between two prefixes", stagingAlias)
	}
	if workers < 1 {
		workers = 16
	}

	srcPrefix := stagingAlias + "/data/"

	type item struct {
		srcKey string
		dstKey string
		size   int64
	}
	var todo []item

	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(srcPrefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return res, fmt.Errorf("promote: list %s: %w", srcPrefix, err)
		}
		for _, o := range page.Contents {
			if o.Key == nil {
				continue
			}
			// Reconstruct the hash exactly as List does — "<xx>/<rest>" is
			// "<xx><rest>" — then validate before it can name a destination.
			rel := strings.TrimPrefix(*o.Key, srcPrefix)
			if len(rel) < 3 || rel[2] != '/' {
				res.Rejected++
				continue
			}
			hash := rel[:2] + rel[3:]
			if err := validHashKey(hash); err != nil {
				res.Rejected++
				continue
			}
			todo = append(todo, item{
				srcKey: *o.Key,
				dstKey: s.key(hash), // never string substitution
				size:   aws.ToInt64(o.Size),
			})
		}
	}
	if len(todo) == 0 {
		return res, nil
	}
	// Never more workers than there is work, and never more than the transport
	// is sized for.
	if workers > len(todo) {
		workers = len(todo)
	}
	if workers > maxPromoteWorkers {
		workers = maxPromoteWorkers
	}

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	work := make(chan item)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range work {
				copied, err := s.promoteOne(ctx, it.srcKey, it.dstKey)
				mu.Lock()
				switch {
				case err != nil:
					errs = append(errs, fmt.Errorf("%s: %w", it.srcKey, err))
				case copied:
					res.Copied++
					res.Bytes += it.size
				default:
					res.Skipped++
				}
				mu.Unlock()
			}
		}()
	}
	for _, it := range todo {
		select {
		case work <- it:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			// Report what went wrong alongside the cancellation: dropping errs
			// here would make the counters look like a clean partial result.
			if len(errs) > 0 {
				return res, fmt.Errorf("promote: cancelled after %d failures: %w",
					len(errs), errors.Join(append(errs, ctx.Err())...))
			}
			return res, ctx.Err()
		}
	}
	close(work)
	wg.Wait()

	if len(errs) > 0 {
		return res, fmt.Errorf("promote: %d of %d objects failed: %w",
			len(errs), len(todo), errors.Join(errs...))
	}
	return res, nil
}

// escapeCopySource percent-encodes a "<bucket>/<key>" for the x-amz-copy-source
// header, per segment so the separators survive: url.PathEscape on the whole
// string would turn every '/' into %2F and address a single, non-existent key.
func escapeCopySource(s string) string {
	parts := strings.Split(s, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// promoteOne copies a single object unless the destination already holds it.
// Reports whether a copy was issued.
func (s *S3) promoteOne(ctx context.Context, srcKey, dstKey string) (bool, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(dstKey),
	})
	if err == nil {
		return false, nil // already in the CAS; never rewrite it
	}
	if !isNotFound(err) {
		// Do not treat AccessDenied or a redirect as "absent": that would copy
		// over an object we simply could not see, which is the one thing this
		// function must not do.
		return false, fmt.Errorf("head %s: %w", dstKey, err)
	}

	in := &s3.CopyObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(dstKey),
		// "<bucket>/<key>", and the SDK does NOT encode this one — it escapes
		// Key but sends CopySource verbatim, so a '?' would be read as the start
		// of a query string and silently target a different object.
		CopySource: aws.String(escapeCopySource(s.bucket + "/" + srcKey)),
	}
	if s.acl != "" {
		in.ACL = types.ObjectCannedACL(s.acl)
	}
	if _, err := s.client.CopyObject(ctx, in); err != nil {
		return false, fmt.Errorf("copy %s: %w", srcKey, err)
	}
	return true, nil
}
