// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// hash builds a syntactically valid 40-character CAS hash: two leading hex
// digits (which become the fan-out directory) plus 38 more. Written this way
// rather than as literals because a hand-typed hash one character short is
// silently rejected by validHashKey — correct behaviour that looks exactly like
// a code bug when the literal is wrong. It was, the first time.
func hash(lead, fill string) string {
	return lead + strings.Repeat(fill, 38)
}

func stageKey(h string) string { return "stage/data/" + h[:2] + "/" + h[2:] }
func repoKey(h string) string  { return "repo/data/" + h[:2] + "/" + h[2:] }

// seed puts an object into the fake directly, bypassing the client.
func seed(f *fakeS3, key, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = []byte(body)
}

func get(f *fakeS3, key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	return string(b), ok
}

func TestPromoteFromCopiesStagedObjects(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	h1, h2 := hash("ab", "1"), hash("cd", "2")
	h3 := hash("ef", "3")[:39] + "C" // catalog: trailing content-type suffix
	seed(fake, stageKey(h1), "one")
	seed(fake, stageKey(h2), "two")
	seed(fake, stageKey(h3), "cat")

	res, err := s.PromoteFrom(context.Background(), "stage", 4)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Copied != 3 || res.Skipped != 0 || res.Rejected != 0 {
		t.Errorf("copied=%d skipped=%d rejected=%d, want 3/0/0",
			res.Copied, res.Skipped, res.Rejected)
	}
	for h, want := range map[string]string{h1: "one", h2: "two", h3: "cat"} {
		got, ok := get(fake, repoKey(h))
		if !ok {
			t.Errorf("%s missing from the CAS", repoKey(h))
			continue
		}
		if got != want {
			t.Errorf("%s = %q, want %q", repoKey(h), got, want)
		}
	}
	// Server-side: the data must not have been read and rewritten by us.
	if fake.copies != 3 {
		t.Errorf("server-side copies = %d, want 3", fake.copies)
	}
	if fake.puts != 0 {
		t.Errorf("client-side puts = %d, want 0 — promotion must not stream data", fake.puts)
	}
}

// An object already in the CAS is not rewritten. This is defence in depth and
// an optimisation, NOT the safety property — content addressing is what stops a
// mismatched object being served. See the doc comment on PromoteFrom.
//
// NEGATIVE CONTROL: remove the HeadObject/skip block in promoteOne and this
// fails on the content assertion, not merely on the counter. Verified.
func TestPromoteFromNeverOverwritesExisting(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	h := hash("ab", "4")
	seed(fake, repoKey(h), "ORIGINAL")
	seed(fake, stageKey(h), "DIFFERENT")

	res, err := s.PromoteFrom(context.Background(), "stage", 2)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Copied != 0 || res.Skipped != 1 {
		t.Errorf("copied=%d skipped=%d, want 0/1", res.Copied, res.Skipped)
	}
	if got, _ := get(fake, repoKey(h)); got != "ORIGINAL" {
		t.Fatalf("existing CAS object was overwritten: %q, want %q", got, "ORIGINAL")
	}
	if fake.copies != 0 {
		t.Errorf("copies = %d, want 0", fake.copies)
	}
}

func TestPromoteFromMixedCopiesOnlyTheAbsent(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	have, absent := hash("ab", "5"), hash("cd", "6")
	seed(fake, repoKey(have), "have")
	seed(fake, stageKey(have), "have")
	seed(fake, stageKey(absent), "new")

	res, err := s.PromoteFrom(context.Background(), "stage", 2)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Copied != 1 || res.Skipped != 1 {
		t.Errorf("copied=%d skipped=%d, want 1/1", res.Copied, res.Skipped)
	}
	if _, ok := get(fake, repoKey(absent)); !ok {
		t.Error("the absent object was not promoted")
	}
}

func TestPromoteFromRefusesRepositoryAlias(t *testing.T) {
	s, _ := newTestS3(t, "repo")
	_, err := s.PromoteFrom(context.Background(), "repo", 2)
	if err == nil {
		t.Fatal("promoting the repository alias onto itself must be refused")
	}
	if !strings.Contains(err.Error(), "repository alias") {
		t.Errorf("error should name the cause, got: %v", err)
	}
}

func TestPromoteFromEmptyStagingIsNoOp(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	res, err := s.PromoteFrom(context.Background(), "stage", 2)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Copied != 0 || res.Skipped != 0 {
		t.Errorf("copied=%d skipped=%d, want 0/0", res.Copied, res.Skipped)
	}
	if fake.copies != 0 || fake.puts != 0 {
		t.Error("an empty staging prefix must issue no writes")
	}
}

func TestPromoteFromRejectsEmptyAlias(t *testing.T) {
	s, _ := newTestS3(t, "repo")
	if _, err := s.PromoteFrom(context.Background(), "", 2); err == nil {
		t.Fatal("an empty staging alias must be refused")
	}
}

// The listing carries sizes; the result reports the bytes moved so a caller can
// compare against what the producer staged.
func TestPromoteFromReportsBytes(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	h := hash("ab", "7")
	body := strings.Repeat("x", 4096)
	seed(fake, stageKey(h), body)

	res, err := s.PromoteFrom(context.Background(), "stage", 1)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Bytes != int64(len(body)) {
		t.Errorf("bytes = %d, want %d", res.Bytes, len(body))
	}
	if got, _ := get(fake, repoKey(h)); got != body {
		t.Error("promoted content differs from the staged content")
	}
}

// A staging prefix is written by a less-privileged producer, so its keys are
// input, not fact. Anything that is not a plain CAS object must be counted and
// dropped, never turned into a destination key.
//
// NEGATIVE CONTROL: derive the destination by prefix substitution instead of
// s.key() and the dot-segment case creates repo/data/../.cvmfspublished.
func TestPromoteFromRejectsKeysThatAreNotCASObjects(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	good := hash("cd", "8")
	seed(fake, "stage/data/../.cvmfspublished", "manifest")      // escapes the prefix
	seed(fake, "stage/data/", "")                                // directory marker
	seed(fake, "stage/data/ab/", "")                             // directory marker
	seed(fake, "stage/data/ab/short", "nope")                    // implausible length
	seed(fake, "stage/data/ab/"+strings.Repeat("z", 38), "nope") // not hex
	seed(fake, stageKey(good), "good")

	res, err := s.PromoteFrom(context.Background(), "stage", 4)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Copied != 1 {
		t.Errorf("copied=%d, want 1 (only the valid object)", res.Copied)
	}
	if res.Rejected != 5 {
		t.Errorf("rejected=%d, want 5", res.Rejected)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for k := range fake.objects {
		if !strings.HasPrefix(k, "repo/") {
			continue
		}
		if !strings.HasPrefix(k, "repo/data/") || strings.Contains(k, "..") {
			t.Errorf("promotion created a key outside the CAS: %q", k)
		}
	}
	if _, ok := fake.objects["repo/data/../.cvmfspublished"]; ok {
		t.Error("dot-segment key escaped the data prefix")
	}
}

// The ACL must be sent on the copy, or promoted objects are unreadable to
// clients over HTTP (403 -> EIO), exactly as for Put.
//
// NEGATIVE CONTROL: drop `if s.acl != ""` from promoteOne and this fails.
func TestPromoteFromAppliesACL(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	h := hash("ab", "9")
	seed(fake, stageKey(h), "x")

	if _, err := s.PromoteFrom(context.Background(), "stage", 1); err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.acls[repoKey(h)]; got != "public-read" {
		t.Errorf("promoted object ACL = %q, want %q", got, "public-read")
	}
}

// Real S3 pages the listing at 1000 keys and a promotion is expected to exceed
// that. The fake pages at 4, so 25 objects span seven pages.
func TestPromoteFromHandlesTruncatedListing(t *testing.T) {
	s, fake := newTestS3(t, "repo")
	const n = 25
	for i := 0; i < n; i++ {
		seed(fake, "stage/data/ab/"+fmt.Sprintf("%038d", i), "x")
	}

	res, err := s.PromoteFrom(context.Background(), "stage", 3)
	if err != nil {
		t.Fatalf("PromoteFrom: %v", err)
	}
	if res.Copied != n {
		t.Errorf("copied=%d, want %d — the paginator dropped pages", res.Copied, n)
	}
}
