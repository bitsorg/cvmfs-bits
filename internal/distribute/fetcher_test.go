// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"bytes"
	"context"
	"io"
	"testing"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

type fakeFetcher struct {
	name string
	body []byte
}

func (f *fakeFetcher) Name() string { return f.name }

func (f *fakeFetcher) Fetch(_ context.Context, _ string, _ manifest.ObjRef) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.body)), nil
}

func TestFetcherRegistry(t *testing.T) {
	f := &fakeFetcher{name: "fake-test", body: []byte("hello")}
	RegisterFetcher(f)

	got, ok := GetFetcher("fake-test")
	if !ok || got.Name() != "fake-test" {
		t.Fatalf("GetFetcher returned %v ok=%v", got, ok)
	}

	var found bool
	for _, n := range FetcherNames() {
		if n == "fake-test" {
			found = true
		}
	}
	if !found {
		t.Fatalf("FetcherNames missing registered fetcher: %v", FetcherNames())
	}
}

func TestFetcherDuplicatePanics(t *testing.T) {
	RegisterFetcher(&fakeFetcher{name: "dup-test"})
	defer func() {
		if recover() == nil {
			t.Fatalf("duplicate RegisterFetcher should panic")
		}
	}()
	RegisterFetcher(&fakeFetcher{name: "dup-test"})
}
