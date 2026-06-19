// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package chunker

import (
	"math/rand"
	"testing"
)

func detData(n int, seed int64) []byte {
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	r.Read(b)
	return b
}

const (
	cMin = 4 << 20
	cAvg = 8 << 20
	cMax = 16 << 20
)

func TestCutsBounds(t *testing.T) {
	c := NewXor32(cMin, cAvg, cMax)
	data := detData(40<<20, 12345)
	cuts := c.Cuts(data)
	if len(cuts) == 0 {
		t.Fatal("expected chunks for a 40 MiB random file, got none")
	}
	prev := int64(0)
	for i, cut := range cuts {
		sz := cut - prev
		if sz < cMin || sz > cMax {
			t.Errorf("chunk %d size %d outside [%d,%d]", i, sz, cMin, cMax)
		}
		if cut <= prev || cut >= int64(len(data)) {
			t.Errorf("cut %d offset %d not strictly inside (prev=%d,n=%d)", i, cut, prev, len(data))
		}
		prev = cut
	}
	final := int64(len(data)) - prev
	if final <= 0 || final > cMax {
		t.Errorf("final chunk size %d invalid", final)
	}
}

func TestCutsDeterministic(t *testing.T) {
	c := NewXor32(cMin, cAvg, cMax)
	data := detData(20<<20, 99)
	a := c.Cuts(data)
	b := c.Cuts(data)
	if len(a) != len(b) {
		t.Fatalf("non-deterministic length %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-deterministic cut %d: %d vs %d", i, a[i], b[i])
		}
	}
}

func TestSmallFileNotChunked(t *testing.T) {
	c := NewXor32(cMin, cAvg, cMax)
	if cuts := c.Cuts(detData(3<<20, 1)); cuts != nil {
		t.Errorf("file < min should not chunk, got %d cuts", len(cuts))
	}
	if cuts := c.Cuts(detData(cMin, 1)); cuts != nil {
		t.Errorf("file == min should not chunk (strict >), got %d cuts", len(cuts))
	}
}
