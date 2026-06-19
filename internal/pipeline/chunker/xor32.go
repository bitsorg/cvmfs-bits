// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package chunker implements CVMFS-compatible content-defined file chunking.
//
// It reproduces the xor32 rolling-checksum cut-point algorithm from CVMFS
// (cvmfs/cvmfs/ingestion/chunk_detector.cc, Xor32Detector) so that bits splits
// files at byte-identical boundaries to native "cvmfs_server ingest". The
// rolling checksum, magic number, threshold and min/avg/max bounds are exact
// ports; correctness against native is asserted by the testbed equivalence
// test (chunk boundary structure), not just the unit tests here.
package chunker

const (
	// xor32Window: the rolling checksum depends only on the last 32 bytes.
	// Matches kXor32Window in chunk_detector.h. min chunk size must be >= this.
	xor32Window = 32
	// xor32Magic: center of the interval the checksum is tested against,
	// UINT32_MAX/2. CVMFS comment: "you should never change this number".
	xor32Magic = int32(0x7FFFFFFF)
)

// Xor32 is a CVMFS-compatible content-defined chunk-boundary detector.
type Xor32 struct {
	min, max  uint64
	threshold int32
}

// NewXor32 builds a detector with the given minimal/average/maximal chunk sizes
// in bytes (CVMFS_MIN/AVG/MAX_CHUNK_SIZE). The average drives the expected cut
// probability via threshold = UINT32_MAX / average.
func NewXor32(min, avg, max uint64) *Xor32 {
	var th int32
	if avg > 0 {
		th = int32(uint64(0xFFFFFFFF) / avg)
	}
	return &Xor32{min: min, max: max, threshold: th}
}

// Cuts returns the chunk boundary offsets for data. Boundaries are the
// exclusive ends of all but the final chunk: chunk k spans [prev, cuts[k]) and
// the final chunk spans [cuts[last], len(data)). A nil/empty result means the
// file is a single (unchunked) object. Files with len(data) <= min are never
// chunked (MightFindChunks: size strictly greater than min).
func (c *Xor32) Cuts(data []byte) []int64 {
	n := int64(len(data))
	if c.min == 0 || n <= int64(c.min) {
		return nil
	}
	var cuts []int64
	lastCut := int64(0)
	for {
		if n-lastCut <= int64(c.min) {
			break // remaining tail is the final chunk; no further cut
		}
		searchStart := lastCut + int64(c.min)
		hardCut := lastCut + int64(c.max)
		searchEnd := hardCut
		if searchEnd > n {
			searchEnd = n
		}
		// Prime the rolling checksum over the 32 bytes ending at searchStart,
		// starting from 0 (a cut resets xor32 to 0). min >= window guarantees a
		// non-negative start.
		var x uint32
		for i := searchStart - xor32Window; i < searchStart; i++ {
			x = (x << 1) ^ uint32(data[i])
		}
		cut := int64(-1)
		for i := searchStart; i < searchEnd; i++ {
			x = (x << 1) ^ uint32(data[i])
			// abs(int32(x) - magic) < threshold, in int32 arithmetic to match
			// the C two's-complement wrap (including the INT32_MIN edge).
			d := int32(x) - xor32Magic
			if d < 0 {
				d = -d
			}
			if d < c.threshold {
				cut = i
				break
			}
		}
		if cut < 0 {
			if searchEnd == hardCut {
				cut = hardCut // forced cut at maximal chunk size
			} else {
				break // ran out of data before max -> tail is the final chunk
			}
		}
		cuts = append(cuts, cut)
		lastCut = cut
	}
	return cuts
}
