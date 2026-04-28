// Package cvmfsxattr implements serialization and deserialization of the
// CVMFS extended attribute (xattr) binary format used in the xattr BLOB
// column of CVMFS SQLite catalogs.
package cvmfsxattr

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// Marshal serializes an xattr map to the CVMFS binary TLV format.
// Returns nil for an empty or nil map so that the catalog xattr column stays
// NULL for entries with no extended attributes.
//
// Keys are sorted before serialization to guarantee deterministic output
// across runs — non-deterministic ordering would produce different BLOB bytes
// for the same logical xattr set, causing spurious catalog hash divergence.
//
// Wire format (all integers little-endian):
//
//	[uint32: N]  number of key-value pairs
//	repeated N times:
//	  [uint16: key_len]
//	  [uint32: val_len]
//	  [key bytes — no null terminator]
//	  [value bytes — binary-safe]
func Marshal(m map[string][]byte) []byte {
	if len(m) == 0 {
		return nil
	}

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Pre-calculate the exact buffer size to avoid reallocations.
	size := 4 // uint32 entry count
	for _, k := range keys {
		size += 2 + 4 + len(k) + len(m[k]) // uint16 + uint32 + key + value
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keys)))
	pos := 4

	for _, k := range keys {
		v := m[k]
		binary.LittleEndian.PutUint16(buf[pos:pos+2], uint16(len(k)))
		pos += 2
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(v)))
		pos += 4
		copy(buf[pos:], k)
		pos += len(k)
		copy(buf[pos:], v)
		pos += len(v)
	}

	return buf
}

// Unmarshal deserializes a CVMFS binary TLV xattr BLOB into a map.
// Returns an empty non-nil map for nil or zero-length input, matching the
// "no xattrs" case without requiring the caller to nil-check.
func Unmarshal(data []byte) (map[string][]byte, error) {
	m := make(map[string][]byte)
	if len(data) == 0 {
		return m, nil
	}

	if len(data) < 4 {
		return nil, fmt.Errorf("cvmfsxattr: buffer too short for count field (%d bytes)", len(data))
	}

	n := binary.LittleEndian.Uint32(data[0:4])
	pos := 4

	for i := uint32(0); i < n; i++ {
		// Need at least 6 bytes for the key+value length fields.
		if pos+6 > len(data) {
			return nil, fmt.Errorf("cvmfsxattr: entry %d header truncated at offset %d", i, pos)
		}

		keyLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		valLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+keyLen+valLen > len(data) {
			return nil, fmt.Errorf("cvmfsxattr: entry %d data truncated at offset %d (need %d+%d bytes, have %d)",
				i, pos, keyLen, valLen, len(data)-pos)
		}

		key := string(data[pos : pos+keyLen])
		pos += keyLen
		val := make([]byte, valLen)
		copy(val, data[pos:pos+valLen])
		pos += valLen

		m[key] = val
	}

	if pos != len(data) {
		return nil, fmt.Errorf("cvmfsxattr: %d trailing bytes after %d entries", len(data)-pos, n)
	}

	return m, nil
}

// Merge merges src into dst, with src winning on key collision.
// dst must be non-nil.  A nil src is a no-op.
func Merge(dst, src map[string][]byte) {
	for k, v := range src {
		dst[k] = v
	}
}
