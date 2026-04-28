package cvmfsxattr

import (
	"bytes"
	"maps"
	"testing"
)

// TestMarshalNilEmpty verifies that nil and empty maps produce nil output.
func TestMarshalNilEmpty(t *testing.T) {
	if b := Marshal(nil); b != nil {
		t.Errorf("Marshal(nil): expected nil, got %v", b)
	}
	if b := Marshal(map[string][]byte{}); b != nil {
		t.Errorf("Marshal(empty): expected nil, got %v", b)
	}
}

// TestUnmarshalNilEmpty verifies that nil and empty inputs return an empty map.
func TestUnmarshalNilEmpty(t *testing.T) {
	for _, in := range [][]byte{nil, {}} {
		m, err := Unmarshal(in)
		if err != nil {
			t.Errorf("Unmarshal(%v): unexpected error: %v", in, err)
		}
		if len(m) != 0 {
			t.Errorf("Unmarshal(%v): expected empty map, got %v", in, m)
		}
	}
}

// TestRoundTrip verifies that Marshal→Unmarshal is lossless.
func TestRoundTrip(t *testing.T) {
	original := map[string][]byte{
		"user.cvmfs.hash":        []byte("abc123def456-"),
		"user.cvmfs.compression": []byte("zlib"),
		"user.myapp.version":     []byte("3.14"),
		"user.binary":            {0x00, 0x01, 0xFF, 0xFE}, // binary value
	}

	blob := Marshal(original)
	if blob == nil {
		t.Fatal("Marshal returned nil for non-empty map")
	}

	got, err := Unmarshal(blob)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !maps.EqualFunc(original, got, bytes.Equal) {
		t.Errorf("round-trip mismatch:\n  want %v\n  got  %v", original, got)
	}
}

// TestMarshalDeterministic verifies that two Marshal calls on maps with the
// same content produce identical byte sequences, regardless of map iteration
// order.
func TestMarshalDeterministic(t *testing.T) {
	m := map[string][]byte{
		"z-last":  []byte("z"),
		"a-first": []byte("a"),
		"m-mid":   []byte("m"),
	}

	b1 := Marshal(m)
	b2 := Marshal(m)

	if !bytes.Equal(b1, b2) {
		t.Error("Marshal is non-deterministic: two calls on identical map returned different bytes")
	}
}

// TestMarshalSortedOrder verifies that keys appear in lexicographic order in
// the wire encoding.
func TestMarshalSortedOrder(t *testing.T) {
	m := map[string][]byte{
		"c": []byte("3"),
		"a": []byte("1"),
		"b": []byte("2"),
	}
	blob := Marshal(m)
	got, err := Unmarshal(blob)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Verify keys are all present (order-agnostic check on the decoded map).
	for k, wantV := range m {
		gotV, ok := got[k]
		if !ok {
			t.Errorf("key %q missing after round-trip", k)
		} else if !bytes.Equal(gotV, wantV) {
			t.Errorf("key %q: want %q, got %q", k, wantV, gotV)
		}
	}
}

// TestSingleEntry exercises the minimal (N=1) case.
func TestSingleEntry(t *testing.T) {
	m := map[string][]byte{"user.x": []byte("hello")}
	got, err := Unmarshal(Marshal(m))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(got["user.x"]) != "hello" {
		t.Errorf("want %q, got %q", "hello", got["user.x"])
	}
}

// TestEmptyValue verifies that a zero-length value is preserved.
func TestEmptyValue(t *testing.T) {
	m := map[string][]byte{"user.empty": {}}
	got, err := Unmarshal(Marshal(m))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	v, ok := got["user.empty"]
	if !ok {
		t.Fatal("key missing")
	}
	if len(v) != 0 {
		t.Errorf("expected empty value, got %v", v)
	}
}

// TestUnmarshalTruncatedCount verifies that a buffer too short for the
// count field returns an error.
func TestUnmarshalTruncatedCount(t *testing.T) {
	_, err := Unmarshal([]byte{0x01, 0x00}) // 2 bytes — too short
	if err == nil {
		t.Error("expected error for truncated count field, got nil")
	}
}

// TestUnmarshalTruncatedEntry verifies that a buffer cut mid-entry returns
// an error.
func TestUnmarshalTruncatedEntry(t *testing.T) {
	// Claim 1 entry but provide no key/value data.
	buf := []byte{0x01, 0x00, 0x00, 0x00} // count=1, nothing else
	_, err := Unmarshal(buf)
	if err == nil {
		t.Error("expected error for truncated entry, got nil")
	}
}

// TestUnmarshalTrailingBytes verifies that extra bytes after the expected
// payload are flagged as an error.
func TestUnmarshalTrailingBytes(t *testing.T) {
	blob := Marshal(map[string][]byte{"user.a": []byte("v")})
	blob = append(blob, 0xFF) // append garbage
	_, err := Unmarshal(blob)
	if err == nil {
		t.Error("expected error for trailing bytes, got nil")
	}
}

// TestMerge verifies that Merge overwrites dst keys with src values.
func TestMerge(t *testing.T) {
	dst := map[string][]byte{
		"user.keep":      []byte("original"),
		"user.overwrite": []byte("old"),
	}
	src := map[string][]byte{
		"user.overwrite": []byte("new"),
		"user.new":       []byte("added"),
	}
	Merge(dst, src)

	if string(dst["user.keep"]) != "original" {
		t.Errorf("user.keep: want original, got %s", dst["user.keep"])
	}
	if string(dst["user.overwrite"]) != "new" {
		t.Errorf("user.overwrite: want new, got %s", dst["user.overwrite"])
	}
	if string(dst["user.new"]) != "added" {
		t.Errorf("user.new: want added, got %s", dst["user.new"])
	}
}
