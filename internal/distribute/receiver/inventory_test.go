package receiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
)

// makeHash returns a synthetic 64-char lowercase hex string for use in tests.
func makeHash(n int) string {
	return fmt.Sprintf("%064x", n)
}

func TestInventory_AddAndContains(t *testing.T) {
	inv := newInventory(1000, 0.01)

	h1 := makeHash(1)
	h2 := makeHash(2)

	if inv.contains(h1) {
		t.Error("empty inventory should not contain h1")
	}

	inv.add(h1)

	if !inv.contains(h1) {
		t.Error("inventory should contain h1 after add")
	}
	// h2 was never added; with a small filter and 1 item it should not be a false positive
	if inv.contains(h2) {
		t.Log("note: false positive for h2 (acceptable but unusual with 1 item)")
	}
}

func TestInventory_ApproximateSize(t *testing.T) {
	inv := newInventory(1000, 0.01)
	if inv.approximateSize() != 0 {
		t.Fatalf("expected size 0, got %d", inv.approximateSize())
	}
	for i := 0; i < 10; i++ {
		inv.add(makeHash(i))
	}
	if inv.approximateSize() != 10 {
		t.Fatalf("expected size 10, got %d", inv.approximateSize())
	}
}

func TestInventory_IsReady(t *testing.T) {
	inv := newInventory(1000, 0.01)
	if inv.isReady() {
		t.Error("inventory should not be ready before CAS walk")
	}
}

func TestInventory_WriteTo(t *testing.T) {
	inv := newInventory(1000, 0.01)
	inv.add(makeHash(42))

	var buf bytes.Buffer
	n, err := inv.writeTo(&buf)
	if err != nil {
		t.Fatalf("writeTo: %v", err)
	}
	if n == 0 {
		t.Error("writeTo wrote 0 bytes")
	}

	// Round-trip: deserialise into a fresh filter and verify the hash is present.
	bf := new(bloom.BloomFilter)
	if _, err := bf.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if !bf.TestString(makeHash(42)) {
		t.Error("deserialised filter does not contain the added hash")
	}
	if bf.TestString(makeHash(99)) {
		t.Log("note: false positive for hash 99 (acceptable)")
	}
}

func TestInventory_PopulateFromCAS(t *testing.T) {
	// Build a synthetic 2-level CAS directory.
	casRoot := t.TempDir()
	hashes := []string{
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
		"1122334455667788990011223344556677889900112233445566778899001122",
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
	}
	for _, h := range hashes {
		dir := filepath.Join(casRoot, h[:2])
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		// Write with compression suffix "C"
		if err := os.WriteFile(filepath.Join(dir, h+"C"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Add a non-CAS file and a non-CAS directory — they should be skipped.
	os.WriteFile(filepath.Join(casRoot, "README"), []byte("not a hash"), 0644)
	os.MkdirAll(filepath.Join(casRoot, "lost+found"), 0755)
	// Add a file with a non-hex name — should be skipped.
	os.MkdirAll(filepath.Join(casRoot, "zz"), 0755)
	os.WriteFile(filepath.Join(casRoot, "zz", "notahash"), []byte("x"), 0644)

	inv := newInventory(1000, 0.01)
	logFn := func(msg string, args ...any) {
		parts := []any{msg}
		parts = append(parts, args...)
		t.Log(parts...)
	}

	if err := inv.populateFromCAS(context.Background(), casRoot, logFn); err != nil {
		t.Fatalf("populateFromCAS: %v", err)
	}

	if !inv.isReady() {
		t.Error("inventory should be ready after CAS walk")
	}
	if got := inv.approximateSize(); got != uint64(len(hashes)) {
		t.Errorf("expected %d objects, got %d", len(hashes), got)
	}
	for _, h := range hashes {
		if !inv.contains(h) {
			t.Errorf("inventory should contain %s after CAS walk", h[:8]+"…")
		}
	}
}

func TestInventory_PopulateFromCAS_EmptyRoot(t *testing.T) {
	casRoot := filepath.Join(t.TempDir(), "nonexistent")
	inv := newInventory(100, 0.01)
	logFn := func(msg string, args ...any) {}

	if err := inv.populateFromCAS(context.Background(), casRoot, logFn); err != nil {
		t.Fatalf("populateFromCAS on nonexistent dir should not error: %v", err)
	}
	if !inv.isReady() {
		t.Error("inventory should be ready even for missing CAS root")
	}
	if inv.approximateSize() != 0 {
		t.Errorf("expected 0 objects, got %d", inv.approximateSize())
	}
}

func TestFilterAbsent(t *testing.T) {
	bf := bloom.NewWithEstimates(1000, 0.01)
	present := makeHash(1)
	absent := makeHash(2)
	bf.AddString(present)

	hashes := []string{present, absent}
	result := filterAbsent(hashes, bf)

	if len(result) != 1 || result[0] != absent {
		t.Errorf("filterAbsent: expected [%s], got %v", absent[:8]+"…", result)
	}
}

// ── Fix #1: per-prefix batching bounds memory ─────────────────────────────────

// TestInventory_PopulateFromCAS_MultiplePrefix verifies that populateFromCAS
// correctly handles a CAS with objects spread across several prefix directories
// (tests the per-prefix batching path that replaced the global slice approach).
func TestInventory_PopulateFromCAS_MultiplePrefix(t *testing.T) {
	casRoot := t.TempDir()
	// Insert hashes under 4 different prefix dirs to exercise the batch-per-prefix loop.
	want := map[string]bool{}
	for _, prefix := range []string{"aa", "bb", "cc", "dd"} {
		if err := os.MkdirAll(filepath.Join(casRoot, prefix), 0755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			// Construct a 64-char hex hash that starts with the chosen prefix.
			h := fmt.Sprintf("%s%062x", prefix, i)
			want[h] = true
			path := filepath.Join(casRoot, prefix, h+"C")
			if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}

	inv := newInventory(1000, 0.01)
	if err := inv.populateFromCAS(context.Background(), casRoot, func(string, ...any) {}); err != nil {
		t.Fatalf("populateFromCAS: %v", err)
	}

	if !inv.isReady() {
		t.Error("inventory should be ready after walk")
	}
	if got := inv.approximateSize(); got != uint64(len(want)) {
		t.Errorf("size: got %d, want %d", got, len(want))
	}
	for h := range want {
		if !inv.contains(h) {
			t.Errorf("inventory missing hash %s…", h[:8])
		}
	}
}

// TestInventory_PopulateFromCAS_ReadyOnSubdirError verifies that isReady()
// becomes true even when an individual prefix subdir is unreadable, so the
// bloom endpoint never gets stuck serving 503.
func TestInventory_PopulateFromCAS_ReadyOnSubdirError(t *testing.T) {
	casRoot := t.TempDir()
	// Create one valid prefix dir.
	goodPrefix := "ab"
	if err := os.MkdirAll(filepath.Join(casRoot, goodPrefix), 0755); err != nil {
		t.Fatal(err)
	}
	h := goodPrefix + fmt.Sprintf("%062x", 1)
	if err := os.WriteFile(filepath.Join(casRoot, goodPrefix, h+"C"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	// Create a second prefix dir and chmod it to 000 so ReadDir fails.
	badPrefix := "cd"
	badDir := filepath.Join(casRoot, badPrefix)
	if err := os.MkdirAll(badDir, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(badDir, 0755) }) // allow temp dir cleanup

	inv := newInventory(1000, 0.01)
	_ = inv.populateFromCAS(context.Background(), casRoot, func(string, ...any) {})

	if !inv.isReady() {
		t.Error("isReady() must be true even when a prefix subdir is unreadable")
	}
	if !inv.contains(h) {
		t.Error("objects from readable prefix should still be in the filter")
	}
}

// ── Fix #2: writeTo serializes to buffer before releasing lock ────────────────

// TestInventory_WriteTo_ConcurrentAdd verifies that concurrent add() calls
// during writeTo do not produce a data race (run with -race).
func TestInventory_WriteTo_ConcurrentAdd(t *testing.T) {
	inv := newInventory(1000, 0.01)
	inv.add(makeHash(1))

	done := make(chan struct{})
	// Concurrently add hashes while writeTo is serialising.
	go func() {
		defer close(done)
		for i := 2; i < 50; i++ {
			inv.add(makeHash(i))
		}
	}()

	var buf bytes.Buffer
	if _, err := inv.writeTo(&buf); err != nil {
		t.Fatalf("writeTo: %v", err)
	}
	<-done

	// The snapshot must be deserializable — we don't care which concurrent
	// adds made it in; the filter is a consistent point-in-time snapshot.
	bf := new(bloom.BloomFilter)
	if _, err := bf.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
}

// ── Fix #7: filterAbsentTrimSuffix uses fixed slice, not TrimRight ────────────

// TestFilterAbsentTrimSuffix_MultiCharSuffix verifies that a hash followed by
// more than one uppercase character (e.g. "CC") is normalised to exactly 64
// chars before testing — not over-stripped by strings.TrimRight.
func TestFilterAbsentTrimSuffix_MultiCharSuffix(t *testing.T) {
	bf := bloom.NewWithEstimates(1000, 0.01)
	base := makeHash(42) // 64-char hex
	bf.AddString(base)

	// "CC" suffix: should normalise to base (64 chars) and find it in the filter.
	withTwoSuffix := base + "CC"
	result := filterAbsentTrimSuffix([]string{withTwoSuffix}, bf)
	if len(result) != 0 {
		t.Errorf("hash with CC suffix: expected filter hit (absent=0), got %d absent", len(result))
	}

	// "C" suffix (standard): should also normalise to base and find it.
	withOneSuffix := base + "C"
	result2 := filterAbsentTrimSuffix([]string{withOneSuffix}, bf)
	if len(result2) != 0 {
		t.Errorf("hash with C suffix: expected filter hit (absent=0), got %d absent", len(result2))
	}

	// A hash genuinely not in the filter should be reported as absent.
	absent := makeHash(99) + "C"
	result3 := filterAbsentTrimSuffix([]string{absent}, bf)
	if len(result3) != 1 {
		t.Errorf("absent hash with C suffix: expected 1 absent, got %d", len(result3))
	}
}

// ── Fix HIGH #1: populateFromCAS must skip orphaned .tmp files ────────────────

// TestInventory_PopulateFromCAS_SkipsTmpFiles is the regression test for the
// bloom false-positive bug: a crashed PUT leaves a file named
// "{hash}C.{8hexchars}.tmp".  Taking name[:64] produces the original CAS hash,
// which passes isHexString — causing the object to appear present in the filter
// even though it was never atomically committed.  The distributor would then
// skip pushing it, leaving the receiver without the object.
func TestInventory_PopulateFromCAS_SkipsTmpFiles(t *testing.T) {
	casRoot := t.TempDir()

	// Write a real CAS object under prefix "aa".
	goodHash := makeHash(1) // starts with "00…01", prefix "00"
	prefix := goodHash[:2]
	dir := filepath.Join(casRoot, prefix)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, goodHash+"C"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Plant an orphaned .tmp file in the same prefix dir.
	// Its first 64 chars are a valid hex hash; without the fix it would be
	// counted in the bloom filter and approximateSize.
	tmpHash := makeHash(2)
	tmpName := tmpHash + "C.deadbeef.tmp" // matches the PUT handler's naming scheme
	if err := os.WriteFile(filepath.Join(dir, tmpName), []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}

	inv := newInventory(1000, 0.01)
	if err := inv.populateFromCAS(context.Background(), casRoot, func(string, ...any) {}); err != nil {
		t.Fatalf("populateFromCAS: %v", err)
	}

	// The real object must be present.
	if !inv.contains(goodHash) {
		t.Error("inventory should contain the committed CAS object")
	}
	// The hash extracted from the .tmp filename must NOT be present.
	if inv.contains(tmpHash) {
		t.Error("inventory must not contain a hash from an orphaned .tmp file")
	}
	// Only 1 object should be counted (the real CAS file, not the tmp).
	if got := inv.approximateSize(); got != 1 {
		t.Errorf("approximateSize: want 1 (no tmp), got %d", got)
	}
}

// TestInventory_PopulateFromCAS_ContextCancel verifies that a cancelled context
// causes the walk to stop between prefix directories, return context.Canceled,
// and still mark the inventory as ready (so the bloom endpoint doesn't serve
// 503 indefinitely after a shutdown).
func TestInventory_PopulateFromCAS_ContextCancel(t *testing.T) {
	casRoot := t.TempDir()
	// Create several prefix directories to give the cancellation check a chance
	// to fire during iteration.
	for _, prefix := range []string{"aa", "bb", "cc", "dd"} {
		if err := os.MkdirAll(filepath.Join(casRoot, prefix), 0755); err != nil {
			t.Fatal(err)
		}
		h := prefix + fmt.Sprintf("%062x", 1)
		if err := os.WriteFile(filepath.Join(casRoot, prefix, h+"C"), []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the very first prefix triggers the Done check

	inv := newInventory(1000, 0.01)
	err := inv.populateFromCAS(ctx, casRoot, func(string, ...any) {})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", err)
	}
	// Even on cancellation the inventory must be marked ready to avoid a
	// permanent 503 on the bloom endpoint.
	if !inv.isReady() {
		t.Error("inventory must be marked ready even when the walk is cancelled")
	}
}

// TestIsErrNoDiskSpace verifies that the ENOSPC check correctly identifies
// wrapped ENOSPC errors via errors.As rather than string matching.
func TestIsErrNoDiskSpace(t *testing.T) {
	// Direct ENOSPC syscall error.
	if !isErrNoDiskSpace(syscall.ENOSPC) {
		t.Error("should identify raw ENOSPC")
	}

	// ENOSPC wrapped in *os.PathError (what io.Copy/os.Write return in practice).
	wrapped := &os.PathError{Op: "write", Path: "/cas/aa/hash", Err: syscall.ENOSPC}
	if !isErrNoDiskSpace(wrapped) {
		t.Error("should unwrap PathError and identify ENOSPC")
	}

	// Non-ENOSPC errno must not match.
	if isErrNoDiskSpace(syscall.ENOENT) {
		t.Error("ENOENT should not be detected as no-disk-space")
	}

	// nil error must not match.
	if isErrNoDiskSpace(nil) {
		t.Error("nil should not be detected as no-disk-space")
	}
}

func TestIsHexString(t *testing.T) {
	cases := []struct {
		s    string
		want bool
	}{
		{"deadbeef", true},
		{"DEADBEEF", false}, // uppercase rejected
		{"deadbee!", false},
		{"", false},
		{"0123456789abcdef", true},
	}
	for _, c := range cases {
		if got := isHexString(c.s); got != c.want {
			t.Errorf("isHexString(%q) = %v, want %v", c.s, got, c.want)
		}
	}
}
