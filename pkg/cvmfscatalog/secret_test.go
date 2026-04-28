package cvmfscatalog

import (
	"strings"
	"testing"
	"time"
)

func TestNewToken(t *testing.T) {
	token1, err := NewToken()
	if err != nil {
		t.Fatalf("NewToken failed: %v", err)
	}

	token2, err := NewToken()
	if err != nil {
		t.Fatalf("NewToken failed: %v", err)
	}

	// Should be 64 hex chars
	if len(token1) != 64 {
		t.Errorf("Expected 64-char token, got %d", len(token1))
	}

	// Should be valid hex
	for _, c := range token1 {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Errorf("Non-hex character in token: %c", c)
		}
	}

	// Should be different each time
	if token1 == token2 {
		t.Errorf("Two tokens should be different")
	}
}

func TestSharePath(t *testing.T) {
	token := "abcd1234"
	contentPath := "file.txt"
	path := SharePath(token, contentPath)

	expected := ".shares/abcd1234/file.txt"
	if path != expected {
		t.Errorf("Expected %q, got %q", expected, path)
	}
}

func TestSharesDirEntry(t *testing.T) {
	now := time.Now().Unix()
	entry := SharesDirEntry(now)

	if entry.FullPath != SharesRoot {
		t.Errorf("Expected FullPath %q, got %q", SharesRoot, entry.FullPath)
	}
	if entry.Name != SharesRoot {
		t.Errorf("Expected Name %q, got %q", SharesRoot, entry.Name)
	}
	if !entry.Mode.IsDir() {
		t.Errorf("Expected directory mode")
	}
	if !entry.IsHidden {
		t.Errorf("Expected IsHidden to be true")
	}
	if entry.Mtime != now {
		t.Errorf("Expected mtime %d, got %d", now, entry.Mtime)
	}

	// Verify flags include FlagHidden
	flags := entry.Flags()
	if (flags & FlagHidden) == 0 {
		t.Errorf("FlagHidden not set in flags")
	}
}

func TestTokenDirEntry(t *testing.T) {
	token := "mytoken123"
	now := time.Now().Unix()
	entry := TokenDirEntry(token, now)

	expectedFullPath := ".shares/mytoken123"
	if entry.FullPath != expectedFullPath {
		t.Errorf("Expected FullPath %q, got %q", expectedFullPath, entry.FullPath)
	}
	if entry.Name != token {
		t.Errorf("Expected Name %q, got %q", token, entry.Name)
	}
	if !entry.Mode.IsDir() {
		t.Errorf("Expected directory mode")
	}
	if !entry.IsHidden {
		t.Errorf("Expected IsHidden to be true")
	}

	// Verify flags include FlagHidden
	flags := entry.Flags()
	if (flags & FlagHidden) == 0 {
		t.Errorf("FlagHidden not set in flags")
	}
}
