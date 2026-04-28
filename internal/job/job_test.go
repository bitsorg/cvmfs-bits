package job

import (
	"strings"
	"testing"
)

// ── ValidateTagName ───────────────────────────────────────────────────────────

// TestValidateTagName_EmptyAlwaysValid checks that an empty tag name (meaning
// "publish without a named snapshot") is always accepted.
func TestValidateTagName_EmptyAlwaysValid(t *testing.T) {
	if err := ValidateTagName(""); err != nil {
		t.Errorf("ValidateTagName(\"\") = %v; want nil", err)
	}
}

// TestValidateTagName_ValidNames checks a representative set of names that
// should pass validation.
func TestValidateTagName_ValidNames(t *testing.T) {
	valid := []string{
		"v1.0.0",
		"release-2024-01-01",
		"my_tag",
		"ATLAS-24.0.1",
		"a",
		"123",
		"release_candidate",
		strings.Repeat("a", 255), // exactly at the 255-char limit
	}
	for _, name := range valid {
		if err := ValidateTagName(name); err != nil {
			t.Errorf("ValidateTagName(%q) = %v; want nil", name, err)
		}
	}
}

// TestValidateTagName_TooLong checks that names longer than 255 characters are rejected.
func TestValidateTagName_TooLong(t *testing.T) {
	name256 := strings.Repeat("a", 256)
	err := ValidateTagName(name256)
	if err == nil {
		t.Error("ValidateTagName(256-char name) = nil; want error")
	}
}

// TestValidateTagName_ContainsSpace checks that any name containing a space is
// rejected (CVMFS tag names must not contain spaces).
func TestValidateTagName_ContainsSpace(t *testing.T) {
	names := []string{
		"tag name",
		"v1 0",
		" leading",
		"trailing ",
		"a b c",
	}
	for _, name := range names {
		if err := ValidateTagName(name); err == nil {
			t.Errorf("ValidateTagName(%q) = nil; want error (contains space)", name)
		}
	}
}

// TestValidateTagName_ContainsSlash checks that any name containing a forward
// slash is rejected (CVMFS rejects slashes in tag names).
func TestValidateTagName_ContainsSlash(t *testing.T) {
	names := []string{
		"v1/0/0",
		"path/name",
		"/leading",
		"trailing/",
		"a/b",
	}
	for _, name := range names {
		if err := ValidateTagName(name); err == nil {
			t.Errorf("ValidateTagName(%q) = nil; want error (contains slash)", name)
		}
	}
}

// TestValidateTagName_AllowlistRejectsSpecialChars verifies that the allowlist
// rejects characters beyond the explicitly permitted set (A-Z a-z 0-9 . _ -),
// such as @, #, +, !, whitespace variants, and shell metacharacters.
func TestValidateTagName_AllowlistRejectsSpecialChars(t *testing.T) {
	invalid := []string{
		"tag@v1",
		"tag#1",
		"v1+fix",
		"build!",
		"a\tb",    // tab
		"a\nb",    // newline
		"v1:2:3",  // colon
		"v1=beta", // equals
		"v1~rc1",  // tilde
		"v1^2",    // caret
		"(test)",  // parens
		"v1 2",    // space (already covered but confirm allowlist catches it)
	}
	for _, name := range invalid {
		if err := ValidateTagName(name); err == nil {
			t.Errorf("ValidateTagName(%q) = nil; want error (character not in allowlist)", name)
		}
	}
}

// TestValidateTagName_Exactly255Chars verifies the boundary: 255 chars is valid,
// 256 is not.
func TestValidateTagName_Boundary(t *testing.T) {
	exactly255 := strings.Repeat("x", 255)
	if err := ValidateTagName(exactly255); err != nil {
		t.Errorf("ValidateTagName(255-char name) = %v; want nil", err)
	}

	exactly256 := strings.Repeat("x", 256)
	if err := ValidateTagName(exactly256); err == nil {
		t.Error("ValidateTagName(256-char name) = nil; want error")
	}
}
