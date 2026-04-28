package cvmfsdirtab

import (
	"testing"
)

func TestParseEmpty(t *testing.T) {
	dt, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil): %v", err)
	}
	if dt.Matches("/anything") {
		t.Error("empty dirtab should never match")
	}
}

func TestParseCommentsAndBlanks(t *testing.T) {
	content := []byte(`
# this is a comment
  # indented comment

  `)
	dt, err := Parse(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dt.Matches("/foo") {
		t.Error("comment-only dirtab should never match")
	}
}

func TestPositiveWildcard(t *testing.T) {
	// /software/releases/* matches every direct child of /software/releases
	dt, err := Parse([]byte("/software/releases/*"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cases := []struct {
		path  string
		want  bool
	}{
		{"/software/releases/2.1.1",   true},
		{"/software/releases/nightly", true},
		// deeper paths are NOT matched (only one level of * )
		{"/software/releases/2.1.1/extra", false},
		// unrelated paths
		{"/software/releases",         false},
		{"/software",                  false},
		{"/other/releases/foo",        false},
	}
	for _, tc := range cases {
		got := dt.Matches(tc.path)
		if got != tc.want {
			t.Errorf("Matches(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestNegationByBasename(t *testing.T) {
	// positive: everything under /data/*
	// negation: any directory named *.svn or *.git
	dt, err := Parse([]byte(`
/data/*
! *.svn
! *.git
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !dt.Matches("/data/run123") {
		t.Error("expected /data/run123 to match")
	}
	if dt.Matches("/data/repo.svn") {
		t.Error("/data/repo.svn should be excluded by negation rule")
	}
	if dt.Matches("/data/repo.git") {
		t.Error("/data/repo.git should be excluded by negation rule")
	}
}

func TestNegationOverridesPositive(t *testing.T) {
	// The same path matches both a positive and a negation rule.
	// Negation must win regardless of rule order.
	dt, err := Parse([]byte(`
/data/*
! /data/ignored
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if dt.Matches("/data/ignored") {
		t.Error("negation rule must override the positive wildcard")
	}
	if !dt.Matches("/data/other") {
		t.Error("/data/other should still match")
	}
}

func TestMultiplePositivePatterns(t *testing.T) {
	dt, err := Parse([]byte(`
/software/releases/*
/conditions_data/runs/*
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !dt.Matches("/software/releases/3.0") {
		t.Error("expected match")
	}
	if !dt.Matches("/conditions_data/runs/2024-01") {
		t.Error("expected match")
	}
	if dt.Matches("/other/path") {
		t.Error("unexpected match")
	}
}

func TestNilDirtab(t *testing.T) {
	var dt *Dirtab
	if dt.Matches("/anything") {
		t.Error("nil Dirtab should never match")
	}
}

func TestInvalidPattern(t *testing.T) {
	_, err := Parse([]byte("/bad/[pattern"))
	if err == nil {
		t.Error("expected error for invalid pattern")
	}
}

func TestLeadingSlashNormalization(t *testing.T) {
	// Pattern without leading slash on a path-like rule should be auto-fixed.
	dt, err := Parse([]byte("software/releases/*"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !dt.Matches("/software/releases/v1") {
		t.Error("expected match after path normalization")
	}
}
