// Package cvmfsdirtab parses and evaluates .cvmfsdirtab files.
//
// A .cvmfsdirtab file is placed in a CVMFS repository (typically at the root)
// and contains path-pattern rules that govern which directories should
// automatically become nested-catalog roots.  It is stored as a regular file
// in the repository and is visible to clients; it is also read by the publish
// tool to determine where to create nested catalogs.
//
// File format (one rule per line):
//
//	# comment
//	/software/releases/*         positive rule: every direct child of this dir gets its own catalog
//	/conditions_data/runs/*
//	! *.svn                      negation rule: exclude dirs whose basename matches
//	! *.git
//
// Pattern syntax:
//   - * matches exactly one path component (not "/")
//   - ? matches a single non-slash character
//   - [abc] matches one character from the set
//   - Patterns without "/" are matched against the directory basename only
//     (convenient for negation rules like "! *.git")
//   - Patterns with "/" are matched against the full absolute path
//
// Negation rules take immediate precedence: a directory matching a negation
// rule is never split, even if it also matches a positive rule.
package cvmfsdirtab

import (
	"bufio"
	"fmt"
	"path"
	"strings"
)

// Dirtab holds the parsed rules from a .cvmfsdirtab file.
type Dirtab struct {
	rules []rule
}

type rule struct {
	pattern  string
	negation bool
}

// Parse parses the content of a .cvmfsdirtab file.
// Returns a non-nil Dirtab even when the file is empty (resulting in no rules).
func Parse(content []byte) (*Dirtab, error) {
	dt := &Dirtab{}
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		negation := false
		if strings.HasPrefix(line, "!") {
			negation = true
			line = strings.TrimSpace(line[1:])
		}
		if line == "" {
			continue
		}

		// Patterns that contain "/" must be absolute; normalise them.
		if strings.Contains(line, "/") && !strings.HasPrefix(line, "/") {
			line = "/" + line
		}

		// Validate the pattern is syntactically well-formed.
		if _, err := path.Match(line, ""); err != nil {
			return nil, fmt.Errorf("line %d: invalid pattern %q: %w", lineNum, line, err)
		}

		dt.rules = append(dt.rules, rule{pattern: line, negation: negation})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning dirtab: %w", err)
	}
	return dt, nil
}

// Matches returns true when dirPath should become a nested-catalog root
// according to this Dirtab.
//
// dirPath must be an absolute CVMFS path without a trailing slash (e.g. "/a/b/c").
// A nil Dirtab or one with no rules never matches.
func (dt *Dirtab) Matches(dirPath string) bool {
	if dt == nil || len(dt.rules) == 0 {
		return false
	}
	if !strings.HasPrefix(dirPath, "/") {
		dirPath = "/" + dirPath
	}
	dirPath = strings.TrimSuffix(dirPath, "/")

	matched := false
	for _, r := range dt.rules {
		var ok bool
		if !strings.Contains(r.pattern, "/") {
			// Basename-only pattern (e.g. "*.svn"): match the last path component.
			ok, _ = path.Match(r.pattern, path.Base(dirPath))
		} else {
			// Full-path pattern: match the entire absolute path.
			ok, _ = path.Match(r.pattern, dirPath)
		}
		if !ok {
			continue
		}
		if r.negation {
			// A negation match overrides any positive match found so far.
			return false
		}
		matched = true
	}
	return matched
}
