// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// State persists the last-synced root hash per repository (ADR R4) so that, after
// any absence, catch-up can compute the old→current diff. Writes are atomic
// (temp + rename); a missing file reads as the empty root.
type State struct {
	dir string
}

// NewState returns a State backed by files under dir.
func NewState(dir string) *State { return &State{dir: dir} }

type rootRecord struct {
	Repo string `json:"repo"`
	Root string `json:"root"`
}

func (s *State) path(repo string) string {
	return filepath.Join(s.dir, sanitize(repo)+".root.json")
}

// Get returns the last-synced root for repo, or "" if none is recorded.
func (s *State) Get(repo string) (string, error) {
	b, err := os.ReadFile(s.path(repo))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	var rr rootRecord
	if err := json.Unmarshal(b, &rr); err != nil {
		return "", err
	}
	return rr.Root, nil
}

// Set records root as the last-synced root for repo.
func (s *State) Set(repo, root string) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(rootRecord{Repo: repo, Root: root})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".root-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, s.path(repo))
}

// sanitize maps a repo name to a safe filename component.
func sanitize(repo string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, repo)
}
