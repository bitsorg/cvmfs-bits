// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"strings"
	"testing"
)

// A receiver without repositories never fetches discovery, so it must not
// start at all.
func TestParseReceiverRepos_RequiresOne(t *testing.T) {
	for _, in := range []string{"", " ", ",", " , "} {
		_, err := parseReceiverRepos(in)
		if err == nil || !strings.Contains(err.Error(), "--repos") {
			t.Errorf("parseReceiverRepos(%q) error = %v, want one naming --repos", in, err)
		}
	}
}

func TestParseReceiverRepos_ParsesAndValidates(t *testing.T) {
	got, err := parseReceiverRepos(" atlas.cern.ch, ,cms.cern.ch ")
	if err != nil || !reflect.DeepEqual(got, []string{"atlas.cern.ch", "cms.cern.ch"}) {
		t.Errorf("got %v, %v", got, err)
	}
	if _, err := parseReceiverRepos("atlas.cern.ch,bad/repo"); err == nil {
		t.Error("an invalid repository name was accepted")
	}
}
