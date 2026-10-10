// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import "testing"

func TestConfirmedObjectName(t *testing.T) {
	for line, want := range map[string]string{
		"test.cvmfs.io/data/ab/cdef01 ok created":          "abcdef01",
		"cvmfs/bits.cern.ch/data/12/3456P ok present":      "123456P",
		"test.cvmfs.io/data/ab/cdef01 failed -":            "",
		"test.cvmfs.io/data/ab/cdef01":                     "",
		"test.cvmfs.io/meta/ab/cdef01 ok created":          "",
		"test.cvmfs.io/data/ab/cd/ef ok created":           "",
		"test.cvmfs.io/data/ab/cdef01-shake128 ok created": "",
		"": "",
	} {
		got, ok := ConfirmedObjectName(line)
		if got != want || ok != (want != "") {
			t.Errorf("%q: got (%q, %v), want %q", line, got, ok, want)
		}
	}
}
