// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import "strings"

// ConfirmedObjectName turns one object-list line into a CVMFS object name.
//
// The publisher writes "<s3 key> ok created", "<s3 key> ok present" or
// "<s3 key> failed -", where the key ends in ".../data/<xx>/<rest>". Only "ok"
// lines name an object that is in S3; the name is <xx><rest> (hash plus
// suffix letter), the form the pull manifests carry. Anything else, including
// names that are not plain alphanumerics, is reported as not usable.
func ConfirmedObjectName(line string) (string, bool) {
	f := strings.Fields(line)
	if len(f) != 3 || f[1] != "ok" {
		return "", false
	}
	i := strings.LastIndex(f[0], "/data/")
	if i < 0 {
		return "", false
	}
	parts := strings.Split(f[0][i+len("/data/"):], "/")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) < 2 {
		return "", false
	}
	name := parts[0] + parts[1]
	for _, c := range name {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return "", false
		}
	}
	return name, true
}
