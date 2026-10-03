// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"reflect"
	"strings"
	"testing"
)

// Distribution completion was never recorded by anything, so the record must
// not declare fields that could only ever be absent.
func TestJob_NoUnsetDistributionFields(t *testing.T) {
	tags := map[string]bool{}
	rt := reflect.TypeOf(Job{})
	for i := 0; i < rt.NumField(); i++ {
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		tags[name] = true
	}
	for _, key := range []string{"distributing_ended_at", "distribution_confirmed", "distribution_total"} {
		if tags[key] {
			t.Errorf("job record still declares %q", key)
		}
	}
	if !tags["distributing_started_at"] {
		t.Error("distributing_started_at (still recorded) is missing")
	}
}
