// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package measure

import (
	"math"
	"sort"
	"strings"
	"time"
)

// Summary is one run reduced to the numbers a comparison table needs. The
// field set is taken from the tables already in MEASUREMENTS.md (§24, §25),
// so a section can be written from this without further arithmetic.
type Summary struct {
	BuildID string `json:"build_id,omitempty"`
	Repo    string `json:"repo,omitempty"`
	// PublishPaths counts records per path, so a run that mixed paths is
	// visible as such instead of being averaged into nonsense.
	PublishPaths map[string]int `json:"publish_paths"`

	Jobs      int `json:"jobs"`
	Published int `json:"published"`
	Failed    int `json:"failed"`
	// Incomplete is jobs that neither published nor failed: a package parked
	// in StateAccumulated waiting for its build to be finalized is the normal
	// case on the coarse path. Counting those as failures reported a healthy
	// 170-package build as 170 failures.
	Incomplete int `json:"incomplete,omitempty"`
	Conflicted int `json:"conflicted"`
	Replaced   int `json:"replaced"`

	First time.Time `json:"first"`
	Last  time.Time `json:"last"`
	// WindowS is first-submission to last-terminal-state: the publish wall
	// clock. Not the sum of the per-job times -- jobs overlap.
	WindowS float64 `json:"window_s"`

	// Backend is the per-publish tool duration, the distribution §24 quotes.
	// Serialised sum vs WindowS is what shows whether the path is serialised.
	Backend Stats `json:"backend_s"`
	// Total is submission-to-terminal per job.
	Total Stats `json:"total_s"`

	TarBytes int64 `json:"tar_bytes,omitempty"`
	Objects  int   `json:"objects,omitempty"`
	// ObjectsPartial marks that some records did not count objects, so
	// Objects is a lower bound rather than the run's total.
	ObjectsPartial bool `json:"objects_partial,omitempty"`
}

// Stats is an exact distribution: computed from every value, not estimated
// from buckets. Max is here precisely because a histogram cannot give it, and
// the tail is what lands on the critical path of a serialised publish.
type Stats struct {
	N      int     `json:"n"`
	Sum    float64 `json:"sum"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
	P90    float64 `json:"p90"`
	P99    float64 `json:"p99"`
	Max    float64 `json:"max"`
}

func statsOf(vals []float64) Stats {
	s := Stats{N: len(vals)}
	if s.N == 0 {
		return s
	}
	sort.Float64s(vals)
	for _, v := range vals {
		s.Sum += v
	}
	s.Mean = round3(s.Sum / float64(s.N))
	s.Sum = round3(s.Sum)
	s.Median = round3(quantile(vals, 0.5))
	s.P90 = round3(quantile(vals, 0.90))
	s.P99 = round3(quantile(vals, 0.99))
	s.Max = round3(vals[len(vals)-1])
	return s
}

// quantile uses nearest-rank on sorted values: with 170 samples the
// interpolation choice moves the answer less than the measurement noise, and
// nearest-rank always returns a value that was actually observed.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func round3(f float64) float64 { return math.Round(f*1000) / 1000 }

// Summarise reduces records to one Summary. Records from several builds can
// be passed; the caller decides what belongs together.
func Summarise(recs []Record) Summary {
	s := Summary{PublishPaths: map[string]int{}}
	var backend, total []float64
	countedObjects, sawUncounted, sawInexact := 0, false, false
	// The run began when its EARLIEST-SUBMITTED job began, which is not
	// necessarily the job that finished first: records are written at terminal
	// time. Deriving the start per record (terminal - total) and taking the
	// minimum needs no heuristic and is right when a long job starts first --
	// the GEANT4 shape §24 is about.
	var earliestStart time.Time

	for _, r := range recs {
		s.Jobs++
		s.PublishPaths[r.PublishPath]++
		switch {
		case r.Outcome == "published":
			s.Published++
		case strings.HasPrefix(r.Outcome, IncompletePrefix):
			s.Incomplete++
		default:
			s.Failed++
		}
		if r.Conflicted {
			s.Conflicted++
		}
		if r.Replaced {
			s.Replaced++
		}
		if s.BuildID == "" {
			s.BuildID = r.BuildID
		}
		if s.Repo == "" {
			s.Repo = r.Repo
		}
		if s.First.IsZero() || r.Timestamp.Before(s.First) {
			s.First = r.Timestamp
		}
		if r.Timestamp.After(s.Last) {
			s.Last = r.Timestamp
		}
		if r.BackendS != nil {
			backend = append(backend, *r.BackendS)
		}
		total = append(total, r.TotalS)
		if r.TarBytes != nil {
			s.TarBytes += *r.TarBytes
		}
		if r.Objects != nil {
			countedObjects += *r.Objects
			if !r.ObjectsExact {
				sawInexact = true
			}
		} else {
			sawUncounted = true
		}
		if !r.Timestamp.IsZero() {
			if st := r.Timestamp.Add(-time.Duration(r.TotalS * float64(time.Second))); earliestStart.IsZero() || st.Before(earliestStart) {
				earliestStart = st
			}
		}
	}

	s.Backend = statsOf(backend)
	s.Total = statsOf(total)
	s.Objects = countedObjects
	// Partial when some record did not count at all, OR when a count that was
	// included is not authoritative (a truncated object list). Either way the
	// total is a lower bound and must not be quoted as the run's figure.
	s.ObjectsPartial = (sawUncounted || sawInexact) && countedObjects > 0
	if !earliestStart.IsZero() {
		s.WindowS = round3(s.Last.Sub(earliestStart).Seconds())
	}
	return s
}
