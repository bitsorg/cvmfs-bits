// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"sync"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/measure"
)

// Measurement recording (see internal/measure): one structured record per
// publish, written at the terminal state.
//
// The numbers are collected in an accumulator keyed by job id rather than on
// the job struct, for two reasons: the job manifest is persisted to the spool
// on every transition and does not need measurement scratch in it, and the
// backend's own stats arrive through a pointer handed to Commit, which has
// nowhere else to live.
//
// The accumulator is created at the top of Run and released by a deferred
// sweep in the same function, NOT by hand-placed calls at each terminal
// point. Run is ~1100 lines with roughly forty returns; a review found three
// exits that a hand audit had missed -- the coarse-publish accumulate path
// (the DEFAULT path, which left one accumulator per job and recorded
// nothing), the finalize success return, and a post-commit transition failure
// after the publish had already happened. Anything the explicit calls do not
// claim is recorded by the sweep with an honest outcome rather than lost.
type measAccum struct {
	started     time.Time
	commit      time.Duration
	stats       lease.PublishStats
	mu          sync.Mutex
	conflicted  bool
	replaced    bool
	commitKnown bool
}

// measBegin starts recording for a job. Safe when measurements are disabled.
func (o *Orchestrator) measBegin(j *job.Job) {
	if o.Measurements == nil || j == nil {
		return
	}
	o.measAcc.Store(j.ID, &measAccum{started: time.Now()})
}

func (o *Orchestrator) measFor(j *job.Job) *measAccum {
	if o.Measurements == nil || j == nil {
		return nil
	}
	if v, ok := o.measAcc.Load(j.ID); ok {
		return v.(*measAccum)
	}
	return nil
}

// measStats returns the sink to hand to a backend's CommitRequest, or nil
// when nothing is recording — backends treat nil as "do not report".
func (o *Orchestrator) measStats(j *job.Job) *lease.PublishStats {
	a := o.measFor(j)
	if a == nil {
		return nil
	}
	return &a.stats
}

// measCommit records the orchestrator-side commit phase duration.
func (o *Orchestrator) measCommit(j *job.Job, d time.Duration) {
	if a := o.measFor(j); a != nil {
		a.mu.Lock()
		a.commit, a.commitKnown = d, true
		a.mu.Unlock()
	}
}

// measConflict notes that this publish hit an already published path, and
// whether the remediation replaced it.
func (o *Orchestrator) measConflict(j *job.Job, replaced bool) {
	if a := o.measFor(j); a != nil {
		a.mu.Lock()
		a.conflicted = true
		a.replaced = a.replaced || replaced
		a.mu.Unlock()
	}
}

// measSweep records anything Run left behind. Deferred once at the top of
// Run: if a terminal path already recorded, the accumulator is gone and this
// does nothing.
//
// The outcome it writes is deliberately not "published" or "failed": these
// are the exits that neither succeeded nor aborted -- a job parked in
// StateAccumulated waiting for its build to be finalized, or a return after
// an error that bypassed abortJob. Calling them "failed" would put phantom
// failures in a run summary; leaving them out entirely is what lost the whole
// default publish path. `state` names where the job actually stopped.
func (o *Orchestrator) measSweep(j *job.Job) {
	if o.Measurements == nil || j == nil {
		return
	}
	if _, pending := o.measAcc.Load(j.ID); !pending {
		return
	}
	o.measFinish(j, measure.IncompletePrefix+string(j.State), nil)
}

// measFinish writes the record and releases the accumulator. It is called
// exactly once per job, from the success path or from abortJob; a second call
// finds nothing stored and does nothing, so a job that fails after a partial
// success cannot produce two records.
//
// Never fails a publish: a measurement that cannot be written is logged and
// forgotten.
func (o *Orchestrator) measFinish(j *job.Job, outcome string, cause error) {
	if o.Measurements == nil || j == nil {
		return
	}
	v, loaded := o.measAcc.LoadAndDelete(j.ID)
	if !loaded {
		return
	}
	a := v.(*measAccum)
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	// Prefer the job's own creation time: it includes the queueing the
	// producer actually waited through, which is what a run's wall clock is
	// made of. Fall back to when this Run started if it is unset.
	start := j.CreatedAt
	if start.IsZero() {
		start = a.started
	}

	publishPath := j.PublishPath
	if publishPath == "" {
		publishPath = DefaultPublishPath
	}

	rec := measure.Record{
		Timestamp:   now.UTC(),
		BuildID:     j.BuildID,
		JobID:       j.ID,
		Repo:        j.Repo,
		Path:        j.Path,
		PublishPath: publishPath,
		Outcome:     outcome,
		TotalS:      now.Sub(start).Seconds(),
		Conflicted:  a.conflicted,
		Replaced:    a.replaced,
	}
	if !a.started.IsZero() {
		rec.QueuedS = measure.Secs(a.started.Sub(start))
	}
	if a.commitKnown {
		rec.CommitS = measure.Secs(a.commit)
	}
	if a.stats.Backend > 0 {
		rec.BackendS = measure.Secs(a.stats.Backend)
	}
	if a.stats.TarBytes != nil {
		rec.TarBytes = a.stats.TarBytes
	}
	if a.stats.Objects != nil {
		rec.Objects = a.stats.Objects
		rec.ObjectsExact = a.stats.ObjectsAuthoritative
	}
	// The pipeline paths populate these on the job; the ingest path does not,
	// and must record nothing rather than the zeros it used to log.
	//
	// Only when the backend reported nothing: j.NObjects is a true object
	// count, while the ingest backend's number is object-list LINES (a
	// "<key> failed -" line counts too, see IngestBackend.Commit). Letting
	// this overwrite the backend's value would relabel an inexact count as
	// exact -- the two are different quantities in one field.
	if j.NObjects > 0 && rec.Objects == nil {
		n := j.NObjects
		rec.Objects = &n
		rec.ObjectsExact = true
	}
	if j.NBytesRaw > 0 {
		rec.BytesRaw = &j.NBytesRaw
	}
	if j.NBytesCompressed > 0 {
		rec.BytesCompressed = &j.NBytesCompressed
	}
	if !j.PipelineStartedAt.IsZero() && !j.PipelineEndedAt.IsZero() {
		rec.PipelineS = measure.Secs(j.PipelineEndedAt.Sub(j.PipelineStartedAt))
	}
	if cause != nil {
		rec.Error = cause.Error()
	}

	if err := o.Measurements.Append(rec); err != nil {
		o.Obs.Logger.Warn("measurement record not written (publish unaffected)",
			"job_id", j.ID, "error", err)
	}
}
