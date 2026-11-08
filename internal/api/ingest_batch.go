// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// IngestBatchConfig is the ingest-batch path's: at most Max packages per commit
// and MaxBytes of tars (0: no limit); Max below 2 offers no such path. A leader
// waits Linger for company before it commits.
type IngestBatchConfig struct {
	Max      int
	MaxBytes int64
	Linger   time.Duration
}

// batchCommitter is the ingest backend as the batches use it.
type batchCommitter interface {
	Acquire(ctx context.Context, repo, path string) (string, error)
	CommitBatch(ctx context.Context, req lease.BatchCommitRequest) error
}

type batchKind int

const (
	batchAlone     batchKind = iota // publish on today's path
	batchLead                       // lead the next batch
	batchSkip                       // already published
	batchPublished                  // published in a batch
)

// batchOutcome is what a queued job learns. For batchPublished the numbers are
// the batch's, except own and lockWait (joining to the commit); confirmed goes
// to the member that speaks for the batch.
type batchOutcome struct {
	kind      batchKind
	id        string
	own       int64 // this job's tar bytes
	size      int
	bytes     int64
	commit    time.Duration
	lockWait  time.Duration
	stats     lease.PublishStats
	confirmed []string
}

type batchMember struct {
	ctx    context.Context // the job's: one cancelled before the commit is left out
	j      *job.Job
	bytes  int64
	joined time.Time
	done   chan batchOutcome
}

type batchQueue struct {
	waiting []*batchMember
	leading bool
}

// ingestBatches is the per-repository queue of ingest jobs ready to commit.
type ingestBatches struct {
	mu     sync.Mutex
	queues map[string]*batchQueue
	alone  map[string]bool // jobs of a failed batch: never batched again
}

func (b *ingestBatches) queue(repo string) *batchQueue {
	if b.queues == nil {
		b.queues, b.alone = map[string]*batchQueue{}, map[string]bool{}
	}
	q := b.queues[repo]
	if q == nil {
		q = &batchQueue{}
		b.queues[repo] = q
	}
	return q
}

// batchable reports whether j may share an ingest commit with others: it asked
// for the ingest-batch path, which the plain ingest path never does.
func (o *Orchestrator) batchable(j *job.Job) bool {
	p := strings.Trim(j.Path, "/")
	if j.PublishPath != IngestBatchPublishPath || o.IngestBatch.Max < 2 || p == "" || path.Clean(p) != p || j.Replace || j.TagName != "" ||
		j.StagingPrefix != "" || j.TarPath == "" {
		return false
	}
	if _, ok := o.leaseFor(j).(batchCommitter); !ok {
		return false
	}
	o.batches.mu.Lock()
	defer o.batches.mu.Unlock()
	return !o.batches.alone[j.ID]
}

// joinBatch queues j for its repository's next batch commit and returns what
// became of it; it leads that batch itself when no other job does.
func (o *Orchestrator) joinBatch(ctx context.Context, j *job.Job, logger *slog.Logger) batchOutcome {
	// The job's directory moves with its state (and back on a retry).
	j.TarPath = filepath.Join(o.Spool.JobDir(j), "payload.tar")
	m := &batchMember{ctx: ctx, j: j, joined: time.Now(), done: make(chan batchOutcome, 1)}
	if fi, err := os.Stat(j.TarPath); err == nil {
		m.bytes = fi.Size()
	}
	o.batches.mu.Lock()
	q := o.batches.queue(j.Repo)
	lead := !q.leading
	if lead {
		q.leading = true
	} else {
		q.waiting = append(q.waiting, m)
	}
	o.batches.mu.Unlock()
	if lead {
		return o.leadBatch(ctx, m, logger)
	}
	select {
	case out := <-m.done:
		if out.kind == batchLead {
			return o.leadBatch(ctx, m, logger)
		}
		return out
	case <-ctx.Done():
		o.batches.mu.Lock()
		for i, w := range q.waiting {
			if w == m { // not taken yet: leave, and publish alone (which sees ctx)
				q.waiting = append(q.waiting[:i], q.waiting[i+1:]...)
				o.batches.mu.Unlock()
				return batchOutcome{kind: batchAlone}
			}
		}
		o.batches.mu.Unlock()
		out := <-m.done // a leader has it: its commit decides
		if out.kind == batchLead {
			o.handOn(q)
			return batchOutcome{kind: batchAlone}
		}
		return out
	}
}

// handOn passes the lead to the first waiting job, or ends it.
func (o *Orchestrator) handOn(q *batchQueue) {
	o.batches.mu.Lock()
	defer o.batches.mu.Unlock()
	if len(q.waiting) == 0 {
		q.leading = false
		return
	}
	next := q.waiting[0]
	q.waiting = q.waiting[1:]
	next.done <- batchOutcome{kind: batchLead}
}

// leadBatch commits one batch, the leader first, under the repository's commit lock.
func (o *Orchestrator) leadBatch(ctx context.Context, leader *batchMember, logger *slog.Logger) batchOutcome {
	j := leader.j
	o.batches.mu.Lock()
	q := o.batches.queue(j.Repo)
	o.batches.mu.Unlock()
	defer o.handOn(q)

	if o.IngestBatch.Linger > 0 { // before the lock: other publish paths do not wait for it
		select {
		case <-time.After(o.IngestBatch.Linger):
		case <-ctx.Done():
		}
	}
	unlock, err := o.acquireCommitLock(ctx, j)
	if err != nil {
		return batchOutcome{kind: batchAlone} // today's path meets the same cancelled ctx
	}
	// The batch is everyone's: one job's cancel leaves that job out, not the rest;
	// it keeps a job's time limit, so a hung ingest cannot hold the lock for good.
	limit := o.JobTimeout
	if limit <= 0 {
		limit = time.Hour
	}
	bctx, cancelBatch := context.WithTimeout(context.WithoutCancel(ctx), limit)
	defer cancelBatch()

	members := []*batchMember{leader}
	total := leader.bytes
	o.batches.mu.Lock()
	rest := q.waiting[:0]
	for _, w := range q.waiting {
		fits := len(members) < o.IngestBatch.Max &&
			(o.IngestBatch.MaxBytes <= 0 || total+w.bytes <= o.IngestBatch.MaxBytes) &&
			w.j.DirectS3 == j.DirectS3 && w.j.ObjectList == j.ObjectList &&
			o.preWarmFor(w.j) == o.preWarmFor(j) && !overlapsAny(w, members)
		if fits {
			members = append(members, w)
			total += w.bytes
		} else {
			rest = append(rest, w)
		}
	}
	q.waiting = rest
	o.batches.mu.Unlock()

	// Each member's checks, under the lock as on today's path.
	outcomes := map[*batchMember]batchOutcome{}
	var ready []*batchMember
	for _, m := range members {
		if m.ctx.Err() != nil { // cancelled: today's path ends it
			outcomes[m] = batchOutcome{kind: batchAlone}
			continue
		}
		mlog := logger.With("job_id", m.j.ID)
		var req lease.CommitRequest
		start := time.Now()
		skip, replace, preErr := o.preCommitChecks(bctx, m.j, &req, mlog)
		o.measPrecheck(m.j, time.Since(start))
		switch {
		case preErr == nil && skip:
			outcomes[m] = batchOutcome{kind: batchSkip}
		case preErr != nil || replace || req.BaseExists:
			outcomes[m] = batchOutcome{kind: batchAlone}
		default:
			ready = append(ready, m)
		}
	}

	if len(ready) > 1 {
		o.commitBatch(bctx, ready, outcomes, logger)
		if out := outcomes[leader]; out.kind == batchPublished {
			out.lockWait = 0 // acquireCommitLock recorded the leader's wait
			outcomes[leader] = out
		}
	} else {
		for _, m := range ready {
			outcomes[m] = batchOutcome{kind: batchAlone}
		}
	}
	unlock()
	for _, m := range members {
		if m != leader {
			m.done <- outcomes[m]
		}
	}
	return outcomes[leader]
}

// commitBatch runs one CommitBatch for ready and fills in their outcomes.
// The first of ready speaks for the batch: its settings, and the object list.
func (o *Orchestrator) commitBatch(ctx context.Context, ready []*batchMember,
	outcomes map[*batchMember]batchOutcome, logger *slog.Logger) {
	leaderJob := ready[0].j
	fail := func(err error) {
		logger.Warn("batch commit failed; its packages publish one by one", "repo", leaderJob.Repo,
			"packages", len(ready), "error", err)
		o.batches.mu.Lock()
		for _, m := range ready {
			o.batches.alone[m.j.ID] = true
			outcomes[m] = batchOutcome{kind: batchAlone}
		}
		o.batches.mu.Unlock()
	}
	paths := make([]string, len(ready))
	for i, m := range ready {
		paths[i] = strings.Trim(m.j.Path, "/")
	}
	base, ok := batchBase(paths)
	if !ok {
		fail(fmt.Errorf("no common directory above %v", paths))
		return
	}
	req := lease.BatchCommitRequest{Base: base, DirectS3: leaderJob.DirectS3,
		ObjectList: leaderJob.ObjectList, Stats: &lease.PublishStats{}}
	for i, m := range ready {
		req.Members = append(req.Members, lease.BatchMember{TarPath: m.j.TarPath,
			RelPath: strings.TrimPrefix(strings.TrimPrefix(paths[i], base), "/")})
	}
	var confirmed []string
	if leaderJob.ObjectList && leaderJob.DirectS3 && o.preWarmFor(leaderJob) {
		req.ConfirmedObjects = &confirmed
	}
	backend := o.leaseFor(leaderJob).(batchCommitter)
	token, err := backend.Acquire(ctx, leaderJob.Repo, base)
	if err != nil {
		fail(err)
		return
	}
	req.Token = token
	start := time.Now()
	if err := backend.CommitBatch(ctx, req); err != nil {
		fail(err)
		return
	}
	commit := time.Since(start)
	var bytes int64
	if req.Stats.TarBytes != nil {
		bytes = *req.Stats.TarBytes
	}
	id := fmt.Sprintf("%s-%d", leaderJob.ID, leaderJob.Attempts)
	logger.Info("published a batch", "repo", leaderJob.Repo, "base", base, "batch_id", id,
		"packages", len(ready), "duration", commit.String())
	for _, m := range ready {
		out := batchOutcome{kind: batchPublished, id: id, size: len(ready), bytes: bytes, own: m.bytes,
			commit: commit, lockWait: start.Sub(m.joined), stats: *req.Stats}
		if m.j == leaderJob {
			out.confirmed = confirmed
		}
		outcomes[m] = out
	}
}

// overlapsAny reports whether w's path is, or is inside or above, a member's.
func overlapsAny(w *batchMember, members []*batchMember) bool {
	p := strings.Trim(w.j.Path, "/")
	for _, m := range members {
		q := strings.Trim(m.j.Path, "/")
		if p == q || strings.HasPrefix(p, q+"/") || strings.HasPrefix(q, p+"/") {
			return true
		}
	}
	return false
}

// batchBase is the deepest directory above every path (repository-relative,
// "" for the root), or false when one path is the base itself.
func batchBase(paths []string) (string, bool) {
	common := strings.Split(paths[0], "/")
	for _, p := range paths[1:] {
		parts := strings.Split(p, "/")
		n := 0
		for n < len(common) && n < len(parts) && common[n] == parts[n] {
			n++
		}
		common = common[:n]
	}
	base := strings.Join(common, "/")
	for _, p := range paths {
		if p == base || path.Clean(p) != p {
			return "", false
		}
	}
	return base, true
}

// finishBatched ends a job published in a batch, as today's path ends one
// published alone (an ingest commit has no manifest barrier or root hash).
func (o *Orchestrator) finishBatched(ctx context.Context, j *job.Job, span trace.Span,
	logger *slog.Logger, out batchOutcome) error {
	j.CommittingAt = time.Now()
	if err := o.transition(ctx, j, job.StateCommitting); err != nil {
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}
	o.measLockWait(j, out.lockWait)
	o.measCommit(j, out.commit)
	o.measBatch(j, out)
	o.Obs.Metrics.JobPhaseDuration.WithLabelValues("commit").Observe(out.commit.Seconds())
	if len(out.confirmed) > 0 {
		go func(names []string, mctx context.Context) {
			if o.storePullManifest(mctx, j, names, j.TarSize, logger) {
				o.publishAnnounce(j, j.Repo, j.ID, j.TarSize)
			}
		}(out.confirmed, context.WithoutCancel(ctx))
	}
	j.PublishedAt = time.Now()
	if err := o.transition(ctx, j, job.StatePublished); err != nil {
		span.RecordError(err)
		return err
	}
	o.submitProvenance(ctx, j, nil, nil, logger)
	o.webhookPublished(j)
	o.Obs.Metrics.JobsCompleted.Inc()
	o.Obs.Metrics.PublishedBytes.Add(float64(publishedBytes(j)))
	o.measFinish(j, "published", nil)
	logger.Info("job completed successfully", "batch_id", out.id, "batch_size", out.size)
	return nil
}
