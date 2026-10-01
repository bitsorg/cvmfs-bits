// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"cvmfs.io/prepub/internal/job"
)

// Collector exports, at scrape time, how many jobs sit in each spool state
// and the publisher host's load, memory and spool disk. Read from disk and
// /proc on every scrape, so the numbers survive restarts and need no agent.
type Collector struct {
	s *Spool

	jobs, waiting, fsSize, fsAvail  *prometheus.Desc
	load1, cpus, memTotal, memAvail *prometheus.Desc
}

// NewCollector returns a Collector for this spool.
func NewCollector(s *Spool) *Collector {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(name, help, labels, nil)
	}
	return &Collector{
		s:        s,
		jobs:     d("cvmfs_prepub_spool_jobs", "Jobs in each spool state.", "state"),
		waiting:  d("cvmfs_prepub_spool_jobs_waiting_retry", "Incoming jobs waiting to retry a failed attempt."),
		fsSize:   d("cvmfs_prepub_spool_fs_size_bytes", "Size of the spool filesystem."),
		fsAvail:  d("cvmfs_prepub_spool_fs_avail_bytes", "Free space on the spool filesystem."),
		load1:    d("cvmfs_prepub_host_load1", "Publisher host 1-minute load average."),
		cpus:     d("cvmfs_prepub_host_cpus", "Publisher host CPU count."),
		memTotal: d("cvmfs_prepub_host_memory_total_bytes", "Publisher host memory."),
		memAvail: d("cvmfs_prepub_host_memory_available_bytes", "Publisher host available memory."),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.jobs, c.waiting, c.fsSize, c.fsAvail,
		c.load1, c.cpus, c.memTotal, c.memAvail} {
		ch <- d
	}
}

// spoolStates are the state directories, in FSM order.
var spoolStates = []job.State{
	job.StateIncoming, job.StateStaging, job.StateUploading, job.StateDistributing,
	job.StateLeased, job.StateCommitting, job.StateAccumulated,
	job.StatePublished, job.StateFailed, job.StateAborted,
}

// Collect implements prometheus.Collector. A source that cannot be read is
// left out rather than reported as zero.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	g := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	for _, st := range spoolStates {
		if ids, err := jobIDs(c.s.stateDir(st)); err == nil {
			g(c.jobs, float64(len(ids)), string(st))
			if st == job.StateIncoming {
				g(c.waiting, float64(c.waitingRetry(ids)))
			}
		}
	}
	var fs syscall.Statfs_t
	if syscall.Statfs(c.s.Root, &fs) == nil {
		g(c.fsSize, float64(fs.Blocks)*float64(fs.Bsize))
		g(c.fsAvail, float64(fs.Bavail)*float64(fs.Bsize))
	}
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		if f := strings.Fields(string(b)); len(f) > 0 {
			if v, err := strconv.ParseFloat(f[0], 64); err == nil {
				g(c.load1, v)
			}
		}
	}
	g(c.cpus, float64(runtime.NumCPU()))
	if total, avail, ok := meminfo(); ok {
		g(c.memTotal, total)
		g(c.memAvail, avail)
	}
}

// jobIDs lists the job directories in a state directory: every entry but the
// journal and other dotted files (job IDs are UUIDs, without dots).
func jobIDs(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	ids := names[:0]
	for _, n := range names {
		if !strings.Contains(n, ".") {
			ids = append(ids, n)
		}
	}
	return ids, nil
}

// waitingRetry counts the incoming jobs that have a next attempt scheduled.
func (c *Collector) waitingRetry(ids []string) int {
	n := 0
	for _, id := range ids {
		b, err := os.ReadFile(filepath.Join(c.s.stateDir(job.StateIncoming), id, "manifest.json"))
		if err != nil {
			continue
		}
		var m struct {
			NextAttemptAt *json.RawMessage `json:"next_attempt_at"`
		}
		if json.Unmarshal(b, &m) == nil && m.NextAttemptAt != nil && string(*m.NextAttemptAt) != "null" {
			n++
		}
	}
	return n
}

// meminfo reads MemTotal and MemAvailable from /proc/meminfo, in bytes.
func meminfo() (total, avail float64, ok bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	found := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() && found < 2 {
		k, rest, _ := strings.Cut(sc.Text(), ":")
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		switch k {
		case "MemTotal":
			total, found = v*1024, found+1
		case "MemAvailable":
			avail, found = v*1024, found+1
		}
	}
	return total, avail, found == 2
}
