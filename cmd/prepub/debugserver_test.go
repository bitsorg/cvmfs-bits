// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

func newTestObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// TestDebugListener_DisabledByDefault: profiles expose heap contents, so the
// listener must not appear unless an operator asked for it.
func TestDebugListener_DisabledByDefault(t *testing.T) {
	for _, addr := range []string{"", "   "} {
		stop, err := startDebugListener(addr, newTestObs(t))
		if err != nil {
			t.Fatalf("empty addr must not be an error: %v", err)
		}
		stop()
	}
}

// TestDebugListener_ServesGoroutineDump is the whole point: a wedged publisher
// must be introspectable WITHOUT killing it. SIGQUIT kills the process and
// writes to stderr, which under systemd is a rate-limited journald pipe — a
// few thousand lines of traceback then trickle out at seconds per line.
func TestDebugListener_ServesGoroutineDump(t *testing.T) {
	obs := newTestObs(t)
	stop, err := startDebugListener("127.0.0.1:0", obs)
	if err != nil {
		t.Fatalf("startDebugListener: %v", err)
	}
	defer stop()

	// The listener picked an ephemeral port; recover it by starting on a known
	// one instead, since the helper does not return the address.
	stop()
	const addr = "127.0.0.1:16060"
	stop2, err := startDebugListener(addr, obs)
	if err != nil {
		t.Skipf("port %s unavailable: %v", addr, err)
	}
	defer stop2()

	resp, err := (&http.Client{Timeout: 5 * time.Second}).
		Get("http://" + addr + "/debug/pprof/goroutine?debug=2")
	if err != nil {
		t.Fatalf("fetching the goroutine dump: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	// debug=2 renders real stacks; that is what names a blocked stage.
	if !strings.Contains(string(body), "goroutine ") {
		t.Errorf("no goroutine stacks in the dump:\n%.400s", body)
	}
	if !strings.Contains(string(body), "startDebugListener") &&
		!strings.Contains(string(body), "prepub") {
		t.Errorf("dump does not look like this process:\n%.400s", body)
	}
}

// TestDebugListener_RejectsBadAddress: a typo must fail at startup, not leave
// the operator believing they have a debug port when they do not.
func TestDebugListener_RejectsBadAddress(t *testing.T) {
	if _, err := startDebugListener("not-an-address", newTestObs(t)); err == nil {
		t.Error("want an error for an unparseable listen address")
	}
}

func TestIsLoopback(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:6060", true},
		{"[::1]:6060", true},
		{"0.0.0.0:6060", false},
		{"192.168.1.10:6060", false},
	} {
		if got := isLoopback(fakeAddr(tc.addr)); got != tc.want {
			t.Errorf("isLoopback(%s) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

type fakeAddr string

func (f fakeAddr) Network() string { return "tcp" }
func (f fakeAddr) String() string  { return string(f) }

// TestDefaultJobTimeout guards the value that stops one stuck job from wedging
// the queue. Zero means "no timeout", which is how a single unanswered S3 PUT
// took every concurrency slot and left the publisher idle-looking and dead.
func TestDefaultJobTimeout(t *testing.T) {
	if defaultJobTimeout <= 0 {
		t.Fatal("a zero/negative default lets a blocked job hold its slot forever")
	}
	if defaultJobTimeout < 10*time.Minute {
		t.Errorf("default %v is too tight for a large package's compress+upload", defaultJobTimeout)
	}
}
