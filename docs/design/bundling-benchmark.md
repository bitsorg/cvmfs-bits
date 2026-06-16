<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: Apache-2.0
-->

# P-A: object bundling go/no-go

ADR-0001 left open whether a receiver should fetch each missing CAS object with
its own HTTP request, or whether many small objects should be **bundled** into a
single request ("any sort of archiving can come on top"). This note records the
benchmark built to decide it and the resulting recommendation.

## What was measured

`internal/distribute/bench` builds a synthetic delta with a realistic
small-file size mix (80 % 1–16 KiB, 15 % 16–128 KiB, 5 % 128 KiB–1 MiB), serves
it over a loopback HTTP server with a configurable simulated round-trip latency
(RTT), and pulls the whole set two ways:

- **per-object** — the production `ObjectHandler` + `Puller.Pull`, one GET per
  object, `Slots` concurrent (default 8);
- **bundled** — `BundleHandler` + `Puller.PullBundle`, a single POST whose
  response streams every object in self-delimiting frames.

Both install into a fresh local CAS and hash-verify every object, so the two
modes are functionally identical; only the request shape differs. Reproduce with:

```
go run ./cmd/distbench -objects 2000 -slots 8 -rtts 0,1ms,5ms,20ms,50ms -threshold 1.5
```

## Result (800 objects, slots = 8)

| RTT  | per-object | bundled | speedup | requests |
|-----:|-----------:|--------:|--------:|---------:|
| 0    | 41.9 ms    | 51.9 ms | 0.81×   | 800 → 1  |
| 1 ms | 173 ms     | 49 ms   | 3.5×    | 800 → 1  |
| 5 ms | 586 ms     | 57 ms   | 10.3×   | 800 → 1  |
| 20 ms| 2.21 s     | 79 ms   | 27.8×   | 800 → 1  |
| 50 ms| 5.19 s     | 96 ms   | 54.0×   | 800 → 1  |

The per-object cost is `≈ ceil(objects/slots) × RTT` plus transfer; bundling pays
a single RTT. At **zero** RTT bundling is actually *slower* — the per-object path
overlaps transfers across 8 connections while the bundle is one serial stream — so
the win is entirely a function of latency, not of the request count alone.

## Recommendation

**Ship bundling, latency-gated.** For any receiver more than a round-trip away
from Stratum 0 (i.e. the real WAN case for off-site Stratum 1s), bundling clears
a conservative 1.5× bar by a wide margin and collapses thousands of round-trips
into one. For same-datacentre / low-RTT peers, keep per-object fetch (concurrent
transfers are as fast or faster, and individual objects stay independently
cacheable). The `Coordinator` can pick per transaction using the manifest object
count and the measured/last-known RTT to the publisher; both paths are already
implemented and verified, so the choice is a policy switch, not new transport.

Bundling is an optimisation layered *on top of* content addressing: every object
is still hash-verified on arrival, a corrupt or short frame is rejected and
re-fetched per-object, and the bundle endpoint adds no new trust assumptions.
