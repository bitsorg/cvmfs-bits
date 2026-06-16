# Distribution transport — survey & rationale (companion to ADR-0001)

This note holds the background research behind
[ADR-0001](../adr/0001-pull-based-distribution.md): the small-file-over-WAN
literature, the control-plane transport alternatives, and the peer-to-peer
option. The ADR records the *decisions*; this note records the *why* and the
prior art, so the ADR stays decision-focused. Nothing here is normative.

## 1. Many small files over WAN — what the literature says

The bottleneck for many small files over a WAN is not bandwidth, it is
**per-file overhead × round-trips** (connection/TLS setup, an RTT per object,
metadata ops). When files are small relative to the bandwidth-delay product,
throughput collapses far below link capacity. Every solution combines five moves:
**pipeline, parallelize, aggregate, deduplicate, cache.**

| Technique | Fixes | Canonical users |
|---|---|---|
| Pipelining / multiplexing (requests in flight, no per-file wait) | RTT-per-file stalls | GridFTP pipelining (added for small files); HTTP/2 multiplexing; gRPC |
| Concurrency + parallelism (many files at once; one file over N streams) | Filling the BDP | Globus/GridFTP "PCP" (auto-tuned); Aspera FASP (UDP) |
| Aggregation / packing (bundle N objects into one delta-compressed stream) | Per-object setup cost | Git smart protocol (want/have → one packfile); tar-pipe |
| Dedup + content-defined chunking (never send what the peer has) | Redundant bytes | rsync; LBFS (origin of CDC); restic/borg/casync; Dropbox 4 MB blocks + SHA-256 |
| Caching + content addressing (amortize cost across consumers) | Fan-out to many replicas | OCI registries + CDN; eStargz/zstd:chunked/SOCI lazy pull; **CernVM-FS** + squid |

The Globus/GridFTP "PCP" work is the most directly relevant (HEP/grid heritage):
it characterizes and auto-tunes pipelining/concurrency/parallelism and states that
**batching is essential when files are small relative to bandwidth**
([IEEE — How GridFTP PCP Work](https://ieeexplore.ieee.org/document/6495855/);
[Kettimuthu et al., CCGrid'14](https://www.mcs.anl.gov/~kettimut/publications/CCGrid14.pdf);
[Globus Transfer FAQ](https://docs.globus.org/faq/transfer-sharing/)).

### The two regimes (the key framing)

The "right" answer flips with the case:

1. **Fan-out to many replicas / repeated reads → content addressing + caching
   wins.** A CDN/proxy hierarchy amortizes per-object cost across consumers (OCI +
   CDN; lazy-pull formats verify each file independently; CVMFS + squid). Bundling
   *hurts* here — a bundle is a unique, uncacheable blob.
2. **One-shot bulk delta, point-to-point, high latency (S0 → a single cold S1) →
   aggregate + pipeline + parallelize wins.** No cache to amortize; latency × N
   dominates. This is Git (want/have → one packfile) and Dropbox (batch small
   files, dedup blocks, send only unknown ones).

Everyone **deduplicates first** (Git want/have, Dropbox known-block check, rsync,
CDC) — the single biggest saver, before any framing decision.

**Mapping to cvmfs-bits.** The dedup is already done — the publish pipeline knows
the exact new-object set (ADR D3). The open question is only the framing of the
remaining bytes, and the literature says use *different framing per regime*, which
is exactly the pluggable-`Fetcher` + benchmark stance (ADR D5 / P-A): per-object
GET over HTTP/2 for cache-friendly incremental fan-out; an aggregated zstd pack
over parallel connections for the cold ~90 GB delta. Note (ADR consequence): the
cross-institution S0→S1 pre-warm hop is cache-*cold*, so it leans toward regime 2.

Sources: [Git pack-protocol](https://git-scm.com/docs/pack-protocol/2.2.3),
[Git Internals — Transfer Protocols](https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols),
[Dropbox — Streaming File Synchronization](https://dropbox.tech/infrastructure/streaming-file-synchronization),
[Rolling hash / CDC](https://en.wikipedia.org/wiki/Rolling_hash),
[eStargz](https://github.com/containerd/stargz-snapshotter/blob/main/docs/estargz.md),
[SOCI lazy pulling](https://www.buildbuddy.io/blog/image-streaming/).

## 2. Control-plane transport — MQTT and alternatives

The pull data plane needs a control plane for discovery, "your turn" notify, lease
grant, and completion ack. ADR D7 keeps MQTT as implementation #1 behind a
transport-agnostic `ControlPlane` interface.

**Benefits MQTT gives us natively:** outbound-only from S1 (single TLS port);
decoupled fan-out (broadcast to a repo topic, no enumeration); presence + offline
detection via *retained* messages + *Last-Will-and-Testament*; retained state for
late joiners; at-least-once (QoS 1); topic ACLs + mTLS; built for many
intermittently-connected clients over lossy links. Retained + LWT have no built-in
equivalent in NATS/RabbitMQ/Kafka
([HiveMQ — retained messages](https://www.hivemq.com/blog/mqtt-essentials-part-8-retained-messages/)).

| Option | S1 outbound-only | Native presence/offline | Late-join state | Extra infra | Notes |
|---|---|---|---|---|---|
| **MQTT** (status quo) | yes | **yes** (retained+LWT) | yes (retained) | a broker (+PKI/HA/ACLs) | strongest presence/QoS fit; implemented & in testbed |
| **NATS / JetStream** | yes | no (emulate via KV/heartbeat) | JetStream KV | a broker | Go-native, lightweight, built-in request/reply leases; best broker swap |
| **AMQP / RabbitMQ** | yes | emulated | emulated | a broker | rich routing + durable queues; heavier; presence not native |
| **Kafka / Redpanda** | yes | no | log replay | heavy broker | overkill; only if an auditable publish log is independently wanted |
| **SSE + HTTPS POST** | yes | implicit (open stream = presence; drop = LWT) | GET state on connect | **none** (reuses object server) | S0→S1 notify over plain HTTP; S1→S0 ack is a POST; auto-reconnect + `Last-Event-ID`; proxy/firewall-clean ([Ably](https://ably.com/blog/websockets-vs-sse)) |
| **WebSocket** | yes | implicit | app-level | none | bidirectional; no built-in reconnect; some DPI firewalls mishandle |
| **gRPC bidi stream** | yes | implicit | app-level | none (S0 is server) | typed/HTTP-2/mTLS, great Go fit; S0 holds N streams; HTTP/2 proxy-sensitive |
| **etcd / Consul watch+lease** | yes | **yes** (lease TTL) | yes (watch from rev) | consensus cluster | watch=notify, lease=LWT, but single-DC scale, not WAN edge |

**Reading.** Two strong options besides MQTT. **SSE + HTTPS POST** is the
strongest *in this context*, because the data plane is already HTTPS — folding the
control plane into the same server deletes a piece of WAN infrastructure, reuses
the same port/mTLS/proxies, and the open stream is the presence signal; the cost
is reimplementing the small slice of pub/sub used (hold N streams, fan out, serve
current state on connect). **NATS** is the cleanest broker alternative for a Go
codebase. Kafka/Redpanda are overkill; etcd/Consul are the wrong deployment scale;
RabbitMQ buys routing we don't need. → ADR **P-B**: keep MQTT, timeboxed SSE spike.

### 2.1 The `ControlPlane` interface and an SSE backend (ADR D7 / P-B)

The control plane does only six things, so they can live behind one interface
(the message payloads — `AnnounceMessage`/`ReadyMessage`/`PublishedMessage`/
`PresenceMessage` — are unchanged):

```go
type ControlPlane interface {
    // S0 (publisher)
    Announce(repo string, m AnnounceMessage) error           // "prepare": txn N
    PublishCommitted(repo string, m PublishedMessage) error  // "committed": root flipped
    GrantLease(node, txn string, rate Budget) error          // admission
    Receivers() []ReceiverState                               // presence registry
    // S1 (receiver)
    Subscribe(repos []string, onEvent func(ServerMsg)) error  // announce/committed/grant
    Send(m NodeMsg) error                                     // ready / ack / lease-req / heartbeat
    SetPresence(online bool, repos []string)                  // online + auto-offline
}
```

The MQTT backend wraps `internal/broker` directly (`Announce`→`Publish` QoS 1;
`Subscribe`→`Subscribe`; presence→retained publish + LWT via `NewWithLWT`;
reconnection→`SetReconnectHandler`). An **SSE backend** turns the duplex link into
two HTTP halves on S0's *existing* server: `GET /cvmfs-bits/{repo}/events` (the
long-lived SSE stream, one per S1, frames carry a monotonic `id:`) and
`POST /cvmfs-bits/{repo}/{ready,ack,lease,presence}` (S1→S0). What MQTT gives
natively maps as:

| MQTT feature | SSE+POST equivalent | Added work |
|---|---|---|
| Broker fan-out | S0 in-process hub `map[repo][]stream` | small hub (N is O(10–100)) |
| Retained presence + LWT | the open stream **is** presence; write-error/cancel = LWT; heartbeat catches half-open | heartbeat + timeout |
| Retained late-join state | emit current in-flight state as the opening events on connect | replay-on-connect |
| QoS 1 + resume | SSE `id:` + `Last-Event-ID` → replay from a per-repo ring buffer; POSTs use client retry + idempotent (txn-scoped) handlers | ring buffer + idempotent POST |
| Topic ACLs | mTLS client cert identifies the node; per-route authz | reuse object-endpoint mTLS |

Net new code is on the order of a few hundred lines (hub, ring buffer, heartbeat,
idempotent handlers) — all inside the HTTP server already run for objects. The
payoff: with SSE the **entire system (control + data) is plain HTTPS on one S0
port** — one PKI, one set of proxies, **no broker** to deploy/secure/cluster/HA.
The cost: you reimplement the thin slice of pub/sub you use (no broker buffering
for extreme/flaky fleets — irrelevant at this scale). With SSE the control plane
is also **not a separate failure domain** (the hub is S0).

**Pluggability (ADR D7 + D10).** Both backends sit behind `ControlPlane`; the
`.cvmfsbits` discovery doc's `control_plane.type` (`mqtt`|`sse`) selects which one
each S1 instantiates, at **runtime, per repo** — no recompile, no per-S1
reconfig. S0 may run both during a migration, but a given fleet standardizes on
one (the manifest/lease/ack semantics are identical, so it is a config flip in
`.cvmfsbits`, not a code fork). The interface is the load-bearing decision; the
two backends are interchangeable leaves.

## 3. Peer-to-peer (swarm) distribution

Drop the star topology and let replicas share chunks — BitTorrent-style swarming
over content-addressed objects. Well-proven for mass distribution: Twitter's
*Murder* (40 min → ~12 s, ≈75×,
[blog](https://blog.x.com/engineering/en_us/a/2010/murder-fast-datacenter-code-deploys-using-bittorrent));
Uber **Kraken** (Go, BitTorrent-based; 20k 100 MB–1 GB blobs in < 30 s; tracker
orchestrates the graph, peers negotiate, [repo](https://github.com/uber/kraken));
CNCF **Dragonfly** (supernode coordinates 4 MB chunks); **IPFS** (content-
addressed, Kademlia-DHT discovery, [Bitswap](https://specs.ipfs.tech/bitswap-protocol/));
edge variants (EdgePier, Spegel).

**Attractive in principle:** offloads the origin (S0 serves ~once, the swarm
multiplies bandwidth) and scales *with* the fleet; and CVMFS objects are already
immutable hash-named chunks — the manifest is functionally a BitTorrent metainfo /
set of IPFS CIDs.

**Poor fit for the S0 ↔ Stratum-1 hop:** (1) **scale mismatch** — the P2P wins are
at thousands of datacenter nodes; an S1 fleet is O(10), where the squid hierarchy
already gives most of the origin-offload as a serve-once tree; (2) **firewalls
fight the mesh** — S1s are one-per-institution, NAT'd, no inbound; an S1↔S1 mesh
needs NAT traversal/relays, reintroducing the problem D1 removed; (3) **datacenter
assumptions** (low-RTT LAN); (4) **complexity & cold-start** — tracker/DHT, choke,
membership ACLs, and fresh objects are the "low-popularity" content DHT/Bitswap
are slowest on; (5) **WLCG reality** — the grid standardizes on hierarchical HTTP
caching; a P2P overlay is a big shift for site admins.

**Where P2P genuinely fits (below cvmfs-bits):** intra-site / worker-node fan-out
on a LAN (EdgePier/Spegel/Dragonfly) — already handled by CVMFS local squid +
alien cache + tiered proxies.

**Conclusion:** **defer** P2P for S0↔S1, but the design stays **P2P-ready** —
content-addressed objects + manifest-of-hashes + the `Fetcher` interface are the
same substrate a swarm needs, and the presence registry could double as a
peer/tracker directory. Revisit if the fleet grows large *and* freely
interconnectable, or S0 egress becomes the publish bottleneck; the first step then
is a firewall-friendly **regional-relay tree** (S0 → relay-S1 → leaf-S1), not a
full mesh.
