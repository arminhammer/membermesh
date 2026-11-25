# `membermesh` Pain Points & Mitigation Register

This living document records transport, distributed-systems, and Rust engineering risks encountered while implementing a HashiCorp `memberlist`-inspired membership protocol over `iroh` (`1.5.x`) and `iroh-gossip` (`0.103.x`), along with the implemented mitigations.

## 1. `iroh-gossip` & `iroh` Transport Pain Points

| Pain Point | Root Cause in `iroh` / `iroh-gossip` | Mitigation in `membermesh` |
|---|---|---|
| **Partial view ≠ cluster view** | `iroh-gossip` implements HyParView (default active view = 5, passive view = 30). `Event::NeighborUp`/`NeighborDown` only reports direct overlay edges, never remote cluster members. | Maintain full cluster state in `membermesh-core::MembershipTable`. Treat `NeighborUp` strictly as an opportunistic sync/discovery hint; never declare cluster-wide death on `NeighborDown`. |
| **Unsigned gossip / no original author** | `iroh_gossip::Message::delivered_from` is the immediate PlumTree relay peer, not the original author, and payloads are unsigned. | Wrap every peer state record in `SignedClaim`, signed with the subject peer's `iroh::SecretKey` (Ed25519) and verified against `PeerId` (`EndpointId`) before merging. |
| **90-second content-hash deduplication** | `iroh-gossip` silently drops any broadcast whose bytes hash identically to a message seen within `BroadcastConfig::message_id_retention` (90s default). | Frame every gossip broadcast in an outer `GossipEnvelope` carrying a monotonic `nonce: u64` + `sender`, ensuring intentional rebroadcasts always have distinct content hashes. |
| **4096-byte default message limit** | `DEFAULT_MAX_MESSAGE_SIZE` is `4096` (~4056 bytes content). Mismatched limits across nodes cause silent drops without sender feedback. | Enforce `MAX_METADATA_BYTES` (512B), validate broadcasts against `GossipSender::max_content_size()`, and keep full Push/Pull state syncs exclusively on `/membermesh/1` QUIC streams. |
| **`Event::Lagged` silent drops** | `GossipReceiver` buffers 2048 events. If the consumer lags, older events are dropped, `Event::Lagged` is emitted, and `neighbors()` becomes stale. | On `Event::Lagged`, immediately schedule an out-of-band Push/Pull anti-entropy sync with a random alive peer. |
| **Broadcast-before-join blackhole** | `subscribe_with_opts` returns before neighbors connect; broadcasts sent before `NeighborUp` are silently dropped. | On `join()`, run direct QUIC Push/Pull sync with bootstrap peer(s) first (to learn `lamport_clock` + initial roster), await `joined()` (bounded timeout for single-node founder), then broadcast `Alive`. |
| **QUIC connection ownership & idle closes** | `iroh-gossip` closes connections 10s after streams go idle and forbids reusing its connections for non-gossip traffic. | Multiplex a dedicated `ProtocolHandler` (`b"/membermesh/1"`) on `iroh::protocol::Router` for SWIM probes and Push/Pull sync. |
| **NAT hole-punching & relay RTT spikes** | Connections often start via an `iroh` Relay (100–400ms RTT) before migrating to direct UDP; RTT jitter can trigger false SWIM failure detections. | Implement HashiCorp Lifeguard (Local Health Awareness multiplier + dynamic suspicion timeout) and provide `Lan`, `Wan`, and `Relay` timing profiles. |
| **Non-neighbor endpoint addressing** | Learning a remote `EndpointId` via gossip does not supply its direct addresses or relay URL if DNS/Pkarr discovery is disabled. | Carry bounded `AddrHint` (`EndpointAddr` hint) inside signed `MemberClaim`s and feed discovered hints into `iroh`'s `MemoryLookup`. |
| **Send-queue backpressure** | `iroh-gossip` shares a 1 MiB per-peer send queue across topics; overflow drops the peer connection and stalls `broadcast()` up to 100ms. | Rate-limit outbound gossip rebroadcasts with a HashiCorp-style `TransmitLimitedQueue` (`retransmit_mult * ceil(log10(N + 1))`) and isolate bulk sync onto `/membermesh/1`. |

## 2. Distributed Systems & Leader Election Pain Points

| Pain Point | Impact | Mitigation |
|---|---|---|
| **AP Split-Brain under partitions** | Gossip membership is eventually consistent (AP), not linearizable (CP). Both halves of a partition mark unreachable peers `Dead` and elect independent leaders. Additionally, Stateright model checking caught that excluding `Suspect` peers from `leader()` immediately causes younger nodes to steal leadership on a single packet drop before suspicion expires. | Keep `Suspect` peers eligible in `members_ordered()` / `leader()` until confirmed `Dead` or `Left` (matching HashiCorp `memberlist::Members()`). `leader_with_quorum(min_alive)` provides **post-detection minority suppression** (at most one partition component holds quorum once failure detection settles); transient overlap during pre-detection SWIM probe windows is inherent to AP gossip, so strict mutual exclusion requires Raft/leases above `membermesh`. |
| **Wall-clock skew** | Ordering peers or expiring leases by `SystemTime` causes leader flapping and clock-skew hijacking. | Order deterministically by `(join_seq, PeerId)` using a Lamport join sequence (`max_seen + 1`); use monotonic clocks (`SimInstant` / `tokio::time::Instant`) for local observed durations and timeouts. |
| **Simultaneous cold-start merge** | Two isolated founders both start at `join_seq = 1`. On merge, both must yield to the same leader. | Total order `(join_seq, PeerId)` breaks ties identically on every peer immediately on anti-entropy sync and emits `LeaderChanged`. |
| **Seniority loss on transient blip** | If a brief GC/CPU pause causes `Suspect`, resetting `join_seq` would churn leadership unnecessarily. | `join_seq` is immutable across `Incarnation` refutations within a `SessionId`. Process restarts (new `SessionId`) or explicit `leave()` rejoin at the back of the line. |
| **Tombstone bloat vs. zombie resurrection** | Reaping `Dead`/`Left` too early allows stale syncs to resurrect dead peers; keeping them forever leaks memory. | Bound tombstone retention (`dead_reap_timeout`, `left_reap_timeout` >> `anti_entropy_interval`) and reject stale `(SessionId, join_seq)` claims. |
| **Suspect storm under local host stall** | A locally CPU-starved node misses ACKs and falsely suspects healthy peers. | Lifeguard Local Health Awareness (`lha: 0..=8`) scales local probe intervals and suspicion deadlines during host overload. |

## 3. Rust Engineering & Testing Pain Points

| Pain Point | Impact | Mitigation |
|---|---|---|
| **Flaky async multi-node QUIC tests** | Real networking tests can flake under heavy concurrency or hang CI indefinitely. | Put protocol/fault-injection BDD coverage on the deterministic virtual-clock `SimCluster` + `iroh_gossip::proto::State` co-simulator; run real QUIC BDD tests offline (`presets::Minimal`, `RelayMode::Disabled`, `MemoryLookup`) with bounded concurrency and hang watchdogs. |
| **`n0-error` boundary leakage** | `iroh` & `iroh-gossip` use `n0-error`, whereas library consumers expect typed `thiserror` errors (`#[non_exhaustive]`). | Convert `n0-error` into typed `MemberMeshError` variants inside `membermesh`; keep `membermesh-core` free of `n0-error`. |
| **Pre-1.0 `iroh-gossip` API churn** | `iroh` is `1.5.x` while `iroh-gossip` is `0.103.x`. | Isolate `iroh-gossip` behind `membermesh` domain types and keep `experimental-proto-internals` strictly in dev-only `membermesh-testkit`. |
