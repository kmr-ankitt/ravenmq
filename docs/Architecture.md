# RavenMQ — Architecture Document

> A distributed, fault-tolerant, Kafka-inspired message broker written in Go.
> **Stack:** Go · gRPC + Protocol Buffers · HashiCorp Raft + BoltDB · custom append-only commit log

| | |
|---|---|
| **Status** | Core broker implemented · microservice split (§6) is the target architecture |
| **Audience** | Contributors, reviewers, and adopters of `pkg/ravenmq` |
| **Diagrams** | SVG |

---

## Table of Contents

1. [Goals and Non-Goals](#1-goals-and-non-goals)
2. [System at a Glance](#2-system-at-a-glance)
3. [Broker Internals: Write Path and Read Path](#3-broker-internals-write-path-and-read-path)
4. [Consumer Group Coordination](#4-consumer-group-coordination)
5. [Storage Engine](#5-storage-engine)
6. [Microservice Architecture](#6-microservice-architecture)
7. [Failure Modes and Recovery](#7-failure-modes-and-recovery)
8. [Decision Log](#8-decision-log)
9. [Known Limitations and Roadmap](#9-known-limitations-and-roadmap)
10. [Repository Map](#10-repository-map)

---

## 1. Goals and Non-Goals

### Goals

- **Durability and consistency first.** An acknowledged write must survive the loss of a minority of nodes.
- **Simple, auditable correctness.** A single consensus mechanism (Raft) governs every state change, so there is one place to reason about ordering.
- **Idempotent production.** Retries after a timeout or leader change must not create duplicate records.
- **Kafka-style consumption.** Consumer groups with automatic partition assignment, heartbeats, and committed offsets.
- **Small operational surface.** A single static Go binary per node, with no external coordinator such as ZooKeeper.

### Non-Goals (for now)

- Matching Kafka's throughput. Writes are serialised through one Raft leader (see [ADR-002](#adr-002--one-raft-group-for-the-whole-cluster)).
- Log compaction, tiered storage, transactions, or exactly-once semantics across topics.
- Dynamic partition rebalancing across brokers, because every broker currently holds every partition.

---

## 2. System at a Glance

![Cluster overview](img/01-cluster-overview.svg)

A cluster of three brokers uses `hashicorp/raft` to elect **one Leader and two Voters**. Every broker stores every partition, so any node can serve a read after a leader failure.

**Request routing**

| Operation | Handled by | Notes |
|---|---|---|
| `Produce`, `CreateTopic`, `Join` | Raft **leader** only | Followers reply `NOT_LEADER` + `LeaderAddr`; the client redirects and retries. |
| `JoinGroup`, `SyncGroup`, `Heartbeat` | Group coordinator (on the leader) | State is in-memory (see §4). |
| `Consume` | Any broker | Direct file read, with no Raft round-trip. |
| `CommitOffset`, `FetchOffset` | Broker-local `offsets.json` | Keyed by `<group>-<topic>-<partition>`. |

**Why this shape.** Funnelling all mutations through a single replicated log gives a total order for free. Every broker applies the same committed entries in the same order, so the logs converge byte for byte. The price is a single write leader (§9).

---

## 3. Broker Internals: Write Path and Read Path

![Broker and Raft FSM](img/02-broker-raft-fsm.svg)

### 3.1 Write path (`Produce`)

1. The client sends a `Produce` RPC to `broker.Server`. A non-leader node rejects it with `NOT_LEADER` + `LeaderAddr`.
2. The leader serialises the command and **proposes** it to `hashicorp/raft`.
3. Raft appends the entry to its own log in `raft.db` (BoltDB), then replicates it to peers over the TCP transport (`raftAddr`).
4. Once a **quorum (2 of 3)** has the entry, it is committed and `cluster.FSM.Apply()` runs **on every broker**.
5. The FSM dispatches on the command type:
   - `PRODUCE` → `storage.CommitLog.Append(...)`
   - `CREATE_TOPIC` → in-memory topic metadata
   - `UPDATE_METADATA` → in-memory node metadata
6. The leader acknowledges the client only after its own `Apply()` completes.

### 3.2 Read path (`Consume`)

`Consume` bypasses Raft entirely. The handler calls `CommitLog.Read(offset)`, which uses lock-free `file.ReadAt` and returns `(payload, nextByteOffset, error)`. Reads therefore scale with the number of replicas and never contend with writers.

### 3.3 Why a Raft log *and* a commit log?

There are two logs, with different jobs:

| | Raft log (`raft.db`) | Commit log (`<partition>.log`) |
|---|---|---|
| **Purpose** | Consensus: ordering and replication of *commands* | Application data: the records consumers read |
| **Format** | BoltDB (managed by `raft-boltdb`) | Custom `[len][payload]` binary file |
| **Consumers read it?** | No | Yes |
| **Truncation** | Raft snapshots and compaction | Not yet (see §9) |

The cost is that every record is written **twice** on each node, once into the Raft log and once into the commit log. We accept this because it keeps the consensus library untouched and the consumer-facing format simple, and we can optimise later without changing the protocol.

---

## 4. Consumer Group Coordination

![Consumer group rebalancing](img/03-consumer-group-rebalancing.svg)

Group membership is managed by `broker/cg_rebalancer.go` and held **in memory on the Raft leader**.

### 4.1 State machine

| State | Meaning | Exit condition |
|---|---|---|
| `RebalanceInProgress` → **JoinPhase** | A 3 s window opens and `generationID` is incremented. Members call `JoinGroup`. | Timer expires (`finishJoinPhase`). |
| → **ElectLeadConsumer** | The first consumer becomes the *group leader*. | Immediately. |
| → **SyncPhase** | The group leader computes **round-robin** assignments. Members call `SyncGroup`. | All members synced (`finishSyncPhase`). |
| `Stable` | Consumers pull records and heartbeat every 2 s. | Member timeout (>10 s) or a new member joins. |
| *(end)* | Group is empty. | n/a |

### 4.2 Timings

| Parameter | Value | Role |
|---|---|---|
| Heartbeat interval | **2 s** | Consumer liveness signal |
| Session timeout | **10 s** | Declares a member dead and triggers a rebalance |
| Rebalance join window | **3 s** | How long the coordinator waits for members to join before assigning |

The 5:1 ratio between session timeout and heartbeat interval tolerates a few lost heartbeats before a healthy consumer is evicted. This follows the usual Kafka rule of thumb.

### 4.3 Design notes

- **Client-side assignment.** The coordinator only elects a group leader. The *consumer* computes the assignment. This keeps the broker free of assignment-strategy code and lets strategies evolve in the client library.
- **Stop-the-world rebalance.** Every membership change pauses the whole group for the join window plus the sync phase. This is simple and correct, but it is the main source of consumption latency during deploys (see §9).
- **Offsets** are committed to `offsets.json` on the broker as `<group>-<topic>-<partition> → byte_offset`. Because offsets are byte positions, a consumer resumes exactly where it stopped, with no lookup.

---

## 5. Storage Engine

![Commit log layout](img/04-commit-log-layout.svg)

### 5.1 File layout

Each `<topic>/<partition>` maps to two files:

```text
<topic>/
├── 0.log          # append-only records
└── 0.log.state    # JSON sidecar (idempotency + replay safety)
```

Records are laid out back to back as `[8-byte big-endian uint64 length][N-byte payload]`.

### 5.2 Offsets are byte positions

| Property | Consequence |
|---|---|
| Offset = byte position of a record's length header | `Read(offset)` needs **no index**: it reads 8 bytes, decodes N, then reads N bytes. |
| `nextByteOffset = offset + 8 + N` | The client can resume or poll without extra bookkeeping. |
| Offsets are **not dense** (they do not count 0, 1, 2…) | Consumer lag is measured in bytes, not messages. Offsets cannot be guessed. |

### 5.3 The `.state` sidecar

| Field | Purpose |
|---|---|
| `last_applied_index` | The Raft index of the last entry applied. If Raft replays entries after a crash, `Apply()` **skips anything at or below this index**, so records are never appended twice. |
| `producer_last_sequence` | `map[producer_id] → sequence`. A repeated `(producer_id, sequence_number)` is dropped, which makes `Produce` **idempotent** across client retries and leader changes. |

### 5.4 Design notes

- **Lock-free reads.** `ReadAt` is a positioned read that does not move a shared file cursor, so concurrent readers need no mutex. Only the single FSM goroutine appends, so there is **one writer per file by construction**.
- **Durability depends on fsync policy.** The Raft log gives crash safety for commands, but the commit log's guarantees depend on when it is flushed. Make the policy explicit (per-append, batched, or OS-managed) and document it.
- **Crash window between data and state.** The log append and the `.state` update are two separate writes. If the process dies between them, the replay logic must be able to detect and heal the gap. This is the first area to cover with crash-injection tests.

---

## 6. Microservice Architecture

![RavenMQ microservice architecture](img/05-microservice-architecture.svg)

The monolithic broker (§2–§5) is being decomposed into **independently deployable services** behind a client SDK, `pkg/ravenmq`. The split follows the **control plane / data plane** boundary.

### 6.1 Services

| # | Service | Plane | Responsibility |
|---|---|---|---|
| 1 | **Coordinator** | Control | Raft-backed cluster metadata: leader discovery, topics, partitions, node registry. Source of truth for topology. |
| 2 | **Consumer Group** | Control | `JoinGroup`, `SyncGroup`, heartbeats, committed offsets. |
| 3 | **Broker / Storage** | Data | gRPC `Produce` / `Consume` and the on-disk commit log. The hot path. |
| 4 | **Admin / HTTP Gateway** *(optional)* | Ops | REST API, Prometheus metrics, health checks. Read-only view over 1 and 2. |

### 6.2 Interaction flows

| Flow | Direction | Purpose |
|---|---|---|
| ① Discover leader / topology | App → Coordinator | The SDK learns where to send traffic and caches the answer. |
| ② Join group and heartbeat | App → Consumer Group | Membership and partition assignment. |
| ③ Produce and consume stream | App → Broker | Direct data traffic. The Coordinator is **not** in the data path. |
| Register and sync metadata | Broker ↔ Coordinator | Brokers announce themselves and receive topic and partition changes. |
| Fetch topic partitions | Consumer Group ↔ Coordinator | Assignments need current partition counts. |
| Inspect cluster and lag | Gateway → Coordinator, Consumer Group | Dashboards and operations. Read-only. |

### 6.3 Why split this way

- **Independent scaling.** The data plane is I/O-bound and needs disk and bandwidth. The control plane is chatty but tiny. They should not share a scaling unit.
- **Blast-radius isolation.** A rebalance storm in the Consumer Group service cannot starve `Produce` traffic on the Broker.
- **Different consistency needs.** Topology must be strongly consistent (Raft). Group state is soft and can be rebuilt (members re-join).
- **Optional observability.** The Gateway is deliberately off the critical path. Losing it costs visibility, never availability.

### 6.4 Trade-offs of the split

| Benefit | Cost |
|---|---|
| Independent deploy, scale, and failure domains | More processes to run, secure, and upgrade. Needs service discovery and mTLS. |
| Clear ownership boundaries and smaller codebases | Cross-service API versioning. Protobuf contracts must stay backward compatible. |
| Data path stays minimal | The SDK carries more logic (topology caching, retries, redirects), so SDK bugs affect every user. |
| Gateway can evolve freely | A stale SDK topology cache can briefly route to the wrong node. It needs invalidation on `NOT_LEADER`. |

**Recommendation.** Ship the split as *modes of a single binary* first (`--role=coordinator|group|broker|gateway|all`). This preserves the one-binary operational story for small deployments, and large deployments can run roles separately.

---

## 7. Failure Modes and Recovery

| Failure | Behaviour | Recovery |
|---|---|---|
| **Follower crashes** | Quorum (2 of 3) is still available. Writes continue. | On restart it catches up through Raft replication or a snapshot. |
| **Leader crashes** | Writes stall during election, then resume on the new leader. | Clients get `NOT_LEADER`, re-discover the leader, and retry. Idempotency prevents duplicates. |
| **Two nodes lost** | No quorum, so writes are refused. Reads from the survivor may work but can be stale. | Restore a node to regain quorum. There is no split-brain, because Raft will not elect without a majority. |
| **Crash mid-`Apply`** | Raft replays the entry. | `last_applied_index` makes the replay a no-op for entries already written. |
| **Client retry after timeout** | The same `(producer_id, sequence)` may arrive twice. | Dropped by `producer_last_sequence`. |
| **Consumer dies** | No heartbeat for 10 s. | Group moves to `RebalanceInProgress` and its partitions are reassigned. |
| **Leader fails over (groups)** | In-memory group state is lost. | Members re-join under a new generation. Committed offsets are not affected. See the §9 caveat about `offsets.json` replication. |
| **Network partition** | The minority side cannot commit. | Heals automatically. The old leader steps down. |

---

## 8. Decision Log

### ADR-001 — Raft via `hashicorp/raft` instead of a bespoke protocol

- **Context.** We need strongly consistent replication with leader election.
- **Decision.** Use `hashicorp/raft` with `raft-boltdb` for log and stable storage.
- **Alternatives.** Hand-rolled Raft; Paxos; an external coordinator (etcd or ZooKeeper).
- **Why.** The library is production-proven (Consul, Nomad) and removes the most error-prone code. It also removes an external dependency.
- **Trade-off.** We inherit its operational model (snapshots, membership changes) and its performance envelope. BoltDB is not the fastest log store.

### ADR-002 — One Raft group for the whole cluster

- **Decision.** All topics and partitions share a single Raft group and a single leader.
- **Alternatives.** One Raft group per partition (as in CockroachDB or TiKV). Per-partition leaders with ISR (as in Kafka).
- **Why.** It is the simplest correct design. There is one total order, one election, and one membership view.
- **Trade-off.** **Write throughput is bounded by one leader**, and every broker stores every partition. This is a scalability ceiling, not a correctness flaw. Sharding partitions across multiple Raft groups is the main upgrade path.

### ADR-003 — Quorum commit before acknowledging

- **Decision.** Acknowledge `Produce` only after the entry is committed (2 of 3).
- **Why.** An acknowledged record survives any single node loss, which matches the durability goal.
- **Trade-off.** Latency includes at least one replication round-trip. There is no `acks=0/1` fast mode.

### ADR-004 — Reads bypass Raft

- **Decision.** `Consume` reads directly from local disk on any replica.
- **Why.** It maximises read throughput and keeps readers off the consensus path.
- **Trade-off.** A lagging follower can serve **slightly stale** data, and consumers may see a short delay on new records. This is acceptable for a log, where consumers already tolerate lag. If linearizable reads are ever required, add a read-index or leader-only read mode.

### ADR-005 — Byte offsets, no index

- **Decision.** Offset = byte position in the `.log` file.
- **Why.** `Read` is O(1) with no index to build, persist, or corrupt, and the format is trivially simple.
- **Trade-off.** Offsets are non-dense and opaque. There is no efficient seek-by-timestamp or message number, no segment rolling, and retention or compaction cannot simply delete a prefix, because that would shift every offset. Moving to segmented files with a sparse index (Kafka's approach) fixes all of these, at the cost of complexity.

### ADR-006 — Sidecar state file for idempotency

- **Decision.** Persist `last_applied_index` and `producer_last_sequence` in `<partition>.log.state`, next to the data.
- **Why.** Co-locating state with the log keeps the idempotency check local and cheap, with no extra lookup service.
- **Trade-off.** Data and state are updated by separate writes, so there is a crash window that needs careful ordering and tests (§5.4). The state file is rewritten on update, which is fine at low rates but should become incremental under load.

### ADR-007 — In-memory consumer-group state on the leader

- **Decision.** Group membership and generations live in memory on the Raft leader.
- **Why.** Group state is ephemeral by nature, and members can reconstruct it by re-joining. Skipping Raft for heartbeats avoids pushing a high-frequency, low-value stream through consensus.
- **Trade-off.** Leader failover forces every group to rebalance. We accept a brief pause in exchange for a simpler, faster coordinator.

### ADR-008 — Client-side partition assignment

- **Decision.** The elected *group leader consumer* computes assignments (round-robin).
- **Why.** Keeps assignment logic out of the broker, and strategies can change without a broker release.
- **Trade-off.** Correctness now depends on client implementations. A buggy client can produce overlapping or missing assignments, so the server should validate the result.

### ADR-009 — gRPC and Protocol Buffers

- **Decision.** All client and inter-service APIs are defined in `api/kafka.proto`.
- **Why.** A strongly typed schema, generated clients, HTTP/2 streaming, and straightforward backward-compatible evolution.
- **Trade-off.** Less friendly to ad-hoc tooling than plain HTTP and JSON. The optional Gateway (§6) exists partly to fill this gap.

### ADR-010 — Control-plane / data-plane service split

- **Decision.** Decompose into Coordinator, Consumer Group, Broker, and an optional Gateway (§6).
- **Why.** Independent scaling, isolation, and clearer ownership.
- **Trade-off.** More operational surface, and the SDK carries topology logic. See §6.4. Mitigate by shipping as roles of one binary.

---

## 9. Known Limitations and Roadmap

Ordered roughly by impact.

| # | Limitation | Impact | Suggested direction |
|---|---|---|---|
| 1 | **Single write leader** (ADR-002) | Throughput ceiling. All brokers hold all data. | Shard partitions across multiple Raft groups. |
| 2 | **No retention, compaction, or segments** (ADR-005) | Logs grow without bound. | Segmented log plus sparse index, time and size retention. |
| 3 | **`offsets.json` is broker-local** | If it is not replicated through Raft, committed offsets can be lost or diverge after failover. This needs verification. | Commit offsets through Raft, or store them in an internal compacted topic. |
| 4 | **Double write per record** (Raft log + commit log) | Write amplification. | Batch appends, tune Raft, or share the storage layer. |
| 5 | **Stop-the-world rebalances** | Consumption pauses during joins and leaves. | Incremental cooperative rebalancing, plus static membership for rolling deploys. |
| 6 | **Crash window between data and `.state`** | Possible gap after a crash at the wrong instant. | Define write ordering, add checksums, and add crash-injection tests. |
| 7 | **Stale reads on followers** (ADR-004) | A consumer may briefly lag the leader. | Optional leader-read or read-index mode. |
| 8 | **Fixed 3-node bootstrap** | Limited fault tolerance (one node). | Dynamic membership and 5-node clusters. |
| 9 | **No auth, TLS, or quotas** | Not safe on untrusted networks. | mTLS between services, per-client auth and quotas. |
| 10 | **Observability** | Hard to operate without metrics. | Prometheus metrics (lag, ISR/quorum, rebalances) through the Gateway. |

**Suggested test strategy.** Treat the distributed behaviour as the product. Prioritise Jepsen-style fault injection (leader kill, partition, clock skew), crash-replay tests for `last_applied_index`, and property tests for the idempotency path.

---

## 10. Repository Map

The repository source layout is documented in the project directories and package files.


### Diagram index

| § | Diagram | Files |
|---|---|---|
| 2 | Cluster overview | `img/01-cluster-overview.svg` |
| 3 | Broker internals and Raft FSM | `img/02-broker-raft-fsm.svg` |
| 4 | Consumer group rebalancing | `img/03-consumer-group-rebalancing.svg` |
| 5 | Commit log layout | `img/04-commit-log-layout.svg` |
| 6 | Microservice architecture | `img/05-microservice-architecture.svg` |
