# Architecture

A Raft-replicated, partitioned commit-log broker with consumer groups, built on `hashicorp/raft` and gRPC.

| # | Section | Covers |
|---|---------|--------|
| 1 | [Cluster Overview](#1-cluster-overview--writeread-flow) | Nodes, replication, produce and consume paths |
| 2 | [Broker Internals](#2-internal-broker--raft-fsm-architecture) | gRPC server, Raft, FSM, storage |
| 3 | [Consumer Group Rebalancing](#3-consumer-group-rebalancing-state-machine) | Join, sync, stable, timeout |
| 4 | [On-Disk Layout](#4-on-disk-commit-log-layout) | `0.log` record framing and `0.log.state` sidecar |

---

## 1. Cluster Overview & Write/Read Flow

Writes go through the Raft leader and are replicated to every voter. Consumers coordinate through the group coordinator on the leader, then read partitions from any replica.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'fontFamily': 'ui-sans-serif, system-ui, sans-serif', 'fontSize': '14px', 'lineColor': '#64748b'}}}%%
flowchart TB
    Producer["Producer Client"]

    subgraph Cluster["Broker Cluster (hashicorp/raft)"]
        direction LR
        subgraph B1["Broker 1 (Leader)"]
            B1_P0["Partition 0<br/>0.log"]
            B1_P1["Partition 1<br/>1.log"]
            B1_CG["Consumer Group<br/>Coordinator"]
        end
        subgraph B2["Broker 2 (Voter)"]
            B2_P0["Partition 0<br/>0.log"]
            B2_P1["Partition 1<br/>1.log"]
        end
        subgraph B3["Broker 3 (Voter)"]
            B3_P0["Partition 0<br/>0.log"]
            B3_P1["Partition 1<br/>1.log"]
        end

        B1 <-->|"Raft consensus<br/>& log replication"| B2
        B1 <-->|"Raft consensus<br/>& log replication"| B3
        B2 <-->|"Raft consensus"| B3
    end

    subgraph CG["Consumer Group"]
        C1["Consumer 1"]
        C2["Consumer 2"]
    end

    Producer -->|"1. Produce RPC (gRPC)"| B1
    B1_CG <-->|"2. JoinGroup / SyncGroup / Heartbeat"| C1
    B1_CG <-->|"2. JoinGroup / SyncGroup / Heartbeat"| C2
    B3_P0 -->|"3. Consume Partition 0"| C1
    B3_P1 -->|"3. Consume Partition 1"| C2

    classDef client fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    classDef part fill:#f1f5f9,stroke:#64748b,color:#0f172a
    classDef coord fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#78350f
    classDef consumer fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#14532d

    class Producer client
    class B1_P0,B1_P1,B2_P0,B2_P1,B3_P0,B3_P1 part
    class B1_CG coord
    class C1,C2 consumer

    style Cluster fill:#f8fafc,stroke:#94a3b8,stroke-width:2px,color:#0f172a
    style B1 fill:#eff6ff,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    style B2 fill:#f8fafc,stroke:#94a3b8,color:#0f172a
    style B3 fill:#f8fafc,stroke:#94a3b8,color:#0f172a
    style CG fill:#f0fdf4,stroke:#16a34a,stroke-width:2px,color:#14532d
```

---

## 2. Internal Broker & Raft FSM Architecture

Every mutating request becomes a Raft log entry. Once a quorum commits it, `Apply()` hands it to the FSM, which updates the commit log or in-memory metadata. Reads bypass Raft and go straight to storage.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'fontFamily': 'ui-sans-serif, system-ui, sans-serif', 'fontSize': '14px', 'lineColor': '#64748b'}}}%%
flowchart LR
    Client["Client (gRPC)"] --> Server["broker.Server<br/>gRPC handlers"]

    Server -->|"Produce / CreateTopic / Join"| Raft["hashicorp/raft"]
    Raft -->|"Persist consensus log"| BoltDB[("raft.db<br/>BoltDB")]
    Raft <-->|"TCP transport (raftAddr)"| Peers["Other broker nodes"]

    Raft -->|"Quorum committed<br/>Apply()"| FSM["cluster.FSM"]

    FSM -->|"PRODUCE"| Log["storage.CommitLog<br/>0.log + 0.log.state"]
    FSM -->|"CREATE_TOPIC"| TopicMeta["In-memory<br/>topic metadata"]
    FSM -->|"UPDATE_METADATA"| NodeMeta["In-memory<br/>node metadata"]

    Server -->|"Consume (ReadAt)"| Log
    Server -->|"CommitOffset / FetchOffset"| Offsets[("offsets.json")]

    classDef edge fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    classDef core fill:#ede9fe,stroke:#7c3aed,stroke-width:2px,color:#4c1d95
    classDef state fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#78350f
    classDef disk fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#14532d
    classDef mem fill:#f1f5f9,stroke:#64748b,color:#0f172a

    class Client,Server edge
    class Raft,FSM core
    class Peers mem
    class BoltDB,Log,Offsets disk
    class TopicMeta,NodeMeta state
```

---

## 3. Consumer Group Rebalancing State Machine

A join opens a 3 second window. When it closes, one consumer is elected lead and computes round-robin assignments. The group becomes stable once every member has synced.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'fontFamily': 'ui-sans-serif, system-ui, sans-serif', 'fontSize': '14px', 'primaryColor': '#ede9fe', 'primaryBorderColor': '#7c3aed', 'primaryTextColor': '#1e1b4b', 'lineColor': '#64748b', 'noteBkgColor': '#fef3c7', 'noteTextColor': '#78350f'}}}%%
stateDiagram-v2
    direction LR

    [*] --> RebalanceInProgress: New consumer joins<br/>(JoinGroup RPC)

    state RebalanceInProgress {
        [*] --> JoinPhase: Start 3s timer<br/>(generationID++)
        JoinPhase --> ElectLeadConsumer: Timer expires<br/>(finishJoinPhase)
        ElectLeadConsumer --> SyncPhase: Lead consumer computes<br/>round-robin assignments
        SyncPhase --> [*]: All members send SyncGroup<br/>(finishSyncPhase)
    }

    RebalanceInProgress --> Stable: Broadcast partition assignments

    state Stable {
        [*] --> Consuming: Pull records,<br/>heartbeat every 2s
        Consuming --> Consuming: Heartbeat OK
    }

    Stable --> RebalanceInProgress: Consumer timeout (>10s)<br/>or new member joins
    Stable --> [*]: Group empty
```

| Parameter | Value |
|-----------|-------|
| Join window | 3 s |
| Heartbeat interval | 2 s |
| Session timeout | 10 s |
| Assignment strategy | Round-robin, computed by the lead consumer |

---

## 4. On-Disk Commit Log Layout

Each partition is an append-only file of length-prefixed records. A sidecar file tracks the last applied Raft index and per-producer sequence numbers, which supports idempotent produce.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'fontFamily': 'ui-sans-serif, system-ui, sans-serif', 'fontSize': '14px', 'lineColor': '#64748b'}}}%%
flowchart LR
    subgraph File["Partition log file (e.g. 0.log)"]
        direction LR
        L0["len<br/>8 B, uint64"] --- D0["data<br/>N bytes"] --- L1["len<br/>8 B, uint64"] --- D1["data<br/>M bytes"] --- Dots["..."]
    end

    subgraph State["Sidecar state (0.log.state)"]
        S["last_applied_index: uint64<br/>producer_last_sequence: map[uint64]int64"]
    end

    File -.->|"tracked by"| State

    classDef len fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    classDef data fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#14532d
    classDef meta fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#78350f
    classDef more fill:#f1f5f9,stroke:#94a3b8,color:#475569

    class L0,L1 len
    class D0,D1 data
    class S meta
    class Dots more

    style File fill:#f8fafc,stroke:#94a3b8,stroke-width:2px,color:#0f172a
    style State fill:#fffbeb,stroke:#d97706,stroke-width:2px,color:#78350f
```

| Field | Size | Description |
|-------|------|-------------|
| `len` | 8 bytes | Payload length as `uint64` |
| `data` | N bytes | Record payload |
| `last_applied_index` | `uint64` | Last Raft index applied to this partition |
| `producer_last_sequence` | `map[uint64]int64` | Last sequence seen per producer, used for deduplication |