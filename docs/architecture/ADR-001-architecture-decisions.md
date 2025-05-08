# ADR 1 - Core Architecture Decisions

*Status: <span style="color:green">Accepted</span> – May 8 2025*

## Context
Lumen aims to offer a lean Elasticsearch‑like experience focused on v2‑era functionality while leveraging modern Rust performance.  Key architectural questions concerned storage layout, consensus mechanism, transport, schema handling, and licensing.

## Decision
| Aspect | Choice |
| ------ | ------ |
| **Storage** | Tantivy segments stored on local NVMe/SSD per shard; no external object store in v1. |
| **WAL** | Custom append‑only binary log per shard (initially memory‑mapped file; evaluate `sled` API later). |
| **Cluster Metadata** | Embedded Raft via `openraft` (MIT) running in dedicated *coord* service. |
| **Replication** | Primary + 1 strategy, pull‑based segment replication with CRC validation. |
| **Internal RPC** | gRPC (tonic) with protobuf definitions in `proto/`. |
| **External API** | REST/JSON modeled after Elasticsearch 2 endpoints; OpenAPI spec auto‑generated. |
| **Query Parsing** | Fork Tantivy `query_grammar` extended to support `AND/OR/NOT`, proximity (`NEAR/k`). |
| **Schema** | User‑provided JSON mapping: `text`, `keyword`, `i64`, `f64`, `date`.  No dynamic fields in v1. |
| **Licensing** | Apache‑2.0 for all crates and associated tooling. |

## Consequences
* Faster startup: single binary, no JVM.
* Cluster size target ≤ 50 nodes; metadata fits in Raft memory.
* WAL and segment format firmly decoupled, simplifying future tiered storage.
* By excluding ingest pipelines and scripting, we reduce attack surface and maintenance burden.

## Alternatives Considered
### 1. `etcd`‑backed metadata
`etcd` is the de‑facto key‑value store for Kubernetes and many distributed systems, so we evaluated using a **separate `etcd` cluster** to hold Lumen’s index → shard → node mapping. Ultimately we chose an **embedded Raft** implementation for v1.

| Factor | Embedded Raft (chosen) | External `etcd` cluster (rejected) |
|--------|-----------------------|------------------------------------|
| **Operational footprint** | No additional services; coord nodes run in‑process Raft. | Requires a separate 3–5‑node `etcd` cluster, its own TLS, monitoring, backup. |
| **Network hops per state change** | 1 RTT ⇒ client → coord Raft group. | 2 RTT ⇒ client → coord service → `etcd`, then back. Adds ~0.5–1 ms per metadata update on LAN, more on WAN. |
| **Consistency guarantees** | Strong consistency (Raft) but state is *local‑to‑process*; no extra (un)marshalling layer. | Also strong, but keys/values must be marshalled over gRPC to/from `etcd`. |
| **Failure modes** | Coord crash ⇒ follower already has same state; quick fail‑over. | Split‑brain risk if `etcd` quorum lost while coord nodes keep running; needs fencing. |
| **Upgrade story** | One binary upgrade path. | Two moving parts: upgrade `etcd` first, then Lumen—double maintenance window. |
| **Ecosystem maturity** | `openraft` / `async‑raft` pass Jepsen and fuzz tests; algorithm identical to `etcd`. | `etcd` is extremely battle‑tested and familiar to many operators. |

**Bottom line :** An external `etcd` cluster would work, but starting with **embedded Raft** keeps Lumen a *single‑binary* deployment and avoids an extra network hop on every cluster‑state change.

---

### 2. Full Raft for the write path
A second option was to make *every* document write flow through a Raft log (similar to CockroachDB). Lumen instead uses **primary/replica replication**—matching Elasticsearch’s model—where only metadata is Raft‑backed.

| Impact area | Full Raft on data path (rejected) | Primary‑based replication (chosen) |
|-------------|-----------------------------------|------------------------------------|
| **Write latency (ack @ quorum)** | Extra 0.5–2 ms per doc on SSD; 3–8 ms on cloud block storage. | Local fsync only; replicas follow behind. |
| **Throughput under burst** | Leader must replicate to majority before commit; bottleneck ~10–20 k ops/s per shard. | Primaries can ingest CPU‑bound (~100 k ops/s) until network saturates. |
| **Failure semantics** | Linearizable writes automatically. | "At least once" semantics; duplicates possible after primary re‑election (same as ES). |
| **Complexity in Tantivy** | Need snapshot/restore hooks to apply Raft entries exactly once. | Tantivy already supports segment replication; no Raft glue required. |
| **Operational familiarity** | Similar to CockroachDB / etcd. | Matches Elasticsearch operator mental model (primary/replica). |

**Bottom line :** Search workloads value ingest throughput and query latency over strict linearizability. Primary‑based replication keeps the hot path short and lets Tantivy run unimpeded. If users need stronger guarantees later we can introduce per‑shard Raft groups or vector‑clock conflict checks under a feature flag.

---

### 3. HTTP for internal RPC
Rejected: gRPC offers framing, streaming, and code‑generated clients with little downside.

---

*© 2025 Maël LE PETIT*
