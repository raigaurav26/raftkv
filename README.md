# raftkv

[![CI](https://github.com/raigaurav26/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/raigaurav26/raftkv/actions/workflows/ci.yml)

A small replicated key-value store in plain Java, built on the
[Raft consensus algorithm](https://raft.github.io/raft.pdf). Three or five nodes keep the same
data; the cluster keeps accepting writes as long as a majority is up, and never loses a write it
has acknowledged.

The goal is a readable implementation of the core of Raft with tests that try hard to break it:
an in-process harness that partitions the network, drops and delays messages, and crashes nodes,
then checks Raft's safety properties.

## Features (planned)

- Leader election with randomized timeouts
- Log replication with fast conflict back-off
- Durable log and term/vote on disk, with CRC checks and crash recovery
- Snapshots, log compaction and InstallSnapshot
- HTTP API: `PUT`, `GET`, `DELETE /kv/{key}`, followers redirect to the leader
- Fault-injection test harness (partitions, drops, delays, crashes)
- Benchmark and a Docker Compose three-node cluster

## Tech

Java 21, Maven, JUnit 5. No frameworks in the core, so the consensus code stays easy to follow.

## Design notes

- **No threads or clocks in the consensus code.** `RaftNode` only reacts to `tick()` and
  `onMessage()`. Tests drive a whole cluster from one thread with a seeded random source, so a
  failing seed replays exactly. The runtime will call the same methods from a single event loop.
- **Commit rule from figure 8 of the paper.** A leader only commits an entry from its own term
  by counting replicas; older entries commit along with it. Each new leader appends a no-op so
  this happens straight away.
- **Fast back-off.** A follower that rejects AppendEntries returns a conflict index, so the
  leader skips a whole stale term per round trip instead of one entry.
- **Tests check Raft's safety properties directly**: one leader per term, log matching, and
  that no committed entry ever changes or disappears, over hundreds of seeded runs with random
  partitions and message loss.
- **Linearizable reads without writing to the log.** A read waits until the leader has
  confirmed with a majority, after the read arrived, that it still leads (read-index from
  Ongaro's thesis, section 6.4). A leader cut off from the cluster can never serve a stale
  value. Heartbeats carry a sequence number so a late reply to an old message cannot count.
- **Term and vote are saved before any reply leaves the node**, so a restart can never lead to
  two votes in one term.

## Build

```bash
mvn verify
```

## Project layout

```
src/main/java/com/raftkv
├── core       NodeId, Role (state machine), Command, LogEntry, PersistentState (term + vote)
├── node       RaftNode (election, replication, commit rule, read barrier), RaftConfig, StateStore
├── kv         KvStateMachine (the map), KvService (put / get / delete as futures)
├── log        RaftLog and the in-memory implementation of the AppendEntries log check
├── rpc        RequestVote, VoteResponse, AppendEntries, AppendResponse (with fast back-off hint)
└── transport  Transport interface and a deterministic in-memory network with partitions and drops
```

## Roadmap

See [docs/PLAN.md](docs/PLAN.md) for the day-by-day plan.

- [x] Day 1: skeleton, core types, in-memory log, CI
- [x] Day 2: RPC messages and transport
- [x] Day 3: leader election
- [x] Day 4: log replication and commit
- [x] Day 5: key-value state machine
- [ ] Day 6: persistent log and state
- [ ] Day 7: crash recovery
- [ ] Day 8: snapshots and log compaction
- [ ] Day 9: InstallSnapshot
- [ ] Day 10: HTTP API and node-to-node transport
- [ ] Day 11: fault-injection harness
- [ ] Day 12: runnable cluster (scripts, Docker Compose)
- [ ] Day 13: architecture diagram and benchmark
- [ ] Day 14: polish
