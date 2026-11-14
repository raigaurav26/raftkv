# Build plan

raftkv is built in 14 steps, one per day. Each day ends with green tests on `main`.

| Day | Goal | Done when |
|-----|------|-----------|
| 1 | Project skeleton, core types, in-memory log, CI | Role state machine, vote rules and the AppendEntries log check are covered by tests |
| 2 | RPC messages and a transport interface | RequestVote / AppendEntries request and response records, an in-memory transport that delivers between nodes |
| 3 | Leader election | Randomized election timeouts on a deterministic clock; exactly one leader per term in tests |
| 4 | Log replication | Leader tracks nextIndex / matchIndex, backs off on mismatch, advances commitIndex only for entries of its own term |
| 5 | Key-value state machine | Committed entries applied in order; PUT / GET / DELETE through the leader; no-op entry on election |
| 6 | Persistent log and state | Append-only log file with length + CRC32 per record, fsync before replying; term and vote stored atomically |
| 7 | Crash recovery | Restart a node from disk mid-run; torn final record is detected and dropped |
| 8 | Snapshots and log compaction | State machine snapshot at a configurable log size; log prefix discarded |
| 9 | InstallSnapshot | Leader ships a snapshot to a follower that has fallen behind the compacted log |
| 10 | HTTP API and node-to-node transport | `GET/PUT/DELETE /kv/{key}`, followers redirect to the leader; RPCs over HTTP between processes |
| 11 | Fault-injection harness | In-process cluster with partitions, message drops, delays and crashes; randomized runs check the safety properties |
| 12 | Runnable cluster | Config file, `scripts/run-cluster.sh`, Dockerfile and Docker Compose for three nodes |
| 13 | Docs and benchmark | Architecture diagram in the README, write/read throughput and latency numbers |
| 14 | Polish | Clean-ups, final README pass |

## Safety properties the tests check

From figure 3 of the Raft paper:

- **Election safety**: at most one leader per term.
- **Leader append-only**: a leader never overwrites or deletes its own entries.
- **Log matching**: if two logs have an entry with the same index and term, the logs are identical up to that index.
- **Leader completeness**: a committed entry is present in the log of every later leader.
- **State machine safety**: no two servers apply different commands at the same index.
