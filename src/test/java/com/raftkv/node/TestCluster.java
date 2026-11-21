package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.kv.KvService;
import com.raftkv.kv.KvStateMachine;
import com.raftkv.log.InMemoryRaftLog;
import com.raftkv.log.RaftLog;
import com.raftkv.storage.FileRaftLog;
import com.raftkv.storage.FileSnapshotStore;
import com.raftkv.storage.FileStateStore;
import com.raftkv.transport.InMemoryNetwork;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Several {@link RaftNode}s on one {@link InMemoryNetwork}, all driven from the test thread.
 * One step = tick every node, then deliver every message (including replies). With a fixed
 * seed every run is identical, so a failing seed can be replayed exactly.
 *
 * <p>It also records every leader it ever sees, by term, to check election safety: at most
 * one leader per term.
 */
final class TestCluster {

    final InMemoryNetwork network = new InMemoryNetwork();
    final Map<NodeId, RaftNode> nodes = new LinkedHashMap<>();
    final Map<NodeId, StateStore> stores = new HashMap<>();
    final Map<NodeId, RaftLog> logs = new HashMap<>();
    final Map<NodeId, SnapshotStore> snapshots = new HashMap<>();
    final Set<NodeId> down = new HashSet<>();
    private final List<NodeId> allIds = new ArrayList<>();
    private final Path dataDir;   // null = in-memory nodes that cannot be restarted
    private final boolean fsync;
    final Map<NodeId, KvStateMachine> kvs = new HashMap<>();
    private final Map<Long, Set<NodeId>> leadersByTerm = new HashMap<>();
    /** Every entry any node ever reported committed, by index. Must never change. */
    private final Map<Long, LogEntry> committed = new HashMap<>();
    private final RaftConfig config;
    private final Random seeds;
    private long steps;

    TestCluster(int size, long seed) {
        this(size, seed, RaftConfig.DEFAULT);
    }

    TestCluster(int size, long seed, RaftConfig config) {
        this(size, seed, config, null, true);
    }

    /** A cluster whose nodes keep their log, term and vote in files under {@code dataDir}. */
    static TestCluster durable(int size, long seed, Path dataDir) {
        return new TestCluster(size, seed, RaftConfig.DEFAULT, dataDir, true);
    }

    static TestCluster durable(int size, long seed, Path dataDir, boolean fsync) {
        return new TestCluster(size, seed, RaftConfig.DEFAULT, dataDir, fsync);
    }

    static TestCluster durable(int size, long seed, Path dataDir, RaftConfig config) {
        return new TestCluster(size, seed, config, dataDir, true);
    }

    private TestCluster(int size, long seed, RaftConfig config, Path dataDir, boolean fsync) {
        this.config = config;
        this.seeds = new Random(seed);
        this.dataDir = dataDir;
        this.fsync = fsync;
        for (int i = 1; i <= size; i++) allIds.add(NodeId.of("n" + i));
        for (NodeId id : allIds) nodes.put(id, startNode(id));
    }

    private Path logFile(NodeId id) {
        return dataDir.resolve(id.value()).resolve("raft.log");
    }

    private RaftNode startNode(NodeId id) {
        Set<NodeId> peers = new HashSet<>(allIds);
        peers.remove(id);
        StateStore store;
        RaftLog log;
        SnapshotStore snapshotStore;
        if (dataDir == null) {
            store = new InMemoryStateStore();
            log = new InMemoryRaftLog();
            snapshotStore = new InMemorySnapshotStore();
        } else {
            store = new FileStateStore(dataDir.resolve(id.value()).resolve("state"));
            log = new FileRaftLog(logFile(id), fsync);
            snapshotStore = new FileSnapshotStore(dataDir.resolve(id.value()).resolve("snapshot"));
        }
        stores.put(id, store);
        logs.put(id, log);
        snapshots.put(id, snapshotStore);
        return newNode(id, peers, store, log, snapshotStore);
    }

    /**
     * Kills a node the way a power cut would: it stops at once, its memory is gone, queued
     * messages addressed to it are lost, and only what it fsynced stays on disk. With
     * {@code tornWrite} the file also ends in the start of a record that never finished,
     * as if the machine died halfway through a write (which was therefore never acknowledged).
     */
    void crash(NodeId id, boolean tornWrite) {
        if (dataDir == null) throw new IllegalStateException("only durable clusters can crash nodes");
        if (!nodes.containsKey(id)) throw new IllegalStateException(id + " is already down");
        network.unregister(id);
        nodes.remove(id);
        ((FileRaftLog) logs.remove(id)).close();
        stores.remove(id);
        snapshots.remove(id);
        kvs.remove(id);
        down.add(id);
        if (tornWrite) {
            try {
                Files.write(logFile(id), new byte[]{0, 0, 1, 0, 0x12, 0x34, 0x56, 0x78, 7, 7, 7},
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    void crash(NodeId id) {
        crash(id, false);
    }

    /** Starts the node again from whatever is on disk, with an empty state machine. */
    RaftNode restart(NodeId id) {
        if (!down.remove(id)) throw new IllegalStateException(id + " is not down");
        RaftNode node = startNode(id);
        nodes.put(id, node);
        return node;
    }

    void restartAll() {
        for (NodeId id : List.copyOf(down)) restart(id);
    }

    void closeFiles() {
        for (RaftLog l : logs.values()) if (l instanceof FileRaftLog f) f.close();
    }

    private RaftNode newNode(NodeId id, Set<NodeId> peers, StateStore store, RaftLog log,
                             SnapshotStore snapshotStore) {
        NodeListener recorder = new NodeListener() {
            @Override
            public void roleChanged(NodeId node, Role role, long term) {
                if (role == Role.LEADER) leadersByTerm.computeIfAbsent(term, t -> new HashSet<>()).add(node);
            }

            @Override
            public void committed(NodeId node, long commitIndex) {
                recordCommitted(nodes.get(node), commitIndex);
            }
        };
        KvStateMachine kv = new KvStateMachine();
        kvs.put(id, kv);
        return new RaftNode(id, peers, network, log, store, kv, snapshotStore, config,
                new Random(seeds.nextLong()), recorder);
    }

    RaftNode node(String id) {
        return nodes.get(NodeId.of(id));
    }

    KvService kv(NodeId id) {
        return new KvService(nodes.get(id), kvs.get(id));
    }

    /** Every node has applied the same entries and holds the same map. */
    void assertStateMachinesAgree() {
        long applied = nodes.values().iterator().next().lastApplied();
        Map<String, String> data = kvs.values().iterator().next().contents();
        for (RaftNode n : nodes.values()) {
            if (n.lastApplied() != applied || !kvs.get(n.id()).contents().equals(data)) {
                throw new AssertionError(n.id() + " applied " + n.lastApplied() + " " + kvs.get(n.id()).contents()
                        + " but others applied " + applied + " " + data);
            }
        }
    }

    List<NodeId> ids() {
        return List.copyOf(allIds);
    }

    List<NodeId> upIds() {
        return List.copyOf(nodes.keySet());
    }

    void step() {
        nodes.values().forEach(RaftNode::tick);
        network.deliverAll();
        steps++;
    }

    void run(int steps) {
        for (int i = 0; i < steps; i++) step();
    }

    /** Steps until the condition holds; fails the test if it does not within {@code maxSteps}. */
    void runUntil(BooleanSupplier condition, int maxSteps, String what) {
        for (int i = 0; i < maxSteps; i++) {
            if (condition.getAsBoolean()) return;
            step();
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("no " + what + " after " + maxSteps + " steps; " + describe());
        }
    }

    List<RaftNode> leaders() {
        return nodes.values().stream().filter(RaftNode::isLeader).toList();
    }

    /** The single leader among {@code among}, if exactly one of them leads. */
    Optional<RaftNode> leaderAmong(Set<NodeId> among) {
        List<RaftNode> l = leaders().stream().filter(n -> among.contains(n.id())).toList();
        return l.size() == 1 ? Optional.of(l.get(0)) : Optional.empty();
    }

    RaftNode awaitLeader(int maxSteps) {
        return awaitLeader(nodes.keySet(), maxSteps);
    }

    /** Waits until exactly one node in {@code among} is leader and the rest of them follow it. */
    RaftNode awaitLeader(Set<NodeId> among, int maxSteps) {
        runUntil(() -> leaderAmong(among).map(l -> among.stream()
                .map(nodes::get)
                .allMatch(n -> n == l || (n.role() == Role.FOLLOWER
                        && n.term() == l.term() && n.leaderId().equals(Optional.of(l.id())))))
                .orElse(false), maxSteps, "stable leader among " + among);
        return leaderAmong(among).orElseThrow();
    }

    /**
     * Called whenever a node advances its commit index. Checks that the entries it now calls
     * committed agree with what any node called committed before (state machine safety).
     */
    private void recordCommitted(RaftNode node, long commitIndex) {
        if (node == null) return; // still constructing
        for (long i = node.log().baseIndex() + 1; i <= commitIndex; i++) {
            LogEntry mine = node.log().entry(i).orElseThrow(
                    () -> new AssertionError(node.id() + " committed past the end of its log"));
            LogEntry earlier = committed.putIfAbsent(i, mine);
            if (earlier != null && !earlier.equals(mine)) {
                throw new AssertionError("index " + mine.index() + " committed as " + earlier
                        + " and later as " + mine + " on " + node.id());
            }
        }
    }

    /** Proposes on the current leader. Fails if there is none. */
    LogEntry propose(Command command) {
        List<RaftNode> l = leaders();
        if (l.isEmpty()) throw new AssertionError("no leader; " + describe());
        return l.stream().max(java.util.Comparator.comparingLong(RaftNode::term)).orElseThrow().propose(command);
    }

    void awaitCommitEverywhere(long index, int maxSteps) {
        runUntil(() -> nodes.values().stream().allMatch(n -> n.commitIndex() >= index), maxSteps,
                "commit of index " + index + " on every node");
    }

    Map<Long, LogEntry> committed() {
        return committed;
    }

    /** Leader completeness: every entry ever committed is in every node's log, unchanged. */
    void assertCommittedEverywhere() {
        for (RaftNode n : nodes.values()) {
            committed.forEach((i, e) -> {
                if (i <= n.log().baseIndex()) return; // covered by a snapshot, which is checked by agreement of state
                if (!n.log().entry(i).equals(Optional.of(e))) {
                    throw new AssertionError(n.id() + " lost committed entry " + e + "; has " + n.log().entry(i));
                }
            });
        }
    }

    /** Log matching: if two logs agree on the term at an index, they agree on everything before it. */
    void assertLogMatching() {
        List<RaftNode> all = List.copyOf(nodes.values());
        for (int a = 0; a < all.size(); a++) {
            for (int b = a + 1; b < all.size(); b++) {
                RaftNode x = all.get(a), y = all.get(b);
                long upTo = Math.min(x.log().lastIndex(), y.log().lastIndex());
                long from = Math.max(x.log().baseIndex(), y.log().baseIndex()) + 1;
                for (long i = upTo; i >= from; i--) {
                    if (x.log().termAt(i).equals(y.log().termAt(i))) {
                        int n = (int) (i - from + 1);
                        if (!x.log().entriesFrom(from, n).equals(y.log().entriesFrom(from, n))) {
                            throw new AssertionError("logs of " + x.id() + " and " + y.id()
                                    + " agree at " + i + " but differ before it");
                        }
                        break;
                    }
                }
            }
        }
    }

    /** Election safety: no term ever had two leaders. */
    void assertElectionSafety() {
        leadersByTerm.forEach((term, ls) -> {
            if (ls.size() > 1) throw new AssertionError("term " + term + " had leaders " + ls);
        });
    }

    Map<Long, Set<NodeId>> leadersByTerm() {
        return leadersByTerm;
    }

    String describe() {
        StringBuilder sb = new StringBuilder("step " + steps + ":");
        nodes.values().forEach(n -> sb.append(' ').append(n.id()).append('=')
                .append(n.role()).append("@t").append(n.term()));
        return sb.toString();
    }
}
