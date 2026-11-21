package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.core.Snapshot;
import com.raftkv.kv.KvService;
import com.raftkv.kv.KvStateMachine;
import com.raftkv.log.InMemoryRaftLog;
import com.raftkv.rpc.RaftMessage;
import com.raftkv.transport.MessageHandler;
import com.raftkv.transport.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTest {

    private static final int MAX = RaftConfig.DEFAULT.electionTimeoutMax();
    private static final RaftConfig EVERY_10 = RaftConfig.DEFAULT.withSnapshotThreshold(10);

    @TempDir
    Path dir;

    private TestCluster cluster;

    @AfterEach
    void closeFiles() {
        if (cluster != null) cluster.closeFiles();
    }

    private static <T> T await(TestCluster c, CompletableFuture<T> f) {
        c.runUntil(f::isDone, 20 * MAX, "future to complete");
        return f.join();
    }

    private void writeKeys(KvService kv, int count) {
        for (int i = 0; i < count; i++) await(cluster, kv.put("k" + i, "v" + i));
    }

    @Test
    void snapshotsKeepTheLogShortOnEveryNode() {
        cluster = TestCluster.durable(3, 1, dir, EVERY_10);
        KvService kv = cluster.kv(cluster.awaitLeader(10 * MAX).id());
        writeKeys(kv, 100);
        cluster.awaitCommitEverywhere(cluster.leaders().get(0).commitIndex(), 10);
        cluster.run(2);

        for (RaftNode n : cluster.nodes.values()) {
            assertTrue(n.snapshotIndex() >= 90, n.id() + " snapshot at " + n.snapshotIndex());
            long held = n.log().lastIndex() - n.log().baseIndex();
            assertTrue(held <= 30, n.id() + " still holds " + held + " entries");
            assertTrue(n.log().baseIndex() <= n.snapshotIndex(), "never trim past the snapshot");
        }
        cluster.assertStateMachinesAgree();
        assertEquals(Optional.of("v0"), await(cluster, cluster.kv(cluster.leaders().get(0).id()).get("k0")),
                "early writes live on in the snapshot");
    }

    @Test
    void snapshotsAreOffByDefault() {
        cluster = TestCluster.durable(3, 2, dir);
        KvService kv = cluster.kv(cluster.awaitLeader(10 * MAX).id());
        writeKeys(kv, 40);
        for (RaftNode n : cluster.nodes.values()) {
            assertEquals(0, n.snapshotIndex());
            assertEquals(0, n.log().baseIndex());
        }
    }

    @Test
    void restartedFollowerStartsFromItsSnapshotThenCatchesUp() {
        cluster = TestCluster.durable(3, 3, dir, EVERY_10);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        writeKeys(cluster.kv(leader.id()), 55);
        NodeId f = cluster.upIds().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        cluster.awaitCommitEverywhere(leader.commitIndex(), 10);
        long snapshotAt = cluster.nodes.get(f).snapshotIndex();
        assertTrue(snapshotAt > 0);

        cluster.crash(f);
        RaftNode back = cluster.restart(f);
        assertEquals(snapshotAt, back.lastApplied(), "state is restored before talking to anyone");
        assertEquals(snapshotAt, back.commitIndex());
        assertEquals(snapshotAt, back.snapshotIndex());
        assertEquals(Optional.of("v3"), cluster.kvs.get(f).get("k3"), "early keys come from the snapshot");
        assertTrue(back.log().baseIndex() >= 1);

        cluster.awaitCommitEverywhere(leader.commitIndex(), 20);
        cluster.run(1);
        cluster.assertStateMachinesAgree();
    }

    @Test
    void wholeClusterRestartWithSnapshotsKeepsEveryWrite() {
        cluster = TestCluster.durable(3, 4, dir, EVERY_10);
        KvService kv = cluster.kv(cluster.awaitLeader(10 * MAX).id());
        writeKeys(kv, 60);
        await(cluster, kv.delete("k5"));
        cluster.run(3);

        for (NodeId id : cluster.ids()) cluster.crash(id);
        cluster.restartAll();
        RaftNode leader = cluster.awaitLeader(20 * MAX);
        KvService after = cluster.kv(leader.id());
        for (int i = 0; i < 60; i++) {
            Optional<String> expected = i == 5 ? Optional.empty() : Optional.of("v" + i);
            assertEquals(expected, await(cluster, after.get("k" + i)), "k" + i);
        }
        cluster.awaitCommitEverywhere(leader.commitIndex(), 20);
        cluster.run(1);
        cluster.assertStateMachinesAgree();
        cluster.assertElectionSafety();
    }

    @Test
    void tornWriteAfterCompactionStillRecovers() {
        cluster = TestCluster.durable(3, 5, dir, EVERY_10);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        writeKeys(cluster.kv(leader.id()), 35);
        NodeId f = cluster.upIds().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        cluster.awaitCommitEverywhere(leader.commitIndex(), 10);
        long last = cluster.nodes.get(f).log().lastIndex();

        cluster.crash(f, true);
        RaftNode back = cluster.restart(f);
        assertEquals(last, back.log().lastIndex());
        writeKeys(cluster.kv(leader.id()), 5);
        cluster.awaitCommitEverywhere(leader.commitIndex(), 20);
        cluster.run(1);
        cluster.assertStateMachinesAgree();
    }

    /**
     * Until InstallSnapshot exists, a leader must not trim entries a follower still needs.
     * One follower's replies stop reaching the leader (it still hears the leader, so it does not
     * start elections). The leader snapshots but keeps the log from that follower's last
     * acknowledged entry, since it cannot know the follower already has more.
     */
    @Test
    void leaderKeepsTheEntriesALaggingFollowerStillNeeds() {
        cluster = TestCluster.durable(3, 6, dir, EVERY_10);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        NodeId slow = cluster.upIds().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        cluster.awaitCommitEverywhere(1, 10);
        long slowMatch = leader.matchIndexOf(slow).orElseThrow();

        cluster.network.dropIf(e -> e.from().equals(slow) && e.message() instanceof com.raftkv.rpc.AppendResponse);
        writeKeys(cluster.kv(leader.id()), 60);
        assertTrue(leader.snapshotIndex() >= 50, "the leader still snapshots");
        assertTrue(leader.log().baseIndex() <= slowMatch,
                "but keeps what the follower is missing: base " + leader.log().baseIndex() + " match " + slowMatch);

        cluster.network.heal();
        cluster.awaitCommitEverywhere(leader.commitIndex(), 30);
        cluster.run(1);
        cluster.assertStateMachinesAgree();
        assertEquals(leader.log().lastIndex(), cluster.nodes.get(slow).log().lastIndex());
        assertEquals(leader.log().lastIndex(), leader.matchIndexOf(slow).orElseThrow(), "the leader learns the truth");
    }

    // ------------------------------------------------------------ node alone, hand-built

    private record Parts(RaftNode node, InMemoryRaftLog log, KvStateMachine kv, InMemorySnapshotStore snapshots) { }

    private static Parts nodeWith(InMemoryRaftLog log, InMemorySnapshotStore snapshots, RaftConfig config) {
        Transport sink = new Transport() {
            @Override public void register(NodeId n, MessageHandler h) { }
            @Override public void unregister(NodeId n) { }
            @Override public void send(NodeId from, NodeId to, RaftMessage m) { }
        };
        KvStateMachine kv = new KvStateMachine();
        RaftNode node = new RaftNode(NodeId.of("n1"), Set.of(), sink, log, new InMemoryStateStore(), kv,
                snapshots, config, new Random(0), NodeListener.NONE);
        return new Parts(node, log, kv, snapshots);
    }

    @Test
    void startupWithASnapshotRestoresStateAndTrimsAnUntrimmedLog() {
        // A crash after the snapshot was saved but before the log was trimmed.
        InMemoryRaftLog log = new InMemoryRaftLog();
        KvStateMachine donor = new KvStateMachine();
        for (int i = 1; i <= 5; i++) {
            LogEntry e = log.append(1, Command.put("k" + i, "v" + i));
            if (i <= 3) donor.apply(e);
        }
        InMemorySnapshotStore snapshots = new InMemorySnapshotStore();
        snapshots.save(new Snapshot(3, 1, donor.snapshot()));

        Parts p = nodeWith(log, snapshots, RaftConfig.DEFAULT);
        assertEquals(3, p.log().baseIndex());
        assertEquals(1, p.log().baseTerm());
        assertEquals(5, p.log().lastIndex(), "entries after the snapshot are kept");
        assertEquals(3, p.node().lastApplied());
        assertEquals(3, p.node().commitIndex());
        assertEquals(Optional.of("v3"), p.kv().get("k3"));
        assertEquals(Optional.empty(), p.kv().get("k4"), "entry 4 is not committed yet, so not applied");
    }

    @Test
    void startupWithALogShorterThanTheSnapshotUsesTheSnapshotAsTheBase() {
        InMemoryRaftLog log = new InMemoryRaftLog(); // all entries were trimmed away
        KvStateMachine donor = new KvStateMachine();
        donor.apply(new LogEntry(2, 1, Command.put("a", "1")));
        donor.apply(new LogEntry(2, 2, Command.put("b", "2")));
        InMemorySnapshotStore snapshots = new InMemorySnapshotStore();
        snapshots.save(new Snapshot(2, 2, donor.snapshot()));

        Parts p = nodeWith(log, snapshots, RaftConfig.DEFAULT);
        assertEquals(2, p.log().lastIndex());
        assertEquals(2, p.log().lastTerm());
        assertEquals(Optional.of("2"), p.kv().get("b"));
    }

    @Test
    void singleNodeTakesSnapshotsOnItsOwnAndTrimsEverything() {
        Parts p = nodeWith(new InMemoryRaftLog(), new InMemorySnapshotStore(), RaftConfig.DEFAULT.withSnapshotThreshold(5));
        while (p.node().role() != Role.LEADER) p.node().tick();
        for (int i = 0; i < 12; i++) p.node().submit(Command.put("k" + i, "v" + i));
        assertEquals(13, p.node().lastApplied()); // no-op + 12 writes
        assertEquals(10, p.node().snapshotIndex(), "snapshots at 5 and 10");
        assertEquals(10, p.log().baseIndex(), "a single node has no followers to wait for");
        Snapshot saved = p.snapshots().load().orElseThrow();
        assertEquals(10, saved.lastIncludedIndex());
        assertEquals(p.node().term(), saved.lastIncludedTerm());
    }

    @Test
    void manualSnapshotDoesNothingWhenNothingIsNew() {
        Parts p = nodeWith(new InMemoryRaftLog(), new InMemorySnapshotStore(), RaftConfig.DEFAULT);
        while (p.node().role() != Role.LEADER) p.node().tick();
        assertTrue(p.node().takeSnapshot());
        assertFalse(p.node().takeSnapshot());
        assertEquals(1, p.node().snapshotIndex());
    }

    @Test
    void snapshotIsSavedBeforeTheLogIsTrimmed() {
        // If the order were reversed, a crash in between would lose committed entries.
        InMemoryRaftLog log = new InMemoryRaftLog();
        List<String> order = new java.util.ArrayList<>();
        InMemorySnapshotStore snapshots = new InMemorySnapshotStore();
        SnapshotStore watching = new SnapshotStore() {
            @Override public Optional<Snapshot> load() { return snapshots.load(); }
            @Override public void save(Snapshot s) { order.add("snapshot saved, base " + log.baseIndex()); snapshots.save(s); }
        };
        Transport sink = new Transport() {
            @Override public void register(NodeId n, MessageHandler h) { }
            @Override public void unregister(NodeId n) { }
            @Override public void send(NodeId from, NodeId to, RaftMessage m) { }
        };
        RaftNode node = new RaftNode(NodeId.of("n1"), Set.of(), sink, log, new InMemoryStateStore(),
                new KvStateMachine(), watching, RaftConfig.DEFAULT, new Random(0), NodeListener.NONE);
        while (node.role() != Role.LEADER) node.tick();
        node.submit(Command.put("a", "1"));
        node.takeSnapshot();
        assertEquals(List.of("snapshot saved, base 0"), order, "the log was still whole when the snapshot became durable");
        assertEquals(2, log.baseIndex());
    }
}
