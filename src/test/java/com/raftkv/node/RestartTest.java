package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.kv.KvService;
import com.raftkv.storage.FileRaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/** Nodes that crash and come back from their files. */
class RestartTest {

    private static final int MAX = RaftConfig.DEFAULT.electionTimeoutMax();

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

    private NodeId follower(RaftNode leader) {
        return cluster.upIds().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
    }

    @Test
    void followerKeepsItsLogTermAndVoteAcrossARestart() {
        cluster = TestCluster.durable(3, 1, dir);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        KvService kv = cluster.kv(leader.id());
        await(cluster, kv.put("a", "1"));
        await(cluster, kv.put("b", "2"));
        NodeId f = follower(leader);
        cluster.awaitCommitEverywhere(leader.commitIndex(), 10);

        RaftNode before = cluster.nodes.get(f);
        long term = before.term();
        long lastIndex = before.log().lastIndex();
        Optional<NodeId> vote = before.votedFor();
        assertTrue(vote.isPresent());

        cluster.crash(f);
        RaftNode after = cluster.restart(f);
        assertEquals(Role.FOLLOWER, after.role());
        assertEquals(term, after.term());
        assertEquals(vote, after.votedFor(), "a restarted node still remembers who it voted for");
        assertEquals(lastIndex, after.log().lastIndex());
        assertEquals(0, after.commitIndex(), "commit index is not stored; the leader tells it again");

        cluster.awaitCommitEverywhere(leader.commitIndex(), 10);
        cluster.run(1);
        assertEquals(leader.lastApplied(), after.lastApplied(), "the log is re-applied after a restart");
        cluster.assertStateMachinesAgree();
        assertEquals(Optional.of("2"), cluster.kvs.get(f).get("b"));
    }

    @Test
    void restartedNodeCannotVoteTwiceInATerm() {
        cluster = TestCluster.durable(3, 2, dir);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        NodeId f = follower(leader);
        long term = cluster.nodes.get(f).term();
        NodeId votedFor = cluster.nodes.get(f).votedFor().orElseThrow();

        cluster.crash(f);
        RaftNode back = cluster.restart(f);
        NodeId other = cluster.ids().stream().filter(id -> !id.equals(f) && !id.equals(votedFor)).findFirst().orElseThrow();
        back.onMessage(other, new com.raftkv.rpc.RequestVote(term, other, back.log().lastIndex(), back.log().lastTerm()));
        assertEquals(votedFor, back.votedFor().orElseThrow(), "the old vote still stands in this term");
    }

    @Test
    void wholeClusterRestartKeepsEveryAcknowledgedWrite() {
        cluster = TestCluster.durable(3, 3, dir);
        KvService kv = cluster.kv(cluster.awaitLeader(10 * MAX).id());
        for (int i = 0; i < 25; i++) await(cluster, kv.put("k" + i, "v" + i));
        await(cluster, kv.delete("k3"));

        for (NodeId id : cluster.ids()) cluster.crash(id);
        assertTrue(cluster.nodes.isEmpty());
        cluster.restartAll();

        RaftNode leader = cluster.awaitLeader(20 * MAX);
        KvService after = cluster.kv(leader.id());
        assertEquals(Optional.of("v24"), await(cluster, after.get("k24")));
        assertEquals(Optional.of("v0"), await(cluster, after.get("k0")));
        assertEquals(Optional.empty(), await(cluster, after.get("k3")), "a delete is not undone by a restart");
        cluster.awaitCommitEverywhere(leader.commitIndex(), 20);
        cluster.run(1);
        cluster.assertStateMachinesAgree();
        assertEquals(24, cluster.kvs.get(leader.id()).size());
        cluster.assertElectionSafety();
    }

    @Test
    void crashedLeaderIsReplacedAndRejoinsAsFollower() {
        cluster = TestCluster.durable(5, 4, dir);
        RaftNode old = cluster.awaitLeader(10 * MAX);
        NodeId oldId = old.id();
        await(cluster, cluster.kv(oldId).put("x", "before"));
        long oldTerm = old.term();

        cluster.crash(oldId);
        RaftNode next = cluster.awaitLeader(10 * MAX);
        assertNotEquals(oldId, next.id());
        assertTrue(next.term() > oldTerm);
        await(cluster, cluster.kv(next.id()).put("x", "after"));

        RaftNode back = cluster.restart(oldId);
        cluster.awaitLeader(10 * MAX);
        cluster.awaitCommitEverywhere(next.commitIndex(), 20);
        cluster.run(1);
        assertEquals(Role.FOLLOWER, back.role());
        assertEquals(Optional.of("after"), cluster.kvs.get(oldId).get("x"));
        cluster.assertStateMachinesAgree();
    }

    @Test
    void halfWrittenRecordFromACrashIsDiscardedAndTheNodeRejoins() {
        cluster = TestCluster.durable(3, 5, dir);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        KvService kv = cluster.kv(leader.id());
        await(cluster, kv.put("a", "1"));
        NodeId f = follower(leader);
        cluster.awaitCommitEverywhere(leader.commitIndex(), 10);
        long lastIndex = cluster.nodes.get(f).log().lastIndex();

        cluster.crash(f, true);
        RaftNode back = cluster.restart(f);
        assertEquals(lastIndex, back.log().lastIndex(), "everything acknowledged before the crash is still there");
        assertTrue(((FileRaftLog) back.log()).droppedTornBytes() > 0);

        await(cluster, kv.put("b", "2"));
        cluster.awaitCommitEverywhere(leader.commitIndex(), 10);
        cluster.run(1);
        cluster.assertStateMachinesAgree();
    }

    @Test
    void minorityOfCrashedNodesDoesNotStopWrites() {
        cluster = TestCluster.durable(5, 6, dir);
        RaftNode leader = cluster.awaitLeader(10 * MAX);
        List<NodeId> others = new ArrayList<>(cluster.upIds());
        others.remove(leader.id());
        cluster.crash(others.get(0));
        cluster.crash(others.get(1));

        KvService kv = cluster.kv(leader.id());
        assertEquals(Optional.empty(), await(cluster, kv.put("a", "1")));

        cluster.crash(others.get(2));  // now 2 of 5 are up: no majority
        CompletableFuture<Optional<String>> stuck = kv.put("b", "2");
        cluster.run(5 * MAX);
        assertFalse(stuck.isDone(), "no commit without a majority");

        cluster.restart(others.get(0));  // majority is back
        assertEquals(Optional.empty(), await(cluster, stuck));
    }

    /**
     * Random crashes (some with a torn last write), restarts, partitions and message loss, with
     * proposals flowing the whole time. Every commit is checked as it happens. At the end all
     * nodes come back from disk and must agree with each other and with everything ever committed.
     */
    @Test
    void safetyUnderRandomCrashesPartitionsAndLoss() {
        for (long seed = 0; seed < 25; seed++) {
            Path seedDir = dir.resolve("seed" + seed);
            TestCluster c = TestCluster.durable(5, seed, seedDir, false);
            try {
                Random rnd = new Random(seed * 17 + 3);
                c.network.dropIf(e -> rnd.nextInt(10) == 0);
                List<NodeId> ids = new ArrayList<>(c.ids());
                int proposed = 0;
                for (int step = 0; step < 1500; step++) {
                    if (step % 40 == 0) {
                        NodeId victim = ids.get(rnd.nextInt(ids.size()));
                        if (c.down.contains(victim)) c.restart(victim);
                        else if (c.down.size() < 3) c.crash(victim, rnd.nextBoolean());
                    }
                    if (step % 150 == 0) {
                        Collections.shuffle(ids, rnd);
                        int cut = rnd.nextInt(ids.size());
                        if (cut == 0) c.network.heal();
                        else c.network.partition(Set.copyOf(ids.subList(0, cut)), Set.copyOf(ids.subList(cut, ids.size())));
                        c.network.dropIf(e -> rnd.nextInt(10) == 0);
                    }
                    for (RaftNode n : c.leaders()) {
                        if (rnd.nextInt(3) == 0) n.propose(Command.put("k" + (proposed % 11), "v" + proposed++));
                    }
                    c.step();
                }
                c.network.heal();
                c.restartAll();
                RaftNode leader = c.awaitLeader(30 * MAX);
                LogEntry marker = leader.propose(Command.put("final", "seed" + seed));
                c.awaitCommitEverywhere(marker.index(), 30 * MAX);
                c.run(1);

                c.assertElectionSafety();
                c.assertLogMatching();
                c.assertCommittedEverywhere();
                c.assertStateMachinesAgree();
                assertTrue(c.committed().size() > 10, "seed " + seed + " made progress");
            } finally {
                c.closeFiles();
            }
        }
    }
}
