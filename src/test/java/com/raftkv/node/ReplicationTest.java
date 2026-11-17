package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationTest {

    private static final int MAX = RaftConfig.DEFAULT.electionTimeoutMax();

    private static Command put(int i) {
        return Command.put("k" + i, "v" + i);
    }

    @Test
    void newLeaderCommitsANoOpEntry() {
        TestCluster c = new TestCluster(3, 1);
        RaftNode leader = c.awaitLeader(10 * MAX);
        LogEntry first = leader.log().entry(1).orElseThrow();
        assertEquals(Command.Op.NOOP, first.command().op());
        assertEquals(leader.term(), first.term());
        c.awaitCommitEverywhere(1, 5);
    }

    @Test
    void singleNodeCommitsImmediately() {
        TestCluster c = new TestCluster(1, 1);
        RaftNode n = c.awaitLeader(MAX + 1);
        LogEntry e = n.propose(put(1));
        assertEquals(e.index(), n.commitIndex());
    }

    @Test
    void proposalsReachEveryNodeInOrder() {
        TestCluster c = new TestCluster(5, 2);
        c.awaitLeader(10 * MAX);
        LogEntry last = null;
        for (int i = 0; i < 20; i++) last = c.propose(put(i));
        c.awaitCommitEverywhere(last.index(), 5);
        RaftNode leader = c.leaders().get(0);
        for (RaftNode n : c.nodes.values()) {
            assertEquals(leader.log().entriesFrom(1, 100), n.log().entriesFrom(1, 100), n.id().toString());
        }
        c.assertLogMatching();
    }

    @Test
    void commitNeedsAMajorityNotEveryone() {
        TestCluster c = new TestCluster(5, 3);
        RaftNode leader = c.awaitLeader(10 * MAX);
        List<NodeId> others = c.ids().stream().filter(id -> !id.equals(leader.id())).toList();
        c.network.isolate(others.get(0));
        c.network.isolate(others.get(1));
        LogEntry e = c.propose(put(1));
        c.runUntil(() -> leader.commitIndex() >= e.index(), 5, "commit with 3 of 5");
        assertEquals(0, c.nodes.get(others.get(0)).log().entry(e.index()).stream().count());
    }

    @Test
    void leaderWithoutMajorityNeverCommits() {
        TestCluster c = new TestCluster(5, 4);
        RaftNode leader = c.awaitLeader(10 * MAX);
        c.awaitCommitEverywhere(1, 5);
        long before = leader.commitIndex();
        Set<NodeId> minority = Set.of(leader.id(), c.ids().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow());
        Set<NodeId> majority = new HashSet<>(c.ids());
        majority.removeAll(minority);
        c.network.partition(minority, majority);

        for (int i = 0; i < 5; i++) leader.propose(put(i));
        c.run(3 * MAX);
        assertEquals(before, leader.commitIndex(), "2 of 5 is not a majority");
    }

    @Test
    void uncommittedEntriesOfDeposedLeaderAreOverwritten() {
        TestCluster c = new TestCluster(5, 5);
        RaftNode old = c.awaitLeader(10 * MAX);
        c.awaitCommitEverywhere(1, 5);
        c.network.isolate(old.id());
        LogEntry lost = old.propose(Command.put("lost", "x"));

        Set<NodeId> rest = new HashSet<>(c.ids());
        rest.remove(old.id());
        c.awaitLeader(rest, 10 * MAX);
        LogEntry kept = c.leaderAmong(rest).orElseThrow().propose(Command.put("kept", "y"));
        c.leaderAmong(rest).orElseThrow(); // still leading
        c.runUntil(() -> c.leaderAmong(rest).orElseThrow().commitIndex() >= kept.index(), 5, "commit in majority");

        c.network.heal();
        RaftNode leader = c.awaitLeader(10 * MAX);
        c.awaitCommitEverywhere(kept.index(), 10 * MAX);
        assertNotEquals(Role.LEADER, old.role());
        assertTrue(old.log().entriesFrom(1, 100).stream().noneMatch(e -> e.equals(lost)),
                "the old leader's uncommitted entry is gone");
        assertEquals(leader.log().entriesFrom(1, 100), old.log().entriesFrom(1, 100));
        c.assertCommittedEverywhere();
    }

    @Test
    void laggingFollowerCatchesUpInBatches() {
        TestCluster c = new TestCluster(3, 6, new RaftConfig(15, 30, 5, 8));
        RaftNode leader = c.awaitLeader(10 * MAX);
        NodeId slow = c.ids().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.network.isolate(slow);
        LogEntry last = null;
        for (int i = 0; i < 200; i++) last = leader.propose(put(i));
        c.run(2);
        assertEquals(1, c.nodes.get(slow).log().lastIndex(), "isolated follower only has the no-op");

        c.network.heal();
        final long target = last.index();
        c.awaitCommitEverywhere(target, 10);
        assertEquals(target, leader.matchIndexOf(slow).orElseThrow());
    }

    @Test
    void followerWithLongConflictingSuffixConverges() {
        // A deposed leader piles up many uncommitted entries; the new leader must probe back
        // past all of them. The conflict hint skips the whole stale term at once.
        TestCluster c = new TestCluster(3, 7);
        RaftNode old = c.awaitLeader(10 * MAX);
        c.awaitCommitEverywhere(1, 5);
        c.network.isolate(old.id());
        for (int i = 0; i < 100; i++) old.propose(Command.put("stale" + i, "x"));

        Set<NodeId> rest = new HashSet<>(c.ids());
        rest.remove(old.id());
        c.awaitLeader(rest, 10 * MAX);
        c.network.heal();
        RaftNode leader = c.awaitLeader(10 * MAX);
        c.runUntil(() -> old.log().lastIndex() == leader.log().lastIndex()
                && old.log().lastTerm() == leader.log().lastTerm(), 10, "old leader's log converging");
        assertEquals(leader.log().entriesFrom(1, 1000), old.log().entriesFrom(1, 1000));
    }

    @Test
    void proposingOnAFollowerRedirectsToTheLeader() {
        TestCluster c = new TestCluster(3, 8);
        RaftNode leader = c.awaitLeader(10 * MAX);
        RaftNode follower = c.nodes.values().stream().filter(n -> n != leader).findFirst().orElseThrow();
        NotLeaderException e = assertThrows(NotLeaderException.class, () -> follower.propose(put(1)));
        assertEquals(leader.id(), e.leaderHint().orElseThrow());
    }

    /**
     * Random partitions, 10% message loss and a steady stream of proposals, over many seeds.
     * Throughout, every commit is checked against every earlier commit; at the end the network
     * heals and every node must hold every committed entry.
     */
    @Test
    void safetyUnderRandomPartitionsAndLoss() {
        for (long seed = 0; seed < 100; seed++) {
            TestCluster c = new TestCluster(5, seed);
            Random rnd = new Random(seed * 31 + 7);
            c.network.dropIf(e -> rnd.nextInt(10) == 0);
            List<NodeId> ids = new ArrayList<>(c.ids());
            int proposed = 0;
            for (int step = 0; step < 1500; step++) {
                if (step % 100 == 0) {
                    java.util.Collections.shuffle(ids, rnd);
                    int cut = rnd.nextInt(ids.size());
                    if (cut == 0) c.network.heal();
                    else c.network.partition(Set.copyOf(ids.subList(0, cut)), Set.copyOf(ids.subList(cut, ids.size())));
                    c.network.dropIf(e -> rnd.nextInt(10) == 0);
                }
                for (RaftNode n : c.leaders()) {
                    if (rnd.nextInt(3) == 0) n.propose(put(proposed++));
                }
                c.step();
            }
            c.network.heal();
            RaftNode leader = c.awaitLeader(20 * MAX);
            LogEntry marker = leader.propose(Command.put("final", "seed" + seed));
            c.awaitCommitEverywhere(marker.index(), 20 * MAX);

            c.assertElectionSafety();
            c.assertLogMatching();
            c.assertCommittedEverywhere();
            assertTrue(c.committed().size() > 10, "seed " + seed + " made progress");
        }
    }
}
