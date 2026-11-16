package com.raftkv.node;

import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.rpc.AppendEntries;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class LeaderElectionTest {

    private static final int MAX = RaftConfig.DEFAULT.electionTimeoutMax();

    @Test
    void singleNodeElectsItself() {
        TestCluster c = new TestCluster(1, 1);
        RaftNode n1 = c.awaitLeader(MAX + 1);
        assertEquals(1, n1.term());
        assertEquals(n1.id(), n1.votedFor().orElseThrow());
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 5, 7})
    void clusterElectsExactlyOneLeader(int size) {
        TestCluster c = new TestCluster(size, 42);
        RaftNode leader = c.awaitLeader(10 * MAX);
        assertEquals(1, c.leaders().size());
        c.nodes.values().forEach(n -> assertEquals(leader.term(), n.term()));
        c.assertElectionSafety();
    }

    @Test
    void heartbeatsKeepAStableLeader() {
        TestCluster c = new TestCluster(5, 7);
        RaftNode leader = c.awaitLeader(10 * MAX);
        long term = leader.term();
        c.run(50 * MAX);
        assertTrue(leader.isLeader());
        assertEquals(term, leader.term(), "no new elections while heartbeats flow");
    }

    @Test
    void majorityElectsNewLeaderWhenLeaderIsCutOff() {
        TestCluster c = new TestCluster(5, 3);
        RaftNode old = c.awaitLeader(10 * MAX);
        c.network.isolate(old.id());

        Set<NodeId> rest = new HashSet<>(c.ids());
        rest.remove(old.id());
        RaftNode next = c.awaitLeader(rest, 10 * MAX);
        assertNotEquals(old.id(), next.id());
        assertTrue(next.term() > old.term());
        assertTrue(old.isLeader(), "the cut-off leader does not know yet");

        c.network.heal();
        c.runUntil(() -> !old.isLeader(), 2 * MAX, "old leader stepping down");
        c.awaitLeader(10 * MAX);
        assertTrue(old.term() >= next.term(), "the old leader learned the newer term");
        c.assertElectionSafety();
    }

    @Test
    void minorityCannotElectALeader() {
        TestCluster c = new TestCluster(5, 11);
        c.awaitLeader(10 * MAX);
        Set<NodeId> minority = Set.of(NodeId.of("n1"), NodeId.of("n2"));
        Set<NodeId> majority = Set.of(NodeId.of("n3"), NodeId.of("n4"), NodeId.of("n5"));
        c.network.partition(minority, majority);

        c.run(20 * MAX);
        minority.forEach(id -> assertNotEquals(Role.LEADER, c.nodes.get(id).role(),
                id + " must not lead with 2 of 5 votes"));
        c.awaitLeader(majority, 10 * MAX);
        c.assertElectionSafety();
    }

    @Test
    void rejoiningMinorityWithHigherTermDoesNotBreakSafety() {
        TestCluster c = new TestCluster(3, 5);
        RaftNode leader = c.awaitLeader(10 * MAX);
        NodeId loner = c.ids().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.network.isolate(loner);
        c.run(10 * MAX); // loner keeps timing out and bumping its term
        assertTrue(c.nodes.get(loner).term() > leader.term());

        c.network.heal();
        RaftNode after = c.awaitLeader(20 * MAX);
        assertTrue(after.term() >= c.nodes.get(loner).term());
        c.assertElectionSafety();
    }

    @Test
    void followerThatMissesHeartbeatsCausesASafeReElection() {
        TestCluster c = new TestCluster(3, 9);
        RaftNode leader = c.awaitLeader(10 * MAX);
        long term = leader.term();
        RaftNode follower = c.nodes.values().stream().filter(n -> n != leader).findFirst().orElseThrow();

        c.network.dropIf(e -> e.to().equals(follower.id()) && e.message() instanceof AppendEntries);
        c.runUntil(() -> follower.term() > term, 2 * MAX, "follower starting an election");
        c.network.heal();
        RaftNode next = c.awaitLeader(10 * MAX);
        assertTrue(next.term() > term);
        c.assertElectionSafety();
    }

    @Test
    void electionTimeoutsAreRandomizedWithinRange() {
        TestCluster c = new TestCluster(7, 1234);
        Set<Integer> seen = new HashSet<>();
        for (RaftNode n : c.nodes.values()) {
            int t = n.electionTimeout();
            assertTrue(t >= RaftConfig.DEFAULT.electionTimeoutMin() && t < MAX, "timeout " + t);
            seen.add(t);
        }
        assertTrue(seen.size() > 1, "timeouts should differ between nodes");
    }

    /** Many seeds with 25% random message loss: never two leaders in one term, and a leader emerges. */
    @Test
    void electionSafetyUnderRandomMessageLoss() {
        for (long seed = 0; seed < 200; seed++) {
            TestCluster c = new TestCluster(5, seed);
            Random loss = new Random(seed);
            c.network.dropIf(e -> loss.nextInt(4) == 0);
            c.run(40 * MAX);
            c.assertElectionSafety();

            c.network.heal();
            c.awaitLeader(20 * MAX);
            c.assertElectionSafety();
        }
    }

    @Test
    void sameSeedGivesSameHistory() {
        TestCluster a = new TestCluster(5, 99);
        TestCluster b = new TestCluster(5, 99);
        a.run(20 * MAX);
        b.run(20 * MAX);
        assertEquals(a.describe(), b.describe());
        assertEquals(a.leadersByTerm(), b.leadersByTerm());
    }
}
