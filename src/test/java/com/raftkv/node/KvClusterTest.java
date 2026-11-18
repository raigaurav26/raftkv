package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.NodeId;
import com.raftkv.kv.KvService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class KvClusterTest {

    private static final int MAX = RaftConfig.DEFAULT.electionTimeoutMax();

    private static <T> T await(TestCluster c, CompletableFuture<T> f) {
        c.runUntil(f::isDone, 10 * MAX, "future to complete");
        return f.join();
    }

    private static Throwable failure(TestCluster c, CompletableFuture<?> f) {
        c.runUntil(f::isDone, 20 * MAX, "future to complete");
        CompletionException e = assertThrows(CompletionException.class, f::join);
        return e.getCause();
    }

    @Test
    void writeThenReadThroughTheLeader() {
        TestCluster c = new TestCluster(3, 1);
        KvService kv = c.kv(c.awaitLeader(10 * MAX).id());
        assertEquals(Optional.empty(), await(c, kv.put("lang", "java")));
        assertEquals(Optional.of("java"), await(c, kv.put("lang", "java21")));
        assertEquals(Optional.of("java21"), await(c, kv.get("lang")));
        assertEquals(Optional.of("java21"), await(c, kv.delete("lang")));
        assertEquals(Optional.empty(), await(c, kv.get("lang")));
    }

    @Test
    void everyNodeAppliesTheSameWrites() {
        TestCluster c = new TestCluster(5, 2);
        KvService kv = c.kv(c.awaitLeader(10 * MAX).id());
        CompletableFuture<?> last = null;
        for (int i = 0; i < 50; i++) last = kv.put("k" + (i % 7), "v" + i);
        await(c, last);
        c.awaitCommitEverywhere(c.leaders().get(0).commitIndex(), 5);
        c.run(1);
        c.assertStateMachinesAgree();
        assertEquals(7, c.kvs.values().iterator().next().size());
    }

    @Test
    void singleNodeServesReadsWithoutARoundTrip() {
        TestCluster c = new TestCluster(1, 3);
        KvService kv = c.kv(c.awaitLeader(MAX + 1).id());
        assertTrue(kv.put("a", "1").isDone());
        CompletableFuture<Optional<String>> read = kv.get("a");
        assertTrue(read.isDone());
        assertEquals(Optional.of("1"), read.join());
    }

    @Test
    void followersRedirectClients() {
        TestCluster c = new TestCluster(3, 4);
        RaftNode leader = c.awaitLeader(10 * MAX);
        NodeId follower = c.ids().stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        Throwable w = assertThrows(CompletionException.class, () -> c.kv(follower).put("a", "1").join()).getCause();
        Throwable r = assertThrows(CompletionException.class, () -> c.kv(follower).get("a").join()).getCause();
        assertEquals(leader.id(), ((NotLeaderException) w).leaderHint().orElseThrow());
        assertInstanceOf(NotLeaderException.class, r);
    }

    /**
     * The classic stale-read bug: a leader that is cut off keeps thinking it leads. If it served
     * reads from local state it would return the old value after the new leader acknowledged a
     * newer write. With the read barrier the read never completes while cut off, and fails when
     * the old leader finds out it was replaced.
     */
    @Test
    void deposedLeaderNeverServesAStaleRead() {
        TestCluster c = new TestCluster(5, 5);
        RaftNode old = c.awaitLeader(10 * MAX);
        await(c, c.kv(old.id()).put("x", "old"));

        c.network.isolate(old.id());
        Set<NodeId> rest = new HashSet<>(c.ids());
        rest.remove(old.id());
        RaftNode next = c.awaitLeader(rest, 10 * MAX);
        await(c, c.kv(next.id()).put("x", "new"));

        CompletableFuture<Optional<String>> staleRead = c.kv(old.id()).get("x");
        c.run(5 * MAX);
        assertFalse(staleRead.isDone(), "no majority confirms the old leader");

        c.network.heal();
        assertInstanceOf(LeadershipLostException.class, failure(c, staleRead));
        assertEquals(Optional.of("new"), await(c, c.kv(c.awaitLeader(10 * MAX).id()).get("x")));
    }

    @Test
    void writeOnDeposedLeaderFailsInsteadOfHanging() {
        TestCluster c = new TestCluster(3, 6);
        RaftNode old = c.awaitLeader(10 * MAX);
        c.awaitCommitEverywhere(1, 5);
        c.network.isolate(old.id());
        CompletableFuture<Optional<String>> lost = c.kv(old.id()).put("x", "lost");

        Set<NodeId> rest = new HashSet<>(c.ids());
        rest.remove(old.id());
        RaftNode next = c.awaitLeader(rest, 10 * MAX);
        await(c, c.kv(next.id()).put("x", "kept"));
        c.network.heal();

        assertInstanceOf(LeadershipLostException.class, failure(c, lost));
        assertEquals(Optional.of("kept"), c.kvs.get(old.id()).get("x"));
    }

    @Test
    void readWaitsForTheNewLeadersFirstCommit() {
        TestCluster c = new TestCluster(3, 7);
        RaftNode leader = c.awaitLeader(10 * MAX);
        // Right after election the no-op may not be committed yet; a read must not jump ahead.
        CompletableFuture<Long> barrier = leader.readBarrier();
        c.runUntil(barrier::isDone, 5, "read barrier");
        long index = barrier.join();
        assertTrue(index >= 1);
        assertEquals(leader.term(), leader.log().termAt(index).orElseThrow());
    }

    /**
     * One client writes a counter 1, 2, 3, ... (each write after the previous one completes)
     * while another reads it, under random partitions and loss. Linearizability for this
     * pattern means a read never returns less than the last write that completed before the
     * read started. At the end every node must hold the same map.
     */
    @Test
    void readsNeverGoBackInTimeUnderFaults() {
        for (long seed = 0; seed < 60; seed++) {
            TestCluster c = new TestCluster(5, seed);
            Random rnd = new Random(seed ^ 0x5DEECE66DL);
            List<NodeId> ids = new ArrayList<>(c.ids());
            long[] lastAcked = {0};
            int[] reads = {0};
            CompletableFuture<?> inflightWrite = null;
            long nextValue = 1;
            List<CompletableFuture<?>> checks = new ArrayList<>();

            for (int step = 0; step < 1200; step++) {
                if (step % 120 == 0) {
                    java.util.Collections.shuffle(ids, rnd);
                    int cut = rnd.nextInt(ids.size());
                    if (cut == 0) c.network.heal();
                    else c.network.partition(Set.copyOf(ids.subList(0, cut)), Set.copyOf(ids.subList(cut, ids.size())));
                    c.network.dropIf(e -> rnd.nextInt(10) == 0);
                }
                List<RaftNode> leaders = c.leaders();
                if (!leaders.isEmpty()) {
                    RaftNode target = leaders.get(rnd.nextInt(leaders.size()));
                    if (inflightWrite == null || inflightWrite.isDone()) {
                        long value = nextValue++;
                        inflightWrite = c.kv(target.id()).put("counter", Long.toString(value))
                                .whenComplete((r, err) -> { if (err == null) lastAcked[0] = Math.max(lastAcked[0], value); });
                    }
                    if (rnd.nextInt(4) == 0) {
                        long floor = lastAcked[0];
                        checks.add(c.kv(target.id()).get("counter").thenAccept(v -> {
                            long seen = v.map(Long::parseLong).orElse(0L);
                            if (seen < floor) throw new AssertionError("read " + seen + " after write " + floor + " completed");
                            reads[0]++;
                        }));
                    }
                }
                c.step();
            }
            c.network.heal();
            RaftNode leader = c.awaitLeader(20 * MAX);
            await(c, c.kv(leader.id()).put("done", "yes"));
            c.awaitCommitEverywhere(leader.commitIndex(), 20 * MAX);
            c.run(1);

            for (CompletableFuture<?> f : checks) {
                if (f.isCompletedExceptionally()) {
                    Throwable t = assertThrows(CompletionException.class, f::join).getCause();
                    if (t instanceof AssertionError ae) throw ae;
                    assertTrue(t instanceof LeadershipLostException || t instanceof NotLeaderException, t.toString());
                }
            }
            c.assertStateMachinesAgree();
            assertTrue(reads[0] > 0, "seed " + seed + " completed some reads");
        }
    }

    @Test
    void submitReturnsStateMachineResult() {
        TestCluster c = new TestCluster(3, 8);
        RaftNode leader = c.awaitLeader(10 * MAX);
        CompletableFuture<Object> f = leader.submit(Command.put("a", "1"));
        assertNotNull(await(c, f));
    }
}
