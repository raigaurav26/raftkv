package com.raftkv.node;

import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.kv.KvStateMachine;
import com.raftkv.log.InMemoryRaftLog;
import com.raftkv.rpc.AppendEntries;
import com.raftkv.rpc.AppendResponse;
import com.raftkv.rpc.RaftMessage;
import com.raftkv.rpc.VoteResponse;
import com.raftkv.transport.MessageHandler;
import com.raftkv.transport.Transport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/** The read barrier's rules, with hand-written replies on a single 3-node leader. */
class ReadIndexTest {

    private static final NodeId ME = NodeId.of("n1");
    private static final NodeId A = NodeId.of("n2");
    private static final NodeId B = NodeId.of("n3");

    private final List<RaftMessage> sent = new ArrayList<>();
    private RaftNode leader;

    @BeforeEach
    void setUp() {
        Transport capture = new Transport() {
            @Override public void register(NodeId n, MessageHandler h) { }
            @Override public void unregister(NodeId n) { }
            @Override public void send(NodeId from, NodeId to, RaftMessage m) { sent.add(m); }
        };
        leader = new RaftNode(ME, Set.of(A, B), capture, new InMemoryRaftLog(), new InMemoryStateStore(),
                new KvStateMachine(), RaftConfig.DEFAULT, new Random(0), NodeListener.NONE);
        while (leader.role() != Role.CANDIDATE) leader.tick();
        leader.onMessage(A, VoteResponse.grant(1));
        assertEquals(Role.LEADER, leader.role());
    }

    private long lastSeq() {
        for (int i = sent.size() - 1; i >= 0; i--) {
            if (sent.get(i) instanceof AppendEntries ae) return ae.seq();
        }
        throw new AssertionError("nothing sent");
    }

    @Test
    void waitsUntilOwnTermEntryIsCommitted() {
        long electionSeq = lastSeq();
        CompletableFuture<Long> read = leader.readBarrier();
        long readSeq = lastSeq();
        assertTrue(readSeq > electionSeq, "the barrier sends a fresh round");

        leader.onMessage(A, AppendResponse.conflict(1, 1).withSeq(readSeq));
        assertFalse(read.isDone(), "leadership confirmed, but the no-op is not committed yet");

        leader.onMessage(A, AppendResponse.success(1, 1).withSeq(readSeq));
        assertEquals(1, leader.commitIndex());
        assertEquals(1L, read.join());
    }

    @Test
    void lateRepliesToOlderMessagesDoNotConfirmLeadership() {
        long before = lastSeq();
        leader.onMessage(A, AppendResponse.success(1, 1).withSeq(before)); // commit the no-op
        CompletableFuture<Long> read = leader.readBarrier();
        leader.onMessage(B, AppendResponse.success(1, 1).withSeq(before)); // answers an old message
        assertFalse(read.isDone());
        leader.onMessage(B, AppendResponse.success(1, 1).withSeq(lastSeq()));
        assertTrue(read.isDone());
    }

    @Test
    void steppingDownFailsWaitingReads() {
        CompletableFuture<Long> read = leader.readBarrier();
        leader.onMessage(B, AppendEntries.heartbeat(2, B, 0, 0, 0));
        assertTrue(read.isCompletedExceptionally());
        assertThrows(Exception.class, read::join);
    }
}
