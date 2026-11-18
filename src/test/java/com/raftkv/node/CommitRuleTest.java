package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.log.InMemoryRaftLog;
import com.raftkv.rpc.AppendEntries;
import com.raftkv.rpc.AppendResponse;
import com.raftkv.rpc.RaftMessage;
import com.raftkv.rpc.VoteResponse;
import com.raftkv.transport.MessageHandler;
import com.raftkv.transport.Transport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** The commit rules, driven by hand-written replies on a single node. */
class CommitRuleTest {

    private static final NodeId ME = NodeId.of("n1");
    private static final NodeId A = NodeId.of("n2");
    private static final NodeId B = NodeId.of("n3");

    private final List<RaftMessage> sent = new ArrayList<>();
    private final InMemoryRaftLog log = new InMemoryRaftLog();
    private final InMemoryStateStore store = new InMemoryStateStore();

    private RaftNode node() {
        Transport capture = new Transport() {
            @Override public void register(NodeId n, MessageHandler h) { }
            @Override public void unregister(NodeId n) { }
            @Override public void send(NodeId from, NodeId to, RaftMessage m) { sent.add(m); }
        };
        return new RaftNode(ME, Set.of(A, B), capture, log, store, new com.raftkv.kv.KvStateMachine(), RaftConfig.DEFAULT, new Random(0), NodeListener.NONE);
    }

    private RaftNode electedLeader() {
        RaftNode n = node();
        while (n.role() != Role.CANDIDATE) n.tick();
        n.onMessage(A, VoteResponse.grant(n.term()));
        assertEquals(Role.LEADER, n.role());
        return n;
    }

    /**
     * Figure 8 of the paper: an entry from an older term that sits on a majority is still not
     * safe to commit, because a different leader could overwrite it. The leader commits it
     * only once an entry from its own term (here the no-op) also reaches a majority.
     */
    @Test
    void doesNotCommitOlderTermEntriesByCountingReplicas() {
        store.save(store.load().advanceTo(2));
        log.append(2, Command.put("old", "x")); // index 1, term 2, left by an earlier leader
        RaftNode leader = electedLeader();      // term 3; appends no-op at index 2
        assertEquals(3, leader.term());
        assertEquals(2, leader.log().lastIndex());

        leader.onMessage(A, AppendResponse.success(3, 1)); // A has index 1 only
        assertEquals(0, leader.commitIndex(), "index 1 is on 2 of 3 nodes but from term 2");

        leader.onMessage(A, AppendResponse.success(3, 2));
        assertEquals(2, leader.commitIndex(), "the no-op commits, and index 1 with it");
    }

    @Test
    void outOfOrderRepliesNeverMoveMatchIndexBackwards() {
        RaftNode leader = electedLeader();
        for (int i = 0; i < 5; i++) leader.propose(Command.put("k" + i, "v"));
        leader.onMessage(A, AppendResponse.success(leader.term(), 6));
        leader.onMessage(A, AppendResponse.success(leader.term(), 3)); // delayed older reply
        assertEquals(6, leader.matchIndexOf(A).orElseThrow());
        assertEquals(6, leader.commitIndex());
    }

    @Test
    void conflictReplyMovesNextIndexBackAndResends() {
        log.append(1, Command.noop());
        log.append(1, Command.noop());
        log.append(1, Command.noop());
        store.save(store.load().advanceTo(1));
        RaftNode leader = electedLeader(); // log now 1 1 1 2(no-op)
        sent.clear();
        leader.onMessage(B, AppendResponse.conflict(leader.term(), 2));
        AppendEntries retry = (AppendEntries) sent.get(sent.size() - 1);
        assertEquals(1, retry.prevLogIndex());
        assertEquals(3, retry.entries().size());
    }

    @Test
    void followerCommitsOnlyWhatTheLeaderVouchedFor() {
        RaftNode f = node();
        // Follower has a stale suffix at index 2 from term 1.
        f.onMessage(A, new AppendEntries(1, A, 0, 0,
                List.of(new com.raftkv.core.LogEntry(1, 1, Command.noop()),
                        new com.raftkv.core.LogEntry(1, 2, Command.put("stale", "x"))), 0));
        // New leader in term 2 commits up to 5 but this heartbeat only checks index 1.
        f.onMessage(B, AppendEntries.heartbeat(2, B, 1, 1, 5));
        assertEquals(1, f.commitIndex(), "index 2 might still be overwritten");
    }

    @Test
    void staleRepliesFromOldTermsAreIgnored() {
        store.save(store.load().advanceTo(4));
        RaftNode leader = electedLeader(); // term 5
        leader.onMessage(A, AppendResponse.success(4, 1));
        assertEquals(0, leader.matchIndexOf(A).orElseThrow());
    }
}
