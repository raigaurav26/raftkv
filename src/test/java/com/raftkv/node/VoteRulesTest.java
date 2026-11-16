package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.log.InMemoryRaftLog;
import com.raftkv.rpc.AppendEntries;
import com.raftkv.rpc.AppendResponse;
import com.raftkv.rpc.RaftMessage;
import com.raftkv.rpc.RequestVote;
import com.raftkv.rpc.VoteResponse;
import com.raftkv.transport.Transport;
import com.raftkv.transport.MessageHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Drives a single node with hand-written messages and checks its replies. */
class VoteRulesTest {

    private static final NodeId ME = NodeId.of("n1");
    private static final NodeId A = NodeId.of("n2");
    private static final NodeId B = NodeId.of("n3");

    private record Sent(NodeId to, RaftMessage message) { }

    private final List<Sent> sent = new ArrayList<>();
    private InMemoryRaftLog log;
    private InMemoryStateStore store;
    private RaftNode node;

    @BeforeEach
    void setUp() {
        sent.clear();
        log = new InMemoryRaftLog();
        store = new InMemoryStateStore();
        node = newNode();
    }

    private RaftNode newNode() {
        Transport capture = new Transport() {
            @Override public void register(NodeId n, MessageHandler h) { }
            @Override public void unregister(NodeId n) { }
            @Override public void send(NodeId from, NodeId to, RaftMessage m) { sent.add(new Sent(to, m)); }
        };
        return new RaftNode(ME, Set.of(A, B), capture, log, store, RaftConfig.DEFAULT, new Random(0), NodeListener.NONE);
    }

    private <T extends RaftMessage> T lastReply(Class<T> type) {
        return type.cast(sent.get(sent.size() - 1).message());
    }

    @Test
    void grantsVoteAndPersistsItBeforeReplying() {
        node.onMessage(A, new RequestVote(1, A, 0, 0));
        assertTrue(lastReply(VoteResponse.class).granted());
        assertEquals(A, store.load().votedFor(), "vote is on disk");
        assertEquals(1, store.load().currentTerm());
    }

    @Test
    void votesForOnlyOneCandidatePerTerm() {
        node.onMessage(A, new RequestVote(1, A, 0, 0));
        node.onMessage(B, new RequestVote(1, B, 0, 0));
        assertFalse(lastReply(VoteResponse.class).granted());
        node.onMessage(A, new RequestVote(1, A, 0, 0));
        assertTrue(lastReply(VoteResponse.class).granted(), "a retried request gets the same answer");
    }

    @Test
    void rejectsStaleTerm() {
        node.onMessage(A, new RequestVote(3, A, 0, 0));
        node.onMessage(B, new RequestVote(2, B, 0, 0));
        VoteResponse r = lastReply(VoteResponse.class);
        assertFalse(r.granted());
        assertEquals(3, r.term(), "the reply tells the stale candidate the newer term");
    }

    @Test
    void rejectsCandidateWithOlderLog() {
        log.append(2, Command.put("x", "1"));
        log.append(2, Command.put("y", "2"));
        node.onMessage(A, new RequestVote(3, A, 5, 1));
        assertFalse(lastReply(VoteResponse.class).granted(), "lower last term");
        node.onMessage(B, new RequestVote(3, B, 1, 2));
        assertFalse(lastReply(VoteResponse.class).granted(), "same last term, shorter log");
        assertEquals(3, node.term(), "the term still advanced");
        assertTrue(node.votedFor().isEmpty());
    }

    @Test
    void newerTermResetsVote() {
        node.onMessage(A, new RequestVote(1, A, 0, 0));
        node.onMessage(B, new RequestVote(2, B, 0, 0));
        assertTrue(lastReply(VoteResponse.class).granted());
        assertEquals(B, node.votedFor().orElseThrow());
    }

    @Test
    void timeoutStartsElectionWithSelfVote() {
        for (int i = 0; i < RaftConfig.DEFAULT.electionTimeoutMax(); i++) node.tick();
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(1, node.term());
        assertEquals(ME, store.load().votedFor());
        List<RequestVote> requests = sent.stream()
                .map(Sent::message).filter(RequestVote.class::isInstance).map(RequestVote.class::cast).toList();
        assertEquals(2, requests.size());
        assertEquals(Set.of(A, B), Set.copyOf(sent.stream().map(Sent::to).toList()));
    }

    @Test
    void ignoresVotesFromOldTerms() {
        for (int i = 0; i < RaftConfig.DEFAULT.electionTimeoutMax(); i++) node.tick(); // term 1
        for (int i = 0; i < RaftConfig.DEFAULT.electionTimeoutMax(); i++) node.tick(); // term 2
        assertEquals(2, node.term());
        node.onMessage(A, VoteResponse.grant(1));
        assertEquals(Role.CANDIDATE, node.role(), "a late vote from term 1 does not count in term 2");
        node.onMessage(A, VoteResponse.grant(2));
        assertEquals(Role.LEADER, node.role());
    }

    @Test
    void candidateStepsDownWhenLeaderOfSameTermAppears() {
        for (int i = 0; i < RaftConfig.DEFAULT.electionTimeoutMax(); i++) node.tick();
        assertEquals(Role.CANDIDATE, node.role());
        node.onMessage(A, AppendEntries.heartbeat(1, A, 0, 0, 0));
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(1, node.term());
        assertEquals(A, node.leaderId().orElseThrow());
        assertEquals(ME, node.votedFor().orElseThrow(), "stepping down in the same term keeps the vote");
    }

    @Test
    void rejectsAppendEntriesFromStaleLeader() {
        node.onMessage(A, new RequestVote(5, A, 0, 0));
        node.onMessage(B, AppendEntries.heartbeat(4, B, 0, 0, 0));
        AppendResponse r = lastReply(AppendResponse.class);
        assertFalse(r.success());
        assertEquals(5, r.term());
        assertTrue(node.leaderId().isEmpty());
    }

    @Test
    void appendEntriesTeachesFollowerItsLeader() {
        node.onMessage(B, AppendEntries.heartbeat(2, B, 0, 0, 0));
        assertEquals(B, node.leaderId().orElseThrow());
        assertEquals(2, node.term());
        assertTrue(lastReply(AppendResponse.class).success());
    }

    @Test
    void conflictHintSkipsWholeTerm() {
        log.append(1, Command.noop());
        log.append(2, Command.noop());
        log.append(2, Command.noop());
        log.append(2, Command.noop());
        node.onMessage(B, AppendEntries.heartbeat(3, B, 4, 3, 0));
        assertEquals(2, lastReply(AppendResponse.class).conflictIndex(), "first index of term 2");
        node.onMessage(B, AppendEntries.heartbeat(3, B, 9, 3, 0));
        assertEquals(5, lastReply(AppendResponse.class).conflictIndex(), "just past a short log");
    }

    @Test
    void restartKeepsTermAndVote() {
        node.onMessage(A, new RequestVote(4, A, 0, 0));
        RaftNode restarted = newNode();
        assertEquals(4, restarted.term());
        assertEquals(A, restarted.votedFor().orElseThrow());
        assertEquals(Role.FOLLOWER, restarted.role());
        restarted.onMessage(B, new RequestVote(4, B, 0, 0));
        assertFalse(lastReply(VoteResponse.class).granted(), "no second vote in term 4 after a restart");
    }
}
