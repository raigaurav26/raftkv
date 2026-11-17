package com.raftkv.node;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;
import com.raftkv.core.PersistentState;
import com.raftkv.core.Role;
import com.raftkv.log.RaftLog;
import com.raftkv.rpc.AppendEntries;
import com.raftkv.rpc.AppendResponse;
import com.raftkv.rpc.RaftMessage;
import com.raftkv.rpc.RequestVote;
import com.raftkv.rpc.VoteResponse;
import com.raftkv.transport.Transport;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * One Raft server. This class holds the consensus rules and nothing else: no threads, no
 * sockets, no clock. Time moves only when {@link #tick()} is called and messages arrive only
 * through {@link #onMessage}, so the caller decides the order of every event. The runtime
 * serialises all calls on one thread; the test cluster calls it directly.
 *
 * <p>Covers leader election (Raft paper, section 5.2) and log replication with the commit
 * rule (sections 5.3 and 5.4). Applying committed entries to the key-value store comes next.
 */
public final class RaftNode {

    private final NodeId id;
    private final Set<NodeId> peers;
    private final Transport transport;
    private final RaftLog log;
    private final StateStore store;
    private final RaftConfig config;
    private final Random random;
    private final NodeListener listener;

    private PersistentState state;
    private Role role = Role.FOLLOWER;
    private NodeId leaderId;
    private final Set<NodeId> votes = new HashSet<>();
    private long commitIndex;

    // Leader-only, rebuilt on every election (figure 2, "volatile state on leaders").
    private final Map<NodeId, Long> nextIndex = new HashMap<>();
    private final Map<NodeId, Long> matchIndex = new HashMap<>();

    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;

    public RaftNode(NodeId id, Set<NodeId> peers, Transport transport, RaftLog log,
                    StateStore store, RaftConfig config, Random random, NodeListener listener) {
        this.id = Objects.requireNonNull(id, "id");
        this.peers = Set.copyOf(peers);
        if (this.peers.contains(id)) throw new IllegalArgumentException("peers must not include the node itself");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.log = Objects.requireNonNull(log, "log");
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.random = Objects.requireNonNull(random, "random");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.state = store.load();
        resetElectionTimer();
        transport.register(id, this::onMessage);
    }

    // ---------------------------------------------------------------- time

    /** Advances this node's clock by one tick. */
    public void tick() {
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= config.heartbeatInterval()) {
                heartbeatElapsed = 0;
                broadcastAppend();
            }
        } else if (++electionElapsed >= electionTimeout) {
            startElection();
        }
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        electionTimeout = config.electionTimeoutMin()
                + random.nextInt(config.electionTimeoutMax() - config.electionTimeoutMin());
    }

    // ---------------------------------------------------------------- elections

    private void startElection() {
        role = role.moveTo(Role.CANDIDATE);
        persist(state.advanceTo(state.currentTerm() + 1).voteFor(id));
        leaderId = null;
        votes.clear();
        votes.add(id);
        resetElectionTimer();
        listener.roleChanged(id, role, term());

        if (hasMajority(votes)) {
            becomeLeader(); // single-node cluster
            return;
        }
        RequestVote request = new RequestVote(term(), id, log.lastIndex(), log.lastTerm());
        for (NodeId peer : peers) transport.send(id, peer, request);
    }

    private void becomeLeader() {
        role = role.moveTo(Role.LEADER);
        leaderId = id;
        heartbeatElapsed = 0;
        nextIndex.clear();
        matchIndex.clear();
        for (NodeId peer : peers) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }
        listener.roleChanged(id, role, term());
        // A leader may only count replicas for entries of its own term (section 5.4.2), so it
        // appends an empty entry at once; committing it also commits everything before it.
        log.append(term(), Command.noop());
        advanceCommitIndex(); // single-node cluster commits on its own
        broadcastAppend(); // also announces the new leader so other candidates step down
    }

    private void becomeFollower(long newTerm, NodeId knownLeader) {
        persist(state.advanceTo(newTerm));
        boolean changed = role != Role.FOLLOWER;
        if (changed) role = role.moveTo(Role.FOLLOWER);
        leaderId = knownLeader;
        votes.clear();
        nextIndex.clear();
        matchIndex.clear();
        resetElectionTimer();
        if (changed) listener.roleChanged(id, role, term());
    }

    private boolean hasMajority(Set<NodeId> granted) {
        return granted.size() * 2 > peers.size() + 1;
    }

    // ---------------------------------------------------------------- replication (leader)

    /**
     * Appends a client command to the leader's log and starts replicating it. The entry is
     * not durable in the cluster until it is committed; callers watch the commit index.
     *
     * @throws NotLeaderException on a follower or candidate, with the leader if known
     */
    public LogEntry propose(Command command) {
        if (role != Role.LEADER) throw new NotLeaderException(id, leaderId);
        LogEntry entry = log.append(term(), command);
        advanceCommitIndex();
        for (NodeId peer : peers) {
            // Only peers that are caught up get a message now; the rest are mid-probe and
            // will get entries with their next reply or heartbeat.
            if (nextIndex.get(peer) == entry.index()) sendAppend(peer);
        }
        return entry;
    }

    /** Sends every follower what it is missing. With nothing missing this is a heartbeat. */
    private void broadcastAppend() {
        for (NodeId peer : peers) sendAppend(peer);
    }

    private void sendAppend(NodeId peer) {
        long next = nextIndex.get(peer);
        long prevIndex = next - 1;
        long prevTerm = log.termAt(prevIndex).orElseThrow();
        List<LogEntry> entries = log.entriesFrom(next, config.maxEntriesPerAppend());
        transport.send(id, peer, new AppendEntries(term(), id, prevIndex, prevTerm, entries, commitIndex));
    }

    private void handleAppendResponse(NodeId from, AppendResponse ar) {
        if (role != Role.LEADER || ar.term() != term()) return; // stale reply
        if (ar.success()) {
            // max(): replies can arrive out of order, and matchIndex must never go backwards.
            long match = Math.max(matchIndex.get(from), ar.matchIndex());
            matchIndex.put(from, match);
            nextIndex.put(from, Math.max(nextIndex.get(from), match + 1));
            advanceCommitIndex();
            if (nextIndex.get(from) <= log.lastIndex()) sendAppend(from); // keep streaming
        } else {
            // Jump back to the follower's hint, but never below what it already matched and
            // always at least one step, so the probe makes progress.
            long current = nextIndex.get(from);
            long next = Math.max(matchIndex.get(from) + 1, Math.min(current - 1, ar.conflictIndex()));
            if (next < current) {
                nextIndex.put(from, next);
                sendAppend(from);
            }
        }
    }

    /**
     * Moves the commit index to the highest N such that a majority has the entry at N and that
     * entry is from the current term (figure 2, last rule for leaders). Entries from earlier
     * terms are never committed by counting replicas; they commit along with a later entry
     * from this term. Skipping that check is the bug shown in figure 8 of the paper.
     */
    private void advanceCommitIndex() {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n).orElseThrow() != term()) break; // older terms below; stop
            int replicas = 1; // the leader itself
            for (long m : matchIndex.values()) if (m >= n) replicas++;
            if (replicas * 2 > peers.size() + 1) {
                setCommitIndex(n);
                return;
            }
        }
    }

    private void setCommitIndex(long index) {
        if (index > commitIndex) {
            commitIndex = index;
            listener.committed(id, commitIndex);
        }
    }

    // ---------------------------------------------------------------- messages

    public void onMessage(NodeId from, RaftMessage message) {
        if (message.term() > term()) {
            // Any newer term makes us a follower. An AppendEntries also tells us who leads it.
            NodeId leader = message instanceof AppendEntries ae ? ae.leaderId() : null;
            becomeFollower(message.term(), leader);
        }
        switch (message) {
            case RequestVote rv -> handleRequestVote(from, rv);
            case VoteResponse vr -> handleVoteResponse(from, vr);
            case AppendEntries ae -> handleAppendEntries(from, ae);
            case AppendResponse ar -> handleAppendResponse(from, ar);
        }
    }

    private void handleRequestVote(NodeId from, RequestVote rv) {
        boolean grant = rv.term() == term()
                && state.canVoteFor(rv.candidateId())
                && log.candidateIsUpToDate(rv.lastLogTerm(), rv.lastLogIndex());
        if (grant) {
            persist(state.voteFor(rv.candidateId())); // durable before the reply leaves
            resetElectionTimer(); // we just helped someone else run; give them time to win
        }
        transport.send(id, from, new VoteResponse(term(), grant));
    }

    private void handleVoteResponse(NodeId from, VoteResponse vr) {
        if (role != Role.CANDIDATE || vr.term() != term() || !vr.granted()) return;
        votes.add(from);
        if (hasMajority(votes)) becomeLeader();
    }

    private void handleAppendEntries(NodeId from, AppendEntries ae) {
        if (ae.term() < term()) {
            transport.send(id, from, AppendResponse.staleTerm(term()));
            return;
        }
        // Same term: a valid leader exists, so a candidate gives up (section 5.2).
        if (role == Role.CANDIDATE) becomeFollower(ae.term(), ae.leaderId());
        if (role == Role.LEADER) {
            throw new IllegalStateException(id + " saw another leader " + ae.leaderId() + " in term " + term());
        }
        leaderId = ae.leaderId();
        resetElectionTimer();

        if (log.appendFromLeader(ae.prevLogIndex(), ae.prevLogTerm(), ae.entries())) {
            // Only up to the last entry this message vouched for: anything after it in our log
            // may still be a stale suffix the leader has not overwritten yet.
            setCommitIndex(Math.min(ae.leaderCommit(), ae.lastNewIndex()));
            transport.send(id, from, AppendResponse.success(term(), ae.lastNewIndex()));
        } else {
            transport.send(id, from, AppendResponse.conflict(term(), conflictHint(ae.prevLogIndex())));
        }
    }

    /**
     * Where the leader should retry from after a failed consistency check: just past our log
     * if it is too short, otherwise the first index of the term that conflicts at prevIndex.
     */
    private long conflictHint(long prevIndex) {
        if (prevIndex > log.lastIndex()) return log.lastIndex() + 1;
        long conflictTerm = log.termAt(prevIndex).orElseThrow();
        long i = prevIndex;
        while (i > 1 && log.termAt(i - 1).orElseThrow() == conflictTerm) i--;
        return i;
    }

    private void persist(PersistentState next) {
        if (!next.equals(state)) {
            store.save(next);
            state = next;
        }
    }

    // ---------------------------------------------------------------- views

    public NodeId id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public long term() {
        return state.currentTerm();
    }

    public Optional<NodeId> votedFor() {
        return state.vote();
    }

    public Optional<NodeId> leaderId() {
        return Optional.ofNullable(leaderId);
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public RaftLog log() {
        return log;
    }

    public long commitIndex() {
        return commitIndex;
    }

    /** Leader view of how far {@code peer}'s log is known to match; empty on non-leaders. */
    public Optional<Long> matchIndexOf(NodeId peer) {
        return Optional.ofNullable(matchIndex.get(peer));
    }

    int electionTimeout() {
        return electionTimeout;
    }
}
