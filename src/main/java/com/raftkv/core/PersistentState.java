package com.raftkv.core;

import java.util.Optional;

/**
 * The state every server must write to stable storage before answering an RPC
 * (Raft paper, figure 2): the latest term it has seen and who it voted for in that term.
 */
public record PersistentState(long currentTerm, NodeId votedFor) {

    public static final PersistentState INITIAL = new PersistentState(0, null);

    public PersistentState {
        if (currentTerm < 0) throw new IllegalArgumentException("term must be >= 0");
    }

    public Optional<NodeId> vote() {
        return Optional.ofNullable(votedFor);
    }

    /** Moves to a newer term, which clears the vote. Terms never go backwards. */
    public PersistentState advanceTo(long term) {
        if (term < currentTerm) {
            throw new IllegalArgumentException("term went backwards: " + currentTerm + " -> " + term);
        }
        return term == currentTerm ? this : new PersistentState(term, null);
    }

    /** Records a vote in the current term. A server votes at most once per term. */
    public PersistentState voteFor(NodeId candidate) {
        if (votedFor != null && !votedFor.equals(candidate)) {
            throw new IllegalStateException("already voted for " + votedFor + " in term " + currentTerm);
        }
        return new PersistentState(currentTerm, candidate);
    }

    public boolean canVoteFor(NodeId candidate) {
        return votedFor == null || votedFor.equals(candidate);
    }
}
