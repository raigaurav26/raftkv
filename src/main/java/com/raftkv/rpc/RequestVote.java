package com.raftkv.rpc;

import com.raftkv.core.NodeId;

import java.util.Objects;

/** Sent by a candidate to ask for a vote (Raft paper, figure 2). */
public record RequestVote(long term, NodeId candidateId, long lastLogIndex, long lastLogTerm)
        implements RaftMessage {

    public RequestVote {
        if (term < 1) throw new IllegalArgumentException("term must be >= 1");
        Objects.requireNonNull(candidateId, "candidateId");
        if (lastLogIndex < 0 || lastLogTerm < 0) {
            throw new IllegalArgumentException("last log index and term must be >= 0");
        }
        if ((lastLogIndex == 0) != (lastLogTerm == 0)) {
            throw new IllegalArgumentException("an empty log has index 0 and term 0 together");
        }
        if (lastLogTerm > term) {
            throw new IllegalArgumentException("last log term cannot be newer than the candidate's term");
        }
    }
}
