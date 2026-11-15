package com.raftkv.rpc;

/** Reply to {@link RequestVote}. {@code term} is the voter's current term. */
public record VoteResponse(long term, boolean granted) implements RaftMessage {

    public VoteResponse {
        if (term < 0) throw new IllegalArgumentException("term must be >= 0");
    }

    public static VoteResponse grant(long term) {
        return new VoteResponse(term, true);
    }

    public static VoteResponse reject(long term) {
        return new VoteResponse(term, false);
    }
}
