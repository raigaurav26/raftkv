package com.raftkv.rpc;

/**
 * Reply to {@link AppendEntries}.
 *
 * <p>On success, {@code matchIndex} is the highest index the follower now knows matches the
 * leader, which the leader uses to advance its commit index. Echoing it back (instead of the
 * leader guessing from what it sent) keeps the leader correct when replies arrive out of order.
 *
 * <p>On failure, {@code conflictIndex} is a hint for where the leader should retry from: the
 * follower's {@code lastIndex + 1} when it is too short, or the first index of the conflicting
 * term. This skips a whole term per round trip instead of one entry (section 5.3, "fast back-off").
 */
public record AppendResponse(long term, boolean success, long matchIndex, long conflictIndex)
        implements RaftMessage {

    public AppendResponse {
        if (term < 0) throw new IllegalArgumentException("term must be >= 0");
        if (success && conflictIndex != 0) {
            throw new IllegalArgumentException("a successful reply has no conflict index");
        }
        if (!success && matchIndex != 0) {
            throw new IllegalArgumentException("a failed reply has no match index");
        }
        if (matchIndex < 0 || conflictIndex < 0) {
            throw new IllegalArgumentException("indexes must be >= 0");
        }
    }

    public static AppendResponse success(long term, long matchIndex) {
        return new AppendResponse(term, true, matchIndex, 0);
    }

    public static AppendResponse conflict(long term, long conflictIndex) {
        return new AppendResponse(term, false, 0, Math.max(1, conflictIndex));
    }

    /** Rejected because the leader's term is stale; the leader will step down on seeing {@code term}. */
    public static AppendResponse staleTerm(long term) {
        return new AppendResponse(term, false, 0, 0);
    }
}
