package com.raftkv.node;

/**
 * Timing settings, in ticks. A tick is one call to {@link RaftNode#tick()}; the runtime calls
 * it on a fixed schedule (10 ms by default from Day 10) and tests call it by hand, so tests never
 * sleep and every run with the same seed behaves the same way.
 *
 * <p>Raft needs {@code heartbeatInterval} to be well below the election timeout, and the
 * timeout to be randomized over a range so that two followers rarely time out together
 * (Raft paper, section 5.2).
 *
 * <p>{@code maxEntriesPerAppend} caps one AppendEntries message, so a follower that is far
 * behind catches up in several bounded messages instead of one huge one.
 */
public record RaftConfig(int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval,
                         int maxEntriesPerAppend) {

    private static final int DEFAULT_BATCH = 64;

    /** 150 to 300 ms election timeout and 50 ms heartbeats with a 10 ms tick, as in the paper. */
    public static final RaftConfig DEFAULT = new RaftConfig(15, 30, 5, DEFAULT_BATCH);

    public RaftConfig(int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval) {
        this(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, DEFAULT_BATCH);
    }

    public RaftConfig {
        if (maxEntriesPerAppend < 1) throw new IllegalArgumentException("maxEntriesPerAppend must be >= 1");
        if (heartbeatInterval < 1) throw new IllegalArgumentException("heartbeatInterval must be >= 1");
        if (electionTimeoutMin <= heartbeatInterval) {
            throw new IllegalArgumentException("election timeout must be longer than the heartbeat interval");
        }
        if (electionTimeoutMax <= electionTimeoutMin) {
            throw new IllegalArgumentException("electionTimeoutMax must be greater than electionTimeoutMin");
        }
    }
}
