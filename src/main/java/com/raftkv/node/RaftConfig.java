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
 *
 * <p>{@code snapshotThreshold} is how many applied entries pile up since the last snapshot before
 * the node takes a new one and trims its log; 0 turns snapshots off.
 */
public record RaftConfig(int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval,
                         int maxEntriesPerAppend, int snapshotThreshold) {

    private static final int DEFAULT_BATCH = 64;

    /** 150 to 300 ms election timeout and 50 ms heartbeats with a 10 ms tick, as in the paper. */
    public static final RaftConfig DEFAULT = new RaftConfig(15, 30, 5, DEFAULT_BATCH, 0);

    public RaftConfig(int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval) {
        this(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, DEFAULT_BATCH, 0);
    }

    public RaftConfig(int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval,
                      int maxEntriesPerAppend) {
        this(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, 0);
    }

    public RaftConfig withSnapshotThreshold(int entries) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval,
                maxEntriesPerAppend, entries);
    }

    public RaftConfig {
        if (snapshotThreshold < 0) throw new IllegalArgumentException("snapshotThreshold must be >= 0");
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
