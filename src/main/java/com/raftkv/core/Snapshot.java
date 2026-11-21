package com.raftkv.core;

import java.util.Arrays;
import java.util.Objects;

/**
 * The state machine as it was after applying every entry up to {@code lastIncludedIndex},
 * which had term {@code lastIncludedTerm} (Raft paper, section 7). Once a snapshot is saved,
 * the log entries it covers can be deleted.
 */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {

    public Snapshot {
        if (lastIncludedIndex < 1) throw new IllegalArgumentException("lastIncludedIndex must be >= 1");
        if (lastIncludedTerm < 1) throw new IllegalArgumentException("lastIncludedTerm must be >= 1");
        Objects.requireNonNull(data, "data");
        data = data.clone();
    }

    @Override
    public byte[] data() {
        return data.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Snapshot s && s.lastIncludedIndex == lastIncludedIndex
                && s.lastIncludedTerm == lastIncludedTerm && Arrays.equals(s.data, data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastIncludedIndex, lastIncludedTerm, Arrays.hashCode(data));
    }

    @Override
    public String toString() {
        return "Snapshot[index=" + lastIncludedIndex + ", term=" + lastIncludedTerm + ", " + data.length + " bytes]";
    }
}
