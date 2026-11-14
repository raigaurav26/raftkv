package com.raftkv.core;

import java.util.Objects;

/** One slot in the replicated log. Indexes start at 1; index 0 is the empty-log sentinel. */
public record LogEntry(long term, long index, Command command) {

    public LogEntry {
        if (term < 1) throw new IllegalArgumentException("term must be >= 1, was " + term);
        if (index < 1) throw new IllegalArgumentException("index must be >= 1, was " + index);
        Objects.requireNonNull(command, "command");
    }
}
