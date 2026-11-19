package com.raftkv.log;

import com.raftkv.core.LogEntry;

import java.util.List;

/** A {@link RaftLog} that keeps nothing on disk. Used by most tests. */
public final class InMemoryRaftLog extends AbstractRaftLog {

    @Override
    protected void persistTruncate(long fromIndex) {
    }

    @Override
    protected void persistAppend(List<LogEntry> newEntries) {
    }
}
