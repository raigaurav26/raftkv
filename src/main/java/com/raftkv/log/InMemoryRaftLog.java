package com.raftkv.log;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A heap-backed {@link RaftLog}. The file-backed version arrives on Day 6. */
public final class InMemoryRaftLog implements RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    @Override
    public synchronized long lastIndex() {
        return entries.size();
    }

    @Override
    public synchronized long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    @Override
    public synchronized Optional<Long> termAt(long index) {
        if (index == 0) return Optional.of(0L);
        return entry(index).map(LogEntry::term);
    }

    @Override
    public synchronized Optional<LogEntry> entry(long index) {
        if (index < 1 || index > entries.size()) return Optional.empty();
        return Optional.of(entries.get((int) index - 1));
    }

    @Override
    public synchronized List<LogEntry> entriesFrom(long fromIndex, int max) {
        if (fromIndex < 1) throw new IllegalArgumentException("fromIndex must be >= 1");
        if (max < 0) throw new IllegalArgumentException("max must be >= 0");
        if (fromIndex > entries.size()) return List.of();
        int from = (int) fromIndex - 1;
        int to = (int) Math.min(entries.size(), (long) from + max);
        return List.copyOf(entries.subList(from, to));
    }

    @Override
    public synchronized LogEntry append(long term, Command command) {
        if (term < lastTerm()) {
            throw new IllegalArgumentException("term " + term + " is older than last term " + lastTerm());
        }
        LogEntry entry = new LogEntry(term, entries.size() + 1L, command);
        entries.add(entry);
        return entry;
    }

    @Override
    public synchronized boolean appendFromLeader(long prevIndex, long prevTerm, List<LogEntry> incoming) {
        Optional<Long> localPrevTerm = termAt(prevIndex);
        if (localPrevTerm.isEmpty() || localPrevTerm.get() != prevTerm) {
            return false;
        }
        long expected = prevIndex + 1;
        for (LogEntry e : incoming) {
            if (e.index() != expected++) {
                throw new IllegalArgumentException("entries are not contiguous after index " + prevIndex);
            }
        }
        for (LogEntry e : incoming) {
            Optional<Long> existing = termAt(e.index());
            if (existing.isPresent()) {
                if (existing.get() == e.term()) {
                    continue; // already have it; a stale or duplicate RPC must not truncate
                }
                truncateFrom(e.index());
            }
            entries.add(e);
        }
        return true;
    }

    private void truncateFrom(long index) {
        entries.subList((int) index - 1, entries.size()).clear();
    }
}
