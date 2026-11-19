package com.raftkv.log;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The log rules, kept once for every storage backend. Entries are always held in memory for
 * fast reads; a subclass makes each change durable through {@link #persistTruncate} and
 * {@link #persistAppend} <em>before</em> the in-memory copy changes, so a failed write can
 * never leave the node acting on entries that are not on disk.
 */
public abstract class AbstractRaftLog implements RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    /** Removes entries from {@code fromIndex} onwards from storage. */
    protected abstract void persistTruncate(long fromIndex);

    /** Writes entries to storage and makes them durable before returning. */
    protected abstract void persistAppend(List<LogEntry> newEntries);

    /** For subclasses loading entries that are already on disk. */
    protected final void loadRecovered(LogEntry entry) {
        if (entry.index() != entries.size() + 1L) {
            throw new IllegalStateException("recovered entry " + entry.index() + " after " + entries.size());
        }
        entries.add(entry);
    }

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
        persistAppend(List.of(entry));
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
        // Skip entries we already have. At the first one that differs, everything from there
        // on is replaced. A stale or duplicate RPC that matches throughout changes nothing,
        // which matters: it must never cut off entries that arrived after it.
        int firstNew = 0;
        while (firstNew < incoming.size()) {
            LogEntry e = incoming.get(firstNew);
            Optional<Long> existing = termAt(e.index());
            if (existing.isEmpty() || existing.get() != e.term()) break;
            firstNew++;
        }
        if (firstNew == incoming.size()) return true;

        List<LogEntry> toWrite = incoming.subList(firstNew, incoming.size());
        long from = toWrite.get(0).index();
        if (from <= entries.size()) {
            persistTruncate(from);
            entries.subList((int) from - 1, entries.size()).clear();
        }
        persistAppend(toWrite);
        entries.addAll(toWrite);
        return true;
    }
}
