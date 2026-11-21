package com.raftkv.log;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The log rules, kept once for every storage backend. Entries are always held in memory for
 * fast reads; a subclass makes each change durable through {@link #persistTruncate},
 * {@link #persistAppend} and {@link #persistCompact} <em>before</em> the in-memory copy
 * changes, so a failed write can never leave the node acting on entries that are not on disk.
 *
 * <p>The in-memory list holds entries {@code baseIndex + 1} onwards. Everything at or below
 * {@code baseIndex} lives in a snapshot.
 */
public abstract class AbstractRaftLog implements RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private long baseIndex;
    private long baseTerm;

    /** Removes entries from {@code fromIndex} onwards from storage. */
    protected abstract void persistTruncate(long fromIndex);

    /** Writes entries to storage and makes them durable before returning. */
    protected abstract void persistAppend(List<LogEntry> newEntries);

    /**
     * Rewrites storage so it holds only {@code keep}, which starts at {@code newBaseIndex + 1}
     * (or is empty). Must be atomic: after a crash, storage holds either all the old entries
     * or exactly {@code keep}.
     */
    protected abstract void persistCompact(long newBaseIndex, long newBaseTerm, List<LogEntry> keep);

    /** For subclasses loading entries that are already on disk. */
    protected final void loadRecovered(LogEntry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalStateException("recovered entry " + entry.index() + " after " + lastIndex());
        }
        entries.add(entry);
    }

    /**
     * For subclasses whose storage starts after a compacted prefix. Only valid before any
     * entry is loaded. The term of the base is unknown until {@link #compactTo} is called with it.
     */
    protected final void setRecoveredBase(long index) {
        if (!entries.isEmpty()) throw new IllegalStateException("base must be set before entries are loaded");
        baseIndex = index;
        baseTerm = 0;
    }

    @Override
    public synchronized long baseIndex() {
        return baseIndex;
    }

    @Override
    public synchronized long baseTerm() {
        return baseTerm;
    }

    @Override
    public synchronized long lastIndex() {
        return baseIndex + entries.size();
    }

    @Override
    public synchronized long lastTerm() {
        return entries.isEmpty() ? baseTerm : entries.get(entries.size() - 1).term();
    }

    @Override
    public synchronized Optional<Long> termAt(long index) {
        if (index == baseIndex) return Optional.of(baseTerm);
        return entry(index).map(LogEntry::term);
    }

    @Override
    public synchronized Optional<LogEntry> entry(long index) {
        if (index <= baseIndex || index > lastIndex()) return Optional.empty();
        return Optional.of(entries.get((int) (index - baseIndex - 1)));
    }

    @Override
    public synchronized List<LogEntry> entriesFrom(long fromIndex, int max) {
        if (fromIndex < 1) throw new IllegalArgumentException("fromIndex must be >= 1");
        if (fromIndex <= baseIndex) {
            throw new IllegalArgumentException("index " + fromIndex + " is compacted; base is " + baseIndex);
        }
        if (max < 0) throw new IllegalArgumentException("max must be >= 0");
        if (fromIndex > lastIndex()) return List.of();
        int from = (int) (fromIndex - baseIndex - 1);
        int to = (int) Math.min(entries.size(), (long) from + max);
        return List.copyOf(entries.subList(from, to));
    }

    @Override
    public synchronized LogEntry append(long term, Command command) {
        if (term < lastTerm()) {
            throw new IllegalArgumentException("term " + term + " is older than last term " + lastTerm());
        }
        LogEntry entry = new LogEntry(term, lastIndex() + 1, command);
        persistAppend(List.of(entry));
        entries.add(entry);
        return entry;
    }

    @Override
    public synchronized boolean appendFromLeader(long prevIndex, long prevTerm, List<LogEntry> incoming) {
        long expected = prevIndex + 1;
        for (LogEntry e : incoming) {
            if (e.index() != expected++) {
                throw new IllegalArgumentException("entries are not contiguous after index " + prevIndex);
            }
        }
        if (prevIndex < baseIndex) {
            // The message overlaps the compacted prefix. Those entries are committed, so they
            // match ours; only the part after the base can add anything.
            int skip = (int) Math.min(incoming.size(), baseIndex - prevIndex);
            incoming = incoming.subList(skip, incoming.size());
            if (incoming.isEmpty()) return true;
        } else {
            Optional<Long> localPrevTerm = termAt(prevIndex);
            if (localPrevTerm.isEmpty() || localPrevTerm.get() != prevTerm) {
                return false;
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
        if (from <= lastIndex()) {
            persistTruncate(from);
            entries.subList((int) (from - baseIndex - 1), entries.size()).clear();
        }
        persistAppend(toWrite);
        entries.addAll(toWrite);
        return true;
    }

    @Override
    public synchronized void compactTo(long index, long term) {
        if (index < baseIndex) {
            throw new IllegalArgumentException("cannot compact to " + index + "; base is already " + baseIndex);
        }
        if (index == baseIndex) {
            baseTerm = term; // the term of a recovered base is learned from the snapshot
            return;
        }
        Optional<Long> held = termAt(index);
        if (held.isPresent() && held.get() == term) {
            int drop = (int) (index - baseIndex);
            List<LogEntry> keep = List.copyOf(entries.subList(drop, entries.size()));
            persistCompact(index, term, keep);
            entries.subList(0, drop).clear();
        } else {
            persistCompact(index, term, List.of());
            entries.clear();
        }
        baseIndex = index;
        baseTerm = term;
    }
}
