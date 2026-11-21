package com.raftkv.log;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.util.List;
import java.util.Optional;

/**
 * The replicated log. Indexes are 1-based; {@code termAt(0)} is 0 so that the
 * consistency check for the very first entry always passes.
 */
public interface RaftLog {

    /**
     * Index of the last entry that was compacted away into a snapshot, 0 if none. The log holds
     * entries {@code baseIndex() + 1} to {@code lastIndex()}.
     */
    long baseIndex();

    /** Term of the entry at {@link #baseIndex()}, 0 if nothing was compacted. */
    long baseTerm();

    /** Index of the last entry, or {@link #baseIndex()} when no entries are held. */
    long lastIndex();

    /** Term of the last entry, or {@link #baseTerm()} when no entries are held. */
    long lastTerm();

    /**
     * Term of the entry at {@code index}. Known for {@code baseIndex()} (the snapshot's last
     * entry) and for every held entry; empty for older (compacted) and newer (missing) indexes.
     * Index 0 gives term 0 while nothing is compacted.
     */
    Optional<Long> termAt(long index);

    Optional<LogEntry> entry(long index);

    /**
     * Up to {@code max} entries starting at {@code fromIndex}, in order.
     *
     * @throws IllegalArgumentException if {@code fromIndex} is at or below {@link #baseIndex()}
     */
    List<LogEntry> entriesFrom(long fromIndex, int max);

    /** Leader side: appends a new entry at {@code lastIndex() + 1}. */
    LogEntry append(long term, Command command);

    /**
     * Discards every entry up to and including {@code index}, which a snapshot now covers.
     * If the log holds the entry at {@code index} with term {@code term}, later entries are
     * kept. Otherwise (the log is shorter, or disagrees) the whole log is dropped and
     * {@code index} becomes the new base. Calling it with the current base only records the term.
     *
     * @throws IllegalArgumentException if {@code index} is below the current base
     */
    void compactTo(long index, long term);

    /**
     * Follower side of AppendEntries (Raft paper, figure 2, receiver rules 2 to 4).
     *
     * @return false if the log has no entry at {@code prevIndex} with term {@code prevTerm};
     *     otherwise removes any conflicting suffix, appends the entries it does not have yet,
     *     and returns true. Entries that overlap the compacted prefix are skipped: they are
     *     committed, so they already match.
     */
    boolean appendFromLeader(long prevIndex, long prevTerm, List<LogEntry> entries);

    /**
     * The check a voter runs before granting its vote (section 5.4.1): is the candidate's log
     * at least as up to date as this one? A higher last term wins, and with equal last terms
     * the longer log wins.
     */
    default boolean candidateIsUpToDate(long candidateLastTerm, long candidateLastIndex) {
        if (candidateLastTerm != lastTerm()) return candidateLastTerm > lastTerm();
        return candidateLastIndex >= lastIndex();
    }
}
