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

    /** Index of the last entry, or 0 when the log is empty. */
    long lastIndex();

    /** Term of the last entry, or 0 when the log is empty. */
    long lastTerm();

    /** Term of the entry at {@code index}, if the log holds it. Index 0 gives term 0. */
    Optional<Long> termAt(long index);

    Optional<LogEntry> entry(long index);

    /** Up to {@code max} entries starting at {@code fromIndex}, in order. */
    List<LogEntry> entriesFrom(long fromIndex, int max);

    /** Leader side: appends a new entry at {@code lastIndex() + 1}. */
    LogEntry append(long term, Command command);

    /**
     * Follower side of AppendEntries (Raft paper, figure 2, receiver rules 2 to 4).
     *
     * @return false if the log has no entry at {@code prevIndex} with term {@code prevTerm};
     *     otherwise removes any conflicting suffix, appends the entries it does not have yet,
     *     and returns true.
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
