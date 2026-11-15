package com.raftkv.rpc;

import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;

import java.util.List;
import java.util.Objects;

/**
 * Sent by the leader to replicate entries, and with no entries as a heartbeat
 * (Raft paper, figure 2).
 */
public record AppendEntries(
        long term,
        NodeId leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit) implements RaftMessage {

    public AppendEntries {
        if (term < 1) throw new IllegalArgumentException("term must be >= 1");
        Objects.requireNonNull(leaderId, "leaderId");
        if (prevLogIndex < 0 || prevLogTerm < 0 || leaderCommit < 0) {
            throw new IllegalArgumentException("indexes and terms must be >= 0");
        }
        entries = List.copyOf(entries);
        long expected = prevLogIndex + 1;
        for (LogEntry e : entries) {
            if (e.index() != expected++) {
                throw new IllegalArgumentException("entries must follow prevLogIndex without gaps");
            }
            if (e.term() > term) {
                throw new IllegalArgumentException("entry term " + e.term() + " is newer than leader term " + term);
            }
        }
    }

    public static AppendEntries heartbeat(long term, NodeId leaderId, long prevLogIndex,
                                          long prevLogTerm, long leaderCommit) {
        return new AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, List.of(), leaderCommit);
    }

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }

    /** Index of the last entry this message would leave in the follower's log if accepted. */
    public long lastNewIndex() {
        return prevLogIndex + entries.size();
    }
}
