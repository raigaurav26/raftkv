package com.raftkv.log;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** Rules every {@link RaftLog} must follow, run against each implementation. */
public abstract class RaftLogContractTest {

    protected RaftLog log;

    protected abstract RaftLog newLog() throws Exception;

    @BeforeEach
    void setUpLog() throws Exception {
        log = newLog();
    }

    protected static LogEntry e(long term, long index) {
        return new LogEntry(term, index, Command.put("k" + index, "t" + term));
    }

    /** Builds a log whose entry i has the term terms[i-1]. */
    protected void seed(long... terms) {
        for (long t : terms) log.append(t, Command.put("k", "v"));
    }

    protected long[] terms() {
        return log.entriesFrom(1, Integer.MAX_VALUE).stream().mapToLong(LogEntry::term).toArray();
    }

    @Test
    void emptyLogHasSentinelAtIndexZero() {
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(Optional.of(0L), log.termAt(0));
        assertTrue(log.termAt(1).isEmpty());
        assertTrue(log.entriesFrom(1, 10).isEmpty());
    }

    @Test
    void leaderAppendAssignsConsecutiveIndexes() {
        LogEntry first = log.append(1, Command.put("a", "1"));
        LogEntry second = log.append(2, Command.delete("a"));
        assertEquals(1, first.index());
        assertEquals(2, second.index());
        assertEquals(2, log.lastTerm());
        assertThrows(IllegalArgumentException.class, () -> log.append(1, Command.noop()),
                "a leader never appends with an older term than its last entry");
    }

    @Test
    void entriesFromRespectsBatchLimit() {
        seed(1, 1, 1, 2, 2);
        assertEquals(List.of(2L, 3L), log.entriesFrom(2, 2).stream().map(LogEntry::index).toList());
        assertEquals(1, log.entriesFrom(5, 100).size());
        assertTrue(log.entriesFrom(6, 100).isEmpty());
    }

    @Test
    void rejectsWhenPrevEntryIsMissing() {
        seed(1);
        assertFalse(log.appendFromLeader(2, 1, List.of(e(1, 3))));
        assertEquals(1, log.lastIndex());
    }

    @Test
    void rejectsWhenPrevTermDiffers() {
        seed(1, 1);
        assertFalse(log.appendFromLeader(2, 2, List.of(e(2, 3))));
        assertArrayEquals(new long[]{1, 1}, terms());
    }

    @Test
    void appendsToEmptyLogFromIndexZero() {
        assertTrue(log.appendFromLeader(0, 0, List.of(e(1, 1), e(1, 2))));
        assertArrayEquals(new long[]{1, 1}, terms());
    }

    @Test
    void heartbeatWithNoEntriesOnlyChecksConsistency() {
        seed(1, 2);
        assertTrue(log.appendFromLeader(2, 2, List.of()));
        assertFalse(log.appendFromLeader(2, 1, List.of()));
        assertEquals(2, log.lastIndex());
    }

    @Test
    void truncatesConflictingSuffix() {
        // Follower has stale entries from term 2 that the new term-3 leader never had.
        seed(1, 1, 2, 2, 2);
        assertTrue(log.appendFromLeader(2, 1, List.of(e(3, 3), e(3, 4))));
        assertArrayEquals(new long[]{1, 1, 3, 3}, terms());
    }

    @Test
    void staleRpcDoesNotTruncateNewerEntries() {
        // A delayed AppendEntries carrying only entry 2 must not drop entries 3 and 4.
        seed(1, 1, 1, 1);
        assertTrue(log.appendFromLeader(1, 1, List.of(e(1, 2))));
        assertArrayEquals(new long[]{1, 1, 1, 1}, terms());
    }

    @Test
    void duplicateRpcIsIdempotent() {
        List<LogEntry> batch = List.of(e(1, 1), e(1, 2), e(2, 3));
        assertTrue(log.appendFromLeader(0, 0, batch));
        assertTrue(log.appendFromLeader(0, 0, batch));
        assertArrayEquals(new long[]{1, 1, 2}, terms());
    }

    @Test
    void rejectsNonContiguousBatch() {
        assertThrows(IllegalArgumentException.class,
                () -> log.appendFromLeader(0, 0, List.of(e(1, 1), e(1, 3))));
    }

    /** Figure 7 of the Raft paper: followers (a) to (f) against a term-8 leader. */
    @Test
    void figureSevenFollowerFConvergesToLeader() {
        // follower (f): 1 1 1 2 2 2 3 3 3 3 3
        seed(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        // leader log: 1 1 1 4 4 5 5 6 6 6 (+ 8). Leader probes back until prevIndex 3 matches.
        assertFalse(log.appendFromLeader(10, 6, List.of()));
        assertFalse(log.appendFromLeader(5, 4, List.of()));
        assertTrue(log.appendFromLeader(3, 1, List.of(
                e(4, 4), e(4, 5), e(5, 6), e(5, 7), e(6, 8), e(6, 9), e(6, 10), e(8, 11))));
        assertArrayEquals(new long[]{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 8}, terms());
    }

    @Test
    void voterComparesLastTermThenLength() {
        seed(1, 2, 2);
        assertTrue(log.candidateIsUpToDate(3, 1), "higher last term wins even if shorter");
        assertFalse(log.candidateIsUpToDate(1, 10), "lower last term loses even if longer");
        assertTrue(log.candidateIsUpToDate(2, 3), "equal is up to date");
        assertTrue(log.candidateIsUpToDate(2, 4));
        assertFalse(log.candidateIsUpToDate(2, 2));
    }

    // ------------------------------------------------------------ compaction

    @Test
    void compactionKeepsLaterEntriesAndAnswersFromTheBase() {
        seed(1, 1, 2, 2, 3);
        log.compactTo(3, 2);
        assertEquals(3, log.baseIndex());
        assertEquals(2, log.baseTerm());
        assertEquals(5, log.lastIndex());
        assertEquals(3, log.lastTerm());
        assertEquals(Optional.of(2L), log.termAt(3), "the base keeps its term for consistency checks");
        assertEquals(Optional.of(2L), log.termAt(4));
        assertTrue(log.termAt(2).isEmpty());
        assertTrue(log.entry(3).isEmpty());
        assertTrue(log.entry(4).isPresent());
        assertEquals(List.of(4L, 5L), log.entriesFrom(4, 10).stream().map(LogEntry::index).toList());
        assertThrows(IllegalArgumentException.class, () -> log.entriesFrom(3, 10));
    }

    @Test
    void appendsContinueAfterCompaction() {
        seed(1, 1, 2);
        log.compactTo(3, 2);
        assertEquals(0, log.entriesFrom(4, 10).size());
        LogEntry next = log.append(3, Command.put("x", "y"));
        assertEquals(4, next.index());
        assertTrue(log.appendFromLeader(4, 3, List.of(e(3, 5))));
        assertEquals(5, log.lastIndex());
    }

    @Test
    void compactingEverythingLeavesTheBaseAsTheLastEntry() {
        seed(1, 2, 2);
        log.compactTo(3, 2);
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());
        assertTrue(log.candidateIsUpToDate(2, 3));
        assertFalse(log.candidateIsUpToDate(2, 2));
        assertTrue(log.appendFromLeader(3, 2, List.of(e(2, 4))));
        assertFalse(log.appendFromLeader(4, 1, List.of(e(2, 5))), "wrong prev term still rejected");
    }

    @Test
    void compactionBeyondTheEndDropsTheWholeLog() {
        seed(1, 1);
        log.compactTo(7, 3);
        assertEquals(7, log.baseIndex());
        assertEquals(7, log.lastIndex());
        assertEquals(3, log.lastTerm());
        assertTrue(log.entriesFrom(8, 5).isEmpty());
    }

    @Test
    void compactionOnADifferentTermDropsTheWholeLog() {
        seed(1, 3, 3);
        log.compactTo(2, 2); // snapshot says entry 2 had term 2, but ours had term 3
        assertEquals(2, log.baseIndex());
        assertEquals(2, log.lastIndex());
        assertEquals(2, log.lastTerm(), "entries that disagree with the snapshot are discarded");
    }

    @Test
    void compactingToTheCurrentBaseOnlyRecordsTheTerm() {
        seed(1, 1, 1);
        log.compactTo(2, 1);
        log.compactTo(2, 1);
        assertEquals(3, log.lastIndex());
        assertThrows(IllegalArgumentException.class, () -> log.compactTo(1, 1));
    }

    @Test
    void messageOverlappingTheCompactedPrefixIsAcceptedAndOnlyTheNewPartCounts() {
        seed(1, 1, 1, 1);
        log.compactTo(3, 1);
        // Leader resends from index 2 (older than our base), carrying entries 3, 4 and a new 5.
        assertTrue(log.appendFromLeader(1, 1, List.of(e(1, 2), e(1, 3), e(1, 4), e(2, 5))));
        assertEquals(5, log.lastIndex());
        assertEquals(2, log.lastTerm());
        // A message that ends inside the compacted part changes nothing.
        assertTrue(log.appendFromLeader(0, 0, List.of(e(1, 1), e(1, 2))));
        assertEquals(5, log.lastIndex());
    }

    @Test
    void conflictingSuffixAfterCompactionIsTruncatedFromTheRightPlace() {
        seed(1, 1, 2, 2, 2);
        log.compactTo(2, 1);
        assertTrue(log.appendFromLeader(3, 2, List.of(e(3, 4), e(3, 5))));
        assertArrayEquals(new long[]{2, 3, 3}, log.entriesFrom(3, 10).stream().mapToLong(LogEntry::term).toArray());
    }
}
