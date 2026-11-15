package com.raftkv.rpc;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.core.NodeId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MessagesTest {

    private static final NodeId N1 = NodeId.of("n1");

    private static LogEntry e(long term, long index) {
        return new LogEntry(term, index, Command.put("k", "v"));
    }

    @Test
    void requestVoteValidatesLogPosition() {
        assertDoesNotThrow(() -> new RequestVote(1, N1, 0, 0));
        assertDoesNotThrow(() -> new RequestVote(3, N1, 7, 2));
        assertThrows(IllegalArgumentException.class, () -> new RequestVote(0, N1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RequestVote(2, N1, 5, 0));
        assertThrows(IllegalArgumentException.class, () -> new RequestVote(2, N1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new RequestVote(2, N1, 5, 3));
    }

    @Test
    void appendEntriesRequiresContiguousEntriesAfterPrev() {
        assertDoesNotThrow(() -> new AppendEntries(2, N1, 3, 1, List.of(e(2, 4), e(2, 5)), 3));
        assertThrows(IllegalArgumentException.class,
                () -> new AppendEntries(2, N1, 3, 1, List.of(e(2, 5)), 3));
        assertThrows(IllegalArgumentException.class,
                () -> new AppendEntries(2, N1, 3, 1, List.of(e(2, 4), e(2, 6)), 3));
        assertThrows(IllegalArgumentException.class,
                () -> new AppendEntries(2, N1, 0, 0, List.of(e(3, 1)), 0),
                "a leader cannot send entries from a future term");
    }

    @Test
    void appendEntriesCopiesTheEntryList() {
        List<LogEntry> list = new ArrayList<>(List.of(e(1, 1)));
        AppendEntries msg = new AppendEntries(1, N1, 0, 0, list, 0);
        list.add(e(1, 2));
        assertEquals(1, msg.entries().size());
        assertThrows(UnsupportedOperationException.class, () -> msg.entries().add(e(1, 2)));
        assertEquals(1, msg.lastNewIndex());
    }

    @Test
    void heartbeatHasNoEntries() {
        AppendEntries hb = AppendEntries.heartbeat(4, N1, 10, 3, 9);
        assertTrue(hb.isHeartbeat());
        assertEquals(10, hb.lastNewIndex());
    }

    @Test
    void appendResponseShapes() {
        AppendResponse ok = AppendResponse.success(2, 7);
        assertTrue(ok.success());
        assertEquals(7, ok.matchIndex());

        AppendResponse conflict = AppendResponse.conflict(2, 4);
        assertFalse(conflict.success());
        assertEquals(4, conflict.conflictIndex());
        assertEquals(1, AppendResponse.conflict(2, 0).conflictIndex(), "retry never goes below index 1");

        assertEquals(0, AppendResponse.staleTerm(5).conflictIndex());
        assertThrows(IllegalArgumentException.class, () -> new AppendResponse(1, true, 3, 2));
        assertThrows(IllegalArgumentException.class, () -> new AppendResponse(1, false, 3, 0));
    }

    @Test
    void everyMessageCarriesATerm() {
        List<RaftMessage> all = List.of(
                new RequestVote(3, N1, 0, 0),
                VoteResponse.grant(3),
                AppendEntries.heartbeat(3, N1, 0, 0, 0),
                AppendResponse.success(3, 0));
        all.forEach(m -> assertEquals(3, m.term()));
    }

    @Test
    void envelopeRejectsSelfMessages() {
        assertThrows(IllegalArgumentException.class,
                () -> new Envelope(N1, N1, VoteResponse.grant(1)));
    }
}
