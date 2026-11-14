package com.raftkv.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PersistentStateTest {

    private static final NodeId A = NodeId.of("n1");
    private static final NodeId B = NodeId.of("n2");

    @Test
    void startsAtTermZeroWithNoVote() {
        assertEquals(0, PersistentState.INITIAL.currentTerm());
        assertTrue(PersistentState.INITIAL.vote().isEmpty());
    }

    @Test
    void votesOncePerTerm() {
        PersistentState s = PersistentState.INITIAL.advanceTo(1).voteFor(A);
        assertTrue(s.canVoteFor(A));
        assertFalse(s.canVoteFor(B));
        assertEquals(s, s.voteFor(A), "re-voting for the same candidate is idempotent");
        assertThrows(IllegalStateException.class, () -> s.voteFor(B));
    }

    @Test
    void newTermClearsTheVote() {
        PersistentState s = PersistentState.INITIAL.advanceTo(1).voteFor(A).advanceTo(2);
        assertEquals(2, s.currentTerm());
        assertTrue(s.canVoteFor(B));
    }

    @Test
    void sameTermKeepsTheVote() {
        PersistentState s = PersistentState.INITIAL.advanceTo(3).voteFor(A);
        assertSame(s, s.advanceTo(3));
    }

    @Test
    void termNeverGoesBackwards() {
        PersistentState s = PersistentState.INITIAL.advanceTo(5);
        assertThrows(IllegalArgumentException.class, () -> s.advanceTo(4));
    }
}
