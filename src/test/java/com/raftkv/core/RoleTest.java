package com.raftkv.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class RoleTest {

    @ParameterizedTest
    @CsvSource({
            "FOLLOWER, CANDIDATE",
            "CANDIDATE, CANDIDATE",
            "CANDIDATE, LEADER",
            "CANDIDATE, FOLLOWER",
            "LEADER, FOLLOWER"
    })
    void allowsTransitionsFromThePaper(Role from, Role to) {
        assertEquals(to, from.moveTo(to));
    }

    @ParameterizedTest
    @CsvSource({
            "FOLLOWER, LEADER",
            "FOLLOWER, FOLLOWER",
            "LEADER, CANDIDATE",
            "LEADER, LEADER"
    })
    void rejectsShortcuts(Role from, Role to) {
        assertFalse(from.canMoveTo(to));
        assertThrows(IllegalStateException.class, () -> from.moveTo(to));
    }

    @Test
    void aFollowerCanOnlyBecomeLeaderThroughAnElection() {
        Role role = Role.FOLLOWER.moveTo(Role.CANDIDATE).moveTo(Role.LEADER);
        assertEquals(Role.LEADER, role);
    }
}
