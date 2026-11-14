package com.raftkv.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CommandTest {

    @Test
    void validatesShapePerOperation() {
        assertDoesNotThrow(() -> Command.put("k", "v"));
        assertDoesNotThrow(() -> Command.put("k", ""));
        assertDoesNotThrow(() -> Command.delete("k"));
        assertDoesNotThrow(Command::noop);

        assertThrows(IllegalArgumentException.class, () -> Command.put("", "v"));
        assertThrows(NullPointerException.class, () -> Command.put("k", null));
        assertThrows(IllegalArgumentException.class, () -> new Command(Command.Op.DELETE, "k", "v"));
        assertThrows(IllegalArgumentException.class, () -> new Command(Command.Op.NOOP, "k", null));
    }

    @Test
    void nodeIdRejectsBadNames() {
        assertEquals("n1", NodeId.of("n1").toString());
        assertThrows(IllegalArgumentException.class, () -> NodeId.of(""));
        assertThrows(IllegalArgumentException.class, () -> NodeId.of("has space"));
        assertThrows(IllegalArgumentException.class, () -> NodeId.of("-leading-dash"));
    }

    @Test
    void logEntryIndexesStartAtOne() {
        assertThrows(IllegalArgumentException.class, () -> new LogEntry(1, 0, Command.noop()));
        assertThrows(IllegalArgumentException.class, () -> new LogEntry(0, 1, Command.noop()));
    }
}
