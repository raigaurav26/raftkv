package com.raftkv.kv;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class KvStateMachineTest {

    @Test
    void appliesCommandsInOrder() {
        KvStateMachine kv = new KvStateMachine();
        assertEquals(Optional.empty(), kv.apply(new LogEntry(1, 1, Command.put("a", "1"))).previous());
        assertEquals(Optional.of("1"), kv.apply(new LogEntry(1, 2, Command.put("a", "2"))).previous());
        kv.apply(new LogEntry(1, 3, Command.noop()));
        assertEquals(Optional.of("2"), kv.get("a"));
        assertEquals(Optional.of("2"), kv.apply(new LogEntry(2, 4, Command.delete("a"))).previous());
        assertEquals(Optional.empty(), kv.get("a"));
        assertEquals(4, kv.lastApplied());
        assertEquals(0, kv.size());
    }

    @Test
    void refusesGapsAndReplays() {
        KvStateMachine kv = new KvStateMachine();
        assertThrows(IllegalStateException.class, () -> kv.apply(new LogEntry(1, 2, Command.noop())));
        kv.apply(new LogEntry(1, 1, Command.noop()));
        assertThrows(IllegalStateException.class, () -> kv.apply(new LogEntry(1, 1, Command.noop())),
                "applying an entry twice would break determinism");
    }

    @Test
    void snapshotIsACopy() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(new LogEntry(1, 1, Command.put("a", "1")));
        var snap = kv.snapshot();
        kv.apply(new LogEntry(1, 2, Command.put("b", "2")));
        assertEquals(1, snap.size());
    }
}
