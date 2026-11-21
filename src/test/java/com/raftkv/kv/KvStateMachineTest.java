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
        var snap = kv.contents();
        kv.apply(new LogEntry(1, 2, Command.put("b", "2")));
        assertEquals(1, snap.size());
    }

    @Test
    void snapshotRoundTripsAndIsDeterministic() {
        KvStateMachine a = new KvStateMachine();
        a.apply(new LogEntry(1, 1, Command.put("zebra", "1")));
        a.apply(new LogEntry(1, 2, Command.put("ünï", "値")));
        a.apply(new LogEntry(1, 3, Command.put("empty", "")));
        a.apply(new LogEntry(1, 4, Command.delete("zebra")));

        KvStateMachine b = new KvStateMachine();
        b.apply(new LogEntry(1, 1, Command.put("empty", "")));
        b.apply(new LogEntry(1, 2, Command.put("ünï", "値")));
        b.apply(new LogEntry(1, 3, Command.noop()));
        b.apply(new LogEntry(1, 4, Command.noop()));
        assertArrayEquals(a.snapshot(), b.snapshot(), "equal contents give equal bytes whatever the history");

        KvStateMachine restored = new KvStateMachine();
        restored.apply(new LogEntry(1, 1, Command.put("old", "state")));
        restored.restore(a.snapshot(), 4);
        assertEquals(a.contents(), restored.contents());
        assertEquals(4, restored.lastApplied());
        assertEquals(Optional.empty(), restored.get("old"), "restore replaces the whole state");
        restored.apply(new LogEntry(2, 5, Command.put("next", "ok")));
        assertEquals(5, restored.lastApplied());
        assertThrows(IllegalStateException.class, () -> restored.apply(new LogEntry(2, 5, Command.noop())));
    }

    @Test
    void emptyStateSnapshots() {
        KvStateMachine kv = new KvStateMachine();
        KvStateMachine other = new KvStateMachine();
        other.apply(new LogEntry(1, 1, Command.put("a", "1")));
        other.restore(kv.snapshot(), 7);
        assertEquals(0, other.size());
        assertEquals(7, other.lastApplied());
    }

    @Test
    void badSnapshotLeavesTheStateUntouched() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(new LogEntry(1, 1, Command.put("a", "1")));
        assertThrows(IllegalArgumentException.class, () -> kv.restore(new byte[]{0, 0, 0, 2, 0, 0, 0, 9}, 5));
        assertThrows(IllegalArgumentException.class, () -> kv.restore(new byte[]{1}, 5));
        assertEquals(Optional.of("1"), kv.get("a"));
        assertEquals(1, kv.lastApplied());
    }
}
