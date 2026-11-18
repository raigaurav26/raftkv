package com.raftkv.kv;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The key-value map. It changes only through {@link #apply}, which the node calls for
 * committed entries in log order; reads see the state as of {@link #lastApplied()}.
 * Single-threaded: the node owns it.
 */
public final class KvStateMachine implements StateMachine {

    private final Map<String, String> data = new HashMap<>();
    private long lastApplied;

    /** Result of applying a command: the value the key held before, if any. */
    public record Result(Optional<String> previous) { }

    @Override
    public Result apply(LogEntry entry) {
        if (entry.index() != lastApplied + 1) {
            throw new IllegalStateException("expected index " + (lastApplied + 1) + " but got " + entry.index());
        }
        lastApplied = entry.index();
        Command c = entry.command();
        return switch (c.op()) {
            case PUT -> new Result(Optional.ofNullable(data.put(c.key(), c.value())));
            case DELETE -> new Result(Optional.ofNullable(data.remove(c.key())));
            case NOOP -> new Result(Optional.empty());
        };
    }

    @Override
    public long lastApplied() {
        return lastApplied;
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public int size() {
        return data.size();
    }

    /** A copy of the whole map, for tests and (later) snapshots. */
    public Map<String, String> snapshot() {
        return Map.copyOf(data);
    }
}
