package com.raftkv.core;

import java.util.Objects;

/**
 * An operation on the key-value state machine, stored inside a log entry.
 *
 * <p>{@link Op#NOOP} is the empty entry a new leader appends at the start of its term so it
 * can commit entries from earlier terms (Raft paper, section 5.4.2).
 */
public record Command(Op op, String key, String value) {

    public enum Op { PUT, DELETE, NOOP }

    public Command {
        Objects.requireNonNull(op, "op");
        switch (op) {
            case PUT -> {
                requireKey(key);
                Objects.requireNonNull(value, "value");
            }
            case DELETE -> {
                requireKey(key);
                if (value != null) throw new IllegalArgumentException("DELETE takes no value");
            }
            case NOOP -> {
                if (key != null || value != null) {
                    throw new IllegalArgumentException("NOOP takes no key or value");
                }
            }
        }
    }

    private static void requireKey(String key) {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("key is required");
    }

    public static Command put(String key, String value) {
        return new Command(Op.PUT, key, value);
    }

    public static Command delete(String key) {
        return new Command(Op.DELETE, key, null);
    }

    public static Command noop() {
        return new Command(Op.NOOP, null, null);
    }
}
