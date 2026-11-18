package com.raftkv.kv;

import com.raftkv.core.LogEntry;

/**
 * What the replicated log drives. Every node applies the same committed entries in the same
 * order, so every node's state machine ends up in the same state (Raft paper, section 2).
 */
public interface StateMachine {

    /**
     * Applies one committed entry. Called exactly once per index, in index order.
     *
     * @return a result for the client that proposed the entry, or null
     */
    Object apply(LogEntry entry);

    /** Index of the last applied entry, 0 if none. */
    long lastApplied();
}
