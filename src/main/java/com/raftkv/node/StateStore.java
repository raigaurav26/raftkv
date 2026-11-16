package com.raftkv.node;

import com.raftkv.core.PersistentState;

/**
 * Where a node keeps its term and vote. {@link #save} must be durable before it returns:
 * a node that forgets its vote after a restart could vote twice in one term and let two
 * leaders be elected. The file-backed store arrives on Day 6.
 */
public interface StateStore {

    PersistentState load();

    void save(PersistentState state);
}
