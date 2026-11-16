package com.raftkv.node;

import com.raftkv.core.PersistentState;

import java.util.Objects;

/** A {@link StateStore} that lives as long as the object. Tests reuse it to simulate a restart. */
public final class InMemoryStateStore implements StateStore {

    private volatile PersistentState state = PersistentState.INITIAL;
    private volatile int saves;

    @Override
    public PersistentState load() {
        return state;
    }

    @Override
    public void save(PersistentState state) {
        this.state = Objects.requireNonNull(state, "state");
        saves++;
    }

    public int saveCount() {
        return saves;
    }
}
