package com.raftkv.node;

import com.raftkv.core.Snapshot;

import java.util.Objects;
import java.util.Optional;

/** A {@link SnapshotStore} that lives as long as the object. */
public final class InMemorySnapshotStore implements SnapshotStore {

    private volatile Snapshot latest;

    @Override
    public Optional<Snapshot> load() {
        return Optional.ofNullable(latest);
    }

    @Override
    public void save(Snapshot snapshot) {
        Snapshot current = latest;
        if (current != null && snapshot.lastIncludedIndex() < current.lastIncludedIndex()) {
            throw new IllegalArgumentException("snapshots only move forward");
        }
        latest = Objects.requireNonNull(snapshot, "snapshot");
    }
}
