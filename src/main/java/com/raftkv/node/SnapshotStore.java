package com.raftkv.node;

import com.raftkv.core.Snapshot;

import java.util.Optional;

/**
 * Keeps the latest snapshot. {@link #save} must be durable before it returns, because the node
 * deletes log entries the snapshot covers right afterwards. Only the newest snapshot is kept.
 */
public interface SnapshotStore {

    Optional<Snapshot> load();

    void save(Snapshot snapshot);
}
