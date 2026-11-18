package com.raftkv.kv;

import com.raftkv.core.Command;
import com.raftkv.node.RaftNode;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Client operations on one node. Writes go through the log; reads use the leader's read
 * barrier so they never return stale data, even from a leader that has just been replaced
 * without knowing it. Must be called on the node's thread, like the node itself.
 */
public final class KvService {

    private final RaftNode node;
    private final KvStateMachine store;

    public KvService(RaftNode node, KvStateMachine store) {
        this.node = Objects.requireNonNull(node, "node");
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Completes with the previous value once the write is committed and applied. */
    public CompletableFuture<Optional<String>> put(String key, String value) {
        return write(Command.put(key, value));
    }

    /** Completes with the removed value once the delete is committed and applied. */
    public CompletableFuture<Optional<String>> delete(String key) {
        return write(Command.delete(key));
    }

    /** A linearizable read: reflects every write that completed before it was called. */
    public CompletableFuture<Optional<String>> get(String key) {
        return node.readBarrier().thenApply(index -> store.get(key));
    }

    private CompletableFuture<Optional<String>> write(Command command) {
        return node.submit(command).thenApply(r -> ((KvStateMachine.Result) r).previous());
    }
}
