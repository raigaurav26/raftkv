package com.raftkv.transport;

import com.raftkv.core.NodeId;
import com.raftkv.rpc.Envelope;
import com.raftkv.rpc.RaftMessage;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A single-threaded, deterministic network for tests. {@link #send} only queues a message;
 * nothing is delivered until the test calls {@link #deliverNext()} or {@link #deliverAll()}.
 * That lets a test decide exactly when, and in what order, each node sees each message.
 *
 * <p>It can cut links between groups of nodes ({@link #partition}) and drop messages that
 * match a rule ({@link #dropIf}). The Day 11 fault-injection harness builds on these.
 * Not thread-safe by design: tests drive it from one thread.
 */
public final class InMemoryNetwork implements Transport {

    private final Map<NodeId, MessageHandler> handlers = new HashMap<>();
    private final Deque<Envelope> inFlight = new ArrayDeque<>();
    private final Set<Link> cut = new HashSet<>();
    private Predicate<Envelope> dropRule = e -> false;

    private long delivered;
    private long dropped;

    private record Link(NodeId a, NodeId b) {
        static Link of(NodeId x, NodeId y) {
            return x.compareTo(y) < 0 ? new Link(x, y) : new Link(y, x);
        }
    }

    @Override
    public void register(NodeId node, MessageHandler handler) {
        Objects.requireNonNull(handler, "handler");
        if (handlers.putIfAbsent(node, handler) != null) {
            throw new IllegalStateException(node + " is already registered");
        }
    }

    @Override
    public void unregister(NodeId node) {
        handlers.remove(node);
    }

    @Override
    public void send(NodeId from, NodeId to, RaftMessage message) {
        inFlight.addLast(new Envelope(from, to, message));
    }

    /**
     * Delivers the oldest queued message. Messages a handler sends while handling it go to the
     * back of the queue, so handlers never run re-entrantly.
     *
     * @return false if nothing was queued
     */
    public boolean deliverNext() {
        Envelope e = inFlight.pollFirst();
        if (e == null) return false;
        MessageHandler handler = handlers.get(e.to());
        if (handler == null || isCut(e.from(), e.to()) || dropRule.test(e)) {
            dropped++;
            return true;
        }
        delivered++;
        handler.onMessage(e.from(), e.message());
        return true;
    }

    /**
     * Delivers until the queue is empty, including replies sent along the way.
     *
     * @return number of messages taken off the queue
     * @throws IllegalStateException if handlers keep sending after {@code limit} messages,
     *     which in a test almost always means a message loop
     */
    public int deliverAll(int limit) {
        int n = 0;
        while (deliverNext()) {
            if (++n >= limit && !inFlight.isEmpty()) {
                throw new IllegalStateException("still " + inFlight.size() + " messages after " + limit);
            }
        }
        return n;
    }

    public int deliverAll() {
        return deliverAll(100_000);
    }

    /** Discards every queued message, as if they were all lost. */
    public int dropAllInFlight() {
        int n = inFlight.size();
        dropped += n;
        inFlight.clear();
        return n;
    }

    /**
     * Splits the cluster: nodes in different groups cannot reach each other, nodes in the same
     * group still can. Nodes not named in any group are cut off from everyone named.
     * Replaces any earlier partition.
     */
    @SafeVarargs
    public final void partition(Collection<NodeId>... groups) {
        cut.clear();
        Set<NodeId> all = new HashSet<>(handlers.keySet());
        for (Collection<NodeId> g : groups) all.addAll(g);
        Map<NodeId, Integer> groupOf = new HashMap<>();
        for (int i = 0; i < groups.length; i++) {
            for (NodeId n : groups[i]) {
                if (groupOf.put(n, i) != null) throw new IllegalArgumentException(n + " is in two groups");
            }
        }
        List<NodeId> nodes = List.copyOf(all);
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                Integer gi = groupOf.get(nodes.get(i));
                Integer gj = groupOf.get(nodes.get(j));
                if (gi == null || !gi.equals(gj)) cut.add(Link.of(nodes.get(i), nodes.get(j)));
            }
        }
    }

    /** Cuts {@code node} off from every other registered node. */
    public void isolate(NodeId node) {
        for (NodeId other : handlers.keySet()) {
            if (!other.equals(node)) cut.add(Link.of(node, other));
        }
    }

    /** Restores every link and removes the drop rule. Queued messages stay queued. */
    public void heal() {
        cut.clear();
        dropRule = e -> false;
    }

    /** Drops every message the rule matches, for example all heartbeats to one node. */
    public void dropIf(Predicate<Envelope> rule) {
        this.dropRule = Objects.requireNonNull(rule, "rule");
    }

    public boolean isCut(NodeId a, NodeId b) {
        return cut.contains(Link.of(a, b));
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    public List<Envelope> inFlight() {
        return List.copyOf(inFlight);
    }

    public long deliveredCount() {
        return delivered;
    }

    public long droppedCount() {
        return dropped;
    }
}
