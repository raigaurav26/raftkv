package com.raftkv.node;

import com.raftkv.core.NodeId;
import com.raftkv.core.Role;
import com.raftkv.log.InMemoryRaftLog;
import com.raftkv.transport.InMemoryNetwork;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Several {@link RaftNode}s on one {@link InMemoryNetwork}, all driven from the test thread.
 * One step = tick every node, then deliver every message (including replies). With a fixed
 * seed every run is identical, so a failing seed can be replayed exactly.
 *
 * <p>It also records every leader it ever sees, by term, to check election safety: at most
 * one leader per term.
 */
final class TestCluster {

    final InMemoryNetwork network = new InMemoryNetwork();
    final Map<NodeId, RaftNode> nodes = new LinkedHashMap<>();
    final Map<NodeId, InMemoryStateStore> stores = new HashMap<>();
    private final Map<Long, Set<NodeId>> leadersByTerm = new HashMap<>();
    private final RaftConfig config;
    private final Random seeds;
    private long steps;

    TestCluster(int size, long seed) {
        this(size, seed, RaftConfig.DEFAULT);
    }

    TestCluster(int size, long seed, RaftConfig config) {
        this.config = config;
        this.seeds = new Random(seed);
        List<NodeId> ids = new ArrayList<>();
        for (int i = 1; i <= size; i++) ids.add(NodeId.of("n" + i));
        for (NodeId id : ids) {
            Set<NodeId> peers = new HashSet<>(ids);
            peers.remove(id);
            InMemoryStateStore store = new InMemoryStateStore();
            stores.put(id, store);
            nodes.put(id, newNode(id, peers, store));
        }
    }

    private RaftNode newNode(NodeId id, Set<NodeId> peers, InMemoryStateStore store) {
        NodeListener recorder = new NodeListener() {
            @Override
            public void roleChanged(NodeId node, Role role, long term) {
                if (role == Role.LEADER) leadersByTerm.computeIfAbsent(term, t -> new HashSet<>()).add(node);
            }
        };
        return new RaftNode(id, peers, network, new InMemoryRaftLog(), store, config,
                new Random(seeds.nextLong()), recorder);
    }

    RaftNode node(String id) {
        return nodes.get(NodeId.of(id));
    }

    List<NodeId> ids() {
        return List.copyOf(nodes.keySet());
    }

    void step() {
        nodes.values().forEach(RaftNode::tick);
        network.deliverAll();
        steps++;
    }

    void run(int steps) {
        for (int i = 0; i < steps; i++) step();
    }

    /** Steps until the condition holds; fails the test if it does not within {@code maxSteps}. */
    void runUntil(BooleanSupplier condition, int maxSteps, String what) {
        for (int i = 0; i < maxSteps; i++) {
            if (condition.getAsBoolean()) return;
            step();
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("no " + what + " after " + maxSteps + " steps; " + describe());
        }
    }

    List<RaftNode> leaders() {
        return nodes.values().stream().filter(RaftNode::isLeader).toList();
    }

    /** The single leader among {@code among}, if exactly one of them leads. */
    Optional<RaftNode> leaderAmong(Set<NodeId> among) {
        List<RaftNode> l = leaders().stream().filter(n -> among.contains(n.id())).toList();
        return l.size() == 1 ? Optional.of(l.get(0)) : Optional.empty();
    }

    RaftNode awaitLeader(int maxSteps) {
        return awaitLeader(nodes.keySet(), maxSteps);
    }

    /** Waits until exactly one node in {@code among} is leader and the rest of them follow it. */
    RaftNode awaitLeader(Set<NodeId> among, int maxSteps) {
        runUntil(() -> leaderAmong(among).map(l -> among.stream()
                .map(nodes::get)
                .allMatch(n -> n == l || (n.role() == Role.FOLLOWER
                        && n.term() == l.term() && n.leaderId().equals(Optional.of(l.id())))))
                .orElse(false), maxSteps, "stable leader among " + among);
        return leaderAmong(among).orElseThrow();
    }

    /** Election safety: no term ever had two leaders. */
    void assertElectionSafety() {
        leadersByTerm.forEach((term, ls) -> {
            if (ls.size() > 1) throw new AssertionError("term " + term + " had leaders " + ls);
        });
    }

    Map<Long, Set<NodeId>> leadersByTerm() {
        return leadersByTerm;
    }

    String describe() {
        StringBuilder sb = new StringBuilder("step " + steps + ":");
        nodes.values().forEach(n -> sb.append(' ').append(n.id()).append('=')
                .append(n.role()).append("@t").append(n.term()));
        return sb.toString();
    }
}
