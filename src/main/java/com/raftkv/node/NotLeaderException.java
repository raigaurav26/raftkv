package com.raftkv.node;

import com.raftkv.core.NodeId;

import java.util.Optional;

/** Thrown when a client command reaches a node that is not the leader. Carries a redirect hint. */
public final class NotLeaderException extends RuntimeException {

    private final transient NodeId leaderHint;

    public NotLeaderException(NodeId node, NodeId leaderHint) {
        super(node + " is not the leader" + (leaderHint == null ? "" : "; try " + leaderHint));
        this.leaderHint = leaderHint;
    }

    public Optional<NodeId> leaderHint() {
        return Optional.ofNullable(leaderHint);
    }
}
