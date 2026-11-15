package com.raftkv.rpc;

import com.raftkv.core.NodeId;

import java.util.Objects;

/** A message in flight from one node to another. */
public record Envelope(NodeId from, NodeId to, RaftMessage message) {

    public Envelope {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(message, "message");
        if (from.equals(to)) throw new IllegalArgumentException("a node does not message itself");
    }
}
