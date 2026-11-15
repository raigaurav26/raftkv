package com.raftkv.transport;

import com.raftkv.core.NodeId;
import com.raftkv.rpc.RaftMessage;

/**
 * Sends Raft messages between nodes. Delivery is best effort: a message may be lost,
 * delayed or reordered, and Raft has to stay safe anyway. Senders never block on a reply;
 * replies come back as ordinary messages to the sender's {@link MessageHandler}.
 */
public interface Transport {

    /** Starts delivering messages addressed to {@code node}. One handler per node. */
    void register(NodeId node, MessageHandler handler);

    void unregister(NodeId node);

    void send(NodeId from, NodeId to, RaftMessage message);
}
