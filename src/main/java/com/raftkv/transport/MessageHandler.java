package com.raftkv.transport;

import com.raftkv.core.NodeId;
import com.raftkv.rpc.RaftMessage;

@FunctionalInterface
public interface MessageHandler {

    void onMessage(NodeId from, RaftMessage message);
}
