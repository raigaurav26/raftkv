package com.raftkv.node;

import com.raftkv.core.NodeId;
import com.raftkv.core.Role;

/** Hooks for tests and, later, metrics. Called on the node's own thread. */
public interface NodeListener {

    NodeListener NONE = new NodeListener() { };

    default void roleChanged(NodeId node, Role role, long term) { }

    /** The node learned that every entry up to {@code commitIndex} is committed. */
    default void committed(NodeId node, long commitIndex) { }
}
