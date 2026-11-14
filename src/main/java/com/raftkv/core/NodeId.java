package com.raftkv.core;

import java.util.Objects;
import java.util.regex.Pattern;

/** Identity of one server in the cluster, such as {@code n1}. */
public record NodeId(String value) implements Comparable<NodeId> {

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}");

    public NodeId {
        Objects.requireNonNull(value, "value");
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid node id: '" + value + "'");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
