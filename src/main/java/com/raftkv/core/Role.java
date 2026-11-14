package com.raftkv.core;

import java.util.EnumSet;
import java.util.Set;

/**
 * The three roles a Raft server moves between (Raft paper, figure 4).
 *
 * <ul>
 *   <li>A follower that hears nothing from a leader times out and becomes a candidate.</li>
 *   <li>A candidate can start another election (stay a candidate), win and become leader,
 *       or discover a current leader or a higher term and step down to follower.</li>
 *   <li>A leader only ever steps down to follower, when it sees a higher term.</li>
 * </ul>
 */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER;

    public Set<Role> allowedNext() {
        return switch (this) {
            case FOLLOWER -> EnumSet.of(CANDIDATE);
            case CANDIDATE -> EnumSet.of(CANDIDATE, LEADER, FOLLOWER);
            case LEADER -> EnumSet.of(FOLLOWER);
        };
    }

    public boolean canMoveTo(Role next) {
        return allowedNext().contains(next);
    }

    /** Returns {@code next} if the move is legal, otherwise throws. */
    public Role moveTo(Role next) {
        if (!canMoveTo(next)) {
            throw new IllegalStateException("illegal role change " + this + " -> " + next);
        }
        return next;
    }
}
