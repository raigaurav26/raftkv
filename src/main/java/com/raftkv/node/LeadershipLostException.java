package com.raftkv.node;

/**
 * The node stopped being leader before a client request finished. For a write this means the
 * entry was overwritten by a new leader and was not applied; the client may retry.
 */
public final class LeadershipLostException extends RuntimeException {

    public LeadershipLostException(String message) {
        super(message);
    }
}
