package com.raftkv.rpc;

/**
 * A message between two Raft servers. Every message carries the sender's current term:
 * a receiver that sees a higher term steps down to follower before doing anything else,
 * and a receiver that sees a lower term rejects the message (Raft paper, section 5.1).
 *
 * <p>The node logic is written in a message-passing style (send now, handle the reply later)
 * so the same code runs over the in-memory network in tests and over HTTP between processes.
 */
public sealed interface RaftMessage
        permits RequestVote, VoteResponse, AppendEntries, AppendResponse {

    long term();
}
