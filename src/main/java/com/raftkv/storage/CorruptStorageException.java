package com.raftkv.storage;

/**
 * A file on disk failed its checksum in a way a crash cannot explain. The node refuses to
 * start rather than guess: serving from a damaged log could break Raft's guarantees.
 */
public final class CorruptStorageException extends RuntimeException {

    public CorruptStorageException(String message) {
        super(message);
    }
}
