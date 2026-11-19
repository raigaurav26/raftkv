package com.raftkv.storage;

import java.io.IOException;

/** An I/O failure while making Raft state durable. The node cannot safely continue. */
public final class StorageException extends RuntimeException {

    public StorageException(String message, IOException cause) {
        super(message, cause);
    }
}
