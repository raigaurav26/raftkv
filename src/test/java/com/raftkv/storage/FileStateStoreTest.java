package com.raftkv.storage;

import com.raftkv.core.NodeId;
import com.raftkv.core.PersistentState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileStateStoreTest {

    @TempDir
    Path dir;

    @Test
    void missingFileMeansFreshNode() {
        assertEquals(PersistentState.INITIAL, new FileStateStore(dir.resolve("state")).load());
    }

    @Test
    void savesTermAndVote() {
        Path file = dir.resolve("data/state");
        FileStateStore store = new FileStateStore(file);
        store.save(new PersistentState(7, NodeId.of("n3")));
        assertEquals(new PersistentState(7, NodeId.of("n3")), new FileStateStore(file).load());
        store.save(new PersistentState(8, null));
        assertEquals(new PersistentState(8, null), new FileStateStore(file).load());
        assertFalse(Files.exists(dir.resolve("data/state.tmp")), "temp file renamed away");
    }

    @Test
    void leftoverTempFileFromACrashIsIgnored() throws IOException {
        Path file = dir.resolve("state");
        FileStateStore store = new FileStateStore(file);
        store.save(new PersistentState(3, NodeId.of("n1")));
        Files.write(dir.resolve("state.tmp"), new byte[]{1, 2, 3}); // crash before rename
        assertEquals(new PersistentState(3, NodeId.of("n1")), new FileStateStore(file).load());
        store.save(new PersistentState(4, null));
        assertEquals(4, store.load().currentTerm());
    }

    @Test
    void corruptFileIsRefused() throws IOException {
        Path file = dir.resolve("state");
        new FileStateStore(file).save(new PersistentState(5, NodeId.of("n2")));
        byte[] bytes = Files.readAllBytes(file);
        bytes[6] ^= 1;
        Files.write(file, bytes);
        assertThrows(CorruptStorageException.class, () -> new FileStateStore(file).load());
        Files.write(file, new byte[]{1, 2});
        assertThrows(CorruptStorageException.class, () -> new FileStateStore(file).load());
    }
}
