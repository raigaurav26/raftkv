package com.raftkv.storage;

import com.raftkv.core.Snapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FileSnapshotStoreTest {

    @TempDir
    Path dir;

    @Test
    void noFileMeansNoSnapshot() {
        assertEquals(Optional.empty(), new FileSnapshotStore(dir.resolve("snap")).load());
    }

    @Test
    void savesAndReplacesTheLatestSnapshot() {
        Path file = dir.resolve("sub/snap");
        FileSnapshotStore store = new FileSnapshotStore(file);
        Snapshot first = new Snapshot(10, 2, new byte[]{1, 2, 3});
        store.save(first);
        assertEquals(Optional.of(first), new FileSnapshotStore(file).load());
        Snapshot second = new Snapshot(25, 3, new byte[0]);
        store.save(second);
        assertEquals(Optional.of(second), new FileSnapshotStore(file).load());
        assertFalse(Files.exists(dir.resolve("sub/snap.tmp")));
    }

    @Test
    void crashWhileSavingKeepsThePreviousSnapshot() throws IOException {
        Path file = dir.resolve("snap");
        FileSnapshotStore store = new FileSnapshotStore(file);
        Snapshot good = new Snapshot(10, 2, new byte[]{7});
        store.save(good);
        Files.write(dir.resolve("snap.tmp"), new byte[]{1, 2, 3, 4, 5}); // half-written replacement
        assertEquals(Optional.of(good), new FileSnapshotStore(file).load());
    }

    @Test
    void corruptFileIsRefused() throws IOException {
        Path file = dir.resolve("snap");
        new FileSnapshotStore(file).save(new Snapshot(10, 2, new byte[]{1, 2, 3, 4, 5, 6}));
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 2] ^= 1;
        Files.write(file, bytes);
        assertThrows(CorruptStorageException.class, () -> new FileSnapshotStore(file).load());
        Files.write(file, new byte[]{1, 2, 3});
        assertThrows(CorruptStorageException.class, () -> new FileSnapshotStore(file).load());
    }

    @Test
    void snapshotCopiesItsBytes() {
        byte[] data = {1, 2, 3};
        Snapshot s = new Snapshot(1, 1, data);
        data[0] = 99;
        assertEquals(1, s.data()[0]);
    }
}
