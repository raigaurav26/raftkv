package com.raftkv.storage;

import com.raftkv.core.Snapshot;
import com.raftkv.node.SnapshotStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * The latest snapshot in one file, replaced atomically (temp file, fsync, rename, fsync the
 * directory). Layout: {@code [CRC32 of rest: int][index: long][term: long][length: int][data]}.
 * A crash while saving leaves the previous snapshot untouched.
 */
public final class FileSnapshotStore implements SnapshotStore {

    private final Path file;
    private final Path temp;

    public FileSnapshotStore(Path file) {
        this.file = file;
        this.temp = file.resolveSibling(file.getFileName() + ".tmp");
    }

    @Override
    public Optional<Snapshot> load() {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new StorageException("cannot read " + file, e);
        }
        if (bytes.length < 24) throw new CorruptStorageException(file + " is too short");
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int crc = buf.getInt();
        CRC32 check = new CRC32();
        check.update(bytes, 4, bytes.length - 4);
        if ((int) check.getValue() != crc) throw new CorruptStorageException(file + ": checksum mismatch");
        long index = buf.getLong();
        long term = buf.getLong();
        int length = buf.getInt();
        if (length < 0 || length != buf.remaining()) throw new CorruptStorageException(file + ": bad data length");
        byte[] data = new byte[length];
        buf.get(data);
        try {
            return Optional.of(new Snapshot(index, term, data));
        } catch (IllegalArgumentException e) {
            throw new CorruptStorageException(file + ": " + e.getMessage());
        }
    }

    @Override
    public void save(Snapshot snapshot) {
        byte[] data = snapshot.data();
        ByteBuffer rest = ByteBuffer.allocate(20 + data.length)
                .putLong(snapshot.lastIncludedIndex()).putLong(snapshot.lastIncludedTerm())
                .putInt(data.length).put(data);
        CRC32 crc = new CRC32();
        crc.update(rest.array());
        ByteBuffer all = ByteBuffer.allocate(4 + rest.capacity()).putInt((int) crc.getValue()).put(rest.array());
        all.flip();
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            try (FileChannel ch = FileChannel.open(temp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                while (all.hasRemaining()) ch.write(all);
                ch.force(true);
            }
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            try (FileChannel dir = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
                dir.force(true);
            } catch (IOException | UnsupportedOperationException ignored) {
                // best effort; not possible on every OS
            }
        } catch (IOException e) {
            throw new StorageException("cannot save " + file, e);
        }
    }
}
