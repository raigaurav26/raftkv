package com.raftkv.storage;

import com.raftkv.core.NodeId;
import com.raftkv.core.PersistentState;
import com.raftkv.node.StateStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Term and vote in a small file, replaced atomically on every change: write a temp file,
 * fsync it, rename it over the old one, fsync the directory. A crash at any point leaves
 * either the old state or the new one, never a mix. Layout:
 * {@code [CRC32 of rest: int][term: long][vote length: int][vote: UTF-8, -1 for none]}.
 */
public final class FileStateStore implements StateStore {

    private final Path file;
    private final Path temp;

    public FileStateStore(Path file) {
        this.file = file;
        this.temp = file.resolveSibling(file.getFileName() + ".tmp");
    }

    @Override
    public PersistentState load() {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            return PersistentState.INITIAL;
        } catch (IOException e) {
            throw new StorageException("cannot read " + file, e);
        }
        if (bytes.length < 16) throw new CorruptStorageException(file + " is too short");
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int crc = buf.getInt();
        byte[] rest = new byte[bytes.length - 4];
        System.arraycopy(bytes, 4, rest, 0, rest.length);
        if (FileRaftLog.crc32(rest) != crc) throw new CorruptStorageException(file + ": checksum mismatch");
        long term = buf.getLong();
        int len = buf.getInt();
        NodeId vote = null;
        if (len >= 0) {
            if (len != buf.remaining()) throw new CorruptStorageException(file + ": bad vote length");
            byte[] v = new byte[len];
            buf.get(v);
            vote = NodeId.of(new String(v, StandardCharsets.UTF_8));
        }
        return new PersistentState(term, vote);
    }

    @Override
    public void save(PersistentState state) {
        byte[] vote = state.votedFor() == null ? null : state.votedFor().value().getBytes(StandardCharsets.UTF_8);
        ByteBuffer body = ByteBuffer.allocate(12 + (vote == null ? 0 : vote.length));
        body.putLong(state.currentTerm()).putInt(vote == null ? -1 : vote.length);
        if (vote != null) body.put(vote);
        byte[] rest = body.array();
        ByteBuffer all = ByteBuffer.allocate(4 + rest.length).putInt(FileRaftLog.crc32(rest)).put(rest);
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
            syncDirectory();
        } catch (IOException e) {
            throw new StorageException("cannot save " + file, e);
        }
    }

    /** Makes the rename itself durable. Not possible on every OS (e.g. Windows); skipped there. */
    private void syncDirectory() {
        try (FileChannel dir = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
            dir.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // best effort
        }
    }
}
