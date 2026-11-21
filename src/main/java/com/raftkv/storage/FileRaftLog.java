package com.raftkv.storage;

import com.raftkv.core.LogEntry;
import com.raftkv.log.AbstractRaftLog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * An append-only log file. Each record is
 *
 * <pre>
 *   [payload length: int][CRC32 of payload: int][payload: EntryCodec bytes]
 * </pre>
 *
 * Appends are written and then {@code fsync}ed before the call returns, so a follower only
 * acknowledges entries that survive a power cut. A batch from one AppendEntries costs one
 * fsync. Truncation (when a new leader overwrites a stale suffix) cuts the file at the start
 * of the first removed record, using the offsets kept in memory. Compaction (after a snapshot)
 * writes the entries that remain to a new file, fsyncs it and renames it over the old one, so
 * a crash leaves either the full old log or the compacted one. The file does not store the base
 * explicitly: it is the first record's index minus one, and its term comes from the snapshot.
 *
 * <p>On open, the file is scanned. A last record that is cut short is the trace of a crash
 * in the middle of a write; it was never acknowledged, so it is dropped and the file is
 * trimmed. A complete record with a bad checksum cannot come from a crash and is reported as
 * corruption instead.
 */
public final class FileRaftLog extends AbstractRaftLog implements Closeable {

    static final int HEADER = 8;
    static final int MAX_RECORD = 16 * 1024 * 1024;

    private final Path file;
    private FileChannel channel;
    private final List<Long> offsets = new ArrayList<>(); // offsets.get(i) = start of entry i+1
    private long size;
    private final boolean fsync;
    private long droppedTornBytes;

    public FileRaftLog(Path file) {
        this(file, true);
    }

    /** {@code fsync = false} is only for benchmarks that measure everything but the disk. */
    public FileRaftLog(Path file, boolean fsync) {
        this.file = file;
        this.fsync = fsync;
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            recover();
        } catch (IOException e) {
            throw new StorageException("cannot open log " + file, e);
        }
    }

    private void recover() throws IOException {
        long fileSize = channel.size();
        long pos = 0;
        ByteBuffer header = ByteBuffer.allocate(HEADER);
        while (pos < fileSize) {
            if (fileSize - pos < HEADER) break; // torn header
            header.clear();
            readFully(header, pos);
            header.flip();
            int length = header.getInt();
            int crc = header.getInt();
            if (length <= 0 || length > MAX_RECORD) {
                throw new CorruptStorageException(file + ": bad record length " + length + " at offset " + pos);
            }
            if (fileSize - pos - HEADER < length) break; // torn payload
            ByteBuffer payload = ByteBuffer.allocate(length);
            readFully(payload, pos + HEADER);
            if (crc32(payload.array()) != crc) {
                throw new CorruptStorageException(file + ": checksum mismatch at offset " + pos);
            }
            LogEntry entry = EntryCodec.decode(payload.array());
            if (offsets.isEmpty() && entry.index() > 1) setRecoveredBase(entry.index() - 1);
            if (entry.index() != lastIndex() + 1) {
                throw new CorruptStorageException(file + ": expected index " + (lastIndex() + 1) + " got " + entry.index());
            }
            if (!offsets.isEmpty() && entry.term() < lastTerm()) {
                throw new CorruptStorageException(file + ": term went backwards at index " + entry.index());
            }
            offsets.add(pos);
            loadRecovered(entry);
            pos += HEADER + length;
        }
        if (pos < fileSize) {
            droppedTornBytes = fileSize - pos;
            channel.truncate(pos);
            channel.force(true);
        }
        size = pos;
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf, position + buf.position());
            if (n < 0) throw new IOException("unexpected end of " + file);
        }
    }

    @Override
    protected void persistTruncate(long fromIndex) {
        long cut = offsets.get((int) (fromIndex - baseIndex() - 1));
        try {
            channel.truncate(cut);
            if (fsync) channel.force(true);
        } catch (IOException e) {
            throw new StorageException("cannot truncate " + file, e);
        }
        offsets.subList((int) (fromIndex - baseIndex() - 1), offsets.size()).clear();
        size = cut;
    }

    @Override
    protected void persistCompact(long newBaseIndex, long newBaseTerm, List<LogEntry> keep) {
        Path temp = file.resolveSibling(file.getFileName() + ".compact");
        try {
            List<Long> newOffsets = new ArrayList<>(keep.size());
            long at = 0;
            try (FileChannel out = FileChannel.open(temp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                for (LogEntry e : keep) {
                    byte[] p = EntryCodec.encode(e);
                    ByteBuffer buf = ByteBuffer.allocate(HEADER + p.length).putInt(p.length).putInt(crc32(p)).put(p);
                    buf.flip();
                    while (buf.hasRemaining()) out.write(buf);
                    newOffsets.add(at);
                    at += HEADER + p.length;
                }
                if (fsync) out.force(true);
            }
            channel.close();
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            offsets.clear();
            offsets.addAll(newOffsets);
            size = at;
            syncDirectory();
        } catch (IOException e) {
            throw new StorageException("cannot compact " + file, e);
        }
    }

    private void syncDirectory() {
        if (!fsync) return;
        try (FileChannel dir = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
            dir.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // best effort; not possible on every OS
        }
    }

    @Override
    protected void persistAppend(List<LogEntry> newEntries) {
        List<byte[]> payloads = new ArrayList<>(newEntries.size());
        int total = 0;
        for (LogEntry e : newEntries) {
            byte[] p = EntryCodec.encode(e);
            if (p.length > MAX_RECORD) throw new IllegalArgumentException("entry " + e.index() + " is too large");
            payloads.add(p);
            total += HEADER + p.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(total);
        List<Long> newOffsets = new ArrayList<>(payloads.size());
        long at = size;
        for (byte[] p : payloads) {
            newOffsets.add(at);
            buf.putInt(p.length).putInt(crc32(p)).put(p);
            at += HEADER + p.length;
        }
        buf.flip();
        try {
            long pos = size;
            while (buf.hasRemaining()) pos += channel.write(buf, pos);
            if (fsync) channel.force(false);
        } catch (IOException e) {
            throw new StorageException("cannot append to " + file, e);
        }
        offsets.addAll(newOffsets);
        size = at;
    }

    static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    /** Bytes of a half-written last record that were discarded when the file was opened. */
    public long droppedTornBytes() {
        return droppedTornBytes;
    }

    public long sizeInBytes() {
        return size;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new StorageException("cannot close " + file, e);
        }
    }
}
