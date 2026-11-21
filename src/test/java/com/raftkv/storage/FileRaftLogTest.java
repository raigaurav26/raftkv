package com.raftkv.storage;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;
import com.raftkv.log.RaftLog;
import com.raftkv.log.RaftLogContractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Runs the shared log rules on the file log, then checks what survives a reopen. */
class FileRaftLogTest extends RaftLogContractTest {

    @TempDir
    Path dir;

    private Path file;

    @Override
    protected RaftLog newLog() {
        file = dir.resolve("raft.log");
        return new FileRaftLog(file);
    }

    @AfterEach
    void close() {
        ((FileRaftLog) log).close();
    }

    private FileRaftLog reopen() {
        ((FileRaftLog) log).close();
        FileRaftLog reopened = new FileRaftLog(file);
        log = reopened;
        return reopened;
    }

    @Test
    void entriesSurviveReopen() {
        log.append(1, Command.put("a", "1"));
        log.append(1, Command.delete("a"));
        log.append(2, Command.noop());
        log.append(2, Command.put("ünïcödé ключ", "値"));
        List<LogEntry> before = log.entriesFrom(1, 100);

        FileRaftLog reopened = reopen();
        assertEquals(before, reopened.entriesFrom(1, 100));
        assertEquals(0, reopened.droppedTornBytes());
        reopened.append(3, Command.put("b", "2"));
        assertEquals(5, reopen().lastIndex());
    }

    @Test
    void truncationSurvivesReopen() {
        seed(1, 1, 2, 2, 2);
        log.appendFromLeader(2, 1, List.of(e(3, 3)));
        FileRaftLog reopened = reopen();
        assertArrayEquals(new long[]{1, 1, 3}, terms());
        assertEquals(3, reopened.lastIndex());
    }

    @Test
    void fileShrinksOnTruncate() throws IOException {
        seed(1, 1, 1, 1);
        long full = Files.size(file);
        log.appendFromLeader(1, 1, List.of(e(2, 2)));
        assertTrue(Files.size(file) < full);
        assertEquals(Files.size(file), ((FileRaftLog) log).sizeInBytes());
    }

    @Test
    void tornLastRecordIsDroppedOnOpen() throws IOException {
        seed(1, 1, 1);
        long good = Files.size(file);
        log.append(1, Command.put("half", "written"));
        long withFourth = Files.size(file);
        ((FileRaftLog) log).close();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(withFourth - 5); // crash in the middle of writing record 4
        }
        FileRaftLog reopened = new FileRaftLog(file);
        log = reopened;
        assertEquals(3, reopened.lastIndex());
        assertEquals(withFourth - 5 - good, reopened.droppedTornBytes());
        assertEquals(good, Files.size(file), "the half record is trimmed from the file");
        reopened.append(1, Command.put("again", "x"));
        assertEquals(4, reopen().lastIndex(), "appending after recovery works");
    }

    @Test
    void tornHeaderIsDroppedOnOpen() throws IOException {
        seed(1, 1);
        long good = Files.size(file);
        ((FileRaftLog) log).close();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(good);
            raf.write(new byte[]{0, 0, 0}); // three bytes of the next header
        }
        log = new FileRaftLog(file);
        assertEquals(2, log.lastIndex());
        assertEquals(good, Files.size(file));
    }

    @Test
    void flippedBitInsideTheLogIsCorruptionNotACrash() throws IOException {
        seed(1, 1, 1);
        ((FileRaftLog) log).close();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(FileRaftLog.HEADER + 3); // inside record 1's payload
            int b = raf.read();
            raf.seek(FileRaftLog.HEADER + 3);
            raf.write(b ^ 0x10);
        }
        assertThrows(CorruptStorageException.class, () -> new FileRaftLog(file));
        log = new FileRaftLog(dir.resolve("other.log")); // so @AfterEach has something to close
    }

    @Test
    void garbageLengthIsCorruption() throws IOException {
        ((FileRaftLog) log).close();
        Files.write(file, new byte[]{0x7f, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3});
        assertThrows(CorruptStorageException.class, () -> new FileRaftLog(file));
        log = new FileRaftLog(dir.resolve("other.log"));
    }

    @Test
    void codecRoundTripsEveryCommandShape() {
        for (Command c : List.of(Command.put("k", ""), Command.put("k", "v"), Command.delete("k"), Command.noop())) {
            LogEntry e = new LogEntry(7, 42, c);
            assertEquals(e, EntryCodec.decode(EntryCodec.encode(e)));
        }
    }

    // ------------------------------------------------------------ compaction on disk

    @Test
    void compactedLogSurvivesReopenAndFileShrinks() throws IOException {
        seed(1, 1, 1, 2, 2, 2);
        long full = Files.size(file);
        log.compactTo(4, 2);
        assertTrue(Files.size(file) < full);
        List<LogEntry> kept = log.entriesFrom(5, 10);

        FileRaftLog reopened = reopen();
        assertEquals(4, reopened.baseIndex(), "the base is the first stored index minus one");
        assertEquals(6, reopened.lastIndex());
        assertEquals(kept, reopened.entriesFrom(5, 10));
        assertEquals(0, reopened.baseTerm(), "the term of the base comes from the snapshot, not the log file");
        reopened.compactTo(4, 2);
        assertEquals(2, reopened.baseTerm());
        reopened.append(3, Command.noop());
        assertEquals(7, reopen().lastIndex());
    }

    @Test
    void fullyCompactedLogReopensEmpty() throws IOException {
        seed(1, 1);
        log.compactTo(2, 1);
        assertEquals(0, Files.size(file));
        FileRaftLog reopened = reopen();
        assertEquals(0, reopened.lastIndex());
        reopened.compactTo(2, 1); // what the node does on startup, from its snapshot
        assertEquals(2, reopened.lastIndex());
        assertEquals(1, reopened.lastTerm());
    }

    @Test
    void truncationAfterCompactionCutsTheRightRecord() {
        seed(1, 1, 2, 2, 2);
        log.compactTo(2, 1);
        log.appendFromLeader(3, 2, List.of(e(3, 4)));
        FileRaftLog reopened = reopen();
        assertEquals(2, reopened.baseIndex());
        assertArrayEquals(new long[]{2, 3}, reopened.entriesFrom(3, 10).stream().mapToLong(LogEntry::term).toArray());
    }

    @Test
    void leftoverCompactionTempFileFromACrashIsIgnored() throws IOException {
        seed(1, 1, 1);
        Files.write(dir.resolve("raft.log.compact"), new byte[]{9, 9, 9}); // crashed before the rename
        FileRaftLog reopened = reopen();
        assertEquals(3, reopened.lastIndex());
        assertEquals(0, reopened.baseIndex());
        reopened.compactTo(2, 1);
        assertEquals(3, reopen().lastIndex());
    }
}
