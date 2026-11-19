package com.raftkv.storage;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Binary form of a log entry: term, index, op, then key and value as length-prefixed UTF-8
 * (length -1 for a missing value). A fixed layout keeps records small and the format obvious.
 */
public final class EntryCodec {

    private EntryCodec() {
    }

    public static byte[] encode(LogEntry e) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(e.term());
            out.writeLong(e.index());
            out.writeByte(e.command().op().ordinal());
            writeString(out, e.command().key());
            writeString(out, e.command().value());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex); // cannot happen on a byte array
        }
        return bytes.toByteArray();
    }

    public static LogEntry decode(byte[] payload) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            long term = in.readLong();
            long index = in.readLong();
            int op = in.readUnsignedByte();
            Command.Op[] ops = Command.Op.values();
            if (op >= ops.length) throw new CorruptStorageException("unknown op " + op);
            String key = readString(in);
            String value = readString(in);
            if (in.available() != 0) throw new CorruptStorageException("trailing bytes in entry " + index);
            return new LogEntry(term, index, new Command(ops[op], key, value));
        } catch (IOException | IllegalArgumentException | NullPointerException ex) {
            throw new CorruptStorageException("cannot decode log entry: " + ex.getMessage());
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) return null;
        if (len < 0 || len > in.available()) throw new IOException("bad string length " + len);
        return new String(in.readNBytes(len), StandardCharsets.UTF_8);
    }
}
