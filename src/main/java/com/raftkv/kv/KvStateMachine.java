package com.raftkv.kv;

import com.raftkv.core.Command;
import com.raftkv.core.LogEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The key-value map. It changes only through {@link #apply}, which the node calls for
 * committed entries in log order; reads see the state as of {@link #lastApplied()}.
 * Single-threaded: the node owns it.
 */
public final class KvStateMachine implements StateMachine {

    private final Map<String, String> data = new HashMap<>();
    private long lastApplied;

    /** Result of applying a command: the value the key held before, if any. */
    public record Result(Optional<String> previous) { }

    @Override
    public Result apply(LogEntry entry) {
        if (entry.index() != lastApplied + 1) {
            throw new IllegalStateException("expected index " + (lastApplied + 1) + " but got " + entry.index());
        }
        lastApplied = entry.index();
        Command c = entry.command();
        return switch (c.op()) {
            case PUT -> new Result(Optional.ofNullable(data.put(c.key(), c.value())));
            case DELETE -> new Result(Optional.ofNullable(data.remove(c.key())));
            case NOOP -> new Result(Optional.empty());
        };
    }

    @Override
    public long lastApplied() {
        return lastApplied;
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public int size() {
        return data.size();
    }

    /** Entry count, then each key and value as length-prefixed UTF-8, keys in sorted order. */
    @Override
    public byte[] snapshot() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : new TreeMap<>(data).entrySet()) {
                writeString(out, e.getKey());
                writeString(out, e.getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // cannot happen on a byte array
        }
        return bytes.toByteArray();
    }

    @Override
    public void restore(byte[] snapshot, long lastApplied) {
        if (lastApplied < 0) throw new IllegalArgumentException("lastApplied must be >= 0");
        Map<String, String> loaded = new HashMap<>();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            int count = in.readInt();
            if (count < 0) throw new IllegalArgumentException("negative entry count");
            for (int i = 0; i < count; i++) {
                String key = readString(in);
                if (loaded.put(key, readString(in)) != null) throw new IllegalArgumentException("duplicate key " + key);
            }
            if (in.available() != 0) throw new IllegalArgumentException("trailing bytes");
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read snapshot: " + e.getMessage(), e);
        }
        data.clear();
        data.putAll(loaded);
        this.lastApplied = lastApplied;
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > in.available()) throw new IOException("bad string length " + len);
        return new String(in.readNBytes(len), StandardCharsets.UTF_8);
    }

    /** A copy of the whole map, for tests. */
    public Map<String, String> contents() {
        return Map.copyOf(data);
    }
}
