package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/** Sorts a complete read by the same key order used by the validation merge. */
final class ValidationSortedRows implements ValidationRowCursor {
    static final long CHUNK_BYTES = 4L * 1024 * 1024;
    // One head per binary level, the current chunk and transient decoding work.
    static final long WORK_BYTES = CHUNK_BYTES + 10L * 1024 * 1024;

    private record Entry(ValidationKey key, ValidationValues.Row values) {}
    private record Run(FileChannel file, long size) {}
    private static final Comparator<Entry> ORDER = Comparator.comparing(Entry::key);
    private static final Set<StandardOpenOption> SPILL_OPTIONS = Set.of(CREATE_NEW, READ, WRITE, DELETE_ON_CLOSE);
    private static final FileAttribute<?> SPILL_DIRECTORY_MODE =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    private static final FileAttribute<?> SPILL_FILE_MODE =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));

    private final ValidationRowCursor input;
    private final Function<ValidationValues.Row, ValidationKey> key;
    private final ValidationValues.Limits limits;
    private final Runnable guard;
    private final long chunkLimit;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<FileChannel> files = new HashSet<>();
    private Path spillDirectory;
    private final List<Run> levels = new ArrayList<>();
    private final List<Entry> chunk = new ArrayList<>();
    private final PriorityQueue<RunReader> heads = new PriorityQueue<>(Comparator.comparing(reader -> reader.head, ORDER));
    private long bytes;
    private long ordinal;
    private boolean initialized;
    private int next;

    ValidationSortedRows(ValidationRowCursor input, Function<ValidationValues.Row, ValidationKey> key,
            ValidationValues.Limits limits, Runnable guard, long chunkLimit) {
        this.input = input;
        this.key = key;
        this.limits = limits;
        this.guard = guard;
        this.chunkLimit = chunkLimit;
    }

    @Override public Optional<Row> next() {
        try {
            active();
            if (!initialized) load();
            Entry entry;
            if (heads.isEmpty()) {
                if (next == chunk.size()) return Optional.empty();
                entry = chunk.set(next++, null);
            } else {
                RunReader reader = heads.remove();
                entry = reader.head;
                if (reader.advance()) heads.add(reader);
                else release(reader.run.file());
            }
            long index = ordinal++;
            return Optional.of(new Row() {
                @Override public long ordinal() { return index; }
                @Override public ValidationValues.Row values() { return entry.values(); }
                @Override public void close() {}
            });
        } catch (IOException failed) {
            if (closed.get() && failed instanceof ClosedChannelException) throw cancelled();
            throw ioFailure(failed);
        }
    }

    private void load() throws IOException {
        long expectedOrdinal = 0;
        for (;;) {
            active();
            var nextRow = input.next();
            if (nextRow.isEmpty()) break;
            try (var row = nextRow.orElseThrow()) {
                if (expectedOrdinal == Long.MAX_VALUE || row.ordinal() != expectedOrdinal++) {
                    throw new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", "validation-sort"), null);
                }
                var values = row.values();
                var entry = new Entry(key.apply(values), values);
                long charge = values.accountedBytes() + entry.key().retainedBytes() + 64;
                if (!chunk.isEmpty() && bytes + charge > chunkLimit) spill();
                chunk.add(entry);
                bytes += charge;
            }
        }
        if (levels.isEmpty()) {
            chunk.sort(ORDER);
        } else {
            if (!chunk.isEmpty()) spill();
            for (Run run : levels) {
                if (run == null) continue;
                var reader = new RunReader(run);
                if (reader.advance()) heads.add(reader);
            }
            levels.clear();
        }
        initialized = true;
    }

    private void spill() throws IOException {
        active();
        chunk.sort(ORDER);
        var file = openFile();
        var out = output(file);
        for (Entry entry : chunk) {
            active();
            ValidationSpillRows.write(out, entry.values().fields());
        }
        out.flush();
        file.position(0);
        Run run = new Run(file, chunk.size());
        chunk.clear();
        bytes = 0;
        // Consolidate runs as they arrive to keep the number of open files logarithmic.
        int level = 0;
        while (level < levels.size() && levels.get(level) != null) {
            run = merge(levels.set(level++, null), run);
        }
        if (level == levels.size()) levels.add(run);
        else levels.set(level, run);
    }

    private Run merge(Run left, Run right) throws IOException {
        var file = openFile();
        var out = output(file);
        var a = new RunReader(left);
        var b = new RunReader(right);
        a.advance();
        b.advance();
        while (a.head != null || b.head != null) {
            active();
            RunReader next = b.head == null || (a.head != null && ORDER.compare(a.head, b.head) <= 0) ? a : b;
            ValidationSpillRows.write(out, next.head.values().fields());
            if (!next.advance()) release(next.run.file());
        }
        out.flush();
        file.position(0);
        return new Run(file, Math.addExact(left.size(), right.size()));
    }

    private FileChannel openFile() throws IOException {
        synchronized (files) {
            active();
            if (spillDirectory == null) {
                spillDirectory = Files.createTempDirectory("tapstate-validation-", SPILL_DIRECTORY_MODE);
            }
            var path = spillDirectory.resolve(UUID.randomUUID() + ".rows");
            var file = FileChannel.open(path, SPILL_OPTIONS, SPILL_FILE_MODE);
            files.add(file);
            return file;
        }
    }

    private static DataOutputStream output(FileChannel file) {
        return new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(file)));
    }

    private void release(FileChannel file) throws IOException {
        synchronized (files) {
            file.close();
            files.remove(file);
        }
    }

    private void active() {
        guard.run();
        if (closed.get()) throw cancelled();
    }

    private static TapstateException cancelled() {
        return new TapstateException(ValidationError.READ_CANCELLED, Map.of("table", "validation-sort"), null);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        var failures = new io.tapstate.core.validation.ValidationFailureLatch();
        synchronized (files) {
            for (var file : files) {
                try { file.close(); }
                catch (IOException failed) { failures.record(ioFailure(failed)); }
            }
            files.clear();
            if (spillDirectory != null) {
                try { Files.deleteIfExists(spillDirectory); }
                catch (IOException failed) { failures.record(ioFailure(failed)); }
            }
        }
        try { input.close(); }
        catch (RuntimeException | Error failed) { failures.record(failed); }
        if (failures.get() instanceof Error failed) throw failed;
        if (failures.get() instanceof RuntimeException failed) throw failed;
    }

    private final class RunReader {
        private final Run run;
        private final DataInputStream in;
        private long remaining;
        private Entry head;

        RunReader(Run run) {
            this.run = run;
            in = new DataInputStream(new BufferedInputStream(Channels.newInputStream(run.file())));
            remaining = run.size();
        }

        boolean advance() throws IOException {
            if (remaining == 0) {
                head = null;
                return false;
            }
            var values = ValidationSpillRows.readRow(in, limits);
            head = new Entry(key.apply(values), values);
            remaining--;
            return true;
        }
    }

    private static TapstateException ioFailure(IOException cause) {
        return new TapstateException(ValidationError.READ_FAILED, Map.of("table", "validation-sort"), cause);
    }
}
