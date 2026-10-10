package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import javax.management.ObjectName;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Default-off, bounded receipts at the first clock observation after a table's write call returns.
 * These observations are not physical per-row commit timestamps. Unknown scope, partial callbacks,
 * lost receipts and unsupported keys remain explicit and never change the connector's write result.
 */
public final class PdkWriteReturnProbe implements PdkWriteReturnProbeMBean {
    static final String PROPERTY = "tapstate.benchmark.write-return";
    static final String OBJECT_NAME = "io.tapstate.benchmark:type=WriteReturn";
    static final int MAX_FRAME_BYTES = 64 * 1024;
    static final int MAX_LEDGER_BYTES = 2 * 1024 * 1024;
    static final int MAX_RECORDS = 131_072;
    static final int MAX_BATCH_RECORDS = 512;
    static final int MAX_CALL_RECORDS = 1024;
    static final int MAX_FRAMES = 512;
    static final int MAX_OPEN_CALLS = 128;
    private static PdkWriteReturnProbe installed;

    private final LongSupplier clock;
    private final List<Frame> frames = new ArrayList<>();
    private final long pid = ProcessHandle.current().pid();
    private final long startMillis = ManagementFactory.getRuntimeMXBean().getStartTime();
    private long sequence;
    private long completedCalls;
    private long failedCalls;
    private long reportedRecords;
    private long openCalls;
    private int retainedBytes;
    private int retainedRecords;
    private int writerSequence;
    private long epoch;
    private boolean active;
    private String window = "";
    private String state = "IDLE";

    PdkWriteReturnProbe(LongSupplier clock) { this.clock = java.util.Objects.requireNonNull(clock); }

    static synchronized Writer forWriter(PdkConnector connector) {
        if (!Boolean.getBoolean(PROPERTY)) { return null; }
        if (installed == null) {
            PdkWriteReturnProbe probe = new PdkWriteReturnProbe(System::nanoTime);
            try {
                ManagementFactory.getPlatformMBeanServer().registerMBean(probe, new ObjectName(OBJECT_NAME));
            } catch (javax.management.JMException | SecurityException unavailable) {
                // Measurement installation must not make a previously valid data write fail.
                return null;
            }
            installed = probe;
        }
        return installed.writer(connector.stateNamespace(), PdkMongoWriteScope.enabled(connector));
    }

    synchronized Writer writer(String identity) {
        return writer(identity, null);
    }

    private synchronized Writer writer(String identity, PdkMongoWriteScope scope) {
        if (writerSequence >= MAX_OPEN_CALLS) {
            unknown("WRITER_ROSTER_OVERFLOW");
            return new Writer(this, -1, identity, scope);
        }
        return new Writer(this, ++writerSequence, identity, scope);
    }

    static final class Writer {
        private final PdkWriteReturnProbe probe;
        private final int id;
        private final String identity;
        private final PdkMongoWriteScope scope;
        private Writer(PdkWriteReturnProbe probe, int id, String identity, PdkMongoWriteScope scope) {
            this.probe = probe; this.id = id; this.identity = identity; this.scope = scope;
        }
        Ticket begin(String stream, TapTable table, List<TapRecordEvent> rows) {
            Ticket ticket = probe.begin(id, identity, stream, table, rows);
            if (ticket != null && scope != null) {
                ticket.scope = scope; ticket.scopeCall = scope.beforeWrite();
            }
            return ticket;
        }
    }

    private synchronized Ticket begin(int writer, String identity, String stream,
                                      TapTable table, List<TapRecordEvent> rows) {
        if (!active) { return null; }
        long began = clock.getAsLong();
        openCalls++;
        Ticket ticket = new Ticket(this, epoch, ++sequence, writer, began);
        if (writer < 1 || openCalls > MAX_OPEN_CALLS || rows.isEmpty() || rows.size() > MAX_CALL_RECORDS) {
            unknown("CALL_OR_BATCH_BOUND");
            return ticket;
        }
        try {
            ticket.rowOrdinals = new java.util.IdentityHashMap<>();
            for (int i = 0; i < rows.size(); i++) {
                if (ticket.rowOrdinals.put(rows.get(i), i) != null) { unknown("DUPLICATE_RECORD_OBJECT"); }
            }
            var keys = List.copyOf(table.primaryKeys());
            if (keys.isEmpty() || keys.size() > 2) { unknown("UNSUPPORTED_KEY_SHAPE"); return ticket; }
            var chunks = new ArrayList<byte[]>();
            for (int first = 0; first < rows.size(); first += MAX_BATCH_RECORDS) {
                int last = Math.min(rows.size(), first + MAX_BATCH_RECORDS);
                var bytes = new BoundedBytes();
                var out = new DataOutputStream(bytes);
                text(out, identity == null ? "UNSCOPED" : identity, 512);
                text(out, stream, 512); text(out, table.getId(), 512);
                out.writeByte(keys.size());
                for (String key : keys) { text(out, key, 128); }
                out.writeInt(last - first);
                for (TapRecordEvent row : rows.subList(first, last)) {
                    Map<String, Object> data;
                    if (row instanceof TapInsertRecordEvent insert) { out.writeByte(1); data = insert.getAfter(); }
                    else if (row instanceof TapUpdateRecordEvent update) { out.writeByte(2); data = update.getAfter(); }
                    else if (row instanceof TapDeleteRecordEvent delete) { out.writeByte(3); data = delete.getBefore(); }
                    else { unknown("UNSUPPORTED_RECORD_KIND"); return ticket; }
                    if (data == null) { unknown("MISSING_RECORD_KEY"); return ticket; }
                    for (String key : keys) {
                        if (!(data.get(key) instanceof Number number)) { unknown("NONNUMERIC_RECORD_KEY"); return ticket; }
                        if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
                            out.writeInt(Math.toIntExact(number.longValue()));
                        } else if (number instanceof Double || number instanceof Float) {
                            double value = number.doubleValue();
                            if (!Double.isFinite(value) || value != Math.rint(value)
                                    || value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                                unknown("UNSUPPORTED_NUMERIC_KEY"); return ticket;
                            }
                            out.writeInt((int) value);
                        } else if (number instanceof java.math.BigInteger integer && integer.bitLength() <= 31) {
                            out.writeInt(integer.intValueExact());
                        } else if (number instanceof BigDecimal decimal && decimal.precision() <= 10
                                && decimal.scale() >= -10 && decimal.scale() <= 10) {
                            out.writeInt(decimal.intValueExact());
                        } else {
                            unknown("UNSUPPORTED_NUMERIC_KEY"); return ticket;
                        }
                    }
                }
                out.flush();
                if (bytes.size() > MAX_FRAME_BYTES - 2048) { unknown("FRAME_OVERFLOW"); return ticket; }
                chunks.add(bytes.toByteArray());
            }
            ticket.rows = rows.size(); ticket.identityChunks = List.copyOf(chunks);
        } catch (IOException | RuntimeException unsupported) {
            unknown("RECORD_IDENTITY_UNAVAILABLE");
        }
        return ticket;
    }

    static final class Ticket {
        private final PdkWriteReturnProbe probe;
        private final long epoch;
        private final long sequence;
        private final int writer;
        private final long began;
        private List<byte[]> identityChunks;
        private int rows;
        private int callbacks;
        private long inserted;
        private long modified;
        private long removed;
        private int errors;
        private java.util.IdentityHashMap<TapRecordEvent, Integer> rowOrdinals;
        private final List<ErrorEntry> errorEntries = new ArrayList<>();
        private boolean finished;
        private long returnObservation;
        private boolean returnObserved;
        private volatile boolean returnStarted;
        private PdkMongoWriteScope scope;
        private PdkMongoWriteScope.Call scopeCall;
        private String scopeEvidence = "UNKNOWN";

        private Ticket(PdkWriteReturnProbe probe, long epoch, long sequence, int writer, long began) {
            this.probe = probe; this.epoch = epoch; this.sequence = sequence; this.writer = writer; this.began = began;
        }
        void callback(WriteListResult<TapRecordEvent> result) {
            synchronized (probe) {
                if (returnStarted || finished || epoch != probe.epoch) { probe.unknown("LATE_CALLBACK"); return; }
                try {
                    callbacks = Math.incrementExact(callbacks);
                    if (callbacks > MAX_BATCH_RECORDS) { probe.unknown("CALLBACK_ROSTER_OVERFLOW"); }
                    if (result.getInsertedCount() < 0 || result.getModifiedCount() < 0 || result.getRemovedCount() < 0) {
                        probe.unknown("INVALID_CALLBACK_COUNT");
                    }
                    inserted = Math.addExact(inserted, result.getInsertedCount());
                    modified = Math.addExact(modified, result.getModifiedCount());
                    removed = Math.addExact(removed, result.getRemovedCount());
                    errors = Math.addExact(errors, result.getErrorMap() == null ? 0 : result.getErrorMap().size());
                    if (result.getErrorMap() != null) {
                        for (var entry : result.getErrorMap().entrySet()) {
                            if (errorEntries.size() >= MAX_BATCH_RECORDS) { probe.unknown("ERROR_EVIDENCE_OVERFLOW"); break; }
                            Integer ordinal = rowOrdinals == null ? null : rowOrdinals.get(entry.getKey());
                            if (ordinal == null || entry.getValue() == null) { probe.unknown("ERROR_RECORD_IDENTITY"); }
                            errorEntries.add(new ErrorEntry(ordinal == null ? -1 : ordinal,
                                    entry.getValue() == null ? "UNKNOWN" : entry.getValue().getClass().getName()));
                        }
                    }
                    if (inserted < 0 || modified < 0 || removed < 0 || errors < 0) { probe.unknown("INVALID_CALLBACK_COUNT"); }
                    if (returnStarted) { probe.unknown("LATE_CALLBACK"); }
                } catch (RuntimeException invalid) { probe.unknown("INVALID_CALLBACK_RESULT"); }
            }
        }
        void returned(Throwable failure) {
            observeReturn(); completed(failure);
        }
        void observeReturn() {
            // Capture before waiting for the receipt lock or encoding its payload.
            returnStarted = true;
            returnObservation = probe.clock.getAsLong(); returnObserved = true;
        }
        void completed(Throwable failure) {
            if (!returnObserved) {
                synchronized (probe) { probe.unknown("RETURN_OBSERVATION_MISSING"); }
            }
            if (scope != null) { scopeEvidence = scope.afterWrite(scopeCall); }
            probe.complete(this, returnObservation, failure);
        }
    }

    private synchronized void complete(Ticket ticket, long observed, Throwable failure) {
        if (ticket.finished) { unknown("DUPLICATE_COMPLETION"); return; }
        ticket.finished = true;
        ticket.rowOrdinals = null;
        openCalls--;
        if (ticket.epoch != epoch || observed < ticket.began) { unknown("CALL_IDENTITY_OR_CLOCK_ORDER"); return; }
        completedCalls++;
        if (failure != null) { failedCalls++; }
        long count;
        try {
            count = Math.addExact(Math.addExact(ticket.inserted, ticket.modified), ticket.removed);
            reportedRecords = Math.addExact(reportedRecords, count);
        } catch (ArithmeticException overflow) { unknown("CALLBACK_COUNT_OVERFLOW"); return; }
        if (ticket.identityChunks == null) { return; }
        try {
            List<byte[]> payloads = new ArrayList<>();
            int bytesTotal = 0;
            for (int part = 0; part < ticket.identityChunks.size(); part++) {
                var bytes = new BoundedBytes();
                var out = new DataOutputStream(bytes);
                out.writeLong(ticket.sequence); out.writeInt(ticket.writer);
                out.writeInt(ticket.rows); out.writeInt(part); out.writeInt(ticket.identityChunks.size());
                out.writeLong(ticket.began); out.writeLong(observed);
                out.writeBoolean(failure == null); out.writeInt(ticket.callbacks);
                out.writeLong(ticket.inserted); out.writeLong(ticket.modified); out.writeLong(ticket.removed);
                out.writeInt(ticket.errors);
                out.writeInt(ticket.errorEntries.size());
                for (ErrorEntry error : ticket.errorEntries) {
                    out.writeInt(error.rowOrdinal); text(out, error.failureType, 256);
                }
                text(out, failure == null ? "" : failure.getClass().getName(), 256);
                // An ordinary return is evidence of the call boundary, not proof of acknowledged write concern.
                text(out, ticket.scopeEvidence, 512);
                out.write(ticket.identityChunks.get(part)); out.flush();
                byte[] payload = bytes.toByteArray();
                if (payload.length > MAX_FRAME_BYTES - 2048 - Integer.BYTES) { unknown("FRAME_OVERFLOW"); return; }
                payloads.add(payload); bytesTotal += payload.length + Integer.BYTES;
            }
            if (frames.size() + payloads.size() > MAX_FRAMES
                    || retainedBytes + bytesTotal > MAX_LEDGER_BYTES
                    || retainedRecords + ticket.rows > MAX_RECORDS) {
                unknown("LEDGER_OVERFLOW"); return;
            }
            for (byte[] payload : payloads) { frames.add(new Frame(ticket.sequence, payload)); }
            retainedBytes += bytesTotal; retainedRecords += ticket.rows;
            if (failure != null || ticket.errors != 0 || ticket.callbacks == 0 || count != ticket.rows) {
                unknown("FAILED_PARTIAL_OR_MISSING_CALLBACK");
            }
        } catch (IOException | RuntimeException unavailable) { unknown("RECEIPT_UNAVAILABLE"); }
    }

    private void unknown(String reason) { if (!state.startsWith("UNKNOWN:")) { state = "UNKNOWN:" + reason; } }

    private static void text(DataOutputStream out, String value, int max) throws IOException {
        if (value == null) { throw new IllegalArgumentException("missing receipt identity"); }
        if (value.length() > max) { throw new IllegalArgumentException("receipt identity exceeds its bound"); }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > max) { throw new IllegalArgumentException("receipt identity exceeds its bound"); }
        out.writeShort(bytes.length); out.write(bytes);
    }

    @Override public long getPid() { return pid; }
    @Override public long getJvmStartTimeMillis() { return startMillis; }
    @Override public long getNanoTime() { return clock.getAsLong(); }
    @Override public synchronized String getWindow() { return window; }
    @Override public synchronized String getState() { return state; }
    @Override public synchronized long getCompletedCalls() { return completedCalls; }
    @Override public synchronized long getFailedCalls() { return failedCalls; }
    @Override public synchronized long getReportedRecords() { return reportedRecords; }
    @Override public synchronized long getOpenCalls() { return openCalls; }
    @Override public synchronized long getRetainedBytes() { return retainedBytes; }

    @Override public synchronized boolean start(String nextWindow) {
        if (active || openCalls != 0 || nextWindow == null || nextWindow.isBlank()
                || nextWindow.length() > 512 || nextWindow.getBytes(StandardCharsets.UTF_8).length > 512) {
            unknown("INVALID_CAPTURE_START"); return false;
        }
        epoch++; sequence = completedCalls = failedCalls = reportedRecords = 0;
        frames.clear(); retainedBytes = retainedRecords = 0;
        window = nextWindow; state = "RECORDED_SCOPE_UNQUALIFIED"; active = true;
        return true;
    }

    @Override public synchronized boolean stop() {
        active = false;
        if (openCalls != 0) { unknown("OPEN_CALLS_AT_STOP"); return false; }
        return true;
    }

    /** Pages use completion order, so a later finishing writer cannot disappear behind another call's id. */
    @Override public synchronized byte[] read(long completionCursor) {
        try {
            if (completionCursor < 0 || completionCursor > frames.size()) {
                unknown("INVALID_COMPLETION_CURSOR"); return new byte[0];
            }
            int first = (int) completionCursor;
            int next = first;
            int pageBytes = 2048;
            while (next < frames.size() && pageBytes + 4 + frames.get(next).bytes.length <= MAX_FRAME_BYTES) {
                pageBytes += 4 + frames.get(next).bytes.length; next++;
            }
            var bytes = new BoundedBytes();
            var out = new DataOutputStream(bytes);
            out.writeInt(0x57525031); out.writeInt(2);
            out.writeLong(epoch); out.writeInt(first); out.writeInt(next); out.writeInt(frames.size());
            text(out, window, 512); text(out, state, 128);
            for (int i = first; i < next; i++) {
                Frame frame = frames.get(i);
                out.writeInt(frame.bytes.length); out.write(frame.bytes);
            }
            out.flush(); return bytes.toByteArray();
        } catch (IOException impossible) { throw new AssertionError(impossible); }
    }

    private record Frame(long sequence, byte[] bytes) { }
    private record ErrorEntry(int rowOrdinal, String failureType) { }

    private static final class BoundedBytes extends ByteArrayOutputStream {
        BoundedBytes() { super(512); }
        @Override public synchronized void write(int value) {
            if (count == MAX_FRAME_BYTES) { throw new IllegalArgumentException("receipt frame exceeds its bound"); }
            super.write(value);
        }
        @Override public synchronized void write(byte[] bytes, int offset, int length) {
            if (length < 0 || length > MAX_FRAME_BYTES - count) {
                throw new IllegalArgumentException("receipt frame exceeds its bound");
            }
            super.write(bytes, offset, length);
        }
    }
}
