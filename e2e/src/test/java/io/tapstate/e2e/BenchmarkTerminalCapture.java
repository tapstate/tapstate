package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

/**
 * An independent PDK reader for one benchmark source chain. It proves its present-start stream is
 * positioned by observing a caller-written warm-up row before measured writes begin, then records
 * the source token of the batch containing that chain's unique terminal row.
 */
final class BenchmarkTerminalCapture implements AutoCloseable {

    private static final int WARMUP_WRITES = 20;
    private static final long WARMUP_CADENCE_NANOS = Duration.ofMillis(250).toNanos();

    private final String table;
    private final long warmupRowId;
    private final long terminalRowId;
    private final Subscription subscription;
    private final Holder listener;
    private boolean closed;

    static BenchmarkTerminalCapture open(String connectorId, Path connectorJar, Map<String, Object> settings,
                                         String table, long warmupRowId, long terminalRowId) {
        return open(connectorId, connectorJar, settings, table, warmupRowId, terminalRowId, null);
    }

    static BenchmarkTerminalCapture open(String connectorId, Path connectorJar, Map<String, Object> settings,
                                         String table, long warmupRowId, long terminalRowId,
                                         BenchmarkBoundaryWrites.Boundary boundary) {
        if (connectorId == null || connectorId.isBlank() || connectorJar == null || settings == null
                || table == null || table.isBlank() || warmupRowId < 0 || terminalRowId < 0
                || warmupRowId == terminalRowId) {
            throw new IllegalArgumentException("one connector, table and two distinct row ids are required");
        }
        if (boundary != null && (!table.equals(boundary.table()) || boundary.rowId() < 0
                || boundary.rowId() == warmupRowId || boundary.rowId() == terminalRowId
                || boundary.field() == null || boundary.field().isBlank()
                || boundary.changedValue() == null || boundary.restoredValue() == null
                || valuesEqual(boundary.changedValue(), boundary.restoredValue()))) {
            throw new IllegalArgumentException("boundary must name a distinct row and two distinct field values");
        }
        var inspected = new ConnectorIntrospector().introspect(List.of(connectorJar));
        ConnectorRef ref = new ConnectorRef(List.of(connectorJar), inspected.className(),
                inspected.pdkApiVersion(), null, inspected.spec());
        PdkCapturePort capture = new PdkCapturePort(id -> {
            if (!connectorId.equals(id)) {
                throw new IllegalStateException("sidecar requested a different connector");
            }
            return ref;
        });
        PipelineNode node = new PipelineNode("benchmark-sidecar-" + UUID.randomUUID(), "source-" + table);
        CaptureConfig config = new CaptureConfig(connectorId, settings, List.of(table), node);
        Holder holder = new Holder(table, warmupRowId, terminalRowId, boundary);
        Subscription subscription = capture.cdc(config, CaptureStart.present(), holder);
        return new BenchmarkTerminalCapture(table, warmupRowId, terminalRowId, subscription, holder);
    }

    private BenchmarkTerminalCapture(String table, long warmupRowId, long terminalRowId,
                                     Subscription subscription, Holder listener) {
        this.table = table;
        this.warmupRowId = warmupRowId;
        this.terminalRowId = terminalRowId;
        this.subscription = subscription;
        this.listener = listener;
    }

    /**
     * Emits the same fixed number of caller-selected changes on every fork, ending in a known row
     * state. An observed change proves this reader was positioned before measured writes begin.
     */
    void awaitWarmup(IntConsumer writeWarmup, Duration timeout) {
        if (writeWarmup == null) {
            throw new IllegalArgumentException("a warm-up writer is required");
        }
        long deadline = deadline(timeout);
        for (int attempt = 1; attempt <= WARMUP_WRITES; attempt++) {
            listener.check();
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("sidecar warm-up schedule exceeded its deadline");
            }
            writeWarmup.accept(attempt);
            if (attempt < WARMUP_WRITES) {
                sleep(WARMUP_CADENCE_NANOS, deadline);
            }
        }
        while (true) {
            listener.check();
            if (listener.warmupSeen()) {
                return;
            }
            long remaining = remaining(deadline);
            if (remaining == 0) {
                throw new AssertionError("sidecar did not observe warm-up row " + warmupRowId + " on " + table);
            }
            listener.waitForChange(remaining);
        }
    }

    /** Waits for the terminal token while keeping the stream open for target-ACK observation. */
    String awaitTerminal(Duration timeout) {
        if (!listener.warmupSeen()) {
            throw new AssertionError("sidecar has not observed its warm-up row");
        }
        long deadline = deadline(timeout);
        while (true) {
            listener.check();
            if (listener.terminalSeen()) {
                return listener.terminalToken();
            }
            long remaining = remaining(deadline);
            if (remaining == 0) {
                throw new AssertionError("sidecar did not observe terminal row " + terminalRowId + " on " + table);
            }
            listener.waitForChange(remaining);
        }
    }

    /** The connector token of the restored boundary row, retained while target ACK catches up. */
    String awaitBoundary(Duration timeout) {
        if (!listener.warmupSeen()) {
            throw new AssertionError("sidecar has not observed its warm-up row");
        }
        if (!listener.hasBoundary()) {
            throw new IllegalStateException("sidecar has no boundary row");
        }
        long deadline = deadline(timeout);
        while (true) {
            listener.check();
            if (listener.boundarySeen()) {
                return listener.boundaryToken();
            }
            long remaining = remaining(deadline);
            if (remaining == 0) {
                throw new AssertionError("sidecar did not observe a restored boundary row on " + table);
            }
            listener.waitForChange(remaining);
        }
    }

    /** Ends boundary matching after its token is ACKed and the restored target is verified. */
    void sealBoundary() {
        listener.sealBoundary();
    }

    /** Closes after target-ACK observation and rejects every duplicate seen while the stream was open. */
    String closeAndTerminal() {
        close();
        listener.check();
        if (listener.hasBoundary()) {
            listener.boundaryToken();
        }
        return listener.terminalToken();
    }

    private static boolean valuesEqual(Object observed, Object expected) {
        if (observed instanceof Number left && expected instanceof Number right) {
            try {
                return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0;
            } catch (NumberFormatException invalid) {
                return false;
            }
        }
        return expected.equals(observed);
    }

    private static void sleep(long nanos, long deadline) {
        long until = Math.min(Math.addExact(System.nanoTime(), nanos), deadline);
        while (true) {
            long left = remaining(until);
            if (left == 0) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(left);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("sidecar warm-up was interrupted", interrupted);
            }
        }
    }

    private static long deadline(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("capture timeout must be positive");
        }
        return Math.addExact(System.nanoTime(), timeout.toNanos());
    }

    private static long remaining(long deadline) {
        return Math.max(0, deadline - System.nanoTime());
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            subscription.close();
        }
    }

    static final class Holder implements CaptureListener {

        private final Object monitor = new Object();
        private final String table;
        private final long warmupRowId;
        private final long terminalRowId;
        private final BenchmarkBoundaryWrites.Boundary boundary;
        private boolean warmupSeen;
        private boolean changedSeen;
        private int restoredCount;
        private String restoredToken;
        private boolean boundarySealed;
        private int terminalCount;
        private String terminalToken;
        private Throwable failure;

        Holder(String table, long warmupRowId, long terminalRowId,
               BenchmarkBoundaryWrites.Boundary boundary) {
            this.table = table;
            this.warmupRowId = warmupRowId;
            this.terminalRowId = terminalRowId;
            this.boundary = boundary;
        }

        @Override
        public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            synchronized (monitor) {
                for (Envelope event : events) {
                    if (!table.equals(event.src())) {
                        failure = new AssertionError("sidecar read a row from the wrong table: " + event.src());
                        break;
                    }
                    long id = rowId(event);
                    if (boundary != null && !boundarySealed && id == boundary.rowId()) {
                        observeBoundary(event, position);
                        if (failure != null) {
                            break;
                        }
                    }
                    if (id == warmupRowId) {
                        warmupSeen = true;
                    }
                    if (id == terminalRowId) {
                        if (boundary != null && restoredCount != 1) {
                            failure = new AssertionError("terminal row arrived before the restored boundary row");
                            break;
                        }
                        terminalCount++;
                        if (terminalCount > 1) {
                            failure = new AssertionError("sidecar saw duplicate terminal row " + terminalRowId);
                            break;
                        }
                        if (position.isEmpty() || position.get().token().isBlank()) {
                            failure = new AssertionError("terminal row had no source position");
                            break;
                        }
                        terminalToken = position.get().token();
                    }
                }
                monitor.notifyAll();
            }
        }

        private void observeBoundary(Envelope event, Optional<SourcePosition> position) {
            Object value = event.after() == null ? null : event.after().get(boundary.field());
            if (value == null) {
                failure = new AssertionError("boundary row has no selected field value");
            } else if (valuesEqual(value, boundary.changedValue())) {
                if (changedSeen || restoredCount != 0) {
                    failure = new AssertionError("sidecar saw duplicate or late changed boundary row");
                } else {
                    changedSeen = true;
                }
            } else if (valuesEqual(value, boundary.restoredValue())) {
                if (!changedSeen || ++restoredCount != 1) {
                    failure = new AssertionError("sidecar saw restored boundary row out of order or twice");
                } else if (position.isEmpty() || position.get().token().isBlank()) {
                    failure = new AssertionError("restored boundary row had no source position");
                } else {
                    restoredToken = position.get().token();
                }
            } else {
                failure = new AssertionError("boundary row has an unexpected field value");
            }
        }

        @Override
        public void onError(Throwable error) {
            synchronized (monitor) {
                failure = error;
                monitor.notifyAll();
            }
        }

        boolean warmupSeen() {
            synchronized (monitor) {
                return warmupSeen;
            }
        }

        boolean terminalSeen() {
            synchronized (monitor) {
                return terminalCount > 0;
            }
        }

        boolean hasBoundary() {
            return boundary != null;
        }

        boolean boundarySeen() {
            synchronized (monitor) {
                return restoredCount > 0;
            }
        }

        String boundaryToken() {
            synchronized (monitor) {
                if (boundary == null || !changedSeen || restoredCount != 1
                        || restoredToken == null || restoredToken.isBlank()) {
                    throw new AssertionError("sidecar has no unique restored boundary source position");
                }
                return restoredToken;
            }
        }

        void sealBoundary() {
            synchronized (monitor) {
                if (failure != null) {
                    throw new AssertionError("sidecar capture failed", failure);
                }
                boundaryToken();
                boundarySealed = true;
            }
        }

        String terminalToken() {
            synchronized (monitor) {
                if (terminalCount != 1 || terminalToken == null || terminalToken.isBlank()) {
                    throw new AssertionError("sidecar has no unique terminal source position");
                }
                return terminalToken;
            }
        }

        void check() {
            synchronized (monitor) {
                if (failure != null) {
                    throw new AssertionError("sidecar capture failed", failure);
                }
            }
        }

        void waitForChange(long nanos) {
            if (nanos <= 0) {
                return;
            }
            synchronized (monitor) {
                try {
                    TimeUnit.NANOSECONDS.timedWait(monitor, nanos);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("sidecar wait was interrupted", interrupted);
                }
            }
        }

        private static long rowId(Envelope event) {
            if (event.after() == null || !(event.after().get("id") instanceof Number id)) {
                return Long.MIN_VALUE;
            }
            long value = id.longValue();
            return id.doubleValue() == value ? value : Long.MIN_VALUE;
        }
    }
}
