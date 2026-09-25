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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        return open(connectorId, connectorJar, settings, table, warmupRowId, terminalRowId, boundary, Map.of());
    }

    static BenchmarkTerminalCapture open(String connectorId, Path connectorJar, Map<String, Object> settings,
                                         String table, long warmupRowId, long terminalRowId,
                                         BenchmarkBoundaryWrites.Boundary boundary,
                                         Map<String, Long> measuredEnds) {
        if (connectorId == null || connectorId.isBlank() || connectorJar == null || settings == null
                || table == null || table.isBlank() || warmupRowId < 0 || terminalRowId < 0
                || warmupRowId == terminalRowId || measuredEnds == null) {
            throw new IllegalArgumentException("one connector, table and two distinct row ids are required");
        }
        if (boundary != null && (!table.equals(boundary.table()) || boundary.rowId() < 0
                || boundary.rowId() == warmupRowId || boundary.rowId() == terminalRowId
                || boundary.field() == null || boundary.field().isBlank()
                || boundary.changedValue() == null || boundary.restoredValue() == null
                || valuesEqual(boundary.changedValue(), boundary.restoredValue()))) {
            throw new IllegalArgumentException("boundary must name a distinct row and two distinct field values");
        }
        Set<Long> rows = new HashSet<>();
        BenchmarkMeasuredEndMarkers.phaseOrder(measuredEnds);
        for (Map.Entry<String, Long> marker : measuredEnds.entrySet()) {
            Long rowId = marker.getValue();
            if (rowId == null || rowId < 0 || !rows.add(rowId) || rowId == warmupRowId
                    || rowId == terminalRowId || (boundary != null && rowId == boundary.rowId())) {
                throw new IllegalArgumentException("measured-end markers must uniquely name rows on this table");
            }
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
        Holder holder = new Holder(table, warmupRowId, terminalRowId, boundary, measuredEnds);
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

    /** The connector token of this phase's unique final source row. */
    String awaitMeasuredEnd(String phaseId, Duration timeout) {
        if (!listener.warmupSeen()) {
            throw new AssertionError("sidecar has not observed its warm-up row");
        }
        if (!listener.hasMeasuredPhase(phaseId)) {
            throw new IllegalArgumentException("sidecar has no measured-end marker for " + phaseId);
        }
        long deadline = deadline(timeout);
        while (true) {
            listener.check();
            String token = listener.measuredToken(phaseId);
            if (token != null) {
                return token;
            }
            long remaining = remaining(deadline);
            if (remaining == 0) {
                throw new AssertionError("sidecar did not observe measured-end row for " + phaseId);
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
            listener.check();
            listener.requireMeasuredComplete();
        }
    }

    static final class Holder implements CaptureListener {

        private final Object monitor = new Object();
        private final String table;
        private final long warmupRowId;
        private final long terminalRowId;
        private final BenchmarkBoundaryWrites.Boundary boundary;
        private final Map<String, Long> measuredEnds;
        private final List<String> measuredOrder;
        private final Map<Long, String> phaseByRow;
        private final Map<String, String> measuredTokens = new HashMap<>();
        private boolean warmupSeen;
        private boolean changedSeen;
        private int restoredCount;
        private String restoredToken;
        private boolean boundarySealed;
        private int nextMeasuredIndex;
        private int terminalCount;
        private String terminalToken;
        private Throwable failure;

        Holder(String table, long warmupRowId, long terminalRowId,
               BenchmarkBoundaryWrites.Boundary boundary) {
            this(table, warmupRowId, terminalRowId, boundary, Map.of());
        }

        Holder(String table, long warmupRowId, long terminalRowId,
               BenchmarkBoundaryWrites.Boundary boundary, Map<String, Long> measuredEnds) {
            this.table = table;
            this.warmupRowId = warmupRowId;
            this.terminalRowId = terminalRowId;
            this.boundary = boundary;
            this.measuredEnds = Map.copyOf(measuredEnds);
            this.measuredOrder = BenchmarkMeasuredEndMarkers.phaseOrder(measuredEnds);
            Map<Long, String> byRow = new HashMap<>();
            this.measuredEnds.forEach((phase, row) -> byRow.put(row, phase));
            this.phaseByRow = Map.copyOf(byRow);
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
                    String measured = phaseByRow.get(id);
                    if (measured != null) {
                        observeMeasured(event, position, measured);
                        if (failure != null) {
                            break;
                        }
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

        private void observeMeasured(Envelope event, Optional<SourcePosition> position, String phaseId) {
            if (!warmupSeen || (boundary != null && !boundarySealed)) {
                failure = new AssertionError("measured-end row arrived before sidecar preflight and boundary");
            } else if (nextMeasuredIndex >= measuredOrder.size()
                    || !measuredOrder.get(nextMeasuredIndex).equals(phaseId)
                    || measuredTokens.containsKey(phaseId)) {
                failure = new AssertionError("measured-end row arrived out of phase order or twice");
            } else if (event.op() != BenchmarkMeasuredEndMarkers.operation(phaseId)) {
                failure = new AssertionError("measured-end row has the wrong source operation");
            } else if (position.isEmpty() || position.get().token().isBlank()) {
                failure = new AssertionError("measured-end row had no source position");
            } else {
                measuredTokens.put(phaseId, position.get().token());
                nextMeasuredIndex++;
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

        boolean hasMeasuredPhase(String phaseId) {
            return measuredEnds.containsKey(phaseId);
        }

        String measuredToken(String phaseId) {
            synchronized (monitor) {
                return measuredTokens.get(phaseId);
            }
        }

        void requireMeasuredComplete() {
            synchronized (monitor) {
                if (nextMeasuredIndex != measuredEnds.size()) {
                    throw new AssertionError("sidecar closed before every configured measured phase ended");
                }
            }
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
