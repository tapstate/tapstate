package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.spi.store.ConnectionConfig;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Independent native scalar reads for an already admitted, fixed connection and artifact. The
 * reader uses the registered PDK count function in the connector's own connection context.
 * Construction is metadata-only. Call count on admitted blocking I/O and close on separate cleanup
 * I/O; neither method belongs on a cooperative processor or deadline thread. The caller reserves
 * connection and driver memory before count, retaining them through actual drain.
 */
public final class PdkValidationCounts implements AutoCloseable {
    public enum Operation { PARTITION_FILTER }

    public record Request(String table) {
        public Request {
            ValidationError.fieldName(table, "count.table");
            if (table.isBlank() || table.length() > 512) throw invalid("table");
        }
    }

    private final ConnectionConfig connection;
    private final ConnectorRef artifact;
    private final Request request;
    private final Runnable requireCurrent;
    private final java.util.function.Consumer<String> admitTable;
    private final Object state = new Object(), cleanup = new Object();
    private final ValidationFailureLatch failures = new ValidationFailureLatch();
    private final CompletableFuture<Void> exited = new CompletableFuture<>();
    // Publishes the original connector to cleanup; the cleanup monitor owns stop and release.
    @SuppressWarnings("java:S3077")
    private volatile PdkConnector connector;
    private boolean started, returned;
    private volatile boolean cancelled;

    public PdkValidationCounts(ConnectionConfig connection, ConnectorRef artifact, Request request, Runnable requireCurrent) {
        this(connection,artifact,request,requireCurrent,table->{});
    }

    /** Scope admission sees every native namespace and must be pure, bounded and nonblocking. */
    public PdkValidationCounts(ConnectionConfig connection,ConnectorRef artifact,Request request,Runnable requireCurrent,
            java.util.function.Consumer<String> admitTable){
        this.connection = Objects.requireNonNull(connection); this.artifact = Objects.requireNonNull(artifact);
        this.request = Objects.requireNonNull(request); this.requireCurrent = Objects.requireNonNull(requireCurrent);
        this.admitTable=Objects.requireNonNull(admitTable);
    }

    /** One original count attempt using the connector's native count contract. */
    public long count() { return attempt(false, false).count(); }

    /** An absent native count function leaves the scan on the PDK-ordered path. */
    public OptionalLong countIfSupported() {
        Long count = attempt(false, true).count();
        return count == null ? OptionalLong.empty() : OptionalLong.of(count);
    }

    /**
     * Independent metadata/capability inspection only: never invokes count, query or value discovery.
     * Method presence and native table enumeration are observations, not proof of a successful future
     * scalar read. The original handle still owns initialization/cleanup and may be attempted once.
     */
    public Operation inspect() { return attempt(true, false).operation(); }

    private record Outcome(Operation operation,Long count) {}
    private Outcome attempt(boolean metadataOnly, boolean optional) {
        synchronized (state) {
            if(cancelled)throw new TapstateException(ValidationError.READ_CANCELLED,Map.of("table",request.table()),null);
            if (started || returned) throw invalid("alreadyAttempted");
            started = true;
        }
        Outcome outcome=null;
        try {
            check();
            var opened = PdkConnector.open(connection.connectorId(), artifact, connection.settings());
            connector = opened;
            check();
            outcome=opened.underLoader(() -> {
                check(); opened.connector().init(opened.context()); check();
                // COUNT never discovers value models: schema discovery may sample business rows.
                var table = new TapTable(request.table());
                if (metadataOnly) requireTable(opened);
                var function = opened.functions().getCountByPartitionFilterFunction();
                if (function == null) {
                    if (optional) return new Outcome(null, null);
                    throw missing("count-by-partition-filter");
                }
                if (metadataOnly) {
                    check();
                    return new Outcome(Operation.PARTITION_FILTER, null);
                }
                long count = PdkValidationCounter.direct(request.table(),
                        () -> function.countByPartitionFilter(opened.context(), table, TapAdvanceFilter.create()), this::check);
                check();
                return new Outcome(Operation.PARTITION_FILTER, count);
            });
        } catch (Throwable failed) {
            record(failed);
            if(failures.get() instanceof InterruptedException)Thread.currentThread().interrupt();
        } finally {
            // Preserve the body outcome, but a fatal cleanup error must dominate an ordinary error.
            boolean interrupted = Thread.interrupted();
            try { finish(); }
            catch (Throwable failed) { record(failed); }
            finally {
                synchronized (state) { returned = true; }
                var failed = failures.get();
                if (failed == null) exited.complete(null); else exited.completeExceptionally(failed);
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
        if(failures.get()!=null)throw failure();
        return Objects.requireNonNull(outcome,"count outcome");
    }

    /** Actual host invocation exit, including cleanup. This does not certify opaque plugin threads. */
    public CompletionStage<Void> callExited() { return exited.minimalCompletionStage(); }

    /** Successful cleanup plus actual invocation exit, not just a cancellation request. */
    public boolean drained() { synchronized (state) { return returned && connector == null; } }

    /** Idempotent retryable cleanup; cancellation during initialization is stopped again on return. */
    @Override public void close() {
        cancelled = true;
        boolean interrupted = Thread.interrupted();
        boolean noCall = false;
        try {
            synchronized (cleanup) {
                var current = connector;
                if (current != null) current.stopForCleanup();
                synchronized (state) {
                    if (!started) { returned = true; noCall = true; }
                    if (returned && current != null) { current.close(); connector = null; }
                }
            }
        } catch (Throwable failed) { record(failed); throw failure(); }
        finally { if (interrupted) Thread.currentThread().interrupt(); }
        // Completion callbacks can synchronously arrange cleanup/observation on another executor.
        if (noCall) exited.complete(null);
    }

    private void finish() throws Throwable {
        synchronized (cleanup) {
            var current = connector;
            if (current == null) return;
            current.stopForCleanup(); current.close(); connector = null;
        }
    }

    /** Metadata enumeration only: statistics may report zero for an absent namespace. */
    private void requireTable(PdkConnector opened) throws Throwable {
        var function = opened.functions().getGetTableNamesFunction();
        if (function == null) throw missing("table-names");
        var names = new Names();
        try { function.tableNames(opened.context(), 64, names::accept); }
        finally { names.seal(); }
        names.requireFound();
    }
    private final class Names {
        private boolean sealed, found;
        private int seen;
        synchronized void accept(List<String> names) {
            try {
                check();
                if (sealed || names == null || names.size() > 64) throw protocol();
                for (var name : names) {
                    if (name == null || name.isBlank() || name.length() > 512) throw protocol();
                    if (++seen > 16384) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                            Map.of("limit", "validation.countTableNames"), null);
                    if (request.table().equals(name)) { if (found) throw protocol(); found = true; }
                    admitTable.accept(name);check();
                }
            } catch (RuntimeException | Error failed) { record(failed); throw failed; }
        }
        synchronized void seal() { sealed = true; }
        synchronized void requireFound() { check(); if (!found) throw protocol(); }
    }

    private void check() {
        if (failures.get() != null) throw failure();
        if (cancelled || Thread.currentThread().isInterrupted()) throw new TapstateException(
                ValidationError.READ_CANCELLED, Map.of("table", request.table()), null);
        requireCurrent.run();
    }
    private void record(Throwable failed) { failures.record(failed); ValidationFailureLatch.addContext(failures.get(), failed); }
    private RuntimeException failure() {
        var failed = failures.get();
        if (failed instanceof Error fatal) throw fatal;
        if (failed instanceof TapstateException coded) return coded;
        if (failed instanceof InterruptedException) Thread.currentThread().interrupt();
        return new TapstateException(ValidationError.READ_FAILED, Map.of("table", request.table()), failed);
    }
    private TapstateException protocol() { return new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", request.table()), null); }
    private TapstateException missing(String capability) { return new TapstateException(ConnectorError.CAPABILITY_MISSING,
            Map.of("connector", connection.connectorId(), "capability", capability), null); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "count." + field), null); }
}
