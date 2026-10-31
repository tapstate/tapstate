package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.spi.store.ConnectionConfig;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Independent native scalar reads for an already admitted, fixed connection and artifact. The
 * assembly selects the exact native operation from its semantics, never from a discovery estimate.
 * Construction is metadata-only. Call count on admitted blocking I/O and close on separate cleanup
 * I/O; neither method belongs on a cooperative processor or deadline thread. The caller reserves
 * connection and driver memory before count, retaining them through actual drain.
 */
public final class PdkValidationCounts implements AutoCloseable {
    /** Existing PDK operations, not a source allowlist or an automatic capability certificate. */
    public enum Operation { PARTITION_FILTER, DOCUMENT_COUNT, NATIVE_PROTOCOL }

    public record Request(String table, String database, Operation operation) {
        public Request {
            ValidationError.fieldName(table, "count.table");
            if (table.isBlank() || table.length() > 512) throw invalid("table");
            Objects.requireNonNull(operation);
            if (database != null) ValidationError.fieldName(database, "count.database");
            if (operation == Operation.DOCUMENT_COUNT && (database == null || database.isBlank())) throw invalid("database");
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
        if (request.database() != null && !request.database().equals(connection.settings().get("database")))
            throw invalid("databaseBinding");
    }

    /** One original count attempt. Failure, cancellation or a missing table never becomes a zero count. */
    public long count() { return attempt(false).count(); }

    /**
     * Independent metadata/capability inspection only: never invokes count, query or value discovery.
     * Method presence and native table enumeration are observations, not proof of a successful future
     * scalar read. The original handle still owns initialization/cleanup and may be attempted once.
     */
    public Operation inspect() { return attempt(true).operation(); }

    private record Outcome(Operation operation,Long count) {}
    private Outcome attempt(boolean metadataOnly) {
        synchronized (state) {
            if(cancelled)throw new TapstateException(ValidationError.READ_CANCELLED,Map.of("table",request.table()),null);
            if (started || returned) throw invalid("alreadyAttempted");
            started = true;
        }
        try {
            check();
            var opened = PdkConnector.open(connection.connectorId(), artifact, connection.settings());
            connector = opened;
            check();
            return opened.underLoader(() -> {
                check(); opened.connector().init(opened.context()); check();
                // COUNT never discovers value models: schema discovery may sample business rows.
                var table = new TapTable(request.table());
                requireTable(opened);
                Operation operation=operation(opened);
                if(operation==Operation.DOCUMENT_COUNT&&(request.database()==null||request.database().isBlank()))throw invalid("database");
                if(operation==Operation.PARTITION_FILTER&&opened.functions().getCountByPartitionFilterFunction()==null)throw missing("count-by-partition-filter");
                if(operation==Operation.DOCUMENT_COUNT&&opened.functions().getExecuteCommandFunction()==null)throw missing("execute-command-count");
                if(metadataOnly){check();return new Outcome(operation,null);}
                long count;
                if (operation == Operation.PARTITION_FILTER) {
                    var function = opened.functions().getCountByPartitionFilterFunction();
                    if (function == null) throw missing("count-by-partition-filter");
                    count = PdkValidationCounter.direct(request.table(),
                            () -> function.countByPartitionFilter(opened.context(), table, TapAdvanceFilter.create()), this::check);
                } else {
                    var function = opened.functions().getExecuteCommandFunction();
                    if (function == null) throw missing("execute-command-count");
                    // Plugins may fill defaults into their own params. Never hand them immutable/shared maps.
                    var params = new LinkedHashMap<String, Object>();
                    params.put("database", request.database()); params.put("collection", request.table());
                    params.put("filter", new LinkedHashMap<String, Object>());
                    var command = TapExecuteCommand.create().command("count").params(params);
                    count = PdkValidationCounter.callback(request.table(),
                            sink -> function.execute(opened.context(), command, result -> sink.accept(result)), this::check);
                }
                requireTable(opened); check(); return new Outcome(operation,count);
            });
        } catch (Throwable failed) {
            record(failed); throw failure();
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
            if (failures.get() != null) throw failure();
        }
    }

    /**
     * Compatibility dispatch for the preserved document connector family, whose partition-count
     * implementation uses a legacy count command. This selects a native protocol, not eligibility:
     * the actual method, fixed namespace and exact scalar contract are still checked on this session.
     * Logical connector IDs never grant a capability, and no missing command falls back to statistics.
     */
    private Operation operation(PdkConnector opened){
        if(request.operation()!=Operation.NATIVE_PROTOCOL)return request.operation();
        for(Class<?> type=opened.connector().getClass();type!=null;type=type.getSuperclass())
            if(type.getName().equals("io.tapdata.mongodb.MongodbConnector"))return Operation.DOCUMENT_COUNT;
        return Operation.PARTITION_FILTER;
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
