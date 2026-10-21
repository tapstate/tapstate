package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * One independent, non-reusable utility-query session. The caller admits the connection, model and
 * driver buffers before opening it, and must cancel it on its authorization/deadline signal. Blocking
 * construction, initialization, discovery and queries run on the query executor. Row delivery/codecs
 * run on a distinct admitted decoding executor, including joined off-thread callbacks, always under
 * the connector loader. Close can call blocking connector stop and therefore belongs on I/O too.
 * Each live read must own a query slot and a decode slot for its duration; backpressured reads must
 * not queue their callbacks behind each other on an undersized executor. This adapter does not
 * establish native ordering, read freshness or driver allocation guarantees.
 */
final class PdkValidationSession {
    /** Runs after independent discovery and before any business query or row decoding. */
    @FunctionalInterface interface ReadAdmission {
        PdkValidationCursor.Decoder prepare(TapTable model, PdkValidationCodecs codecs);
    }

    /** Empty projection reads the complete row; a nonempty projection lists literal physical fields. */
    record Scan(String table, List<String> ascendingKeys, List<String> projection, int batchSize) {
        Scan(String table, List<String> ascendingKeys, int batchSize) {
            this(table, ascendingKeys, List.of(), batchSize);
        }

        Scan {
            Objects.requireNonNull(table, "table");
            ascendingKeys = List.copyOf(ascendingKeys);
            projection = List.copyOf(projection);
            if (table.isBlank() || table.length() > 512 || ascendingKeys.isEmpty()
                    || ascendingKeys.size() > 32 || batchSize < 1 || batchSize > 64
                    || ascendingKeys.stream().anyMatch(key -> key.isBlank() || key.length() > 512)
                    || ascendingKeys.stream().distinct().count() != ascendingKeys.size()
                    || projection.size() > 4096
                    || projection.stream().anyMatch(field -> field.isBlank() || field.length() > 512)
                    || projection.stream().distinct().count() != projection.size()
                    || (!projection.isEmpty() && !projection.containsAll(ascendingKeys))) {
                throw new IllegalArgumentException("invalid independent scan shape");
            }
        }
    }

    private final String connectorId;
    private final ConnectorRef artifact;
    private final Map<String, Object> settings;
    private final String codecBinding;
    private final Scan scan;
    private final ReadAdmission admission;
    private final ExecutorService decodeIo;
    private final Runnable requireAuthorized;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private final AtomicReference<FutureTask<Void>> delivery = new AtomicReference<>();
    private final Object callbackLock = new Object();
    private boolean queryOpen;
    private final Object stopLock = new Object();
    private volatile PdkConnector connector;
    private PdkValidationCursor.Decoder decoder;

    private PdkValidationSession(String connectorId, ConnectorRef artifact, Map<String, Object> settings,
            String codecBinding, Scan scan, ReadAdmission admission, Runnable requireAuthorized, ExecutorService decodeIo) {
        this.connectorId = Objects.requireNonNull(connectorId, "connectorId");
        this.artifact = Objects.requireNonNull(artifact, "artifact");
        // Nested settings must already belong to the admitted, fixed run configuration.
        this.settings = Collections.unmodifiableMap(new LinkedHashMap<>(settings));
        this.codecBinding = Objects.requireNonNull(codecBinding, "codecBinding");
        this.scan = Objects.requireNonNull(scan, "scan");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.decodeIo = Objects.requireNonNull(decodeIo, "decodeIo");
        this.requireAuthorized = Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (connectorId.isBlank() || codecBinding.isBlank()) throw new IllegalArgumentException("missing reader identity");
    }

    static ValidationRowCursor scan(String connectorId, ConnectorRef artifact, Map<String, Object> settings,
            String codecBinding, Scan scan, ReadAdmission admission, Runnable requireAuthorized,
            ExecutorService io, ExecutorService decodeIo, int maxInFlightRows, ValidationValues.Limits rowLimits) {
        if (io == decodeIo) throw new IllegalArgumentException("query and decoding require distinct executors");
        var session = new PdkValidationSession(connectorId, artifact, settings, codecBinding, scan,
                admission, requireAuthorized, decodeIo);
        return new PdkValidationCursor(scan.table(), session::read, row -> session.decoder.decode(session.project(row)),
                session::checkActive, session::cancel, io, maxInFlightRows, rowLimits);
    }

    private void read(Consumer<FilterResults> rows) throws Throwable {
        Throwable primary = null;
        try {
            checkActive();
            // No production node, notes store, connection, registry or mutable connector is borrowed.
            PdkConnector opened = PdkConnector.open(connectorId, artifact, settings);
            connector = opened;
            checkActive();
            opened.underLoader(() -> {
                var query = opened.functions().getQueryByAdvanceFilterFunction();
                if (query == null) throw new TapstateException(ConnectorError.CAPABILITY_MISSING,
                        Map.of("connector", connectorId, "capability", "query-by-advance-filter"), null);
                invoke(() -> { opened.connector().init(opened.context()); return null; });
                checkAlive(opened);
                TapTable model = discover(opened);
                checkAlive(opened);
                decoder = Objects.requireNonNull(admission.prepare(model,
                        new PdkValidationCodecs(connectorId, codecBinding, opened.codecs())), "admitted decoder");
                checkAlive(opened);
                TapAdvanceFilter filter = TapAdvanceFilter.create().batchSize(scan.batchSize());
                for (String key : scan.ascendingKeys()) filter.sort(SortOn.ascending(key));
                if (!scan.projection().isEmpty()) {
                    Projection projection = Projection.create();
                    scan.projection().forEach(projection::include);
                    filter.projection(projection);
                }
                // One complete invocation; callback size is a hint, never an overall row limit or offset.
                synchronized (callbackLock) { queryOpen = true; }
                try {
                    invoke(() -> {
                        query.query(opened.context(), filter, model, result -> deliver(opened, rows, result));
                        return null;
                    });
                } finally {
                    synchronized (callbackLock) {
                        queryOpen = false;
                        if (delivery.get() != null) failure.record(protocol());
                    }
                }
                checkAlive(opened);
                return null;
            });
        } catch (Throwable reported) {
            failure.record(reported);
            primary = failure.get();
            ValidationFailureLatch.addContext(primary, reported);
            throw primary;
        } finally {
            try { finish(); }
            catch (Throwable cleanup) {
                failure.record(cleanup);
                Throwable dominant = failure.get();
                ValidationFailureLatch.addContext(dominant, primary);
                ValidationFailureLatch.addContext(dominant, cleanup);
                if (primary == null || dominant instanceof Error) throw dominant;
            }
        }
    }

    private void deliver(PdkConnector opened, Consumer<FilterResults> consumer, FilterResults result) {
        FutureTask<Void> task = new FutureTask<>(() -> {
            try {
                return opened.underLoader(() -> {
                    checkActive();
                    consumer.accept(result);
                    return null;
                });
            } catch (RuntimeException | Error failure) { throw failure; }
            catch (Throwable failure) { throw readFailed(failure); }
        });
        try {
            synchronized (callbackLock) {
                checkActive();
                if (!queryOpen || !delivery.compareAndSet(null, task)) throw protocol();
            }
            // A callback retains its driver batch until decoding has completed. Concurrent callbacks
            // are refused before submitting, so this bridge never builds a queue of raw batches.
            decodeIo.execute(task);
            task.get();
        } catch (RejectedExecutionException rejected) {
            task.cancel(true);
            var failure = readFailed(rejected);
            this.failure.record(failure);
            throw failure;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            task.cancel(true);
            var failure = new TapstateException(ValidationError.READ_CANCELLED, Map.of("table", scan.table()), interrupted);
            this.failure.record(failure);
            throw failure;
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            failure.record(cause);
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw readFailed(cause);
        } catch (CancellationException stopped) {
            var failure = new TapstateException(ValidationError.READ_CANCELLED, Map.of("table", scan.table()), stopped);
            this.failure.record(failure);
            throw failure;
        } catch (RuntimeException | Error failed) {
            task.cancel(true);
            failure.record(failed);
            throw failed;
        } finally { delivery.compareAndSet(task, null); }
    }

    private TapstateException readFailed(Throwable cause) {
        return new TapstateException(ValidationError.READ_FAILED, Map.of("table", scan.table()), cause);
    }

    private TapTable discover(PdkConnector opened) throws Throwable {
        var discovery = new Discovery();
        try {
            invoke(() -> {
                opened.connector().discoverSchema(opened.context(), List.of(scan.table()), 2, discovery::accept);
                return null;
            });
        } finally { discovery.seal(); }
        TapTable model = discovery.result();
        if (model.getNameFieldMap() == null || model.getNameFieldMap().size() > 4096) throw protocol();
        if (!model.getNameFieldMap().keySet().containsAll(scan.ascendingKeys())
                || !model.getNameFieldMap().keySet().containsAll(scan.projection())) throw protocol();
        if (!scan.projection().isEmpty()) {
            var required = new LinkedHashMap<String, io.tapdata.entity.schema.TapField>();
            for (String field : scan.projection()) required.put(field, model.getNameFieldMap().get(field));
            // Unused fields must not participate in type/codec admission for a key-only read.
            model.setNameFieldMap(required);
        }
        invoke(() -> { opened.fillFieldTypes(model); return null; });
        return model;
    }

    private Map<String, Object> project(Map<String, Object> row) {
        if (scan.projection().isEmpty()) return row;
        Map<String, Object> selected = new LinkedHashMap<>();
        for (String field : scan.projection()) {
            if (row.containsKey(field)) selected.put(field, row.get(field));
        }
        // Some connectors return extra fields even with an inclusion projection. Their allocation
        // still requires separate admission, but their values must not invoke codecs for this read.
        // Preserve absent versus explicit null: never fill a missing physical key from the request.
        return Collections.unmodifiableMap(selected);
    }

    private final class Discovery {
        private boolean sealed;
        private TapTable model;

        private synchronized void accept(List<TapTable> tables) {
            try {
                checkActive();
                if (sealed || tables == null || tables.size() > 2) throw protocol();
                for (TapTable table : tables) {
                    if (table == null || model != null || !(scan.table().equals(table.getId())
                            || scan.table().equals(table.getName()))) throw protocol();
                    model = table;
                }
            } catch (TapstateException invalid) {
                failure.record(invalid);
                throw invalid;
            }
        }

        private synchronized void seal() { sealed = true; }
        private synchronized TapTable result() {
            throwFailure();
            if (model == null) throw protocol();
            return model;
        }
    }

    private <T> T invoke(PdkConnector.Action<T> operation) throws Throwable {
        try { return operation.run(); }
        catch (TapstateException | Error reported) {
            failure.record(reported);
            throw reported;
        } catch (Throwable reported) {
            var coded = readFailed(reported);
            failure.record(coded);
            throw coded;
        }
    }

    private void throwFailure() {
        Throwable first = failure.get();
        if (first instanceof RuntimeException runtime) throw runtime;
        if (first instanceof Error error) throw error;
        if (first != null) throw readFailed(first);
    }

    private void checkActive() {
        throwFailure();
        try {
            if (cancelled.get()) throw new TapstateException(ValidationError.READ_CANCELLED,
                    Map.of("table", scan.table()), null);
            requireAuthorized.run();
        } catch (RuntimeException | Error reported) {
            failure.record(reported);
            throwFailure();
        }
    }

    private void checkAlive(PdkConnector opened) {
        checkActive();
        if (!opened.isAlive()) throw new TapstateException(ConnectorError.READ_ABANDONED,
                Map.of("connector", connectorId), null);
    }

    private TapstateException protocol() {
        return new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", scan.table()), null);
    }

    private void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        FutureTask<Void> pending = delivery.get();
        if (pending != null) pending.cancel(true);
        synchronized (stopLock) {
            PdkConnector opened = connector;
            if (opened != null) stop(opened);
        }
    }

    private void finish() {
        synchronized (stopLock) {
            PdkConnector opened = connector;
            if (opened == null) return;
            try {
                // Cancellation can race initialization; stop again after that invocation exits so late
                // resources cannot survive an earlier stop. This never touches a successor's handle.
                stop(opened);
            } finally {
                connector = null;
                opened.close();
            }
        }
    }

    private void stop(PdkConnector opened) {
        try {
            opened.underLoader(() -> invoke(() -> {
                opened.connector().stop(opened.context());
                return null;
            }));
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Throwable failure) {
            throw new TapstateException(ValidationError.READ_FAILED, Map.of("table", scan.table()), failure);
        }
    }
}
