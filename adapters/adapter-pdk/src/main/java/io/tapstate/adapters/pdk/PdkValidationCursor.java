package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Bounded delivery from one synchronous PDK query invocation. The session owner supplies an isolated
 * connector, loader context, an admitted blocking-I/O executor and a driver-materialization reservation
 * before opening this bridge. This queue bounds decoded rows only; it grants none of those resources.
 */
final class PdkValidationCursor implements ValidationRowCursor {
    @FunctionalInterface interface QueryCall { void run(Consumer<FilterResults> consumer) throws Throwable; }
    @FunctionalInterface interface Decoder { ValidationValues.Row decode(Map<String, Object> row); }

    private final String table;
    private final QueryCall query;
    private final Decoder decoder;
    private final Runnable requireAuthorized;
    private final Runnable cancelQuery;
    private final ValidationValues.Limits rowLimits;
    private final ArrayBlockingQueue<BorrowedRow> rows;
    private final Semaphore slots;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean callbackActive = new AtomicBoolean();
    private final Object invocationLock = new Object();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean invocationOpen;
    private volatile boolean done;
    private volatile Future<?> worker;
    private long ordinal;

    PdkValidationCursor(String table, QueryCall query, Decoder decoder, Runnable requireAuthorized,
            Runnable cancelQuery, ExecutorService executor, int maxInFlightRows, ValidationValues.Limits rowLimits) {
        this.table = Objects.requireNonNull(table, "table");
        this.query = Objects.requireNonNull(query, "query");
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.requireAuthorized = Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        this.cancelQuery = Objects.requireNonNull(cancelQuery, "cancelQuery");
        this.rowLimits = Objects.requireNonNull(rowLimits, "rowLimits");
        Objects.requireNonNull(executor, "executor");
        if (table.isBlank() || maxInFlightRows < 1 || maxInFlightRows > 64
                || rowLimits.bytes() * maxInFlightRows > 128L * 1024 * 1024) {
            throw new IllegalArgumentException("validation row queue exceeds its bounded admission limits");
        }
        this.rows = new ArrayBlockingQueue<>(maxInFlightRows);
        this.slots = new Semaphore(maxInFlightRows);
        try {
            requireAuthorized.run();
            worker = executor.submit(this::produce);
            if (closed.get()) worker.cancel(true);
        } catch (RuntimeException rejected) {
            closed.set(true);
            RuntimeException primary = rejected instanceof RejectedExecutionException
                    ? new TapstateException(ValidationError.READ_FAILED, Map.of("table", table), rejected) : rejected;
            try { cancelQuery.run(); }
            catch (RuntimeException cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            throw primary;
        }
    }

    private void produce() {
        try {
            checkActive();
            synchronized (invocationLock) { invocationOpen = true; }
            query.run(this::accept);
            synchronized (invocationLock) {
                invocationOpen = false;
                if (callbackActive.get()) throw protocol();
            }
            checkActive();
        } catch (Throwable stopped) {
            recordFailure(stopped);
        } finally {
            synchronized (invocationLock) { invocationOpen = false; }
            done = true;
        }
    }

    private void accept(FilterResults result) {
        synchronized (invocationLock) {
            if (!invocationOpen || !callbackActive.compareAndSet(false, true)) {
                TapstateException violation = protocol();
                recordFailure(violation);
                throw violation;
            }
        }
        try {
            checkActive();
            if (result == null) throw protocol();
            if (result.getError() != null) {
                throw new TapstateException(ValidationError.READ_FAILED, Map.of("table", table), result.getError());
            }
            if (result.getResults() == null) throw protocol();
            for (Map<String, Object> raw : result.getResults()) {
                if (raw == null) throw protocol();
                takeSlot();
                BorrowedRow owned = null;
                boolean handedOver = false;
                try {
                    checkActive();
                    ValidationValues.Row decoded = Objects.requireNonNull(decoder.decode(raw), "decoded row");
                    if (decoded.accountedBytes() > rowLimits.bytes() || decoded.nodes() > rowLimits.nodes()
                            || decoded.depth() > rowLimits.depth()) {
                        throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "row"), null);
                    }
                    checkActive();
                    if (ordinal == Long.MAX_VALUE) throw protocol();
                    owned = new BorrowedRow(ordinal++, decoded);
                    if (!rows.offer(owned)) throw new IllegalStateException("reserved validation row slot was unavailable");
                    handedOver = true;
                    // close may have drained the queue before this offer. Reclaim this exact late item.
                    if (closed.get() && rows.remove(owned)) owned.close();
                } finally {
                    if (!handedOver) slots.release();
                }
            }
        } catch (RuntimeException | Error failed) {
            recordFailure(failed);
            throw failed;
        } finally {
            callbackActive.set(false);
        }
    }

    private void takeSlot() {
        try {
            while (true) {
                checkActive();
                if (slots.tryAcquire(50, TimeUnit.MILLISECONDS)) return;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw cancelled();
        }
    }

    @Override public Optional<Row> next() {
        try {
            return readNext();
        } catch (RuntimeException | Error failed) {
            recordFailure(failed);
            close();
            throwFailure();
            throw failed;
        }
    }

    private Optional<Row> readNext() {
        while (true) {
            throwFailure();
            checkActive();
            BorrowedRow row;
            try { row = rows.poll(50, TimeUnit.MILLISECONDS); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw cancelled();
            }
            if (row != null) {
                try {
                    throwFailure();
                    checkActive();
                    return Optional.of(row);
                } catch (RuntimeException | Error failed) {
                    row.close();
                    throw failed;
                }
            }
            if (done && rows.isEmpty()) {
                throwFailure();
                checkActive();
                return Optional.empty();
            }
        }
    }

    private void checkActive() {
        if (closed.get()) throw cancelled();
        requireAuthorized.run();
    }

    private void recordFailure(Throwable error) {
        failure.compareAndSet(null, error);
    }

    private void throwFailure() {
        Throwable failed = failure.get();
        if (failed instanceof RuntimeException runtime) throw runtime;
        if (failed instanceof Error error) throw error;
        if (failed != null) throw new TapstateException(ValidationError.READ_FAILED, Map.of("table", table), failed);
    }

    private TapstateException protocol() {
        return new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", table), null);
    }

    private TapstateException cancelled() {
        return new TapstateException(ValidationError.READ_CANCELLED, Map.of("table", table), null);
    }

    int availableRowSlots() { return slots.availablePermits(); }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        boolean cancellationFirst = failure.compareAndSet(null, cancelled());
        if (worker != null) worker.cancel(true);
        BorrowedRow queued;
        while ((queued = rows.poll()) != null) queued.close();
        try { cancelQuery.run(); }
        catch (RuntimeException | Error cleanup) {
            Throwable primary = failure.get();
            if (primary != cleanup) primary.addSuppressed(cleanup);
            if (cancellationFirst) throw cleanup;
        }
    }

    private final class BorrowedRow implements Row {
        private final long rowOrdinal;
        private final ValidationValues.Row values;
        private final AtomicBoolean released = new AtomicBoolean();

        private BorrowedRow(long ordinal, ValidationValues.Row values) {
            this.rowOrdinal = ordinal;
            this.values = values;
        }
        @Override public long ordinal() { return rowOrdinal; }
        @Override public ValidationValues.Row values() {
            if (released.get()) throw new IllegalStateException("validation row reservation has been released");
            return values;
        }
        @Override public void close() { if (released.compareAndSet(false, true)) slots.release(); }
    }
}
