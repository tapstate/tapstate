package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** One node-local default Source test slot; an unfinished owned call cannot create replacement threads. */
final class DefaultSourcePdkTest implements AutoCloseable {
    interface Call {
        ConnectionTestResult run();
        void cancel();
        boolean resourcesReleased();
        default Throwable observedFailure() { return null; }
        default Object owner() { return this; }
    }
    private final Duration budget;
    private final Duration cleanupBudget;
    private final Clock clock;
    private final AtomicReference<Flight> current = new AtomicReference<>();
    private volatile boolean closed;
    DefaultSourcePdkTest(Clock clock, Duration budget, Duration cleanupBudget) {
        this.clock = java.util.Objects.requireNonNull(clock);
        this.budget = java.util.Objects.requireNonNull(budget);
        this.cleanupBudget = java.util.Objects.requireNonNull(cleanupBudget);
        if (budget.isZero() || budget.isNegative() || cleanupBudget.isNegative()) throw new IllegalArgumentException("invalid test budget");
    }
    boolean available() { return !closed && current.get() == null; }
    ConnectionTestResult test(ConnectionConfig config, Call call) {
        return test(config, call, System.nanoTime() + budget.toNanos());
    }
    ConnectionTestResult test(ConnectionConfig config, Call call, long deadlineNanos) {
        if (closed || current.get() != null) throw busy(config);
        if (deadlineNanos - System.nanoTime() <= 0) throw timeout(config);
        var flight = new Flight(call);
        if (closed || !current.compareAndSet(null, flight)) throw busy(config);
        if (closed) { current.compareAndSet(flight, null); throw busy(config); }
        if (deadlineNanos - System.nanoTime() <= 0) { current.compareAndSet(flight, null); throw timeout(config); }
        flight.worker.start();
        try {
            boolean finished = flight.exited.await(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (!finished) {
                flight.cancel();
                flight.cleanupFinished.await(Math.max(0, flight.cleanupDeadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                long remaining = flight.cleanupDeadline - System.nanoTime();
                if (remaining > 0) flight.exited.await(remaining, TimeUnit.NANOSECONDS);
                rethrowDefect(flight.call.observedFailure());
                rethrowDefect(flight.cleanupFailure);
                rethrowDefect(flight.failure);
                if (!flight.released()) throw timeout(config);
                return failed(config, ConnectorError.TEST_TIMEOUT.code());
            }
            rethrowDefect(flight.call.observedFailure());
            rethrowDefect(flight.cleanupFailure);
            rethrowDefect(flight.failure);
            if (flight.cancelling || flight.interrupted || !flight.released()) throw timeout(config);
            if (flight.failure != null) {
                if (flight.failure instanceof TapstateException coded && coded.code().code().startsWith("connector.")
                        && !(coded.getCause() instanceof InterruptedException))
                    return failed(config, coded.code().code());
                if (flight.failure instanceof RuntimeException runtime) throw runtime;
                if (flight.failure instanceof Error error) throw error;
                throw new IllegalStateException("unexpected default test failure", flight.failure);
            }
            return flight.result;
        } catch (InterruptedException interrupted) {
            flight.cancel(); Thread.currentThread().interrupt(); throw timeout(config);
        } finally { flight.returned = true; flight.release(); }
    }
    private static void rethrowDefect(Throwable failure) {
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime && !(failure instanceof TapstateException)) throw runtime;
    }

    private ConnectionTestResult failed(ConnectionConfig config, String code) {
        return new ConnectionTestResult(config.id(), config.connectorId(), ConnectionTestResult.Outcome.FAILED,
                List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.FAILED,
                        null, null, null, code)), clock.millis());
    }
    private TapstateException busy(ConnectionConfig config) {
        return new TapstateException(ConnectorError.INSTANCES_BUSY, Map.of("connector", config.connectorId(), "timeout", "0s"), null);
    }
    private TapstateException timeout(ConnectionConfig config) {
        return new TapstateException(ConnectorError.TEST_TIMEOUT, Map.of("connector", config.connectorId(), "timeout", budget.toString()), null);
    }
    void cancelOwned(Object owner) {
        var active = current.get(); if (active != null && active.call.owner() == owner) active.cancel();
    }
    @Override public void close() { closed = true; var active = current.get(); if (active != null) active.cancel(); }
    private final class Flight {
        private final Call call;
        private final CountDownLatch exited = new CountDownLatch(1);
        private final CountDownLatch cleanupFinished = new CountDownLatch(1);
        private volatile ConnectionTestResult result;
        private volatile Throwable failure;
        private volatile Throwable cleanupFailure;
        private volatile boolean returned;
        private volatile boolean interrupted;
        private volatile boolean cancelling;
        private volatile long cleanupDeadline;
        private final Thread worker;
        Flight(Call call) {
            this.call = call;
            worker = new Thread(() -> {
                try { result = call.run(); }
                catch (RuntimeException | Error failed) { failure = failed; }
                finally { interrupted = Thread.currentThread().isInterrupted(); exited.countDown(); release(); }
            }, "tapstate-default-source-test");
            worker.setDaemon(true);
        }
        synchronized void cancel() {
            if (cancelling) return;
            cancelling = true;
            cleanupDeadline = System.nanoTime() + cleanupBudget.toNanos();
            worker.interrupt();
            var cleanup = new Thread(() -> {
                try { call.cancel(); }
                catch (RuntimeException | Error failed) { cleanupFailure = failed; }
                finally { cleanupFinished.countDown(); release(); }
            }, "tapstate-default-source-test-cleanup");
            cleanup.setDaemon(true); cleanup.start();
        }
        boolean released() {
            return exited.getCount() == 0 && (!cancelling || cleanupFinished.getCount() == 0)
                    && cleanupFailure == null && call.resourcesReleased();
        }
        void release() { if (returned && released()) current.compareAndSet(this, null); }
    }
}
