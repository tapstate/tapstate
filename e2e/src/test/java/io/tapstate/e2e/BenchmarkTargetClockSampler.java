package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Owned fixed-cadence hello reads qualify sampled interior clock behavior and record their external cost. */
final class BenchmarkTargetClockSampler implements AutoCloseable {
    private final Supplier<BenchmarkTargetClock.Reading> source;
    private final Runnable closeSource;
    private final ScheduledThreadPoolExecutor worker = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
    private final List<BenchmarkTargetClock.Reading> readings = new ArrayList<>();
    private Throwable failure;
    private boolean closed;

    static BenchmarkTargetClockSampler open(String uri) {
        var client = MongoClients.create(uri);
        try {
            var admin = client.getDatabase("admin").withTimeout(1, TimeUnit.SECONDS);
            admin.runCommand(new org.bson.Document("ping", 1));
            return new BenchmarkTargetClockSampler(() -> BenchmarkTargetClock.read(admin), client::close);
        } catch (RuntimeException | Error failure) { client.close(); throw failure; }
    }

    BenchmarkTargetClockSampler(Supplier<BenchmarkTargetClock.Reading> source, Runnable closeSource) {
        this.source = java.util.Objects.requireNonNull(source);
        this.closeSource = java.util.Objects.requireNonNull(closeSource);
        worker.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        read();
        worker.scheduleWithFixedDelay(this::read, BenchmarkTargetClock.INTERIOR_SAMPLE_INTERVAL_MILLIS,
                BenchmarkTargetClock.INTERIOR_SAMPLE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void read() {
        synchronized (this) { if (closed || failure != null) { return; } }
        try {
            var next = source.get();
            synchronized (this) {
                if (readings.size() >= BenchmarkTargetClock.MAX_INTERIOR_SAMPLES) { throw new AssertionError("clock sampling exceeded its bounded trace"); }
                readings.add(next);
            }
        } catch (RuntimeException | Error problem) {
            synchronized (this) { if (failure == null) { failure = problem; } }
        }
    }

    synchronized Map<String, Object> evidence() {
        if (!closed) { throw new IllegalStateException("clock sampling is still active"); }
        if (failure != null) { throw new AssertionError("target clock sampling failed", failure); }
        var result = new java.util.LinkedHashMap<String, Object>(BenchmarkTargetClock.validateSeries(List.copyOf(readings)));
        result.put("helloCommands", readings.size());
        result.put("setupPingCommands", 1);
        result.put("scope", "EXTERNAL_TARGET_PRIMARY_HELLO_REQUEST_BRACKETS");
        return Map.copyOf(result);
    }

    synchronized int samplesRecorded() { return readings.size(); }

    synchronized List<BenchmarkTargetClock.Reading> readings() {
        if (!closed) { throw new IllegalStateException("clock sampling is still active"); }
        if (failure != null) { throw new AssertionError("target clock sampling failed", failure); }
        return List.copyOf(readings);
    }

    @Override public void close() {
        synchronized (this) { if (closed) { return; } }
        worker.shutdown();
        try {
            if (!worker.awaitTermination(2, TimeUnit.SECONDS)) {
                worker.shutdownNow();
                throw new AssertionError("target clock sampling did not finish its owned read");
            }
            synchronized (this) { closed = true; }
        } catch (InterruptedException interrupted) {
            worker.shutdownNow(); Thread.currentThread().interrupt();
            throw new AssertionError("target clock sampling shutdown was interrupted", interrupted);
        } finally { closeSource.run(); }
    }
}
