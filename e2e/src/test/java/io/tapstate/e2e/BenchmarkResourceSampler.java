package io.tapstate.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Samples one child JVM throughout a measured window, preserving unavailable readings as failure. */
final class BenchmarkResourceSampler implements AutoCloseable {

    private final BenchmarkProcessProbe probe;
    private final Supplier<BenchmarkProcessProbe.Snapshot> source;
    private final Duration interval;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("benchmark-resource-sampler").factory());
    private final List<BenchmarkProcessProbe.Snapshot> samples = new ArrayList<>();
    private Throwable failure;
    private boolean started;
    private boolean finished;

    private BenchmarkResourceSampler(BenchmarkProcessProbe probe,
                                     Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval) {
        this.probe = probe;
        this.source = Objects.requireNonNull(source, "sample source");
        this.interval = Objects.requireNonNull(interval, "sample interval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("resource sample interval must be positive");
        }
    }

    static BenchmarkResourceSampler open(long childPid, Duration interval) {
        BenchmarkProcessProbe probe = BenchmarkProcessProbe.open(childPid);
        return new BenchmarkResourceSampler(probe, probe::sample, interval);
    }

    static BenchmarkResourceSampler from(Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval) {
        return new BenchmarkResourceSampler(null, source, interval);
    }

    synchronized void start() {
        if (started || finished) {
            throw new IllegalStateException("resource sampler can start only once");
        }
        started = true;
        record();
        worker.scheduleWithFixedDelay(this::record, interval.toNanos(), interval.toNanos(),
                TimeUnit.NANOSECONDS);
    }

    Summary finish() {
        synchronized (this) {
            if (!started || finished) {
                throw new IllegalStateException("resource sampler has no open measured window");
            }
            finished = true;
        }
        worker.shutdown();
        try {
            if (!worker.awaitTermination(5, TimeUnit.SECONDS)) {
                worker.shutdownNow();
                throw new AssertionError("resource sampler did not stop within five seconds");
            }
        } catch (InterruptedException interrupted) {
            worker.shutdownNow();
            Thread.currentThread().interrupt();
            throw new AssertionError("resource sampling was interrupted", interrupted);
        }
        synchronized (this) {
            record();
            if (failure != null) {
                throw new AssertionError("external JVM resource sampling failed", failure);
            }
            return summarize(samples);
        }
    }

    private synchronized void record() {
        if (failure != null) {
            return;
        }
        try {
            BenchmarkProcessProbe.Snapshot sample = source.get();
            if (sample == null || !sample.complete()) {
                throw new AssertionError("external JVM resource sample is incomplete: " + sample);
            }
            samples.add(sample);
        } catch (Throwable error) {
            failure = error;
        }
    }

    static Summary summarize(List<BenchmarkProcessProbe.Snapshot> readings) {
        if (readings == null || readings.size() < 2) {
            throw new AssertionError("resource window needs at least a start and end sample");
        }
        long peakHeap = 0;
        long peakRss = 0;
        for (BenchmarkProcessProbe.Snapshot reading : readings) {
            if (reading == null || !reading.complete()) {
                throw new AssertionError("resource window contains an incomplete sample: " + reading);
            }
            peakHeap = Math.max(peakHeap, reading.heapUsedBytes().orElseThrow());
            peakRss = Math.max(peakRss, reading.rssBytes().orElseThrow());
        }
        BenchmarkProcessProbe.Snapshot first = readings.getFirst();
        BenchmarkProcessProbe.Snapshot last = readings.getLast();
        long cpu = last.cpuNanos().orElseThrow() - first.cpuNanos().orElseThrow();
        long gc = last.gcPauseMillis().orElseThrow() - first.gcPauseMillis().orElseThrow();
        if (cpu < 0 || gc < 0 || peakHeap <= 0 || peakRss <= 0) {
            throw new AssertionError("resource counter moved backward or memory was unavailable");
        }
        return new Summary(cpu, gc, peakHeap, peakRss, readings.size());
    }

    record Summary(long cpuNanos, long gcPauseMillis, long peakHeapBytes, long peakRssBytes, int sampleCount) {
    }

    @Override
    public void close() {
        worker.shutdownNow();
        if (probe != null) {
            probe.close();
        }
    }
}
