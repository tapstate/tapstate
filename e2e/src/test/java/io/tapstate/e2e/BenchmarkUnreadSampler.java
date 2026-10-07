package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Fixed cadence point reads retain their own cost; they do not claim an atomic runtime queue size. */
final class BenchmarkUnreadSampler implements AutoCloseable {
    private final Supplier<List<Map<String, Object>>> source;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private final List<Map<String, Object>> samples = new ArrayList<>();
    private Throwable failure;
    private boolean closed;

    BenchmarkUnreadSampler(BenchmarkTableCaptureSet tables) {
        this(tables::unreadSamples);
    }

    BenchmarkUnreadSampler(Supplier<List<Map<String, Object>>> source) {
        this.source = java.util.Objects.requireNonNull(source);
        read();
        worker.scheduleWithFixedDelay(this::read, 200, 200, TimeUnit.MILLISECONDS);
    }

    private void read() {
        synchronized (this) { if (failure != null || closed) { return; } }
        try {
            var next = source.get();
            synchronized (this) {
                if (samples.size() + next.size() > 12_000) { throw new AssertionError("unread sampling exceeded its bounded trace"); }
                samples.addAll(next);
            }
        } catch (RuntimeException | Error problem) {
            synchronized (this) { if (failure == null) { failure = problem; } }
        }
    }

    synchronized List<Map<String, Object>> samples() {
        if (!closed) { throw new IllegalStateException("unread sampling is still active"); }
        if (failure != null) { throw new AssertionError("unread sampling failed", failure); }
        return List.copyOf(samples);
    }

    @Override public void close() {
        synchronized (this) { if (closed) { return; } }
        worker.shutdown();
        try {
            if (!worker.awaitTermination(5, TimeUnit.SECONDS)) {
                worker.shutdownNow();
                throw new AssertionError("unread sampling did not finish its owned read");
            }
        } catch (InterruptedException interrupted) {
            worker.shutdownNow(); Thread.currentThread().interrupt();
            throw new AssertionError("unread sampling shutdown was interrupted", interrupted);
        }
        synchronized (this) { closed = true; }
    }
}
