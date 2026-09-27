package io.tapstate.adapters.otel;

import jdk.jfr.consumer.RecordingStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Accumulates only GC pause events observed by a bounded, process-local JFR stream. */
final class JfrGcPauses implements AutoCloseable {

    record Reading(Instant since, long durationNanos) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(JfrGcPauses.class);
    private static final long MAX_REPOSITORY_BYTES = 8L * 1_024 * 1_024;
    private final RecordingStream stream;
    private volatile Instant startedAt;
    private final AtomicLong durationNanos = new AtomicLong();
    private final AtomicBoolean observed = new AtomicBoolean();
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    static JfrGcPauses start() {
        RecordingStream stream = null;
        try {
            stream = new RecordingStream();
            JfrGcPauses pauses = new JfrGcPauses(stream, null);
            stream.setSettings(Map.of());
            stream.enable("jdk.GCPhasePause").withoutThreshold().withoutStackTrace();
            stream.enable("jdk.DataLoss");
            stream.setMaxAge(Duration.ofMinutes(1));
            stream.setMaxSize(MAX_REPOSITORY_BYTES);
            stream.onEvent("jdk.GCPhasePause", event -> pauses.record(event.getDuration()));
            stream.onEvent("jdk.DataLoss", ignored -> pauses.invalidate("JFR data loss", null));
            stream.onError(error -> pauses.invalidate("JFR stream failed", error));
            stream.onClose(() -> pauses.healthy.set(false));
            pauses.startedAt = Instant.now();
            stream.startAsync();
            return pauses;
        } catch (RuntimeException | LinkageError unavailable) {
            if (stream != null) {
                stream.close();
            }
            LOG.warn("JFR GC pause observation is unavailable", unavailable);
            return new JfrGcPauses(null, Instant.now(), false);
        }
    }

    JfrGcPauses(RecordingStream stream, Instant startedAt) {
        this(stream, startedAt, true);
    }

    private JfrGcPauses(RecordingStream stream, Instant startedAt, boolean healthy) {
        this.stream = stream;
        this.startedAt = startedAt;
        this.healthy.set(healthy);
    }

    void record(Duration pause) {
        if (!healthy.get() || pause.isNegative()) {
            return;
        }
        try {
            long elapsed = pause.toNanos();
            durationNanos.updateAndGet(previous -> Math.addExact(previous, elapsed));
            observed.set(true);
        } catch (ArithmeticException overflow) {
            invalidate("JFR GC pause duration overflowed", overflow);
        }
    }

    Optional<Reading> snapshot() {
        if (!healthy.get() || !observed.get()) {
            return Optional.empty();
        }
        long nanos = durationNanos.get();
        return healthy.get() ? Optional.of(new Reading(startedAt, nanos)) : Optional.empty();
    }

    void invalidate(String reason, Throwable cause) {
        if (healthy.getAndSet(false)) {
            if (cause == null) {
                LOG.warn("{}; GC pause facts will remain absent", reason);
            } else {
                LOG.warn("{}; GC pause facts will remain absent", reason, cause);
            }
        }
    }

    @Override
    public void close() {
        healthy.set(false);
        if (stream != null) {
            stream.close();
        }
    }
}
