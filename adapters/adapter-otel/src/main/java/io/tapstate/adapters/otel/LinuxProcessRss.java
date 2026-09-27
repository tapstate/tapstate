package io.tapstate.adapters.otel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Samples Linux resident memory away from a metrics reader and expires stale measurements. */
final class LinuxProcessRss implements AutoCloseable {

    private static final Path ROLLUP = Path.of("/proc/self/smaps_rollup");
    private static final long PERIOD_SECONDS = 15;
    private static final long MAX_AGE_NANOS = Duration.ofSeconds(30).toNanos();
    private static final long BYTES_PER_KIB = 1_024;

    record Sample(long bytes, Instant observedAt) {
    }

    private record Reading(Sample sample, long sampledAtNanos) {
    }

    private final Path path;
    private final LongSupplier nanoTime;
    private final Clock clock;
    private final ScheduledExecutorService worker;
    private final AtomicReference<Reading> last = new AtomicReference<>();
    private volatile boolean closed;

    static LinuxProcessRss start() {
        if (!"Linux".equalsIgnoreCase(System.getProperty("os.name")) || !Files.isReadable(ROLLUP)) {
            return new LinuxProcessRss(null, System::nanoTime, Clock.systemUTC(), false);
        }
        return new LinuxProcessRss(ROLLUP, System::nanoTime, Clock.systemUTC(), true);
    }

    LinuxProcessRss(Path path, LongSupplier nanoTime, Clock clock, boolean schedule) {
        this.path = path;
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (schedule) {
            Objects.requireNonNull(path, "path");
            worker = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "tapstate-process-rss");
                thread.setDaemon(true);
                return thread;
            });
            worker.scheduleWithFixedDelay(this::refresh, 0, PERIOD_SECONDS, TimeUnit.SECONDS);
        } else {
            worker = null;
        }
    }

    Optional<Sample> reading() {
        Reading current = last.get();
        if (current == null || closed) {
            return Optional.empty();
        }
        long age = nanoTime.getAsLong() - current.sampledAtNanos();
        return age >= 0 && age <= MAX_AGE_NANOS ? Optional.of(current.sample()) : Optional.empty();
    }

    void refresh() {
        if (path == null || closed) {
            return;
        }
        try {
            long bytes = parse(Files.readString(path));
            synchronized (this) {
                if (!closed) {
                    last.set(bytes < 0 ? null : new Reading(
                            new Sample(bytes, clock.instant()), nanoTime.getAsLong()));
                }
            }
        } catch (IOException | SecurityException | ArithmeticException unreadable) {
            synchronized (this) {
                if (!closed) {
                    last.set(null);
                }
            }
        }
    }

    static long parse(String rollup) {
        for (String line : rollup.split("\\R")) {
            if (!line.startsWith("Rss:")) {
                continue;
            }
            String[] fields = line.substring(4).trim().split("\\s+");
            if (fields.length != 2 || !"kB".equals(fields[1])) {
                return -1;
            }
            try {
                long kib = Long.parseLong(fields[0]);
                return kib >= 0 ? Math.multiplyExact(kib, BYTES_PER_KIB) : -1;
            } catch (NumberFormatException | ArithmeticException invalid) {
                return -1;
            }
        }
        return -1;
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            last.set(null);
        }
        if (worker != null) {
            worker.shutdownNow();
        }
    }
}
