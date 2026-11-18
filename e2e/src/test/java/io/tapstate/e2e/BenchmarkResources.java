package io.tapstate.e2e;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Samples only the owned OP JVM. RSS is unavailable on hosts without procfs. */
final class BenchmarkResources implements AutoCloseable {
    private final AtomicLong peak = new AtomicLong();
    private final java.util.concurrent.ScheduledExecutorService timer;

    BenchmarkResources(long pid) {
        Path status = Path.of("/proc", Long.toString(pid), "status");
        timer = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        timer.scheduleAtFixedRate(() -> sample(status), 0, 200, TimeUnit.MILLISECONDS);
    }

    Long peakKiB() {
        long observed = peak.get();
        return observed == 0 ? null : observed;
    }

    private void sample(Path status) {
        if (!Files.isReadable(status)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(status)) {
                if (line.startsWith("VmHWM:")) {
                    peak.accumulateAndGet(Long.parseLong(line.trim().split("\\s+")[1]), Math::max);
                }
            }
        } catch (IOException | NumberFormatException ignored) {
            // The process can exit between the existence check and the read.
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
