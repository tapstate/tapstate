package io.tapstate.adapters.otel;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reads JVM and process resources only when a metrics reader collects them. */
final class JdkProcessFacts {

    /** Negative values mean that the host or JVM could not measure that quantity. */
    record Sample(long cpuNanos, double cpuLoad, long heapUsedBytes, long heapCommittedBytes,
            long gcCollections, long gcCollectionMillis) {
    }

    @FunctionalInterface
    interface Probe {
        Sample read();
    }

    private final Probe probe;
    private final Instant processStartedAt;
    private final Clock clock;

    JdkProcessFacts() {
        this(new JdkProbe(), Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime()),
                Clock.systemUTC());
    }

    JdkProcessFacts(Probe probe, Instant processStartedAt, Clock clock) {
        this.probe = Objects.requireNonNull(probe, "probe");
        this.processStartedAt = Objects.requireNonNull(processStartedAt, "processStartedAt");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    List<MetricFact> snapshot() {
        Sample sample = probe.read();
        Instant now = clock.instant();
        List<MetricFact> facts = new ArrayList<>(6);
        if (sample.cpuNanos() >= 0) {
            facts.add(counter("tapstate.process.cpu.time", "ns", sample.cpuNanos(), now));
        }
        if (Double.isFinite(sample.cpuLoad()) && sample.cpuLoad() >= 0 && sample.cpuLoad() <= 1) {
            // MetricFact currently carries integral readings, so this is an integer percentage.
            facts.add(gauge("tapstate.process.cpu.load", "%", Math.round(sample.cpuLoad() * 100), now));
        }
        if (sample.heapUsedBytes() >= 0) {
            facts.add(gauge("tapstate.process.jvm.heap.used", "By", sample.heapUsedBytes(), now));
        }
        if (sample.heapCommittedBytes() >= 0) {
            facts.add(gauge("tapstate.process.jvm.heap.committed", "By", sample.heapCommittedBytes(), now));
        }
        if (sample.gcCollections() >= 0) {
            facts.add(counter("tapstate.process.jvm.gc.collections", "{collection}",
                    sample.gcCollections(), now));
        }
        if (sample.gcCollectionMillis() >= 0) {
            // MXBean collection elapsed time is not necessarily stop-the-world pause time.
            facts.add(counter("tapstate.process.jvm.gc.collection.time", "ms",
                    sample.gcCollectionMillis(), now));
        }
        return List.copyOf(facts);
    }

    private MetricFact counter(String name, String unit, long value, Instant now) {
        return MetricFact.single(name, MetricType.COUNTER, unit,
                MetricPoint.accumulated(Map.of(), processStartedAt, now, value));
    }

    private static MetricFact gauge(String name, String unit, long value, Instant now) {
        return MetricFact.single(name, MetricType.GAUGE, unit,
                MetricPoint.reading(Map.of(), now, value));
    }

    private static final class JdkProbe implements Probe {
        private final java.lang.management.OperatingSystemMXBean operatingSystem =
                ManagementFactory.getOperatingSystemMXBean();
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();

        @Override
        public Sample read() {
            long cpuNanos = -1;
            double cpuLoad = -1;
            if (operatingSystem instanceof com.sun.management.OperatingSystemMXBean extended) {
                try {
                    cpuNanos = extended.getProcessCpuTime();
                    cpuLoad = extended.getProcessCpuLoad();
                } catch (RuntimeException unavailable) {
                    // JVMs without these optional process readings leave both absent.
                }
            }
            long heapUsed = -1;
            long heapCommitted = -1;
            try {
                MemoryUsage heap = memory.getHeapMemoryUsage();
                heapUsed = heap.getUsed();
                heapCommitted = heap.getCommitted();
            } catch (RuntimeException unavailable) {
                // A management failure must not turn an unknown reading into zero.
            }
            long collections = sumCollectorReadings(true);
            long collectionMillis = sumCollectorReadings(false);
            return new Sample(cpuNanos, cpuLoad, heapUsed, heapCommitted,
                    collections, collectionMillis);
        }

        private long sumCollectorReadings(boolean count) {
            if (collectors.isEmpty()) {
                return -1;
            }
            long sum = 0;
            try {
                for (GarbageCollectorMXBean collector : collectors) {
                    long reading = count ? collector.getCollectionCount() : collector.getCollectionTime();
                    if (reading < 0) {
                        return -1;
                    }
                    sum = Math.addExact(sum, reading);
                }
                return sum;
            } catch (RuntimeException unavailable) {
                return -1;
            }
        }
    }
}
