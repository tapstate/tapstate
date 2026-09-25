package io.tapstate.e2e;

import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

/** Samples a child JVM from outside the product; unavailable readings remain absent. */
final class BenchmarkProcessProbe implements AutoCloseable {

    private static final Duration PS_TIMEOUT = Duration.ofSeconds(2);
    private static final long BYTES_PER_KIB = 1_024;

    private final ProcessHandle child;
    private final JMXConnector jmx;
    private final MBeanServerConnection connection;

    private BenchmarkProcessProbe(ProcessHandle child, JMXConnector jmx, MBeanServerConnection connection) {
        this.child = child;
        this.jmx = jmx;
        this.connection = connection;
    }

    static BenchmarkProcessProbe open(long pid) {
        if (pid <= 0) {
            throw new IllegalArgumentException("a child JVM PID must be positive");
        }
        ProcessHandle child = ProcessHandle.of(pid).orElse(null);
        if (child == null || !child.isAlive()) {
            return new BenchmarkProcessProbe(null, null, null);
        }

        VirtualMachine attached = null;
        JMXConnector jmx = null;
        MBeanServerConnection connection = null;
        try {
            attached = VirtualMachine.attach(Long.toString(pid));
            String address = attached.getAgentProperties().getProperty(
                    "com.sun.management.jmxremote.localConnectorAddress");
            if (address == null || address.isBlank()) {
                address = attached.startLocalManagementAgent();
            }
            jmx = JMXConnectorFactory.connect(new JMXServiceURL(address));
            connection = jmx.getMBeanServerConnection();
        } catch (AttachNotSupportedException | IOException | SecurityException e) {
            closeQuietly(jmx);
            jmx = null;
            connection = null;
        } finally {
            if (attached != null) {
                try {
                    attached.detach();
                } catch (IOException ignored) {
                    // A live JMX connection remains usable after the attach transport closes.
                }
            }
        }
        return new BenchmarkProcessProbe(child, jmx, connection);
    }

    record Snapshot(OptionalLong cpuNanos, OptionalLong heapUsedBytes, OptionalLong rssBytes,
                    OptionalLong gcPauseMillis) {
        boolean complete() {
            return cpuNanos.isPresent() && heapUsedBytes.isPresent()
                    && rssBytes.isPresent() && gcPauseMillis.isPresent();
        }
    }

    Snapshot sample() {
        if (child == null || !child.isAlive()) {
            return unavailable();
        }
        OptionalLong cpu = child.info().totalCpuDuration().stream()
                .mapToLong(Duration::toNanos).findFirst();
        if (cpu.isEmpty()) {
            cpu = jmxCpu();
        }
        return new Snapshot(cpu, heapUsed(), rss(), gcPause());
    }

    private OptionalLong jmxCpu() {
        if (connection == null) {
            return OptionalLong.empty();
        }
        try {
            com.sun.management.OperatingSystemMXBean operatingSystem = ManagementFactory.newPlatformMXBeanProxy(
                    connection, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
                    com.sun.management.OperatingSystemMXBean.class);
            long nanos = operatingSystem.getProcessCpuTime();
            return nanos >= 0 ? OptionalLong.of(nanos) : OptionalLong.empty();
        } catch (IOException | RuntimeException e) {
            return OptionalLong.empty();
        }
    }

    private OptionalLong heapUsed() {
        if (connection == null) {
            return OptionalLong.empty();
        }
        try {
            MemoryMXBean heap = ManagementFactory.newPlatformMXBeanProxy(
                    connection, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            long used = heap.getHeapMemoryUsage().getUsed();
            return used >= 0 ? OptionalLong.of(used) : OptionalLong.empty();
        } catch (IOException | RuntimeException e) {
            return OptionalLong.empty();
        }
    }

    private OptionalLong gcPause() {
        if (connection == null) {
            return OptionalLong.empty();
        }
        try {
            List<GarbageCollectorMXBean> collectors =
                    ManagementFactory.getPlatformMXBeans(connection, GarbageCollectorMXBean.class);
            if (collectors.isEmpty()) {
                return OptionalLong.empty();
            }
            long total = 0;
            for (GarbageCollectorMXBean collector : collectors) {
                long pause = collector.getCollectionTime();
                if (pause < 0) {
                    return OptionalLong.empty();
                }
                total = Math.addExact(total, pause);
            }
            return OptionalLong.of(total);
        } catch (IOException | RuntimeException e) {
            return OptionalLong.empty();
        }
    }

    private OptionalLong rss() {
        Process command;
        try {
            command = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(child.pid()))
                    .redirectErrorStream(true).start();
        } catch (IOException | SecurityException e) {
            return OptionalLong.empty();
        }
        try {
            if (!command.waitFor(PS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                command.destroyForcibly();
                return OptionalLong.empty();
            }
            if (command.exitValue() != 0) {
                return OptionalLong.empty();
            }
            String output = new String(command.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            if (!output.matches("[0-9]+")) {
                return OptionalLong.empty();
            }
            long kib = Long.parseLong(output);
            return kib > 0 ? OptionalLong.of(Math.multiplyExact(kib, BYTES_PER_KIB)) : OptionalLong.empty();
        } catch (IOException | NumberFormatException | ArithmeticException e) {
            return OptionalLong.empty();
        } catch (InterruptedException e) {
            command.destroyForcibly();
            Thread.currentThread().interrupt();
            return OptionalLong.empty();
        }
    }

    private static Snapshot unavailable() {
        return new Snapshot(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty());
    }

    @Override
    public void close() {
        closeQuietly(jmx);
    }

    private static void closeQuietly(JMXConnector connector) {
        if (connector != null) {
            try {
                connector.close();
            } catch (IOException ignored) {
                // Connection loss only makes subsequent readings unavailable.
            }
        }
    }
}
