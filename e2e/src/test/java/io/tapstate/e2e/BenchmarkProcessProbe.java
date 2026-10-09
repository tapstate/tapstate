package io.tapstate.e2e;

import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

/** Samples a child JVM from outside the product; unavailable readings remain absent. */
final class BenchmarkProcessProbe implements AutoCloseable {

    private static final Duration PS_TIMEOUT = Duration.ofSeconds(2);
    private static final long BYTES_PER_KIB = 1_024;

    private final long ownedPid;
    private final ProcessHandle child;
    private final JMXConnector jmx;
    private final MBeanServerConnection connection;
    private CompilationAccess compilationAccess;

    private BenchmarkProcessProbe(long ownedPid, ProcessHandle child, JMXConnector jmx,
                                  MBeanServerConnection connection) {
        this.ownedPid = ownedPid;
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
            return new BenchmarkProcessProbe(pid, null, null, null);
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
        return new BenchmarkProcessProbe(pid, child, jmx, connection);
    }

    enum CompilationState { SUCCESS, UNKNOWN }

    enum CompilationUnknownReason {
        NONE, CHILD_EXITED, JMX_UNAVAILABLE, OWNED_PID_MISMATCH, INVALID_COMPILER_NAME,
        MONITORING_UNSUPPORTED, INVALID_COUNTER, READ_FAILED
    }

    /** An actual cumulative elapsed compilation reading; this value is not compiler CPU time. */
    record CompilationReading(long ownedPid, String compilerName, Boolean monitoringSupported,
                              OptionalLong totalCompilationMillis, CompilationState state,
                              CompilationUnknownReason unknownReason, String failureType) {
        CompilationReading {
            if (ownedPid <= 0) { throw new IllegalArgumentException("compilation reading needs an owned PID"); }
            Objects.requireNonNull(totalCompilationMillis, "compilation counter availability");
            Objects.requireNonNull(state, "compilation reading state");
            Objects.requireNonNull(unknownReason, "compilation unknown reason");
            if (state == CompilationState.SUCCESS) {
                if (compilerName == null || compilerName.isBlank() || !Boolean.TRUE.equals(monitoringSupported)
                        || totalCompilationMillis.isEmpty() || totalCompilationMillis.getAsLong() < 0
                        || unknownReason != CompilationUnknownReason.NONE || failureType != null) {
                    throw new IllegalArgumentException("successful compilation reading lacks actual supported metadata");
                }
            } else if (totalCompilationMillis.isPresent() || unknownReason == CompilationUnknownReason.NONE) {
                throw new IllegalArgumentException("unknown compilation reading must not invent a counter");
            }
        }
    }

    /** Uses the already owned connection, without changing the ordinary four-resource sample. */
    CompilationReading compilationRead() {
        if (child == null || !child.isAlive()) {
            return unknownCompilation(null, null, CompilationUnknownReason.CHILD_EXITED, null);
        }
        if (connection == null) {
            return unknownCompilation(null, null, CompilationUnknownReason.JMX_UNAVAILABLE, null);
        }
        CompilationAccess access = compilationAccess();
        if (access.unknownReason() != CompilationUnknownReason.NONE) {
            return unknownCompilation(access.compilerName(), access.monitoringSupported(),
                    access.unknownReason(), access.failureType());
        }
        try {
            long millis = access.compiler().getTotalCompilationTime();
            if (!child.isAlive()) {
                return unknownCompilation(access.compilerName(), true, CompilationUnknownReason.CHILD_EXITED, null);
            }
            if (millis < 0) {
                return unknownCompilation(access.compilerName(), true, CompilationUnknownReason.INVALID_COUNTER, null);
            }
            return new CompilationReading(ownedPid, access.compilerName(), true, OptionalLong.of(millis),
                    CompilationState.SUCCESS, CompilationUnknownReason.NONE, null);
        } catch (RuntimeException failure) {
            return unknownCompilation(access.compilerName(), true, CompilationUnknownReason.READ_FAILED,
                    failure.getClass().getName());
        }
    }

    private synchronized CompilationAccess compilationAccess() {
        if (compilationAccess != null) { return compilationAccess; }
        String name = null;
        Boolean supported = null;
        try {
            RuntimeMXBean runtime = ManagementFactory.newPlatformMXBeanProxy(
                    connection, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
            if (runtime.getPid() != ownedPid) {
                compilationAccess = new CompilationAccess(null, null, null,
                        CompilationUnknownReason.OWNED_PID_MISMATCH, null);
            } else {
                CompilationMXBean compiler = ManagementFactory.newPlatformMXBeanProxy(
                        connection, ManagementFactory.COMPILATION_MXBEAN_NAME, CompilationMXBean.class);
                name = compiler.getName();
                supported = compiler.isCompilationTimeMonitoringSupported();
                CompilationUnknownReason reason = name == null || name.isBlank()
                        ? CompilationUnknownReason.INVALID_COMPILER_NAME
                        : supported ? CompilationUnknownReason.NONE : CompilationUnknownReason.MONITORING_UNSUPPORTED;
                compilationAccess = new CompilationAccess(compiler, name, supported, reason, null);
            }
        } catch (IOException | RuntimeException failure) {
            compilationAccess = new CompilationAccess(null, name, supported,
                    CompilationUnknownReason.READ_FAILED, failure.getClass().getName());
        }
        return compilationAccess;
    }

    private CompilationReading unknownCompilation(String name, Boolean supported,
                                                  CompilationUnknownReason reason, String failureType) {
        return new CompilationReading(ownedPid, name, supported, OptionalLong.empty(), CompilationState.UNKNOWN,
                reason, failureType);
    }

    private record CompilationAccess(CompilationMXBean compiler, String compilerName, Boolean monitoringSupported,
                                     CompilationUnknownReason unknownReason, String failureType) { }

    /** Lazily reuses this exact child's connection; ordinary resource and compilation reads are unchanged. */
    BenchmarkThreadPointDiagnostics.Reader threadPointReader() {
        return new BenchmarkThreadPointDiagnostics.Reader() {
            private RuntimeMXBean runtime;
            private com.sun.management.ThreadMXBean threads;

            @Override public long ownedPid() { return ownedPid; }

            private void requireConnection() {
                if (connection == null || child == null || !child.isAlive()) {
                    throw new IllegalStateException("owned JVM thread readings are unavailable");
                }
            }

            private com.sun.management.ThreadMXBean threads() {
                requireConnection();
                if (threads == null) {
                    try {
                        threads = ManagementFactory.newPlatformMXBeanProxy(connection,
                                ManagementFactory.THREAD_MXBEAN_NAME, com.sun.management.ThreadMXBean.class);
                    } catch (IOException failure) { throw new UncheckedIOException(failure); }
                }
                return threads;
            }

            @Override public BenchmarkThreadPointDiagnostics.Identity identity() {
                requireConnection();
                if (runtime == null) {
                    try {
                        runtime = ManagementFactory.newPlatformMXBeanProxy(connection,
                                ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
                    } catch (IOException failure) { throw new UncheckedIOException(failure); }
                }
                return new BenchmarkThreadPointDiagnostics.Identity(runtime.getPid(), runtime.getStartTime(), child.isAlive());
            }

            @Override public BenchmarkThreadPointDiagnostics.CpuCapability cpuCapability() {
                boolean supported = threads().isThreadCpuTimeSupported();
                return new BenchmarkThreadPointDiagnostics.CpuCapability(supported,
                        supported ? threads().isThreadCpuTimeEnabled() : null);
            }

            @Override public long[] threadIds() { return threads().getAllThreadIds(); }

            @Override public long[] cpuNanos(long[] ids) { return threads().getThreadCpuTime(ids); }

            @Override public List<BenchmarkThreadPointDiagnostics.ThreadRow> threadInfo(long[] ids, int maxDepth) {
                if (maxDepth < 0 || maxDepth > 64) { throw new IllegalArgumentException("thread point depth is outside its bound"); }
                ThreadInfo[] actual = threads().getThreadInfo(ids, maxDepth);
                var result = new ArrayList<BenchmarkThreadPointDiagnostics.ThreadRow>(actual.length);
                for (ThreadInfo info : actual) {
                    if (info == null) { result.add(null); continue; }
                    var frames = new ArrayList<BenchmarkThreadPointDiagnostics.Frame>();
                    for (StackTraceElement frame : info.getStackTrace()) {
                        frames.add(new BenchmarkThreadPointDiagnostics.Frame(frame.getClassName(), frame.getMethodName(),
                                frame.isNativeMethod(), frame.getLineNumber()));
                    }
                    result.add(new BenchmarkThreadPointDiagnostics.ThreadRow(info.getThreadId(), info.getThreadName(),
                            info.getThreadState().name(), List.copyOf(frames)));
                }
                return java.util.Collections.unmodifiableList(result);
            }
        };
    }

    record Snapshot(OptionalLong cpuNanos, OptionalLong heapUsedBytes, OptionalLong rssBytes,
                    OptionalLong gcCollectionMillis) {
        boolean complete() {
            return cpuNanos.isPresent() && heapUsedBytes.isPresent()
                    && rssBytes.isPresent() && gcCollectionMillis.isPresent();
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
        return new Snapshot(cpu, heapUsed(), rss(), gcCollectionTime());
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

    private OptionalLong gcCollectionTime() {
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
                long elapsed = collector.getCollectionTime();
                if (elapsed < 0) {
                    return OptionalLong.empty();
                }
                total = Math.addExact(total, elapsed);
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
