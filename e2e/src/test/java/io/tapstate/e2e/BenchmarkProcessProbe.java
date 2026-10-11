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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Samples a child JVM from outside the product; unavailable readings remain absent. */
final class BenchmarkProcessProbe implements AutoCloseable {

    private static final Duration PS_TIMEOUT = Duration.ofSeconds(2);
    private static final long BYTES_PER_KIB = 1_024;

    private final long ownedPid;
    private final BenchmarkReturnWorkBudget.Scope workScope = BenchmarkReturnWorkBudget.current();
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
        var scope = BenchmarkReturnWorkBudget.current();
        var opening = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, BenchmarkReturnWorkBudget.Work.call());
        if (pid <= 0) {
            var invalid = new IllegalArgumentException("a child JVM PID must be positive");
            BenchmarkReturnWorkBudget.failed(opening, invalid); throw invalid;
        }
        ProcessHandle child = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO,
                () -> ProcessHandle.of(pid).orElse(null));
        if (child == null || !BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive)) {
            BenchmarkReturnWorkBudget.done(opening);
            return new BenchmarkProcessProbe(pid, null, null, null);
        }

        VirtualMachine attached = null;
        JMXConnector jmx = null;
        MBeanServerConnection connection = null;
        BenchmarkReturnWorkBudget.Ticket attaching = null;
        try {
            attaching = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, BenchmarkReturnWorkBudget.Work.call());
            BenchmarkReturnWorkBudget.issued(attaching);
            attached = VirtualMachine.attach(Long.toString(pid));
            BenchmarkReturnWorkBudget.done(attaching);
            VirtualMachine actualAttached = attached;
            var properties = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, actualAttached::getAgentProperties);
            BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ,
                    BenchmarkReturnWorkBudget.material(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, properties));
            String address = properties.getProperty(
                    "com.sun.management.jmxremote.localConnectorAddress");
            if (address == null || address.isBlank()) {
                address = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                        actualAttached::startLocalManagementAgent);
            }
            JMXServiceURL actualAddress = new JMXServiceURL(address);
            jmx = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                    () -> JMXConnectorFactory.connect(actualAddress));
            JMXConnector actualJmx = jmx;
            connection = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ,
                    actualJmx::getMBeanServerConnection);
        } catch (AttachNotSupportedException | IOException | SecurityException e) {
            BenchmarkReturnWorkBudget.failed(attaching, e); BenchmarkReturnWorkBudget.failed(opening, e);
            closeQuietly(jmx, scope);
            jmx = null;
            connection = null;
        } finally {
            if (attached != null) {
                try {
                    VirtualMachine actualAttached = attached;
                    BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.CLEANUP, () -> { actualAttached.detach(); return null; });
                } catch (IOException ignored) {
                    // A live JMX connection remains usable after the attach transport closes.
                }
            }
        }
        BenchmarkReturnWorkBudget.done(opening);
        return new BenchmarkProcessProbe(pid, child, jmx, connection);
    }

    enum CompilationState { SUCCESS, UNKNOWN }

    BenchmarkWriteReturnReader writeReturnReader() {
        var runtime = runtimeEvidence();
        if (!"COMPLETE".equals(runtime.get("status")) || connection == null || child == null
                || !(runtime.get("jvmStartTimeMillis") instanceof Long start)) {
            throw new AssertionError("owned return probe has no verified resource runtime");
        }
        return new BenchmarkWriteReturnReader(new BenchmarkCausalClock.Identity(ownedPid, start),
                connection, () -> BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive), io.tapstate.adapters.pdk.PdkBenchmarkClock::nanoTime);
    }

    /** Retains runtime identity through the existing resource connection before the measured window. */
    Map<String, Object> runtimeEvidence() {
        BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, BenchmarkReturnWorkBudget.Work.call());
        long before = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
        boolean aliveBefore = child != null && BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive);
        if (!aliveBefore || connection == null) {
            return Map.of("status", "UNKNOWN", "reason", "OWNED_RUNTIME_UNAVAILABLE", "ownedPid", ownedPid);
        }
        try {
            RuntimeMXBean runtime = runtimeProxy();
            long actualPid = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getPid);
            long startMillis = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getStartTime);
            List<String> arguments = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getInputArguments);
            String vmName = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getVmName);
            String vmVersion = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getVmVersion);
            String vmVendor = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getVmVendor);
            Map<String, String> version = Map.of("vmName", vmName, "vmVersion", vmVersion, "vmVendor", vmVendor);
            boolean aliveAfter = BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive);
            if (actualPid != ownedPid || startMillis <= 0 || !aliveAfter || arguments.size() > 128
                    || arguments.stream().anyMatch(value -> value.length() > 16384)
                    || arguments.stream().mapToLong(value -> 4L * value.length() + 16).sum() > 2L * 1024 * 1024 - 4096
                    || version.values().stream().anyMatch(value -> value.isBlank() || value.length() > 4096)) {
                return Map.of("status", "UNKNOWN", "reason", "OWNED_RUNTIME_IDENTITY_OR_SHAPE",
                        "ownedPid", ownedPid, "actualPid", actualPid);
            }
            Map<String, Object> out = new java.util.LinkedHashMap<>(version);
            out.put("status", "COMPLETE"); out.put("scope", "EXISTING_OWNED_RESOURCE_JMX_PRE_WINDOW");
            out.put("ownedPid", ownedPid); out.put("actualPid", actualPid); out.put("jvmStartTimeMillis", startMillis);
            out.put("inputArguments", List.copyOf(arguments)); out.put("driverStartNanos", before);
            out.put("driverEndNanos", io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime()); out.put("capturedAt", java.time.Instant.now().toString());
            out.put("aliveBefore", aliveBefore); out.put("aliveAfter", aliveAfter);
            return Map.copyOf(out);
        } catch (IOException | RuntimeException failure) {
            return Map.of("status", "UNKNOWN", "reason", "OWNED_RUNTIME_READ_FAILED", "ownedPid", ownedPid,
                    "failureType", failure.getClass().getName());
        }
    }

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
        if (child == null || !BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive)) {
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
            long millis = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, access.compiler()::getTotalCompilationTime);
            if (!BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive)) {
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
            RuntimeMXBean runtime = runtimeProxy();
            if (BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getPid) != ownedPid) {
                compilationAccess = new CompilationAccess(null, null, null,
                        CompilationUnknownReason.OWNED_PID_MISMATCH, null);
            } else {
                CompilationMXBean compiler = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ,
                        () -> ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.COMPILATION_MXBEAN_NAME, CompilationMXBean.class));
                name = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, compiler::getName);
                supported = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, compiler::isCompilationTimeMonitoringSupported);
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
                if (connection == null || child == null || !BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive)) {
                    throw new IllegalStateException("owned JVM thread readings are unavailable");
                }
            }

            private com.sun.management.ThreadMXBean threads() {
                requireConnection();
                if (threads == null) {
                    try {
                        threads = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ,
                                () -> ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.THREAD_MXBEAN_NAME, com.sun.management.ThreadMXBean.class));
                    } catch (IOException failure) { throw new UncheckedIOException(failure); }
                }
                return threads;
            }

            @Override public BenchmarkThreadPointDiagnostics.Identity identity() {
                requireConnection();
                if (runtime == null) {
                    try {
                        runtime = runtimeProxy();
                    } catch (IOException failure) { throw new UncheckedIOException(failure); }
                }
                return new BenchmarkThreadPointDiagnostics.Identity(
                        BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getPid),
                        BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, runtime::getStartTime), BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive));
            }

            @Override public BenchmarkThreadPointDiagnostics.CpuCapability cpuCapability() {
                boolean supported = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> threads().isThreadCpuTimeSupported());
                return new BenchmarkThreadPointDiagnostics.CpuCapability(supported,
                        supported ? BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> threads().isThreadCpuTimeEnabled()) : null);
            }

            @Override public long[] threadIds() { return BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> threads().getAllThreadIds()); }

            @Override public long[] cpuNanos(long[] ids) { return BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> threads().getThreadCpuTime(ids)); }

            @Override public List<BenchmarkThreadPointDiagnostics.ThreadRow> threadInfo(long[] ids, int maxDepth) {
                if (maxDepth < 0 || maxDepth > 64) { throw new IllegalArgumentException("thread point depth is outside its bound"); }
                ThreadInfo[] actual = BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> threads().getThreadInfo(ids, maxDepth));
                BenchmarkReturnWorkBudget.gap(workScope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, "THREAD_PAYLOAD_LOCAL_SHAPE_FORMULA_UNAVAILABLE");
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
        if (child == null || !BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::isAlive)) {
            return unavailable();
        }
        OptionalLong cpu = BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, child::info).totalCpuDuration().stream()
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
            com.sun.management.OperatingSystemMXBean operatingSystem = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ,
                    () -> ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, com.sun.management.OperatingSystemMXBean.class));
            long nanos = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.CPU_FALLBACK, operatingSystem::getProcessCpuTime);
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
            MemoryMXBean heap = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ,
                    () -> ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class));
            long used = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.HEAP_READ, heap::getHeapMemoryUsage).getUsed();
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
                    BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.GC_ROSTER,
                            () -> ManagementFactory.getPlatformMXBeans(connection, GarbageCollectorMXBean.class));
            BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.GC_ROSTER, BenchmarkReturnWorkBudget.Work.pass(collectors.size()));
            if (collectors.isEmpty()) {
                return OptionalLong.empty();
            }
            long total = 0;
            for (GarbageCollectorMXBean collector : collectors) {
                long elapsed = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.GC_READ, collector::getCollectionTime);
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
            command = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS,
                    () -> new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(child.pid())).redirectErrorStream(true).start());
        } catch (IOException | SecurityException e) {
            return OptionalLong.empty();
        }
        try {
            if (!BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS,
                    () -> command.waitFor(PS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))) {
                BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.CLEANUP, command::destroyForcibly);
                BenchmarkReturnWorkBudget.gap(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS, "RSS_EXIT_NOT_OBSERVED_AFTER_ABORT");
                return OptionalLong.empty();
            }
            if (BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS, command::exitValue) != 0) {
                return OptionalLong.empty();
            }
            var input = command.getInputStream();
            byte[] raw = BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS,
                    () -> workScope == null ? input.readAllBytes() : input.readNBytes(BenchmarkReturnWorkBudget.MAX_MATERIAL_LEAF_CHARACTERS + 1));
            if (workScope != null) {
                BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.CLEANUP, () -> { input.close(); return null; });
            }
            if (workScope != null && raw.length > BenchmarkReturnWorkBudget.MAX_MATERIAL_LEAF_CHARACTERS) {
                BenchmarkReturnWorkBudget.gap(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS, "RSS_LOCAL_RESPONSE_BOUND_EXCEEDED"); return OptionalLong.empty();
            }
            String output = new String(raw, StandardCharsets.UTF_8).trim();
            BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS, BenchmarkReturnWorkBudget.Work.bytes(Math.multiplyExact(4L, output.length())));
            if (!output.matches("[0-9]+")) {
                return OptionalLong.empty();
            }
            long kib = Long.parseLong(output);
            return kib > 0 ? OptionalLong.of(Math.multiplyExact(kib, BYTES_PER_KIB)) : OptionalLong.empty();
        } catch (IOException | NumberFormatException | ArithmeticException e) {
            return OptionalLong.empty();
        } catch (InterruptedException e) {
            BenchmarkReturnWorkBudget.opaqueRequest(workScope, BenchmarkReturnWorkBudget.Activity.CLEANUP, command::destroyForcibly);
            BenchmarkReturnWorkBudget.gap(workScope, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS, "RSS_EXIT_NOT_OBSERVED_AFTER_ABORT");
            Thread.currentThread().interrupt();
            return OptionalLong.empty();
        }
    }

    private RuntimeMXBean runtimeProxy() throws IOException {
        return BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ,
                () -> ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class));
    }

    private static Snapshot unavailable() {
        return new Snapshot(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty());
    }

    @Override
    public void close() {
        closeQuietly(jmx, workScope);
    }

    private static void closeQuietly(JMXConnector connector, BenchmarkReturnWorkBudget.Scope scope) {
        if (connector != null) {
            try {
                BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.CLEANUP, () -> { connector.close(); return null; });
            } catch (IOException ignored) {
                // Connection loss only makes subsequent readings unavailable.
            }
        }
    }
}
