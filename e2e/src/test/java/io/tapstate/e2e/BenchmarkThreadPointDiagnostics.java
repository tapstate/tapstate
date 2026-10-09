package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Four caller-driven positive stack points, with no claim of capture coverage or exclusive CPU attribution. */
final class BenchmarkThreadPointDiagnostics {
    static final List<Long> POINT_ORDINALS = List.of(28L, 33L, 45L, 50L);
    interface Reader {
        long ownedPid();
        Identity identity();
        CpuCapability cpuCapability();
        long[] threadIds();
        List<ThreadRow> threadInfo(long[] ids, int maxDepth);
        long[] cpuNanos(long[] ids);
    }
    record Identity(long pid, long startTimeMillis, boolean alive) { }
    record CpuCapability(boolean supported, Boolean enabled) { }
    record Frame(String className, String methodName, boolean nativeMethod, int lineNumber) { }
    record ThreadRow(long id, String name, String state, List<Frame> frames) { }

    private final Reader reader;
    private final LongSupplier clock;
    private final long ownedPid;
    private final Object lifecycle = new Object();
    private final Set<Long> attempted = new LinkedHashSet<>();
    private final List<Map<String, Object>> points = new ArrayList<>();
    private final Map<Long, Long> previousCpu = new LinkedHashMap<>();
    private final Map<Long, String> candidates = new LinkedHashMap<>();
    private volatile Map<String, Object> published;
    private Map<String, Object> preparation = Map.of("state", "NOT_RECORDED");
    private Identity baseline;
    private boolean ready;
    private boolean busy;
    private List<Map<String, Object>> openReads = List.of();

    private BenchmarkThreadPointDiagnostics(Reader reader, LongSupplier clock) {
        this.reader = Objects.requireNonNull(reader, "owned thread reader");
        this.clock = Objects.requireNonNull(clock, "thread point clock");
        ownedPid = reader.ownedPid();
        if (ownedPid <= 0) { throw new IllegalArgumentException("thread diagnostics need an owned PID"); }
        publish(null);
    }

    /** Construction performs no remote reads; resource ordinal one prepares exactly once before source issue. */
    static BenchmarkThreadPointDiagnostics prepare(Reader reader, LongSupplier clock) {
        return new BenchmarkThreadPointDiagnostics(reader, clock);
    }

    void recordAttempt(long ordinal) {
        if (ordinal != 1 && !POINT_ORDINALS.contains(ordinal)) { return; }
        synchronized (lifecycle) {
            if (attempted.contains(ordinal)) { return; }
            if (busy) { throw new IllegalStateException("owned thread read is already pending"); }
            attempted.add(ordinal);
            busy = true;
            openReads = List.of();
        }
        var result = new LinkedHashMap<String, Object>();
        var reads = new ArrayList<Map<String, Object>>();
        result.put("resourceAttemptOrdinal", ordinal);
        Error invariant = null;
        try {
            if (ordinal == 1) { prepareOnce(result, reads); }
            else if (!ready) { result.put("unknownReason", "PREPARATION_NOT_QUALIFIED"); }
            else { point(ordinal, result, reads); }
            result.put("state", result.containsKey("unknownReason") ? "UNKNOWN" : "RECORDED");
        } catch (Unknown unknown) {
            result.put("state", "UNKNOWN"); result.put("unknownReason", unknown.reason.name());
            previousCpu.clear();
        } catch (RuntimeException failure) {
            result.put("state", "UNKNOWN"); result.put("unknownReason", "RPC_ERROR");
            result.put("failureType", failure.getClass().getName()); previousCpu.clear();
        } catch (Error failure) {
            result.put("state", "UNKNOWN"); result.put("unknownReason", "INVARIANT_ERROR");
            result.put("failureType", failure.getClass().getName()); previousCpu.clear(); invariant = failure;
        } finally {
            result.put("reads", List.copyOf(reads));
            long total = 0, maximum = 0;
            int knownDurations = 0;
            try {
                for (var read : reads) {
                    if (read.get("durationNanos") instanceof Long duration) {
                        total = Math.addExact(total, duration);
                        maximum = Math.max(maximum, duration);
                        knownDurations++;
                    }
                }
                result.put("qualifiedDurationReadCount", knownDurations);
                result.put("missingDurationReadCount", reads.size() - knownDurations);
                if (reads.isEmpty()) {
                    result.put("readDurationState", "NOT_RECORDED");
                } else if (knownDurations == reads.size()) {
                    result.put("readDurationState", "RECORDED");
                    result.put("totalReadDurationNanos", total);
                    result.put("maxReadDurationNanos", maximum);
                } else {
                    result.put("readDurationState", "UNKNOWN");
                }
            } catch (ArithmeticException overflow) {
                result.put("state", "UNKNOWN"); result.put("unknownReason", "OVERFLOW");
                result.put("readDurationState", "UNKNOWN");
            }
            synchronized (lifecycle) {
                if (ordinal == 1) { preparation = immutable(result); }
                else { points.add(immutable(result)); }
                busy = false;
                publish(null);
            }
        }
        if (invariant != null) { throw invariant; }
    }

    private void prepareOnce(Map<String, Object> result, List<Map<String, Object>> reads) {
        Identity identity = rpc(1, "IDENTITY", reader::identity, reads);
        result.put("identity", identityEvidence(identity));
        require(identity != null && identity.alive() && identity.pid() == ownedPid
                && identity.startTimeMillis() > 0, Reason.IDENTITY_MISMATCH);
        CpuCapability capability = rpc(1, "CPU_CAPABILITY", reader::cpuCapability, reads);
        require(capability != null, Reason.MALFORMED_CAPABILITY);
        result.put("cpuSupported", capability.supported());
        if (capability.enabled() != null) { result.put("cpuEnabled", capability.enabled()); }
        require(capability.supported(), Reason.CPU_UNSUPPORTED);
        require(Boolean.TRUE.equals(capability.enabled()), Reason.CPU_DISABLED);
        long[] ids = rpc(1, "SHALLOW_THREAD_IDS", reader::threadIds, reads);
        require(ids != null && ids.length <= 512, Reason.THREAD_ROSTER_BOUND);
        var unique = new LinkedHashSet<Long>();
        for (long id : ids) { require(id > 0 && unique.add(id), Reason.MALFORMED_ROSTER); }
        result.put("discoveredPlatformThreads", ids.length);
        List<ThreadRow> rows = rpc(1, "SHALLOW_THREAD_INFO", () -> reader.threadInfo(ids.clone(), 0), reads);
        require(rows != null && rows.size() == ids.length, Reason.MALFORMED_ROSTER);
        int missing = 0;
        for (int i = 0; i < ids.length; i++) {
            ThreadRow row = rows.get(i);
            if (row == null) { missing++; continue; }
            require(row.id() == ids[i] && text(row.name(), 256) && validState(row.state())
                    && row.frames() != null && row.frames().isEmpty(), Reason.MALFORMED_ROSTER);
            if (hint(row.name())) { candidates.put(row.id(), row.name()); }
        }
        result.put("missingShallowThreadInfo", missing);
        require(candidates.size() <= 8, Reason.CANDIDATE_BOUND);
        require(!candidates.isEmpty(), Reason.NO_CANDIDATES);
        result.put("selectedThreadHints", candidates.entrySet().stream()
                .map(entry -> Map.of("id", entry.getKey(), "name", entry.getValue())).toList());
        Identity after = rpc(1, "IDENTITY_AFTER", reader::identity, reads);
        result.put("identityAfter", identityEvidence(after));
        require(same(identity, after), Reason.IDENTITY_CHANGED);
        baseline = identity;
        ready = true;
    }

    private void point(long ordinal, Map<String, Object> result, List<Map<String, Object>> reads) {
        Identity before = rpc(ordinal, "IDENTITY_BEFORE", reader::identity, reads);
        result.put("identityBefore", identityEvidence(before));
        require(same(baseline, before), Reason.IDENTITY_CHANGED);
        CpuCapability capability = rpc(ordinal, "CPU_CAPABILITY", reader::cpuCapability, reads);
        require(capability != null && capability.supported() && Boolean.TRUE.equals(capability.enabled()), Reason.CPU_DISABLED);
        long[] ids = candidates.keySet().stream().mapToLong(Long::longValue).toArray();
        long[] cpu = rpc(ordinal, "BULK_CPU", () -> reader.cpuNanos(ids.clone()), reads);
        require(cpu != null && cpu.length == ids.length, Reason.MALFORMED_CPU);
        result.put("rawCpuNanos", java.util.Arrays.stream(cpu).boxed().toList());
        List<ThreadRow> rows = rpc(ordinal, "STACK_POINT", () -> reader.threadInfo(ids.clone(), 64), reads);
        require(rows != null && rows.size() == ids.length, Reason.MALFORMED_STACK);
        var actual = new ArrayList<Map<String, Object>>();
        var nextCpu = new LinkedHashMap<Long, Long>();
        Reason unknown = null;
        for (int i = 0; i < ids.length; i++) {
            ThreadRow row = rows.get(i);
            var value = new LinkedHashMap<String, Object>();
            value.put("id", ids[i]); value.put("rawCpuNanos", cpu[i]);
            Reason invalid = row == null ? Reason.THREAD_INFO_MISSING : null;
            if (row != null) {
                require(row.id() == ids[i] && text(row.name(), 256) && validState(row.state()), Reason.MALFORMED_STACK);
                value.put("name", row.name()); value.put("threadState", row.state());
                require(row.frames() != null && row.frames().size() <= 64, Reason.FRAME_BOUND);
                var frames = new ArrayList<Map<String, Object>>();
                for (Frame frame : row.frames()) {
                    require(frame != null && text(frame.className(), 512) && text(frame.methodName(), 256), Reason.MALFORMED_STACK);
                    frames.add(Map.of("className", frame.className(), "methodName", frame.methodName(),
                            "nativeMethod", frame.nativeMethod(), "lineNumber", frame.lineNumber()));
                }
                value.put("frames", List.copyOf(frames)); value.put("depthBoundReached", row.frames().size() == 64);
                value.put("role", role(row.frames()));
                if (!row.name().equals(candidates.get(ids[i]))) { invalid = Reason.THREAD_NAME_CHANGED; }
            }
            if (cpu[i] < 0) { invalid = Reason.CPU_UNAVAILABLE; }
            Long previous = previousCpu.get(ids[i]);
            if (invalid == null && previous != null) {
                if (cpu[i] < previous) { invalid = Reason.CPU_DECREASED; }
                else { value.put("conditionalCpuDeltaNanos", Math.subtractExact(cpu[i], previous)); }
            }
            if (invalid == null) { nextCpu.put(ids[i], cpu[i]); }
            else { if (unknown == null) { unknown = invalid; } value.put("unknownReason", invalid.name()); }
            value.put("state", invalid == null ? "RECORDED" : "UNKNOWN");
            actual.add(immutable(value));
        }
        result.put("threads", List.copyOf(actual));
        Identity after = rpc(ordinal, "IDENTITY_AFTER", reader::identity, reads);
        result.put("identityAfter", identityEvidence(after));
        require(same(baseline, after), Reason.IDENTITY_CHANGED);
        previousCpu.clear();
        if (unknown == null) { previousCpu.putAll(nextCpu); }
        else { result.put("unknownReason", unknown.name()); }
    }

    private <T> T rpc(long ordinal, String operation, Supplier<T> source, List<Map<String, Object>> reads) {
        long started = clock.getAsLong();
        synchronized (lifecycle) { publish(Map.of("resourceAttemptOrdinal", ordinal, "operation", operation, "startedAtNanos", started)); }
        T value = null;
        Throwable failure = null;
        try { value = source.get(); } catch (RuntimeException | Error caught) { failure = caught; }
        var fact = new LinkedHashMap<String, Object>();
        fact.put("operation", operation); fact.put("startedAtNanos", started);
        Long completed = null;
        try {
            completed = clock.getAsLong();
        } catch (RuntimeException | Error clockFailure) {
            fact.put("clockUnknownReason", "COMPLETION_CLOCK_UNAVAILABLE");
            fact.put("clockFailureType", clockFailure.getClass().getName());
            if (!(failure instanceof Error) && (clockFailure instanceof Error || failure == null)) {
                failure = clockFailure;
            }
        }
        if (completed != null) {
            fact.put("completedAtNanos", completed);
            try {
                long duration = Math.subtractExact(completed, started);
                require(duration >= 0, Reason.CLOCK_BACKWARD);
                fact.put("durationNanos", duration);
            } catch (ArithmeticException overflow) {
                fact.put("clockUnknownReason", "OVERFLOW");
                if (!(failure instanceof Error)) { failure = new Unknown(Reason.OVERFLOW); }
            } catch (Unknown invalidClock) {
                fact.put("clockUnknownReason", invalidClock.reason.name());
                if (!(failure instanceof Error)) { failure = invalidClock; }
            }
        }
        fact.put("state", failure == null ? "RECORDED" : "UNKNOWN");
        if (failure != null) { fact.put("failureType", failure.getClass().getName()); }
        reads.add(immutable(fact));
        synchronized (lifecycle) { openReads = List.copyOf(reads); publish(null); }
        if (failure instanceof Error error) { throw error; }
        if (failure instanceof RuntimeException error) { throw error; }
        return value;
    }

    Map<String, Object> evidence() { return published; }
    Map<String, Object> wireEvidence() { return published; }

    private void publish(Map<String, Object> pending) {
        var result = new LinkedHashMap<String, Object>();
        result.put("ownedPid", ownedPid); result.put("preparation", preparation);
        result.put("fixedResourceAttemptOrdinals", POINT_ORDINALS); result.put("pointGroups", List.copyOf(points));
        result.put("attemptedResourceOrdinals", List.copyOf(attempted));
        result.put("maximumPointGroups", 4); result.put("maximumSelectedThreads", 8); result.put("maximumFramesPerThread", 64);
        result.put("scope", "OWNED_PLATFORM_THREAD_POSITIVE_POINTS_ONLY"); result.put("roleCoverage", "UNKNOWN");
        result.put("clockScope", "INDEPENDENT_DRIVER_MONOTONIC_RPC_BRACKETS_NOT_EXECUTION_TIMESTAMPS");
        result.put("cpuDeltaScope", "CONDITIONAL_OBSERVED_ID_NAME_NOT_PROVEN_LIFESPAN_OR_ROLE_EXCLUSIVITY");
        result.put("performanceAcceptanceEligible", false); result.put("mayUseAbsenceToExcludeCause", false);
        if (busy) { result.put("openGroupCompletedReads", openReads); }
        if (pending != null) { result.put("pending", pending); }
        published = immutable(result);
    }

    private static boolean hint(String name) {
        return name.equals("tapstate-cdc-mysql") || name.matches("hz\\..+\\.jet\\.blocking\\.thread-[0-9]+");
    }
    private static boolean text(String value, int max) { return value != null && !value.isBlank() && value.length() <= max; }
    private static boolean validState(String state) {
        try { return state != null && Thread.State.valueOf(state) != null; }
        catch (IllegalArgumentException invalid) { return false; }
    }
    private static boolean same(Identity expected, Identity actual) {
        return actual != null && actual.alive() && expected != null && expected.pid() == actual.pid()
                && expected.startTimeMillis() == actual.startTimeMillis();
    }
    private static Map<String, Object> identityEvidence(Identity identity) {
        return identity == null ? Map.of("state", "UNKNOWN") : Map.of("pid", identity.pid(),
                "startTimeMillis", identity.startTimeMillis(), "alive", identity.alive());
    }
    private static String role(List<Frame> frames) {
        if (has(frames, "io.tapstate.runtime.srs.SrsRingReader", "fill")) {
            if (has(frames, "io.tapstate.adapters.mongostore.MongoSrsMetaStore", "advanceConsumerReadSeq")) { return "SOURCE_CURSOR_PUBLICATION"; }
            return "SOURCE_RING_READER";
        }
        if (has(frames, "io.tapstate.runtime.srs.CdcPhase", "admit")) { return "CAPTURE_ADMISSION"; }
        boolean handover = has(frames, "io.tapstate.adapters.pdk.PdkCapturePort$CdcDelivery", "accept");
        if (handover && has(frames, "io.tapstate.adapters.mongostore.MongoSrsMetaStore", "advanceCaptureCheckpoint")) { return "CAPTURE_CHECKPOINT"; }
        if (handover && has(frames, "io.tapstate.adapters.pdk.PdkCapturePort", "handOver")) { return "CAPTURE_HANDOVER"; }
        if (has(frames, "io.tapstate.adapters.pdk.PdkCapturePort$CdcDelivery", "acknowledge")) { return "CAPTURE_OFFSET_FLUSH"; }
        if (has(frames, "io.tapstate.runtime.srs.SrsSourceProcessor", "complete")) { return "SOURCE_PROCESSOR"; }
        return "UNKNOWN";
    }
    private static boolean has(List<Frame> frames, String type, String method) {
        return frames.stream().anyMatch(frame -> frame.className().equals(type) && frame.methodName().equals(method));
    }
    private static Map<String, Object> immutable(Map<String, Object> map) { return Collections.unmodifiableMap(new LinkedHashMap<>(map)); }
    private static void require(boolean valid, Reason reason) { if (!valid) { throw new Unknown(reason); } }
    private enum Reason {
        IDENTITY_MISMATCH, IDENTITY_CHANGED, MALFORMED_CAPABILITY, CPU_UNSUPPORTED, CPU_DISABLED,
        THREAD_ROSTER_BOUND, MALFORMED_ROSTER, CANDIDATE_BOUND, NO_CANDIDATES, MALFORMED_CPU,
        MALFORMED_STACK, FRAME_BOUND, THREAD_INFO_MISSING, THREAD_NAME_CHANGED, CPU_UNAVAILABLE,
        CPU_DECREASED, CLOCK_BACKWARD, OVERFLOW
    }
    private static final class Unknown extends RuntimeException {
        private final Reason reason;
        private Unknown(Reason reason) { super(reason.name()); this.reason = reason; }
    }
}
