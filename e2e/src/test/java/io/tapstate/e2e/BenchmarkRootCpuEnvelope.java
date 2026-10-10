package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/** Root process CPU accounting facts; counter precision supplies no accounting accuracy bound. */
final class BenchmarkRootCpuEnvelope {
    static final int MAX_OPERATIONS = 4;
    static final int MAX_EVIDENCE_BYTES = 4096;
    static final Duration WAIT_LIMIT = Duration.ofSeconds(5);

    enum Cutoff {
        BEFORE_COLLECTION,
        AFTER_ACK_COMMON_CHECKPOINTS,
        AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL
    }

    record ProviderIdentity(String className, String methodName, String nativeLibraryName, String nativeLibrarySha256) {
        ProviderIdentity {
            text(className, 96); text(methodName, 64); text(nativeLibraryName, 32);
            if (nativeLibrarySha256 == null || !(nativeLibrarySha256.equals("UNAVAILABLE")
                    || nativeLibrarySha256.matches("[a-f0-9]{64}"))) {
                throw new AssertionError("root CPU provider fingerprint is invalid");
            }
        }
    }

    record Identity(long pid, long jvmStartTimeMillis, boolean alive, ProviderIdentity provider) { }

    interface Provider {
        Identity identity();
        long processCpuTime();
    }

    @FunctionalInterface
    interface Waiter {
        void await(CompletableFuture<Void> completion, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException;
    }

    static final class Ticket {
        private final BenchmarkRootCpuEnvelope owner;
        private final String name;
        private final long startedAtNanos;
        private final CompletableFuture<Void> done = new CompletableFuture<>();
        private Long completedAtNanos;
        private boolean completionAttempted;

        private Ticket(BenchmarkRootCpuEnvelope owner, String name, long startedAtNanos) {
            this.owner = owner; this.name = name; this.startedAtNanos = startedAtNanos;
        }
    }

    static final class Refusal extends AssertionError {
        private final Map<String, Object> retainedEvidence;

        private Refusal(String reason, Throwable cause, Map<String, Object> retainedEvidence) {
            super("root CPU envelope refused: " + reason, cause);
            this.retainedEvidence = retainedEvidence;
        }

        Map<String, Object> retainedEvidence() { return retainedEvidence; }
    }

    private final Object lifecycle = new Object();
    private final Identity expected;
    private final Provider provider;
    private final LongSupplier clock;
    private final Waiter waiter;
    private final Set<String> common;
    private final Set<String> roster;
    private final Map<String, Ticket> operations = new LinkedHashMap<>();
    private final List<ProviderIdentity> providers = new ArrayList<>();
    private final List<Map<String, Object>> readings = new ArrayList<>();
    private final Set<Cutoff> attempted = new LinkedHashSet<>();
    private Map<String, Object> pending;
    private volatile Map<String, Object> published;
    private Throwable primary;
    private String unknownReason;
    private boolean busy;
    private boolean finalRequested;

    static BenchmarkRootCpuEnvelope open(Set<String> commonOperations, Set<String> tailOperations) {
        var bean = ManagementFactory.getOperatingSystemMXBean();
        var runtime = ManagementFactory.getRuntimeMXBean();
        ProviderIdentity descriptor = nativeProvider(bean.getClass().getName());
        long pid = ProcessHandle.current().pid(), start = runtime.getStartTime();
        Provider source = new Provider() {
            @Override public Identity identity() {
                return new Identity(ProcessHandle.current().pid(), runtime.getStartTime(), ProcessHandle.current().isAlive(), descriptor);
            }
            @Override public long processCpuTime() {
                return bean instanceof com.sun.management.OperatingSystemMXBean cpu ? cpu.getProcessCpuTime() : -1;
            }
        };
        return from(new Identity(pid, start, true, descriptor), source,
                io.tapstate.adapters.pdk.PdkBenchmarkClock::nanoTime, commonOperations, tailOperations,
                (completion, timeout, unit) -> completion.get(timeout, unit));
    }

    static BenchmarkRootCpuEnvelope from(Identity expected, Provider provider, LongSupplier clock,
            Set<String> commonOperations, Set<String> tailOperations, Waiter waiter) {
        return new BenchmarkRootCpuEnvelope(expected, provider, clock, commonOperations, tailOperations, waiter);
    }

    private BenchmarkRootCpuEnvelope(Identity expected, Provider provider, LongSupplier clock,
            Set<String> commonOperations, Set<String> tailOperations, Waiter waiter) {
        if (expected == null || expected.pid() <= 0 || expected.jvmStartTimeMillis() <= 0
                || !expected.alive() || expected.provider() == null) {
            throw new AssertionError("root CPU envelope needs a fixed live root identity");
        }
        this.expected = expected; this.provider = Objects.requireNonNull(provider); this.clock = Objects.requireNonNull(clock);
        this.waiter = Objects.requireNonNull(waiter);
        common = names(commonOperations); Set<String> tail = names(tailOperations);
        var all = new LinkedHashSet<>(common);
        for (String name : tail) { if (!all.add(name)) { throw new AssertionError("root CPU operation scopes overlap"); } }
        if (all.size() > MAX_OPERATIONS) { throw new AssertionError("root CPU operation roster exceeds its finite budget"); }
        roster = Collections.unmodifiableSet(all); providers.add(expected.provider()); publish();
    }

    Ticket begin(String name) {
        synchronized (lifecycle) { requireBegin(name); }
        long started;
        try { started = clock.getAsLong(); }
        catch (RuntimeException | Error failure) {
            synchronized (lifecycle) { throw refuse("OPERATION_START_CLOCK_UNAVAILABLE", failure); }
        }
        synchronized (lifecycle) {
            requireBegin(name);
            try { ordered((long) readings.getLast().get("completedAtNanos"), started); }
            catch (RuntimeException | Error invalid) { throw refuse("OPERATION_START_CLOCK_ORDER", invalid); }
            Ticket ticket = new Ticket(this, name, started); operations.put(name, ticket); publish(); return ticket;
        }
    }

    private void requireBegin(String name) {
        requireOpen();
        if (readings.isEmpty() || finalRequested || !roster.contains(name) || operations.containsKey(name)
                || (!common.contains(name) && readings.size() < 2)
                || (common.contains(name) && attempted.contains(Cutoff.AFTER_ACK_COMMON_CHECKPOINTS))) {
            throw refuse("OPERATION_ORDER_OR_ROSTER", new AssertionError("root CPU operation is unregistered, repeated or outside its scope"));
        }
    }

    void complete(Ticket ticket) {
        synchronized (lifecycle) {
            if (ticket == null || ticket.owner != this || operations.get(ticket.name) != ticket || ticket.completionAttempted) {
                throw refuse("OPERATION_COMPLETION_IDENTITY", new AssertionError("root CPU operation completion is foreign or repeated"));
            }
            ticket.completionAttempted = true;
        }
        long completed;
        try { completed = clock.getAsLong(); }
        catch (RuntimeException | Error failure) {
            synchronized (lifecycle) { throw refuse("OPERATION_COMPLETION_CLOCK_UNAVAILABLE", failure); }
        }
        synchronized (lifecycle) {
            ticket.completedAtNanos = completed;
            try { ordered(ticket.startedAtNanos, completed); }
            catch (RuntimeException | Error invalid) { throw refuse("OPERATION_CLOCK_ORDER", invalid); }
            publish(); ticket.done.complete(null);
        }
    }

    void read(Cutoff cutoff) {
        // This deadline clock only bounds caller waiting; it is not a measurement-domain point.
        long waitStartedAt = System.nanoTime();
        CompletableFuture<Void> completion;
        synchronized (lifecycle) {
            requireOpen();
            if (cutoff == null || busy || cutoff.ordinal() != readings.size() || !attempted.add(cutoff)) {
                throw refuse("CUTOFF_ORDER", new AssertionError("root CPU cutoffs must be read once in fixed order"));
            }
            busy = true; finalRequested = cutoff == Cutoff.AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL;
            Set<String> required = cutoff == Cutoff.BEFORE_COLLECTION ? Set.of() : finalRequested ? roster : common;
            pending = immutable(Map.of("cutoff", cutoff.name(), "state", "REQUESTED", "waitStage", "OPERATION_COMPLETION")); publish();
            for (String name : required) {
                if (!operations.containsKey(name)) {
                    busy = false;
                    throw refuse("OPERATION_NOT_STARTED", new AssertionError("root CPU cutoff has an unstarted registered operation"));
                }
            }
            completion = CompletableFuture.allOf(required.stream().map(name -> operations.get(name).done).toArray(CompletableFuture[]::new));
        }
        try {
            awaitBounded(completion, waitStartedAt);
            updatePending("waitStage", "CPU_READ", "CPU_WORKER_REQUESTED");
            var readDone = new CompletableFuture<Void>();
            Thread.ofPlatform().daemon(true).name("benchmark-root-cpu-reader").start(() -> {
                try { cpuRead(cutoff); readDone.complete(null); }
                catch (RuntimeException | Error failure) {
                    synchronized (lifecycle) { busy = false; readDone.completeExceptionally(refuse("CPU_READ_UNAVAILABLE", failure)); }
                }
            });
            awaitBounded(readDone, waitStartedAt);
            synchronized (lifecycle) { requireOpen(); busy = false; publish(); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            synchronized (lifecycle) { busy = false; throw refuse("COMPLETION_WAIT_INTERRUPTED", interrupted); }
        } catch (ExecutionException failed) {
            synchronized (lifecycle) {
                busy = false;
                if (failed.getCause() instanceof Refusal refusal) { throw refusal; }
                throw refuse("COMPLETION_WAIT_UNAVAILABLE", failed.getCause());
            }
        } catch (TimeoutException failure) {
            synchronized (lifecycle) { busy = false; throw refuse("COMPLETION_WAIT_UNAVAILABLE", failure); }
        } catch (RuntimeException | Error failure) {
            synchronized (lifecycle) {
                busy = false;
                if (failure instanceof Refusal refusal) { throw refusal; }
                throw refuse("READ_OR_IDENTITY_UNAVAILABLE", failure);
            }
        }
    }

    private void awaitBounded(CompletableFuture<Void> completion, long waitStartedAt)
            throws InterruptedException, ExecutionException, TimeoutException {
        long remaining = WAIT_LIMIT.toNanos() - (System.nanoTime() - waitStartedAt);
        if (remaining <= 0) { throw new TimeoutException("root CPU envelope caller deadline exhausted"); }
        if (!completion.isDone()) { waiter.await(completion, remaining, TimeUnit.NANOSECONDS); }
        if (!completion.isDone()) { throw new AssertionError("root CPU waiter returned while its stage remained unfinished"); }
        completion.get();
    }

    private void cpuRead(Cutoff cutoff) {
        long started = clock.getAsLong();
        updatePending("startedAtNanos", started, "READING_CPU");
        synchronized (lifecycle) {
            Set<String> required = cutoff == Cutoff.BEFORE_COLLECTION ? Set.of()
                    : cutoff == Cutoff.AFTER_ACK_COMMON_CHECKPOINTS ? common : roster;
            for (String name : required) { ordered(operations.get(name).completedAtNanos, started); }
        }
        Identity before = provider.identity(); updatePending("identityBefore", identity(before), "READING_CPU"); same(before);
        long cpu = provider.processCpuTime(); updatePending("processCpuTimeNanos", cpu, "READING_CPU");
        Identity after = provider.identity(); updatePending("identityAfter", identity(after), "READING_CPU");
        long completed = clock.getAsLong();
        synchronized (lifecycle) {
            var reading = new LinkedHashMap<>(pending); reading.put("completedAtNanos", completed); reading.put("state", "RETURNED_UNVALIDATED");
            readings.add(immutable(reading)); pending = null; publish();
            same(after); ordered(started, completed);
            if (cpu < 0) { throw new AssertionError("root CPU counter is unsupported or unavailable"); }
            if (readings.size() > 1) {
                var prior = readings.get(readings.size() - 2); ordered((long) prior.get("completedAtNanos"), started);
                if (cpu < (long) prior.get("processCpuTimeNanos")) { throw new AssertionError("root CPU counter moved backward"); }
            }
            reading.put("state", "RECORDED_DIAGNOSTIC"); readings.set(readings.size() - 1, immutable(reading)); publish();
        }
    }

    Map<String, Object> evidence() { return published; }

    private void updatePending(String key, Object value, String state) {
        synchronized (lifecycle) {
            var update = new LinkedHashMap<>(pending); update.put(key, value); update.put("state", state);
            pending = immutable(update); publish();
        }
    }

    private Map<String, Object> identity(Identity actual) {
        synchronized (lifecycle) {
            if (actual == null || actual.provider() == null) { return Map.of("state", "UNAVAILABLE"); }
            int index = providers.indexOf(actual.provider());
            if (index < 0) { providers.add(actual.provider()); index = providers.size() - 1; }
            return Map.of("pid", actual.pid(), "jvmStartTimeMillis", actual.jvmStartTimeMillis(), "alive", actual.alive(), "providerIndex", index);
        }
    }

    private void same(Identity actual) {
        if (!expected.equals(actual)) { throw new AssertionError("root CPU runtime or provider identity changed"); }
    }

    private void requireOpen() {
        if (primary != null) { throw new Refusal(unknownReason, primary, published); }
    }

    private Refusal refuse(String reason, Throwable failure) {
        if (primary == null) { primary = failure; unknownReason = reason; }
        publish(); return new Refusal(unknownReason, primary, published);
    }

    private void publish() {
        var result = new LinkedHashMap<String, Object>();
        result.put("state", primary != null ? "UNKNOWN" : readings.size() == 3 ? "RECORDED_DIAGNOSTIC" : "PENDING");
        result.put("scope", "PARTIAL_ROOT_PROCESS_CPU_ACCOUNTING_ONLY");
        result.put("expectedRoot", identity(expected));
        result.put("providers", providers.stream().map(value -> Map.<String, Object>of("className", value.className(),
                "methodName", value.methodName(), "nativeLibraryName", value.nativeLibraryName(), "nativeLibrarySha256", value.nativeLibrarySha256())).toList());
        result.put("providerFingerprintScope", "CURRENT_JDK_FILE_AT_CONSTRUCTION_NOT_LOADED_CODE_OR_ACCURACY_PROOF");
        result.put("counterUnit", "ns"); result.put("counterUnitScope", "PROVIDER_PRECISION_NOT_ACCOUNTING_ACCURACY");
        result.put("commonOperations", List.copyOf(common)); result.put("tailOperations", roster.stream().filter(name -> !common.contains(name)).toList());
        result.put("readings", List.copyOf(readings));
        result.put("operations", roster.stream().map(name -> {
            var row = new LinkedHashMap<String, Object>(); row.put("name", name); Ticket actual = operations.get(name);
            row.put("state", actual == null ? "NOT_STARTED" : actual.completedAtNanos == null ? "PENDING" : "COMPLETED");
            if (actual != null) { row.put("startedAtNanos", actual.startedAtNanos); }
            if (actual != null && actual.completedAtNanos != null) { row.put("completedAtNanos", actual.completedAtNanos); }
            return immutable(row);
        }).toList());
        if (pending != null) { result.put("pendingRead", pending); }
        var deltas = new LinkedHashMap<String, Object>();
        if (!readings.isEmpty()) {
            long baseline = (long) readings.getFirst().get("processCpuTimeNanos");
            for (var reading : readings) {
                long current = (long) reading.get("processCpuTimeNanos");
                if (baseline >= 0 && current >= 0) { deltas.put((String) reading.get("cutoff"), current - baseline); }
            }
        }
        result.put("recordedCounterDeltasNanos", immutable(deltas));
        result.put("accountingErrorAllowance", Map.of("state", "UNKNOWN", "reason", "FINITE_ACCOUNTING_ERROR_BOUND_UNAVAILABLE"));
        result.put("collectionCpuUpperBound", Map.of("state", "UNKNOWN", "reason", "ACCOUNTING_ALLOWANCE_AND_OTHER_PROCESS_COVERAGE_UNAVAILABLE"));
        for (String flag : List.of("accountingErrorBoundQualified", "wholeMethodCostQualified", "collectionCostUpperBoundQualified",
                "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
            result.put(flag, false);
        }
        if (primary != null) { result.put("reason", unknownReason); result.put("failureType", boundedType(primary)); }
        Map<String, Object> snapshot = immutable(result);
        if (JsonWriter.write(snapshot).getBytes(StandardCharsets.UTF_8).length > MAX_EVIDENCE_BYTES) {
            throw new AssertionError("root CPU evidence exceeded its byte budget");
        }
        published = snapshot;
    }

    private static ProviderIdentity nativeProvider(String className) {
        String name = System.mapLibraryName("management_ext"), fingerprint = "UNAVAILABLE";
        if (name.length() <= 32) {
            try (var input = Files.newInputStream(Path.of(System.getProperty("java.home"), "lib", name))) {
                byte[] bytes = input.readNBytes(4 * 1024 * 1024 + 1);
                if (bytes.length <= 4 * 1024 * 1024) { fingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
            } catch (Exception unavailable) { fingerprint = "UNAVAILABLE"; }
        } else { name = "UNAVAILABLE"; }
        return new ProviderIdentity(className, "getProcessCpuTime", name, fingerprint);
    }

    private static Set<String> names(Set<String> values) {
        Objects.requireNonNull(values);
        if (values.size() > MAX_OPERATIONS) { throw new AssertionError("root CPU operation roster exceeds its finite budget"); }
        var result = new LinkedHashSet<String>();
        for (String name : values.stream().sorted().toList()) { text(name, 32); result.add(name); }
        return Collections.unmodifiableSet(result);
    }

    private static void text(String value, int maximum) {
        if (value == null || value.isBlank() || value.length() > maximum || !value.matches("[A-Za-z0-9_.$-]+")) {
            throw new AssertionError("root CPU identity or operation text is invalid");
        }
    }

    private static void ordered(long first, long second) {
        if (Math.subtractExact(second, first) < 0) { throw new AssertionError("root CPU read or operation clock moved backward"); }
    }

    private static String boundedType(Throwable failure) {
        String name = failure.getClass().getName();
        return name.length() <= 96 ? name : "TYPE_NAME_EXCEEDS_BOUND";
    }

    private static Map<String, Object> immutable(Map<String, Object> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
