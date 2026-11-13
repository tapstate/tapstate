package io.tapstate.adapters.mongostore;

import com.mongodb.event.CommandStartedEvent;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Optional synchronous request diagnostics; never retains command bodies, URIs or credentials. */
public final class MongoHttpDiagnosticsScope implements AutoCloseable {
    public enum Phase {
        REQUEST_OTHER, AUTH_ENTRY, AUTH_ATTEMPT, AUTH_VERIFY, AUTH_REVALIDATE, CONTEXT_SELECT, CONTINUATION_WAIT,
        SDK_EXCHANGE, SDK_VERIFY, SDK_CALLBACK, SDK_STATUS, SDK_EGRESS, SDK_CONFIG_CLAIM, SDK_CONFIG_COMPLETE,
        PIPELINE_ELIGIBILITY, PIPELINE_SCAN, PIPELINE_PASS, PIPELINE_INTENT, PIPELINE_PREPARATION, PIPELINE_CLAIM, PIPELINE_CONVERGE,
        PIPELINE_START, PIPELINE_DAG_PREPARE, PIPELINE_CONNECTORS, PIPELINE_STOP_PREVIOUS, PIPELINE_EXECUTION_CLAIM,
        PIPELINE_OPERATOR_DATABASES, PIPELINE_OPERATOR_MAPS, PIPELINE_CAPTURE_START, PIPELINE_CAPTURE_BIND,
        PIPELINE_DAG_BUILD, PIPELINE_PLAN_RECORD, PIPELINE_JET_SUBMIT, PIPELINE_PUBLISH, PIPELINE_SAMPLE,
        PIPELINE_ALERTS, PIPELINE_EXPORT, PIPELINE_RETAIN, PIPELINE_LIFECYCLE_READ, PIPELINE_TARGET_CONFIRM,
        CAPTURE_SOURCE_PREPARE, CAPTURE_SOURCE_START,
        CAPTURE_RETENTION, CAPTURE_MAINTENANCE
    }

    static final com.mongodb.event.ConnectionPoolListener POOL_LISTENER = new com.mongodb.event.ConnectionPoolListener() {
        @Override public void connectionCheckOutStarted(com.mongodb.event.ConnectionCheckOutStartedEvent event) {
            var scope = CURRENT.get(); if (scope != null) scope.checkoutsStarted++;
        }
        @Override public void connectionCheckedOut(com.mongodb.event.ConnectionCheckedOutEvent event) {
            var scope = CURRENT.get(); if (scope != null) scope.checkoutFinished(event.getElapsedTime(TimeUnit.NANOSECONDS), false);
        }
        @Override public void connectionCheckOutFailed(com.mongodb.event.ConnectionCheckOutFailedEvent event) {
            var scope = CURRENT.get(); if (scope != null) scope.checkoutFinished(event.getElapsedTime(TimeUnit.NANOSECONDS), true);
        }
    };

    private static final ThreadLocal<MongoHttpDiagnosticsScope> CURRENT = new ThreadLocal<>();
    private static final Stage NO_STAGE = new Stage(null, null, null, 0);
    private static final int MAX_GROUPS = 32;
    private final MongoHttpDiagnosticsScope previous;
    private final long cpuStarted;
    private final Map<Integer, Pending> pending = new HashMap<>();
    private final Map<Group, DriverStats> groups = new LinkedHashMap<>();
    private final Map<Phase, PhaseStats> phases = new EnumMap<>(Phase.class);
    private final Map<Phase, DriverStats> driverPhases = new EnumMap<>(Phase.class);
    private final DriverStats driver = new DriverStats();
    private Stage active;
    private int unclosedStages;
    private int unmatchedCompletions;
    private int groupedAsOther;
    private long checkoutsStarted;
    private long checkoutsCompleted;
    private long checkoutFailures;
    private long checkoutNanos;
    private long maxCheckoutNanos;

    private void checkoutFinished(long nanos, boolean failed) {
        checkoutsCompleted++; if (failed) checkoutFailures++;
        checkoutNanos += nanos; maxCheckoutNanos = Math.max(maxCheckoutNanos, nanos);
    }

    private static final class Cpu {
        private static final ThreadMXBean BEAN = ManagementFactory.getThreadMXBean();
        private static long current() {
            return BEAN.isCurrentThreadCpuTimeSupported() && BEAN.isThreadCpuTimeEnabled()
                    ? BEAN.getCurrentThreadCpuTime() : -1;
        }
    }

    private MongoHttpDiagnosticsScope() {
        previous = CURRENT.get();
        cpuStarted = Cpu.current();
        CURRENT.set(this);
    }

    public static MongoHttpDiagnosticsScope open() { return new MongoHttpDiagnosticsScope(); }

    public static Stage phase(Phase phase) {
        var scope = CURRENT.get();
        if (scope == null) return NO_STAGE;
        var stage = new Stage(scope, phase, scope.active, System.nanoTime());
        scope.active = stage;
        scope.unclosedStages++;
        scope.phases.computeIfAbsent(phase, ignored -> new PhaseStats()).calls++;
        return stage;
    }

    /** Classifies only a provider failure, never its message, response or authentication input. */
    public static void providerFailure(Phase phase) {
        var scope = CURRENT.get();
        if (scope != null) scope.phases.computeIfAbsent(phase, ignored -> new PhaseStats()).failures++;
    }

    static void started(CommandStartedEvent event) {
        var scope = CURRENT.get();
        if (scope == null) return;
        String command = safe(event.getCommandName());
        var value = event.getCommand().get(event.getCommandName());
        String collection = value != null && value.isString() ? safe(value.asString().getValue()) : "-";
        Group group = new Group(safe(event.getDatabaseName()), collection, command);
        if (!scope.groups.containsKey(group) && scope.groups.size() >= MAX_GROUPS) {
            group = new Group("other", "other", "other");
            scope.groupedAsOther++;
        }
        Phase phase = scope.active == null ? Phase.REQUEST_OTHER : scope.active.phase;
        scope.pending.put(event.getRequestId(), new Pending(group, phase));
        scope.driver.started++;
        scope.groups.computeIfAbsent(group, ignored -> new DriverStats()).started++;
        if (phase != null) scope.driverPhases.computeIfAbsent(phase, ignored -> new DriverStats()).started++;
    }

    static void finished(int requestId, long nanos, boolean failed) {
        var scope = CURRENT.get();
        if (scope == null) return;
        Pending observed = scope.pending.remove(requestId);
        if (observed == null) { scope.unmatchedCompletions++; return; }
        scope.driver.finish(nanos, failed);
        scope.groups.get(observed.group()).finish(nanos, failed);
        if (observed.phase() != null) scope.driverPhases.get(observed.phase()).finish(nanos, failed);
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        long cpu = Cpu.current();
        result.put("thread_cpu_ms", cpuStarted < 0 || cpu < 0 ? null : millis(cpu - cpuStarted));
        result.put("mongo", driver.snapshot());
        result.put("pool", Map.of("started", checkoutsStarted, "completed", checkoutsCompleted,
                "failures", checkoutFailures, "checkout_ms", millis(checkoutNanos),
                "max_checkout_ms", millis(maxCheckoutNanos), "pending", checkoutsStarted - checkoutsCompleted));
        result.put("pending_commands", pending.size());
        result.put("unmatched_completions", unmatchedCompletions);
        result.put("unclosed_stages", unclosedStages);
        result.put("grouped_as_other", groupedAsOther);
        result.put("complete", pending.isEmpty() && unmatchedCompletions == 0 && unclosedStages == 0 && checkoutsStarted == checkoutsCompleted);
        var details = new ArrayList<Map<String, Object>>();
        groups.forEach((group, stats) -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("database", group.database()); row.put("collection", group.collection());
            row.put("command", group.command()); row.putAll(stats.snapshot()); details.add(row);
        });
        result.put("mongo_groups", details);
        Map<String, Object> byPhase = new LinkedHashMap<>();
        driverPhases.forEach((phase, stats) -> byPhase.put(label(phase), stats.snapshot()));
        result.put("mongo_by_phase", byPhase);
        Map<String, Object> timings = new LinkedHashMap<>();
        phases.forEach((phase, stats) -> timings.put(label(phase), Map.of(
                "calls", stats.calls, "inclusive_ms", millis(stats.nanos), "provider_failures", stats.failures)));
        result.put("phases", timings);
        result.put("timing_semantics", "phases_are_inclusive_not_additive;mongo_is_driver_elapsed_not_server_cpu");
        return result;
    }

    @Override public void close() {
        if (previous == null) CURRENT.remove(); else CURRENT.set(previous);
    }

    public static final class Stage implements AutoCloseable {
        private final MongoHttpDiagnosticsScope owner;
        private final Phase phase;
        private final Stage previous;
        private final long started;
        private Stage(MongoHttpDiagnosticsScope owner, Phase phase, Stage previous, long started) {
            this.owner = owner; this.phase = phase; this.previous = previous; this.started = started;
        }
        @Override public void close() {
            if (owner == null) return;
            owner.phases.get(phase).nanos += System.nanoTime() - started;
            owner.active = previous;
            owner.unclosedStages--;
        }
    }

    private record Group(String database, String collection, String command) { }
    private record Pending(Group group, Phase phase) { }
    private static final class PhaseStats { long calls; long nanos; long failures; }
    private static final class DriverStats {
        long started; long completed; long failures; long nanos; long maxNanos;
        void finish(long elapsed, boolean failed) {
            completed++; if (failed) failures++; nanos += elapsed; maxNanos = Math.max(maxNanos, elapsed);
        }
        Map<String, Object> snapshot() {
            return Map.of("started", started, "completed", completed, "failures", failures,
                    "driver_ms", millis(nanos), "max_driver_ms", millis(maxNanos));
        }
    }
    private static String label(Phase phase) { return phase.name().toLowerCase(Locale.ROOT); }
    private static double millis(long nanos) { return nanos / (double) TimeUnit.MILLISECONDS.toNanos(1); }
    private static String safe(String value) {
        if (value == null) return "-";
        var result = new StringBuilder();
        for (int i = 0; i < Math.min(value.length(), 96); i++) {
            char c = value.charAt(i);
            result.append(Character.isISOControl(c) || Character.getType(c) == Character.FORMAT
                    || c == '\u2028' || c == '\u2029' ? '?' : c);
        }
        return result.toString();
    }
}
