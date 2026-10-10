package io.tapstate.e2e;

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Reads the bounded return probe only through the existing, exact owned JVM resource connection. */
final class BenchmarkWriteReturnReader {
    private static final String NAME = "io.tapstate.benchmark:type=WriteReturn";
    private static final int MAX_PAGE_BYTES = 64 * 1024;
    private static final String ENABLED_PROPERTY = "-Dtapstate.benchmark.write-return";
    private static final String COST_STAGES_PROPERTY = "-Dtapstate.benchmark.write-return-cost-stages";
    private static final int MAX_RUNTIME_ARGUMENTS = 128;
    private static final int MAX_RUNTIME_ARGUMENT_LENGTH = 16_384;
    private static final long MAX_RUNTIME_ARGUMENT_BYTES = 2L * 1024 * 1024 - 4096;
    private final BenchmarkCausalClock.Identity identity;
    private final MBeanServerConnection connection;
    private final BooleanSupplier alive;
    private final LongSupplier clock;
    private final ObjectName name;
    private final ObjectName runtimeName;

    BenchmarkWriteReturnReader(BenchmarkCausalClock.Identity identity, MBeanServerConnection connection,
                              BooleanSupplier alive, LongSupplier clock) {
        this.identity = java.util.Objects.requireNonNull(identity);
        this.connection = java.util.Objects.requireNonNull(connection);
        this.alive = java.util.Objects.requireNonNull(alive);
        this.clock = java.util.Objects.requireNonNull(clock);
        try {
            name = new ObjectName(NAME);
            runtimeName = new ObjectName(java.lang.management.ManagementFactory.RUNTIME_MXBEAN_NAME);
        }
        catch (javax.management.MalformedObjectNameException impossible) { throw new AssertionError(impossible); }
    }

    /** A cold registration check uses the existing connection and never reads or controls the probe. */
    Map<String, Object> registrationEvidence(boolean expectedEnabled) {
        try {
            RuntimeRead before = runtimeRead(expectedEnabled);
            requireAlive();
            boolean registered = connection.isRegistered(name);
            requireAlive();
            RuntimeRead after = runtimeRead(expectedEnabled);
            if (!before.arguments().equals(after.arguments())) {
                throw new AssertionError("return registration runtime arguments changed");
            }
            if (Math.subtractExact(after.startedAtNanos(), before.completedAtNanos()) < 0) {
                throw new AssertionError("return registration root reads are not serial");
            }
            if (registered != expectedEnabled) {
                throw new AssertionError("return registration does not match the explicit runtime flag");
            }
            var result = new java.util.LinkedHashMap<String, Object>();
            result.put("state", expectedEnabled ? "RECORDED_ENABLED" : "RECORDED_DISABLED");
            result.put("reason", "EXACT_OWNED_RUNTIME_FLAG_AND_REGISTRATION_MATCH");
            result.put("scope", "EXISTING_OWNED_RESOURCE_JMX_OUTSIDE_MEASUREMENT_WINDOW");
            result.put("pid", identity.pid()); result.put("jvmStartTimeMillis", identity.jvmStartTimeMillis());
            result.put("expectedEnabled", expectedEnabled); result.put("actualRegistered", registered);
            result.put("matchedRuntimeFlag", ENABLED_PROPERTY + "=" + expectedEnabled);
            result.put("argumentCount", before.arguments().size());
            result.put("beforeRuntimeRead", bracket(before.startedAtNanos(), before.completedAtNanos()));
            // These root points enclose the registration call and liveness checks, not its service cost.
            result.put("registrationRead", bracket(before.completedAtNanos(), after.startedAtNanos()));
            result.put("afterRuntimeRead", bracket(after.startedAtNanos(), after.completedAtNanos()));
            result.put("performanceAcceptanceEligible", false); result.put("formalPerformance", false);
            result.put("costAcceptanceEligible", false); result.put("samplingCostQualified", false);
            result.put("returnCaptureDelayQualified", false); result.put("phaseAttributionQualified", false);
            return Map.copyOf(result);
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return registration is unavailable", unavailable);
        }
    }

    private record RuntimeRead(List<String> arguments, long startedAtNanos, long completedAtNanos) { }

    private RuntimeRead runtimeRead(boolean expectedEnabled) throws java.io.IOException, javax.management.JMException {
        return runtimeRead(expectedEnabled, false, null, null);
    }

    /** Called by capture only after successful stop and complete original page validation. */
    Map<String, Object> costStages(long epoch, String window, long completedCalls) {
        var retained = new java.util.LinkedHashMap<String, Object>();
        retained.put("state", "UNKNOWN"); retained.put("reason", "COST_STAGES_READ_REFUSED");
        retained.put("performanceAcceptanceEligible", false); retained.put("samplingCostQualified", false);
        retained.put("costAcceptanceEligible", false); retained.put("formalPerformance", false);
        retained.put("causalOverheadQualified", false); retained.put("rawAvailable", false); retained.put("rawRetained", false);
        retained.put("expectedPid", identity.pid()); retained.put("expectedJvmStartTimeMillis", identity.jvmStartTimeMillis());
        try {
            RuntimeRead before = runtimeRead(true, true, retained, "beforeRuntimeRead");
            requireAlive();
            long started = clock.getAsLong();
            retained.put("costStagesRead", Map.of("startedAtNanos", started));
            Object value = connection.getAttribute(name, "CostStages");
            String raw = value instanceof String text ? text : null;
            retained.putAll(BenchmarkWriteReturnCostStages.rawEvidence(raw));
            if (value != null && raw == null) { retained.put("responseType", value.getClass().getName()); }
            long completed = clock.getAsLong();
            retained.put("costStagesRead", bracket(started, completed));
            requireAlive();
            RuntimeRead after = runtimeRead(true, true, retained, "afterRuntimeRead");
            if (!before.arguments().equals(after.arguments())) { throw new AssertionError("return cost stages runtime arguments changed"); }
            if (Math.subtractExact(started, before.completedAtNanos()) < 0
                    || Math.subtractExact(completed, started) < 0
                    || Math.subtractExact(after.startedAtNanos(), completed) < 0) {
                throw new AssertionError("return cost stages root reads are not serial");
            }
            var result = new java.util.LinkedHashMap<>(retained);
            result.putAll(BenchmarkWriteReturnCostStages.parse(raw, identity, epoch, window, completedCalls));
            result.put("matchedRuntimeFlag", ENABLED_PROPERTY + "=true");
            result.put("matchedCostStagesFlag", COST_STAGES_PROPERTY + "=true");
            result.put("argumentCount", before.arguments().size());
            result.put("transportScope", "EXISTING_OWNED_RESOURCE_JMX_CALLER_POST_STOP");
            return Map.copyOf(result);
        } catch (java.io.IOException | javax.management.JMException | RuntimeException | Error refusal) {
            throw new CostStagesRefusal(refusal, retained);
        }
    }

    static final class CostStagesRefusal extends AssertionError {
        private final Map<String, Object> retained;
        CostStagesRefusal(Throwable cause, Map<String, Object> evidence) {
            super("owned return cost stages were refused", cause);
            retained = Map.copyOf(evidence);
        }
        Map<String, Object> retainedEvidence() { return retained; }
    }

    private RuntimeRead runtimeRead(boolean expectedEnabled, boolean costStages,
            Map<String, Object> retained, String evidenceField) throws java.io.IOException, javax.management.JMException {
        requireAlive();
        long before = clock.getAsLong();
        if (retained != null) { retained.put(evidenceField, Map.of("startedAtNanos", before)); }
        var attributes = connection.getAttributes(runtimeName, new String[]{"Pid", "StartTime", "InputArguments"});
        long after = clock.getAsLong();
        if (retained != null) { retained.put(evidenceField, bracket(before, after)); }
        requireAlive();
        if (Math.subtractExact(after, before) < 0) { throw new AssertionError("return registration root bracket moved backward"); }
        Map<String, Object> values = new HashMap<>();
        for (Object entry : attributes) {
            if (!(entry instanceof Attribute attribute) || values.containsKey(attribute.getName())) {
                throw new AssertionError("return registration runtime attributes are malformed or duplicate");
            }
            values.put(attribute.getName(), attribute.getValue());
        }
        if (!values.keySet().equals(java.util.Set.of("Pid", "StartTime", "InputArguments"))) {
            throw new AssertionError("return registration runtime attributes are incomplete");
        }
        if (retained != null) {
            var actualRead = new java.util.LinkedHashMap<String, Object>(bracket(before, after));
            if (values.get("Pid") instanceof Long pid) { actualRead.put("actualPid", pid); }
            if (values.get("StartTime") instanceof Long start) { actualRead.put("jvmStartTimeMillis", start); }
            if (values.get("InputArguments") instanceof String[] args) { actualRead.put("argumentCount", args.length); }
            retained.put(evidenceField, Map.copyOf(actualRead));
        }
        var actual = new BenchmarkCausalClock.Identity(number(values, "Pid"), number(values, "StartTime"));
        if (!identity.equals(actual)) { throw new AssertionError("return registration has another owned runtime identity"); }
        if (!(values.get("InputArguments") instanceof String[] arguments) || arguments.length > MAX_RUNTIME_ARGUMENTS) {
            throw new AssertionError("return registration runtime argument roster exceeds its bound");
        }
        long bytes = 0;
        int flags = 0;
        int costFlags = 0;
        String expected = ENABLED_PROPERTY + "=" + expectedEnabled;
        for (String argument : arguments) {
            if (argument == null || argument.length() > MAX_RUNTIME_ARGUMENT_LENGTH) {
                throw new AssertionError("return registration runtime argument exceeds its length bound");
            }
            bytes = Math.addExact(bytes, 4L * argument.length() + 16);
            if (bytes > MAX_RUNTIME_ARGUMENT_BYTES) { throw new AssertionError("return registration runtime argument bytes exceed their bound"); }
            if (argument.equals(ENABLED_PROPERTY) || argument.startsWith(ENABLED_PROPERTY + "=")) {
                flags++;
                if (!argument.equals(expected)) { throw new AssertionError("return registration runtime flag contradicts the declared mode"); }
            }
            if (costStages && (argument.equals(COST_STAGES_PROPERTY) || argument.startsWith(COST_STAGES_PROPERTY + "="))) {
                costFlags++;
                if (!argument.equals(COST_STAGES_PROPERTY + "=true")) {
                    throw new AssertionError("return cost stages runtime flag contradicts the declared mode");
                }
            }
        }
        if (flags != 1) { throw new AssertionError("return registration requires exactly one explicit runtime flag"); }
        if (costStages && costFlags != 1) { throw new AssertionError("return cost stages require exactly one explicit runtime flag"); }
        return new RuntimeRead(List.copyOf(java.util.Arrays.asList(arguments.clone())), before, after);
    }

    private static Map<String, Long> bracket(long before, long after) {
        return Map.of("startedAtNanos", before, "completedAtNanos", after);
    }

    BenchmarkCausalClock.Sample clockSample(long sequence) {
        requireAlive();
        long before = clock.getAsLong();
        try {
            var attributes = connection.getAttributes(name, new String[]{"Pid", "JvmStartTimeMillis", "NanoTime"});
            long after = clock.getAsLong();
            requireAlive();
            Map<String, Object> values = new HashMap<>();
            for (Object entry : attributes) {
                if (!(entry instanceof Attribute attribute) || values.containsKey(attribute.getName())) {
                    throw new AssertionError("return clock attributes are malformed or duplicate");
                }
                values.put(attribute.getName(), attribute.getValue());
            }
            if (!values.keySet().equals(java.util.Set.of("Pid", "JvmStartTimeMillis", "NanoTime"))) {
                throw new AssertionError("return clock attributes are incomplete");
            }
            var actual = new BenchmarkCausalClock.Identity(number(values, "Pid"), number(values, "JvmStartTimeMillis"));
            if (!identity.equals(actual)) { throw new AssertionError("return clock has another owned runtime identity"); }
            return new BenchmarkCausalClock.Sample(sequence, actual, before, after, number(values, "NanoTime"));
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return clock is unavailable", unavailable);
        }
    }

    boolean start(String window) { return control("start", new Object[]{window}, new String[]{String.class.getName()}); }
    boolean stop() { return control("stop", new Object[0], new String[0]); }

    record Summary(String window, String state, long completedCalls, long failedCalls,
                   long reportedRecords, long openCalls, long retainedBytes) { }

    /** Terminal totals are actual probe reads; caller closure still requires stop and full page coverage. */
    Summary summary() {
        clockSample(0);
        String[] names = {"Pid", "JvmStartTimeMillis", "Window", "State", "CompletedCalls", "FailedCalls",
                "ReportedRecords", "OpenCalls", "RetainedBytes"};
        try {
            var attributes = connection.getAttributes(name, names);
            requireAlive();
            Map<String, Object> values = new HashMap<>();
            for (Object entry : attributes) {
                if (!(entry instanceof Attribute attribute) || values.containsKey(attribute.getName())) {
                    throw new AssertionError("return summary attributes are malformed or duplicate");
                }
                values.put(attribute.getName(), attribute.getValue());
            }
            if (!values.keySet().equals(java.util.Set.of(names))) { throw new AssertionError("return summary is incomplete"); }
            var actual = new BenchmarkCausalClock.Identity(number(values, "Pid"), number(values, "JvmStartTimeMillis"));
            if (!identity.equals(actual)) { throw new AssertionError("return summary has another owned runtime identity"); }
            for (String field : java.util.List.of("CompletedCalls", "FailedCalls", "ReportedRecords", "OpenCalls", "RetainedBytes")) {
                if (number(values, field) < 0) { throw new AssertionError("return summary counter is negative"); }
            }
            String window = boundedText(values, "Window", 512), state = boundedText(values, "State", 128);
            long completed = number(values, "CompletedCalls"), failures = number(values, "FailedCalls");
            if (failures > completed || number(values, "RetainedBytes") > 2L * 1024 * 1024) {
                throw new AssertionError("return summary counters contradict their bounded domain");
            }
            return new Summary(window, state, completed, failures, number(values, "ReportedRecords"),
                    number(values, "OpenCalls"), number(values, "RetainedBytes"));
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return probe summary is unavailable", unavailable);
        }
    }

    byte[] page(long completionCursor) {
        clockSample(0);
        try {
            Object result = connection.invoke(name, "read", new Object[]{completionCursor}, new String[]{"long"});
            requireAlive();
            if (!(result instanceof byte[] bytes) || bytes.length < 8 || bytes.length > MAX_PAGE_BYTES) {
                throw new AssertionError("return receipt page is missing or exceeds its bound");
            }
            return bytes;
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return receipt page is unavailable", unavailable);
        }
    }

    private boolean control(String operation, Object[] arguments, String[] signature) {
        clockSample(0);
        try {
            Object result = connection.invoke(name, operation, arguments, signature);
            requireAlive();
            if (!(result instanceof Boolean accepted)) { throw new AssertionError("return probe control has no typed result"); }
            return accepted;
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return probe control is unavailable", unavailable);
        }
    }

    private void requireAlive() {
        if (!alive.getAsBoolean()) { throw new AssertionError("owned return probe JVM has exited"); }
    }
    private static long number(Map<String, Object> values, String field) {
        if (!(values.get(field) instanceof Long value)) { throw new AssertionError("return clock counter is not an exact long"); }
        return value;
    }
    private static String boundedText(Map<String, Object> values, String field, int maximum) {
        if (!(values.get(field) instanceof String text) || text.length() > maximum
                || text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maximum) {
            throw new AssertionError("return summary text is missing or exceeds its bound");
        }
        return text;
    }
}
