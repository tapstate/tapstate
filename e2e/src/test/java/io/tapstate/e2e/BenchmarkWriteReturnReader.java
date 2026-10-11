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
    private static final String NATIVE_CLOCK_PROPERTY = "-Dtapstate.benchmark.native-clock-library";
    private static final int MAX_RUNTIME_ARGUMENTS = 128;
    private static final int MAX_RUNTIME_ARGUMENT_LENGTH = 16_384;
    private static final long MAX_RUNTIME_ARGUMENT_BYTES = 2L * 1024 * 1024 - 4096;
    private final BenchmarkCausalClock.Identity identity;
    private final MBeanServerConnection connection;
    private final BooleanSupplier alive;
    private final LongSupplier clock;
    private final ObjectName name;
    private final ObjectName runtimeName;
    private final ClockRequests clockRequests;

    enum ClockRequestRole {
        PERIODIC_CLOCK(512), START_VALIDATION(1), STOP_VALIDATION(1), SUMMARY_VALIDATION(1), PAGE_VALIDATION(514);
        private final long maximumRequests;
        ClockRequestRole(long maximumRequests) { this.maximumRequests = maximumRequests; }
    }

    BenchmarkWriteReturnReader(BenchmarkCausalClock.Identity identity, MBeanServerConnection connection,
                              BooleanSupplier alive, LongSupplier clock) {
        this.identity = java.util.Objects.requireNonNull(identity);
        this.connection = java.util.Objects.requireNonNull(connection);
        this.alive = java.util.Objects.requireNonNull(alive);
        this.clock = java.util.Objects.requireNonNull(clock);
        clockRequests = "true".equals(System.getProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY))
                ? new ClockRequests(identity) : null;
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

    /** Cold provenance reads never invoke the probe's counter or measurement controls. */
    Map<String, Object> nativeClockEvidence(String expectedLibraryPath, String actualRootMetadata) {
        var retained = new java.util.LinkedHashMap<String, Object>();
        retained.put("state", "UNKNOWN"); retained.put("reason", "NATIVE_CLOCK_READ_REFUSED");
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> retained.put(flag, false));
        retained.put("rootMetadata", BenchmarkWriteReturnCostStages.rawEvidence(actualRootMetadata));
        retained.put("rawAvailable", false); retained.put("rawRetained", false);
        retained.put("expectedPid", identity.pid()); retained.put("expectedJvmStartTimeMillis", identity.jvmStartTimeMillis());
        try {
            if (expectedLibraryPath == null || expectedLibraryPath.isEmpty()) { throw new AssertionError("native clock library path is missing"); }
            var rootIdentity = new BenchmarkCausalClock.Identity(ProcessHandle.current().pid(),
                    java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime());
            retained.put("expectedRootPid", rootIdentity.pid()); retained.put("expectedRootJvmStartTimeMillis", rootIdentity.jvmStartTimeMillis());
            RuntimeRead before = runtimeRead(true, false, retained, "beforeRuntimeRead", expectedLibraryPath);
            requireAlive();
            long started = clock.getAsLong(); retained.put("clockMetadataRead", Map.of("startedAtNanos", started));
            Object value = connection.getAttribute(name, "ClockMetadata");
            String raw = value instanceof String text ? text : null;
            retained.putAll(BenchmarkWriteReturnCostStages.rawEvidence(raw));
            if (value != null && raw == null) { retained.put("responseType", value.getClass().getName()); }
            long completed = clock.getAsLong(); retained.put("clockMetadataRead", bracket(started, completed));
            requireAlive();
            RuntimeRead after = runtimeRead(true, false, retained, "afterRuntimeRead", expectedLibraryPath);
            if (!before.arguments().equals(after.arguments())) { throw new AssertionError("native clock runtime arguments changed"); }
            if (Math.subtractExact(started, before.completedAtNanos()) < 0 || Math.subtractExact(completed, started) < 0
                    || Math.subtractExact(after.startedAtNanos(), completed) < 0) {
                throw new AssertionError("native clock root reads are not serial");
            }
            var owned = BenchmarkNativeClockEvidence.parse(raw, identity, expectedLibraryPath);
            retained.put("ownedMetadata", owned);
            var root = BenchmarkNativeClockEvidence.parse(actualRootMetadata, rootIdentity, expectedLibraryPath);
            retained.put("parsedRootMetadata", root);
            BenchmarkNativeClockEvidence.requireMatchingNative(owned, root);
            var result = new java.util.LinkedHashMap<>(retained); result.putAll(owned);
            result.put("matchedRuntimeFlag", ENABLED_PROPERTY + "=true");
            result.put("matchedNativeClockFlag", NATIVE_CLOCK_PROPERTY + "=" + expectedLibraryPath);
            result.put("argumentCount", before.arguments().size());
            result.put("transportScope", "EXISTING_OWNED_RESOURCE_JMX_COLD_METADATA_READ");
            return Map.copyOf(result);
        } catch (java.io.IOException | javax.management.JMException | RuntimeException | Error refusal) {
            throw new NativeClockRefusal(refusal, retained);
        }
    }

    static final class NativeClockRefusal extends AssertionError {
        private final Map<String, Object> retained;
        NativeClockRefusal(Throwable cause, Map<String, Object> evidence) {
            super("owned native clock metadata were refused", cause); retained = Map.copyOf(evidence);
        }
        Map<String, Object> retainedEvidence() { return retained; }
    }

    private RuntimeRead runtimeRead(boolean expectedEnabled, boolean costStages,
            Map<String, Object> retained, String evidenceField) throws java.io.IOException, javax.management.JMException {
        return runtimeRead(expectedEnabled, costStages, retained, evidenceField, null);
    }

    private RuntimeRead runtimeRead(boolean expectedEnabled, boolean costStages,
            Map<String, Object> retained, String evidenceField, String nativeLibrary) throws java.io.IOException, javax.management.JMException {
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
        int nativeFlags = 0;
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
            if (nativeLibrary != null && (argument.equals(NATIVE_CLOCK_PROPERTY) || argument.startsWith(NATIVE_CLOCK_PROPERTY + "="))) {
                nativeFlags++;
                if (!argument.equals(NATIVE_CLOCK_PROPERTY + "=" + nativeLibrary)) {
                    throw new AssertionError("native clock runtime library flag contradicts the declared route");
                }
            }
        }
        if (flags != 1) { throw new AssertionError("return registration requires exactly one explicit runtime flag"); }
        if (costStages && costFlags != 1) { throw new AssertionError("return cost stages require exactly one explicit runtime flag"); }
        if (nativeLibrary != null && nativeFlags != 1) { throw new AssertionError("native clock requires exactly one explicit runtime library flag"); }
        return new RuntimeRead(List.copyOf(java.util.Arrays.asList(arguments.clone())), before, after);
    }

    private static Map<String, Long> bracket(long before, long after) {
        return Map.of("startedAtNanos", before, "completedAtNanos", after);
    }

    BenchmarkCausalClock.Sample clockSample(long sequence) {
        return clockSample(sequence, ClockRequestRole.PERIODIC_CLOCK);
    }

    /** Validation points remain outside the periodic sample roster. */
    BenchmarkCausalClock.Sample clockSample(long sequence, ClockRequestRole role) {
        ClockRequests.Ticket request = clockRequests == null ? null : clockRequests.begin(role);
        try {
            requireAlive();
            long before = clock.getAsLong();
            if (request != null) { clockRequests.started(request, before); }
            var attributes = connection.getAttributes(name, new String[]{"Pid", "JvmStartTimeMillis", "NanoTime"});
            long after = clock.getAsLong();
            if (request != null) { clockRequests.returned(request, after); }
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
            var sample = new BenchmarkCausalClock.Sample(sequence, actual, before, after, number(values, "NanoTime"));
            if (request != null) { clockRequests.finished(request, null); }
            return sample;
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            if (request != null) { clockRequests.finished(request, unavailable); }
            throw new AssertionError("owned return clock is unavailable", unavailable);
        } catch (RuntimeException | Error failure) {
            if (request != null) { clockRequests.finished(request, failure); }
            throw failure;
        }
    }

    Map<String, Object> clockRequestsEvidence() { return clockRequests == null ? Map.of() : clockRequests.evidence(); }

    boolean start(String window) { return control("start", new Object[]{window}, new String[]{String.class.getName()}); }
    boolean stop() { return control("stop", new Object[0], new String[0]); }

    record Summary(String window, String state, long completedCalls, long failedCalls,
                   long reportedRecords, long openCalls, long retainedBytes) { }

    /** Terminal totals are actual probe reads; caller closure still requires stop and full page coverage. */
    Summary summary() {
        clockSample(0, ClockRequestRole.SUMMARY_VALIDATION);
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
        clockSample(0, ClockRequestRole.PAGE_VALIDATION);
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
        clockSample(0, operation.equals("start") ? ClockRequestRole.START_VALIDATION : ClockRequestRole.STOP_VALIDATION);
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

    /** One Reader instance's existing clock-attribute request brackets, not all JMX or method activity. */
    private static final class ClockRequests {
        private static final int MAX_PARTIALS = 8;
        private final BenchmarkCausalClock.Identity owner;
        private final java.util.EnumMap<ClockRequestRole, Row> rows = new java.util.EnumMap<>(ClockRequestRole.class);
        private final java.util.LinkedHashMap<Long, Ticket> partials = new java.util.LinkedHashMap<>();
        private long ordinal;
        private long omittedPartials;
        private String unknownReason;

        private static final class Ticket {
            private final long ordinal;
            private final ClockRequestRole role;
            private Long before;
            private Long after;
            private boolean finished;
            private String failureType;
            Ticket(long ordinal, ClockRequestRole role) { this.ordinal = ordinal; this.role = role; }
        }

        private static final class Row {
            long attempts, requests, successes, failures, brackets, sum, maximum;
            boolean elapsedComplete = true;
            Map<String, Object> first = Map.of(), last = Map.of();
            long firstOrdinal = Long.MAX_VALUE, lastOrdinal = Long.MIN_VALUE;
        }

        ClockRequests(BenchmarkCausalClock.Identity owner) {
            this.owner = owner;
            for (ClockRequestRole role : ClockRequestRole.values()) { rows.put(role, new Row()); }
        }

        synchronized Ticket begin(ClockRequestRole role) {
            java.util.Objects.requireNonNull(role);
            Row row = rows.get(role); row.attempts = increment(row.attempts); ordinal = increment(ordinal);
            var ticket = new Ticket(ordinal, role);
            if (partials.size() < MAX_PARTIALS) { partials.put(ordinal, ticket); }
            else { omittedPartials = increment(omittedPartials); unknown("PARTIAL_RETENTION_LIMIT"); }
            return ticket;
        }

        synchronized void started(Ticket ticket, long before) {
            ticket.before = before; Row row = rows.get(ticket.role); row.requests = increment(row.requests);
            if (row.requests > ticket.role.maximumRequests) { unknown("ROLE_REQUEST_LIMIT"); }
            bounds(ticket);
        }

        synchronized void returned(Ticket ticket, long after) { ticket.after = after; bounds(ticket); }

        synchronized void finished(Ticket ticket, Throwable failure) {
            ticket.finished = true; Row row = rows.get(ticket.role);
            if (failure == null) { row.successes = increment(row.successes); }
            else {
                row.failures = increment(row.failures); unknown("CLOCK_REQUEST_FAILED");
                String type = failure.getClass().getName(); ticket.failureType = type.length() <= 96 ? type : "TYPE_NAME_EXCEEDS_BOUND";
            }
            bounds(ticket);
            if (ticket.before != null && ticket.after != null) {
                row.brackets = increment(row.brackets);
                try {
                    long duration = Math.subtractExact(ticket.after, ticket.before);
                    if (duration < 0) { throw new ArithmeticException("clock request bracket moved backward"); }
                    row.maximum = Math.max(row.maximum, duration); row.sum = Math.addExact(row.sum, duration);
                } catch (ArithmeticException invalid) { row.elapsedComplete = false; unknown("ELAPSED_ORDER_OR_OVERFLOW"); }
                partials.remove(ticket.ordinal);
            } else { row.elapsedComplete = false; }
        }

        private void bounds(Ticket ticket) {
            if (ticket.before == null) { return; }
            Row row = rows.get(ticket.role); var actual = new java.util.LinkedHashMap<String, Object>();
            actual.put("requestOrdinal", ticket.ordinal); actual.put("startedAtNanos", ticket.before);
            if (ticket.after != null) { actual.put("completedAtNanos", ticket.after); }
            actual.put("outcome", !ticket.finished ? "PENDING" : ticket.failureType == null ? "SUCCESS" : "FAILED");
            if (ticket.ordinal <= row.firstOrdinal) { row.firstOrdinal = ticket.ordinal; row.first = Map.copyOf(actual); }
            if (ticket.ordinal >= row.lastOrdinal) { row.lastOrdinal = ticket.ordinal; row.last = Map.copyOf(actual); }
        }

        synchronized Map<String, Object> evidence() {
            var result = new java.util.LinkedHashMap<String, Object>();
            boolean pending = partials.values().stream().anyMatch(ticket -> !ticket.finished);
            result.put("state", unknownReason != null || pending ? "UNKNOWN" : "RECORDED_DIAGNOSTIC");
            result.put("reason", unknownReason != null ? unknownReason : pending ? "REQUEST_IN_FLIGHT" : "OBSERVED_REQUEST_BRACKETS_ONLY");
            result.put("scope", "ACTUAL_CAPTURE_READER_INSTANCE_CLOCK_ATTRIBUTE_REQUESTS_ONLY");
            result.put("excluded", "OTHER_READERS_REGISTRATION_NATIVE_METADATA_CONTROL_PAGE_BODIES_UNBRACKETED_VALIDATION_BOOKKEEPING_AND_JMX_TAIL");
            result.put("elapsedScope", "EXISTING_CALLER_POINTS_MAY_INCLUDE_INSIDE_BRACKET_BOOKKEEPING_NOT_EXACT_GETTER_SERVICE_CPU_OR_ACTIVITY_DURATION");
            result.put("ownedPid", owner.pid()); result.put("ownedJvmStartTimeMillis", owner.jvmStartTimeMillis());
            var roles = new java.util.LinkedHashMap<String, Object>();
            for (var entry : rows.entrySet()) {
                Row row = entry.getValue(); var value = new java.util.LinkedHashMap<String, Object>();
                value.put("maximumRequests", entry.getKey().maximumRequests); value.put("attemptCount", row.attempts);
                value.put("requestCount", row.requests); value.put("successCount", row.successes); value.put("failureCount", row.failures);
                value.put("closedBracketCount", row.brackets); value.put("elapsedSumNanos", row.sum); value.put("elapsedMaxNanos", row.maximum);
                value.put("elapsedAggregationComplete", row.elapsedComplete && row.requests == row.brackets
                        && row.successes <= row.attempts && row.failures == row.attempts - row.successes);
                value.put("firstActualBracket", row.first); value.put("lastActualBracket", row.last);
                roles.put(entry.getKey().name(), Map.copyOf(value));
            }
            result.put("roles", Map.copyOf(roles)); result.put("omittedPartialRequests", omittedPartials);
            result.put("partialRequests", partials.values().stream().map(ticket -> {
                var value = new java.util.LinkedHashMap<String, Object>(); value.put("requestOrdinal", ticket.ordinal); value.put("role", ticket.role.name());
                value.put("state", ticket.finished ? "FAILED_WITHOUT_COMPLETE_BRACKET" : "PENDING");
                if (ticket.before != null) { value.put("startedAtNanos", ticket.before); }
                if (ticket.after != null) { value.put("completedAtNanos", ticket.after); }
                if (ticket.failureType != null) { value.put("failureType", ticket.failureType); }
                return Map.copyOf(value);
            }).toList());
            result.put("clockQualified", false); result.put("samplingCostQualified", false); result.put("wholeMethodCostQualified", false);
            result.put("roleCpuQualified", false); result.put("causalOverheadQualified", false); result.put("performanceAcceptanceEligible", false);
            Map<String, Object> snapshot = Map.copyOf(result);
            if (io.tapstate.core.common.JsonWriter.write(snapshot).getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 8192) {
                throw new AssertionError("clock request evidence exceeds its fixed byte budget");
            }
            return snapshot;
        }

        private long increment(long value) {
            try { return Math.addExact(value, 1); }
            catch (ArithmeticException overflow) { unknown("COUNT_OVERFLOW"); return value; }
        }

        private void unknown(String reason) { if (unknownReason == null) { unknownReason = reason; } }
    }
}
