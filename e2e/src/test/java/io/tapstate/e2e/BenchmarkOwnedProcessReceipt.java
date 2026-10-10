package io.tapstate.e2e;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Local metadata for one owned launch; none of these reads belongs to a measured interval. */
final class BenchmarkOwnedProcessReceipt {
    static final long MAX_BYTES = 2L * 1024 * 1024;
    static final int MAX_ARGUMENTS = 512, MAX_STRING = 8192;
    private static final Set<String> REQUIRED_BINDINGS = Set.of("namespace", "sourceSettings", "storeUri",
            "externalTargetUri", "managedViewsUri", "operatorDatabase", "operatorStateUri");
    private static final Set<String> SAFE_VALUES = Set.of("namespace", "operatorDatabase", "database", "schema",
            "host", "hostname", "port", "sourceDatabase", "controlDatabase", "targetDatabase",
            "storeUri", "externalTargetUri", "managedViewsUri", "operatorStateUri", "targetUri", "uri", "url", "jdbcUrl",
            "forkId", "workload", "sourceDatabaseKind", "captureBoundary");
    private static final Set<String> SAFE_OPTIONS = Set.of("--role", "--server.address", "--server.port",
            "--tapstate.store.mongo.enabled", "--tapstate.store.mongo.uri",
            "--tapstate.store.mongo.operator-state-database", "--tapstate.connectors.plugins-dir",
            "--tapstate.store.mongo.server-selection-timeout", "--tapstate.connectors.also-accept-ids");

    /** Missing operating-system fields stay null, rather than becoming inferred launch facts. */
    record Snapshot(long pid, Instant start, String executable, List<String> arguments, boolean alive,
            String readFailure) {
        Snapshot {
            require(pid > 0, "positive process id required");
            bounded(executable); bounded(readFailure);
            if (arguments != null) { boundedArguments(arguments); arguments = List.copyOf(arguments); }
        }
        Map<String, Object> evidence() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("pid", pid); out.put("startInstant", start == null ? null : start.toString());
            out.put("executable", executable); out.put("arguments", arguments == null ? null : sanitizedArguments(arguments));
            out.put("argumentsSha256", arguments == null ? null : digestArguments(arguments));
            out.put("alive", alive); out.put("readFailure", readFailure);
            return Collections.unmodifiableMap(out);
        }
    }

    private final RealProcessServer server;
    private final Path application;
    private final String applicationSha256;
    private final long applicationBytes;
    private final Snapshot driver, captured;
    private final List<String> command;
    private final Map<String, Object> bindings, runtime;
    private final List<String> reasons = new ArrayList<>();
    private final Instant capturedAt;
    private Map<String, Object> childEnvironment = Map.of("status", "UNKNOWN");
    private final List<Map<String, Object>> jvmRuntime = new ArrayList<>();
    private Long jvmStartTimeMillis;
    private Snapshot beforeClose;
    private Instant verifiedAt, finishedAt;
    private Boolean ownedAliveAfterClose;
    private Integer ownedExitCode;
    private Map<String, Object> finishedEvidence;

    private BenchmarkOwnedProcessReceipt(RealProcessServer server, Path application, String sha, long size,
            Snapshot driver, Snapshot captured, List<String> command, Map<String, Object> bindings,
            Map<String, Object> runtime, List<String> problems) {
        this.server = server; this.application = application; this.applicationSha256 = sha;
        this.applicationBytes = size; this.driver = driver; this.captured = captured;
        this.command = List.copyOf(command); this.bindings = bindings; this.runtime = runtime;
        this.reasons.addAll(problems); capturedAt = Instant.now();
    }

    static BenchmarkOwnedProcessReceipt capture(RealProcessServer server, Path jar, Map<String, Object> bindings) {
        Objects.requireNonNull(server); Objects.requireNonNull(jar); Objects.requireNonNull(bindings);
        List<String> reasons = new ArrayList<>();
        Path application = jar.toAbsolutePath().normalize();
        List<String> command = server.launchCommand(); boundedArguments(command);
        boolean aliveBefore = server.isAlive();
        Snapshot captured = read(server.pid()), driver = read(ProcessHandle.current().pid());
        if (!aliveBefore || !server.isAlive()) { reasons.add("OWNED_PROCESS_NOT_ALIVE_AT_CAPTURE"); }
        reasons.addAll(verify(captured, server.pid(), null, command));
        if (driver.start() == null || !driver.alive()) { reasons.add("DRIVER_IDENTITY_UNAVAILABLE"); }
        if (driver.executable() == null || driver.arguments() == null) { reasons.add("DRIVER_OS_COMMAND_UNAVAILABLE"); }
        for (String key : REQUIRED_BINDINGS) {
            if (!bindings.containsKey(key) || bindings.get(key) == null) { reasons.add("MISSING_BINDING:" + key); }
        }
        boundedValue(bindings, 0);
        Map<String, Object> safeBindings = safeMap(bindings, 0);
        String sha = null; long size = -1;
        try {
            size = Files.size(application); sha = hashFile(application, size);
            int at = command.indexOf("-jar");
            if (at < 0 || at + 1 >= command.size()
                    || !Path.of(command.get(at + 1)).toAbsolutePath().normalize().equals(application)) {
                reasons.add("APPLICATION_LAUNCH_PATH_MISMATCH");
            }
        } catch (Exception | AssertionError unavailable) { reasons.add("APPLICATION_FACTS_UNAVAILABLE:" + unavailable.getClass().getSimpleName()); }
        Map<String, Object> runtime = driverRuntime();
        BenchmarkOwnedProcessReceipt receipt = new BenchmarkOwnedProcessReceipt(server, application, sha, size,
                driver, captured, command, safeBindings, runtime, reasons);
        receipt.childEnvironment = environmentEvidence(server.launchEnvironment());
        return receipt;
    }

    /** Runtime packets come from the caller's existing owned JMX connection before measurement. */
    void recordJvmRuntime(Map<String, Object> packet) {
        require(verifiedAt == null && finishedAt == null, "runtime metadata precedes cleanup verification");
        List<String> problems = runtimeReasons(packet, captured.pid(), jvmStartTimeMillis);
        Snapshot now = read(server.pid());
        problems = new ArrayList<>(problems);
        problems.addAll(verify(now, captured.pid(), captured.start(), command));
        if (captured.start() == null || !server.isAlive()) { problems.add("RUNTIME_OWNED_IDENTITY_UNAVAILABLE"); }
        if (!problems.isEmpty()) { reasons.addAll(problems); return; }
        require(jvmRuntime.size() < MAX_ARGUMENTS, "runtime packet count exceeded");
        jvmStartTimeMillis = ((Number) packet.get("jvmStartTimeMillis")).longValue();
        Map<String, Object> copy = new LinkedHashMap<>();
        for (String key : List.of("status", "scope", "ownedPid", "actualPid", "jvmStartTimeMillis", "vmName", "vmVersion", "vmVendor",
                "driverStartNanos", "driverEndNanos", "aliveBefore", "aliveAfter", "capturedAt")) { copy.put(key, packet.get(key)); }
        @SuppressWarnings("unchecked") List<String> arguments = (List<String>) packet.get("inputArguments");
        copy.put("inputArguments", sanitizedArguments(arguments)); copy.put("inputArgumentsSha256", digestArguments(arguments));
        if (!jvmRuntime.isEmpty()) {
            Map<String, Object> first = jvmRuntime.getFirst();
            for (String key : List.of("vmName", "vmVersion", "vmVendor", "inputArgumentsSha256")) {
                if (!Objects.equals(first.get(key), copy.get(key))) { reasons.add("CHILD_JVM_RUNTIME_IDENTITY_CHANGED:" + key); return; }
            }
        }
        List<Map<String, Object>> candidate = new ArrayList<>(jvmRuntime); candidate.add(Collections.unmodifiableMap(copy));
        boundedValue(candidate, 0); jvmRuntime.add(Collections.unmodifiableMap(copy));
    }

    static List<String> runtimeReasons(Map<String, Object> packet, long pid, Long previousStart) {
        List<String> problems = new ArrayList<>();
        if (packet == null) { return List.of("CHILD_JVM_RUNTIME_UNAVAILABLE"); }
        try { boundedValue(packet, 0); }
        catch (AssertionError invalid) { return List.of("CHILD_JVM_RUNTIME_BOUND_EXCEEDED"); }
        if (!"COMPLETE".equals(packet.get("status"))) { problems.add("CHILD_JVM_RUNTIME_INCOMPLETE"); }
        if (!"EXISTING_OWNED_RESOURCE_JMX_PRE_WINDOW".equals(packet.get("scope"))) { problems.add("CHILD_JVM_RUNTIME_SCOPE_UNAVAILABLE"); }
        if (!integral(packet.get("actualPid")) || ((Number) packet.get("actualPid")).longValue() != pid) { problems.add("CHILD_JVM_ACTUAL_PID_MISMATCH"); }
        if (!integral(packet.get("ownedPid")) || ((Number) packet.get("ownedPid")).longValue() != pid) { problems.add("CHILD_JVM_PID_MISMATCH"); }
        if (!integral(packet.get("jvmStartTimeMillis")) || ((Number) packet.get("jvmStartTimeMillis")).longValue() <= 0) { problems.add("CHILD_JVM_START_UNAVAILABLE"); }
        else if (previousStart != null && previousStart.longValue() != ((Number) packet.get("jvmStartTimeMillis")).longValue()) { problems.add("CHILD_JVM_START_CHANGED"); }
        for (String key : List.of("vmName", "vmVersion", "vmVendor")) {
            if (!(packet.get(key) instanceof String text) || text.isBlank()) { problems.add("CHILD_JVM_FIELD_UNAVAILABLE:" + key); }
        }
        if (!(packet.get("inputArguments") instanceof List<?> args) || args.stream().anyMatch(value -> !(value instanceof String))) { problems.add("CHILD_JVM_ARGUMENTS_UNAVAILABLE"); }
        if (!Boolean.TRUE.equals(packet.get("aliveBefore")) || !Boolean.TRUE.equals(packet.get("aliveAfter"))) { problems.add("CHILD_JVM_NOT_ALIVE_DURING_READ"); }
        if (!integral(packet.get("driverStartNanos")) || !integral(packet.get("driverEndNanos"))) { problems.add("CHILD_JVM_READ_BRACKET_UNAVAILABLE"); }
        else {
            try { if (Math.subtractExact(((Number) packet.get("driverEndNanos")).longValue(), ((Number) packet.get("driverStartNanos")).longValue()) < 0) { problems.add("CHILD_JVM_READ_BRACKET_INVALID"); } }
            catch (ArithmeticException overflow) { problems.add("CHILD_JVM_READ_BRACKET_INVALID"); }
        }
        return List.copyOf(problems);
    }

    private static boolean integral(Object value) { return value instanceof Long || value instanceof Integer; }

    static Map<String, Object> environmentEvidence(Map<String, Object> packet) {
        if (packet == null || !"COMPLETE".equals(packet.get("status"))
                || !"OWNED_PROCESS_BUILDER_BEFORE_START".equals(packet.get("scope"))
                || !(packet.get("values") instanceof Map<?, ?> values)
                || !(packet.get("absentKeys") instanceof List<?> absent)) { return Map.of("status", "UNKNOWN"); }
        boundedValue(packet, 0); Map<String, Object> safe = new LinkedHashMap<>();
        Set<String> allowed = Set.of("LANG", "LC_ALL", "LC_CTYPE", "TZ", "JAVA_HOME", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "MAVEN_OPTS");
        for (var entry : values.entrySet()) {
            if (!(entry.getKey() instanceof String key) || !allowed.contains(key) || !(entry.getValue() instanceof String text)) { return Map.of("status", "UNKNOWN"); }
            safe.put(key, (key.endsWith("OPTIONS") || key.equals("MAVEN_OPTS")) ? sanitizedArguments(List.of(text.split("\\s+"))) : text);
        }
        for (Object key : absent) { if (!(key instanceof String text) || !allowed.contains(text) || values.containsKey(text)) { return Map.of("status", "UNKNOWN"); } }
        java.util.Set<Object> selected = new java.util.HashSet<>(values.keySet()); selected.addAll(absent);
        if (!selected.equals(allowed) || new java.util.HashSet<>(absent).size() != absent.size()) { return Map.of("status", "UNKNOWN"); }
        return Map.of("status", "COMPLETE", "scope", packet.get("scope"), "values", Collections.unmodifiableMap(safe), "absentKeys", List.copyOf(absent));
    }

    /** Called once after every measured/ACK/resource window, immediately before owned cleanup. */
    void verifyBeforeClose() {
        require(verifiedAt == null && finishedAt == null, "receipt verification occurs once before finish");
        boolean aliveBefore = server.isAlive();
        beforeClose = read(server.pid());
        reasons.addAll(verify(beforeClose, captured.pid(), captured.start(), command));
        if (captured.start() == null) { reasons.add("CAPTURED_START_IDENTITY_UNAVAILABLE"); }
        if (!aliveBefore || !server.isAlive()) { reasons.add("OWNED_PROCESS_NOT_ALIVE_BEFORE_CLOSE"); }
        Snapshot driverNow = read(ProcessHandle.current().pid());
        if (driver.start() == null || driverNow.pid() != driver.pid() || !driver.start().equals(driverNow.start())) {
            reasons.add("DRIVER_IDENTITY_CHANGED_OR_UNAVAILABLE");
        }
        verifiedAt = Instant.now();
    }

    /** Uses the original owned Process outcome, never the absence of a PID in a lookup. */
    void finishAfterClose() {
        require(finishedAt == null, "receipt finishes once");
        if (verifiedAt == null) { reasons.add("BEFORE_CLOSE_VERIFICATION_NOT_RECORDED"); }
        ownedAliveAfterClose = server.isAlive();
        if (ownedAliveAfterClose) { reasons.add("OWNED_PROCESS_STILL_ALIVE_AFTER_CLOSE"); }
        else {
            try { ownedExitCode = server.exitValue(); }
            catch (IllegalThreadStateException unavailable) { reasons.add("OWNED_EXIT_OUTCOME_UNAVAILABLE"); }
        }
        try {
            if (applicationSha256 == null || Files.size(application) != applicationBytes
                    || !applicationSha256.equals(hashFile(application, applicationBytes))) {
                reasons.add("APPLICATION_CHANGED_OR_UNAVAILABLE_AFTER_CLOSE");
            }
        } catch (Exception | AssertionError unavailable) { reasons.add("APPLICATION_CHANGED_OR_UNAVAILABLE_AFTER_CLOSE"); }
        finishedAt = Instant.now(); finishedEvidence = buildEvidence();
    }

    Map<String, Object> evidence() { return finishedEvidence == null ? buildEvidence() : finishedEvidence; }

    private Map<String, Object> buildEvidence() {
        Map<String, Object> out = new LinkedHashMap<>();
        List<String> allReasons = new ArrayList<>(reasons);
        if (!"COMPLETE".equals(childEnvironment.get("status"))) { allReasons.add("CHILD_LAUNCH_ENVIRONMENT_UNAVAILABLE"); }
        if (jvmRuntime.isEmpty()) { allReasons.add("CHILD_JVM_RUNTIME_NOT_RECORDED"); }
        if (finishedAt == null) { allReasons.add("OWNED_EXIT_NOT_RECORDED"); }
        out.put("status", allReasons.isEmpty() ? "QUALIFIED" : "UNKNOWN");
        out.put("reasons", List.copyOf(allReasons)); out.put("capturedAt", capturedAt.toString());
        out.put("verifiedBeforeCloseAt", verifiedAt == null ? null : verifiedAt.toString());
        out.put("finishedAfterCloseAt", finishedAt == null ? null : finishedAt.toString());
        out.put("applicationPath", application.toString()); out.put("applicationSha256", applicationSha256);
        out.put("applicationBytes", applicationBytes); out.put("driver", driver.evidence());
        out.put("productAtCapture", captured.evidence());
        out.put("productBeforeClose", beforeClose == null ? null : beforeClose.evidence());
        out.put("retainedLaunchCommand", sanitizedArguments(command));
        out.put("retainedLaunchCommandSha256", digestArguments(command));
        out.put("bindings", bindings); out.put("driverRuntime", runtime);
        out.put("childEnvironment", childEnvironment); out.put("childJvmRuntime", List.copyOf(jvmRuntime));
        out.put("ownedAliveAfterClose", ownedAliveAfterClose); out.put("ownedExitCode", ownedExitCode);
        out.put("exitEvidenceSource", "ORIGINAL_OWNED_PROCESS_OBJECT");
        out.put("measurementWindowReads", false); out.put("performanceAcceptanceEligible", false);
        boundedValue(out, 0);
        return Collections.unmodifiableMap(out);
    }

    static List<String> verify(Snapshot observed, long expectedPid, Instant expectedStart, List<String> retainedCommand) {
        boundedArguments(retainedCommand);
        List<String> reasons = new ArrayList<>();
        if (observed.pid() != expectedPid) { reasons.add("OWNED_PID_MISMATCH"); }
        if (!observed.alive()) { reasons.add("OWNED_OS_PROCESS_NOT_ALIVE"); }
        if (observed.start() == null) { reasons.add("OS_START_IDENTITY_UNAVAILABLE"); }
        else if (expectedStart != null && !expectedStart.equals(observed.start())) { reasons.add("OWNED_START_IDENTITY_MISMATCH"); }
        if (observed.executable() == null) { reasons.add("OS_EXECUTABLE_UNAVAILABLE"); }
        else if (retainedCommand.isEmpty() || !sameExecutable(observed.executable(), retainedCommand.getFirst())) {
            reasons.add("OS_EXECUTABLE_MISMATCH");
        }
        if (observed.arguments() == null) { reasons.add("OS_ARGUMENTS_UNAVAILABLE"); }
        else if (retainedCommand.isEmpty() || !observed.arguments().equals(retainedCommand.subList(1, retainedCommand.size()))) {
            reasons.add("OS_ARGUMENTS_MISMATCH");
        }
        if (observed.readFailure() != null) { reasons.add("OS_METADATA_READ_UNAVAILABLE:" + observed.readFailure()); }
        return List.copyOf(reasons);
    }

    static Snapshot read(long pid) {
        try {
            var handle = ProcessHandle.of(pid);
            if (handle.isEmpty()) { return new Snapshot(pid, null, null, null, false, "HANDLE_UNAVAILABLE"); }
            ProcessHandle process = handle.orElseThrow(); var info = process.info();
            return new Snapshot(pid, info.startInstant().orElse(null), info.command().orElse(null),
                    info.arguments().map(java.util.Arrays::asList).orElse(null), process.isAlive(), null);
        } catch (RuntimeException | AssertionError unavailable) {
            return new Snapshot(pid, null, null, null, false, unavailable.getClass().getSimpleName());
        }
    }

    private static boolean sameExecutable(String actual, String expected) {
        try {
            Path a = Path.of(actual).toAbsolutePath().normalize(), b = Path.of(expected).toAbsolutePath().normalize();
            return a.equals(b) || a.toRealPath().equals(b.toRealPath());
        } catch (Exception unavailable) { return false; }
    }

    private static Map<String, Object> driverRuntime() {
        Map<String, Object> out = new LinkedHashMap<>();
        for (String key : List.of("java.version", "java.vendor", "java.vm.name", "java.vm.version", "os.name", "os.arch", "os.version")) {
            String value = System.getProperty(key); bounded(value); out.put(key, value);
        }
        out.put("jvmInputArguments", sanitizedArguments(ManagementFactory.getRuntimeMXBean().getInputArguments()));
        Map<String, Object> env = new LinkedHashMap<>();
        for (String key : List.of("LANG", "LC_ALL", "LC_CTYPE", "TZ")) {
            String value = System.getenv(key); bounded(value); env.put(key, value);
        }
        out.put("whitelistedEnvironment", Collections.unmodifiableMap(env));
        return Collections.unmodifiableMap(out);
    }

    static List<String> sanitizedArguments(List<String> arguments) {
        boundedArguments(arguments);
        List<String> out = new ArrayList<>(); boolean path = false;
        for (int at = 0; at < arguments.size(); at++) {
            String value = arguments.get(at);
            if (path) { out.add(safeUri(value)); path = false; continue; }
            if (value.equals("-jar") || value.equals("-cp") || value.equals("-classpath")) { out.add(value); path = true; continue; }
            if (value.matches("-Xm[sx][0-9]+[kKmMgG]?") || value.matches("-XX:[+-][A-Za-z0-9]+")
                    || value.matches("-XX:[A-Za-z0-9]+=[0-9]+[kKmMgG]?")) { out.add(value); continue; }
            int equal = value.indexOf('=');
            if (equal > 0 && SAFE_OPTIONS.contains(value.substring(0, equal))) {
                out.add(value.substring(0, equal + 1) + safeUri(value.substring(equal + 1))); continue;
            }
            out.add(equal > 0 ? value.substring(0, equal + 1) + "[REDACTED]" : "[REDACTED]");
        }
        return List.copyOf(out);
    }

    static String safeUri(String value) {
        if (!value.contains("://")) { return value; }
        String safe = value.replaceFirst("(://)[^/?#]*@", "$1[REDACTED]@");
        int query = safe.indexOf('?');
        if (query >= 0) {
            String head = safe.substring(0, query + 1); List<String> parts = new ArrayList<>();
            for (String part : safe.substring(query + 1).split("&", -1)) {
                int equal = part.indexOf('='); String key = equal < 0 ? part : part.substring(0, equal);
                parts.add(Set.of("replicaSet", "tls", "directConnection", "authSource", "serverSelectionTimeoutMS").contains(key)
                        ? part : key + "=[REDACTED]");
            }
            safe = head + String.join("&", parts);
        }
        int fragment = safe.indexOf('#');
        return fragment < 0 ? safe : safe.substring(0, fragment) + "#[REDACTED]";
    }

    private static Map<String, Object> safeMap(Map<String, ?> input, int depth) {
        require(depth <= 4, "binding depth exceeded"); Map<String, Object> out = new LinkedHashMap<>();
        input.forEach((key, value) -> {
            if (value instanceof Map<?, ?> map) {
                Map<String, Object> nested = new LinkedHashMap<>();
                map.forEach((k, v) -> { require(k instanceof String, "binding key must be a string"); nested.put((String) k, v); });
                out.put(key, safeMap(nested, depth + 1));
            } else if (value instanceof String text) { out.put(key, SAFE_VALUES.contains(key) ? safeUri(text) : "[REDACTED]"); }
            else if (value == null || value instanceof Number || value instanceof Boolean) { out.put(key, value); }
            else { throw new AssertionError("unsupported binding value"); }
        });
        return Collections.unmodifiableMap(out);
    }

    private static String hashFile(Path path, long size) throws Exception {
        require(size >= 0 && Files.isRegularFile(path), "application must be a sized regular file");
        MessageDigest digest = MessageDigest.getInstance("SHA-256"); byte[] buffer = new byte[65536]; long remaining = size;
        try (InputStream input = Files.newInputStream(path)) {
            while (remaining > 0) {
                int count = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (count < 0) { throw new AssertionError("application changed during hashing"); }
                digest.update(buffer, 0, count); remaining -= count;
            }
            require(input.read() == -1 && Files.size(path) == size, "application changed during hashing");
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String digestArguments(List<String> values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private static void boundedArguments(List<String> values) {
        require(values != null && values.size() <= MAX_ARGUMENTS, "argument count exceeded");
        long bytes = 0; for (String value : values) { require(value != null, "null argument"); bounded(value); bytes += 4L * value.length() + 16; }
        require(bytes <= MAX_BYTES, "argument bytes exceeded");
    }
    private static void bounded(String value) { require(value == null || value.length() <= MAX_STRING, "string bound exceeded"); }
    private static long boundedValue(Object value, int depth) {
        require(depth <= 8, "receipt depth exceeded"); long bytes = 32;
        if (value == null || value instanceof Number || value instanceof Boolean) { return 16; }
        if (value instanceof String text) { bounded(text); return 4L * text.length() + 16; }
        if (value instanceof Map<?, ?> map) {
            require(map.size() <= 64, "receipt map count exceeded");
            for (var entry : map.entrySet()) { bytes += boundedValue(entry.getKey(), depth + 1) + boundedValue(entry.getValue(), depth + 1); require(bytes <= MAX_BYTES, "receipt byte budget exceeded"); }
        } else if (value instanceof Collection<?> items) {
            require(items.size() <= MAX_ARGUMENTS, "receipt list count exceeded");
            for (Object item : items) { bytes += boundedValue(item, depth + 1); require(bytes <= MAX_BYTES, "receipt byte budget exceeded"); }
        } else { throw new AssertionError("unsupported receipt value"); }
        return bytes;
    }
    private static void require(boolean condition, String message) { if (!condition) { throw new AssertionError(message); } }
}
