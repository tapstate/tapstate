package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkOwnedProcessReceiptTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00.123456789Z");
    private static final List<String> COMMAND = List.of("/owned/java", "-Xmx128m", "-jar", "/owned/app.jar");

    @Test
    void actualCommandAndExactNanosecondIdentityQualifyWithoutInferringArguments() {
        assertThat(BenchmarkOwnedProcessReceipt.verify(snapshot(91, START, COMMAND.getFirst(), COMMAND.subList(1, 4), true),
                91, START, COMMAND)).isEmpty();
    }

    @Test
    void unavailableOsArgumentsRemainUnknownDespiteRetainedCommand() {
        var absent = snapshot(91, START, COMMAND.getFirst(), null, true);
        assertThat(BenchmarkOwnedProcessReceipt.verify(absent, 91, START, COMMAND)).contains("OS_ARGUMENTS_UNAVAILABLE");
        assertThat(absent.evidence()).containsEntry("arguments", null).containsEntry("argumentsSha256", null);
    }

    @Test
    void samePidWithChangedStartCannotQualifyAReusedProcess() {
        assertThat(BenchmarkOwnedProcessReceipt.verify(snapshot(91, START.plusNanos(1), COMMAND.getFirst(), COMMAND.subList(1, 4), true),
                91, START, COMMAND)).contains("OWNED_START_IDENTITY_MISMATCH");
    }

    @Test
    void wrongPidExecutableArgumentsOrDeadProcessNeverQualify() {
        assertThat(BenchmarkOwnedProcessReceipt.verify(snapshot(92, START, "/foreign/java", List.of("-jar", "/foreign/app.jar"), false),
                91, START, COMMAND)).contains("OWNED_PID_MISMATCH", "OS_EXECUTABLE_MISMATCH", "OS_ARGUMENTS_MISMATCH", "OWNED_OS_PROCESS_NOT_ALIVE");
        assertThat(BenchmarkOwnedProcessReceipt.verify(snapshot(91, null, null, null, true), 91, START, COMMAND))
                .contains("OS_START_IDENTITY_UNAVAILABLE", "OS_EXECUTABLE_UNAVAILABLE", "OS_ARGUMENTS_UNAVAILABLE");
    }

    @Test
    void credentialsAreRedactedButOriginalArgumentDigestsRemainDistinct() {
        List<String> one = List.of("--tapstate.store.mongo.uri=mongodb://alice:secret_one@localhost:123/control?password=query_secret&replicaSet=rs0",
                "-Dtoken=property_secret", "--password", "bare_secret");
        var first = snapshot(91, START, "/owned/java", one, true);
        var second = snapshot(91, START, "/owned/java", List.of("-Dtoken=different_secret"), true);
        String exported = JsonWriter.write(first.evidence());
        assertThat(exported).doesNotContain("alice", "secret_one", "query_secret", "property_secret", "bare_secret")
                .contains("localhost:123/control", "replicaSet=rs0", "[REDACTED]");
        assertThat(first.evidence().get("argumentsSha256")).isNotEqualTo(second.evidence().get("argumentsSha256"));
    }

    @Test
    void aJavaPathSuffixCannotTurnASensitiveArgumentValueIntoAnExecutable() {
        String property = "-Dpassword=/fixture-private/java";
        var captured = snapshot(91, START, "/owned/java", List.of(property), true);
        assertThat(JsonWriter.write(captured.evidence())).doesNotContain("/fixture-private/java");
        assertThat(BenchmarkOwnedProcessReceipt.sanitizedArguments(List.of(property)))
                .containsExactly("-Dpassword=[REDACTED]");
    }

    @Test
    void snapshotCopiesArgumentsAndExportsImmutableEvidence() {
        List<String> mutable = new ArrayList<>(COMMAND.subList(1, 4));
        var frozen = snapshot(91, START, COMMAND.getFirst(), mutable, true); mutable.clear();
        assertThat(frozen.arguments()).containsExactlyElementsOf(COMMAND.subList(1, 4));
        assertThatThrownBy(() -> frozen.arguments().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> frozen.evidence().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void excessiveStringsArgumentCountsAndAggregateBytesAreRejectedBeforeCopying() {
        assertThatThrownBy(() -> snapshot(91, START, "x".repeat(8193), List.of(), true)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> snapshot(91, START, "/java", java.util.Collections.nCopies(513, "x"), true)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> snapshot(91, START, "/java", java.util.Collections.nCopies(100, "x".repeat(8192)), true)).isInstanceOf(AssertionError.class);
    }

    @Test
    void jvmIdentityUsesItsOwnStartMillisAndRequiresActualPidAndReadBracket() {
        var packet = runtimePacket(91, 12345, 10, 11);
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(packet, 91, 12345L)).isEmpty();
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(packet, 91, 12346L)).contains("CHILD_JVM_START_CHANGED");
        var wrongActualPid = new java.util.LinkedHashMap<>(packet); wrongActualPid.put("actualPid", 92L);
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(wrongActualPid, 91, null)).contains("CHILD_JVM_ACTUAL_PID_MISMATCH");
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(runtimePacket(91, 12345, 11, 10), 91, null)).contains("CHILD_JVM_READ_BRACKET_INVALID");
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(runtimePacket(91, 12345, Long.MIN_VALUE, Long.MAX_VALUE), 91, null)).contains("CHILD_JVM_READ_BRACKET_INVALID");
        var fractional = new java.util.LinkedHashMap<>(packet); fractional.put("ownedPid", 91.0);
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(fractional, 91, null)).contains("CHILD_JVM_PID_MISMATCH");
    }

    @Test
    void incompleteOrUnboundJvmPacketsCannotBeUpgradedByKnownRuntimeStrings() {
        var packet = new java.util.LinkedHashMap<>(runtimePacket(91, 12345, 10, 11));
        packet.put("status", "UNKNOWN"); packet.put("aliveAfter", false); packet.remove("inputArguments");
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(packet, 91, null))
                .contains("CHILD_JVM_RUNTIME_INCOMPLETE", "CHILD_JVM_NOT_ALIVE_DURING_READ", "CHILD_JVM_ARGUMENTS_UNAVAILABLE");
        assertThat(BenchmarkOwnedProcessReceipt.runtimeReasons(null, 91, null)).contains("CHILD_JVM_RUNTIME_UNAVAILABLE");
    }

    @Test
    void launchEnvironmentRequiresEveryWhitelistedValueOrExplicitAbsenceAndRedactsOptions() {
        List<String> keys = List.of("LANG", "LC_ALL", "LC_CTYPE", "TZ", "JAVA_HOME", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "MAVEN_OPTS");
        Map<String, Object> packet = Map.of("status", "COMPLETE", "scope", "OWNED_PROCESS_BUILDER_BEFORE_START",
                "values", Map.of("JAVA_TOOL_OPTIONS", "-Xmx128m -Dpassword=environment_secret"),
                "absentKeys", keys.stream().filter(key -> !key.equals("JAVA_TOOL_OPTIONS")).toList());
        var safe = BenchmarkOwnedProcessReceipt.environmentEvidence(packet);
        assertThat(safe).containsEntry("status", "COMPLETE");
        assertThat(JsonWriter.write(safe)).doesNotContain("environment_secret").contains("-Xmx128m", "[REDACTED]");
        var missing = new java.util.LinkedHashMap<>(packet); missing.put("absentKeys", List.of());
        assertThat(BenchmarkOwnedProcessReceipt.environmentEvidence(missing)).containsEntry("status", "UNKNOWN");
        var overlapping = new java.util.LinkedHashMap<>(packet); overlapping.put("absentKeys", keys);
        assertThat(BenchmarkOwnedProcessReceipt.environmentEvidence(overlapping)).containsEntry("status", "UNKNOWN");
        var wrongScope = new java.util.LinkedHashMap<>(packet); wrongScope.put("scope", "INFERRED_INHERITANCE");
        assertThat(BenchmarkOwnedProcessReceipt.environmentEvidence(wrongScope)).containsEntry("status", "UNKNOWN");
    }

    @Test
    void anOwnedBenignJavaProcessRetainsItsActualIdentityAndExitWithoutAProductLaunch(@TempDir Path scratch) throws Exception {
        Path jar = childJar(scratch), java = Path.of(System.getProperty("java.home"), "bin", "java");
        List<String> command = List.of(java.toString(), "-jar", jar.toString());
        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        RealProcessServer owned = new RealProcessServer(child, URI.create("http://127.0.0.1:1"),
                scratch.resolve("unused-server.out"), scratch.resolve("unused-staging"), command);
        try {
            BufferedReader output = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!output.ready() && child.isAlive() && System.nanoTime() - deadline < 0) { Thread.sleep(10); }
            assertThat(output.ready()).as("the owned benign JVM reached its input barrier").isTrue();
            assertThat(output.readLine()).isEqualTo("READY");
            var receipt = BenchmarkOwnedProcessReceipt.capture(owned, jar, bindings());
            receipt.verifyBeforeClose(); child.getOutputStream().close();
            assertThat(child.waitFor(5, TimeUnit.SECONDS)).isTrue();
            receipt.finishAfterClose();
            Map<String, Object> evidence = receipt.evidence();
            assertThat(evidence).containsEntry("ownedAliveAfterClose", false).containsEntry("ownedExitCode", 0)
                    .containsEntry("exitEvidenceSource", "ORIGINAL_OWNED_PROCESS_OBJECT").containsEntry("status", "UNKNOWN");
            assertThat(((List<?>) evidence.get("reasons")).contains("CHILD_LAUNCH_ENVIRONMENT_UNAVAILABLE")).isTrue();
            assertThat(((List<?>) evidence.get("reasons")).contains("CHILD_JVM_RUNTIME_NOT_RECORDED")).isTrue();
            assertThat(evidence.get("applicationSha256")).isInstanceOf(String.class);
            assertThat(((Map<?, ?>) evidence.get("productAtCapture")).get("pid")).isEqualTo(child.pid());
            assertThatThrownBy(receipt::verifyBeforeClose).isInstanceOf(AssertionError.class);
            assertThatThrownBy(receipt::finishAfterClose).isInstanceOf(AssertionError.class);
            assertThat(receipt.evidence()).isSameAs(evidence);
        } finally {
            if (child.isAlive()) { child.destroyForcibly(); child.waitFor(5, TimeUnit.SECONDS); }
        }
    }

    private static BenchmarkOwnedProcessReceipt.Snapshot snapshot(long pid, Instant start, String executable,
            List<String> arguments, boolean alive) {
        return new BenchmarkOwnedProcessReceipt.Snapshot(pid, start, executable, arguments, alive, null);
    }
    private static Map<String, Object> runtimePacket(long pid, long start, long begin, long end) {
        return Map.ofEntries(Map.entry("status", "COMPLETE"), Map.entry("scope", "EXISTING_OWNED_RESOURCE_JMX_PRE_WINDOW"),
                Map.entry("ownedPid", pid), Map.entry("actualPid", pid), Map.entry("jvmStartTimeMillis", start),
                Map.entry("vmName", "owned VM"), Map.entry("vmVersion", "21"), Map.entry("vmVendor", "owned vendor"),
                Map.entry("inputArguments", List.of("-Xmx128m")), Map.entry("driverStartNanos", begin),
                Map.entry("driverEndNanos", end), Map.entry("aliveBefore", true), Map.entry("aliveAfter", true));
    }
    private static Map<String, Object> bindings() {
        return Map.of("namespace", "owned_receipt", "sourceSettings", Map.of("host", "127.0.0.1", "port", 1, "database", "source"),
                "storeUri", "mongodb://localhost:1/control", "externalTargetUri", "mongodb://localhost:1/target",
                "managedViewsUri", "mongodb://localhost:1/views", "operatorDatabase", "operator", "operatorStateUri", "mongodb://localhost:1/operator");
    }
    private static Path childJar(Path scratch) throws Exception {
        Manifest manifest = new Manifest(); manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, Child.class.getName());
        Path jar = scratch.resolve("benign-owned-child.jar");
        String resource = Child.class.getName().replace('.', '/') + ".class";
        try (var bytes = Child.class.getClassLoader().getResourceAsStream(resource);
                var output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            assertThat(bytes).isNotNull(); byte[] body = bytes.readNBytes(65537); assertThat(body.length).isLessThanOrEqualTo(65536);
            output.putNextEntry(new JarEntry(resource)); output.write(body); output.closeEntry();
        }
        return jar;
    }
    public static final class Child {
        public static void main(String[] args) throws Exception {
            System.out.println("READY"); System.out.flush(); while (System.in.read() != -1) { }
        }
    }
}
