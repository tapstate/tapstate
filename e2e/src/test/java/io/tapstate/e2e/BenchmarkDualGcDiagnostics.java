package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Startup event logging for the owned driver and one owned product, with no additional attach. */
final class BenchmarkDualGcDiagnostics {
    static final String ENABLED_PROPERTY = "tapstate.e2e.benchmark-smoke.dual-gc-diagnostics";
    static final String DIRECTORY_PROPERTY = "tapstate.e2e.benchmark-smoke.dual-gc-directory";
    private static final String LOG_SUFFIX = ":utctime,uptimenanos,level,tags:filecount=1,filesize=4m";

    private BenchmarkDualGcDiagnostics() { }

    static Session open() {
        String configured = System.getProperty(DIRECTORY_PROPERTY);
        if (configured == null || configured.isBlank()) {
            throw new AssertionError("owned dual GC diagnostics require a directory");
        }
        Path directory = Path.of(configured);
        if (!directory.isAbsolute() || !directory.equals(directory.normalize())
                || !Files.isDirectory(directory) || configured.contains(":")) {
            throw new AssertionError("owned dual GC diagnostics require an existing normalized absolute directory without a colon");
        }
        var runtime = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtime.getInputArguments();
        String driverOption = logOption(directory.resolve("driver-%p.log"));
        if (!arguments.contains(driverOption)) {
            throw new AssertionError("the owned driver was not launched with its registered GC logging option");
        }
        if (arguments.stream().anyMatch(argument -> argument.startsWith("-XX:StartFlightRecording")
                || argument.startsWith("-javaagent:") || argument.startsWith("-agentlib:jdwp"))) {
            throw new AssertionError("owned dual GC diagnostics require a driver without JFR, Java agents or JDWP");
        }
        for (String variable : List.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "MAVEN_OPTS")) {
            String value = System.getenv(variable);
            if (value != null && !value.isBlank()) {
                throw new AssertionError("owned dual GC diagnostics require absent inherited JVM and Maven options");
            }
        }
        long driverPid = ProcessHandle.current().pid();
        Path driverLog = directory.resolve("driver-" + driverPid + ".log");
        if (!Files.isRegularFile(driverLog)) {
            throw new AssertionError("the owned driver GC log is absent");
        }
        Session session = new Session(directory, Map.of(
                "pid", driverPid,
                "log", driverLog.toString(),
                "jvmLoggingArguments", List.of(driverOption),
                "jvmStartedAtUtc", Instant.ofEpochMilli(runtime.getStartTime()).toString(),
                "metadataObservedAtUtc", Instant.now().toString(),
                "metadataObservedUptimeMillis", runtime.getUptime()));
        System.out.println("benchmark-dual-gc-driver=" + JsonWriter.write(session.evidence()));
        return session;
    }

    private static String logOption(Path file) {
        return "-Xlog:gc*,safepoint:file=" + file + LOG_SUFFIX;
    }

    static final class Session {
        private final Path directory;
        private final Map<String, Object> driver;
        private Map<String, Object> product = Map.of("state", "NOT_LAUNCHED");

        private Session(Path directory, Map<String, Object> driver) {
            this.directory = directory;
            this.driver = driver;
        }

        BenchmarkForkEnvironment.OwnedBoot start(String storeUri, String operatorDatabase, Path jar)
                throws Exception {
            if (product.containsKey("pid")) {
                throw new AssertionError("owned dual GC diagnostics allow only one product launch");
            }
            List<String> options = List.of(logOption(directory.resolve("product-%p.log")));
            RealProcessServer server = RealProcessServer.start(storeUri, operatorDatabase, jar, "127.0.0.1",
                    ignored -> List.of(), options);
            try {
                Path log = directory.resolve("product-" + server.pid() + ".log");
                if (!Files.isRegularFile(log)) {
                    throw new AssertionError("the owned product GC log is absent");
                }
                product = Map.of("pid", server.pid(), "log", log.toString(),
                        "applicationJar", jar.toString(), "jvmLoggingArguments", options,
                        "metadataObservedAtUtc", Instant.now().toString());
                System.out.println("benchmark-dual-gc-product=" + JsonWriter.write(evidence()));
                return new BenchmarkForkEnvironment.OwnedBoot(server, null);
            } catch (Exception | Error failure) {
                try { server.close(); }
                catch (Exception | Error cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                throw failure;
            }
        }

        Map<String, Object> evidence() {
            return Map.of("schemaVersion", 1, "enabled", true,
                    "eventScope", "GC_AND_SAFEPOINT_STARTUP_LOGGING_ONLY",
                    "jfrEnabled", false, "rotationFileCount", 1, "rotationFileSizeBytes", 4 * 1024 * 1024,
                    "driver", driver, "product", product,
                    "performanceAcceptanceEligible", false,
                    "additionalLiveAttach", false);
        }
    }
}
