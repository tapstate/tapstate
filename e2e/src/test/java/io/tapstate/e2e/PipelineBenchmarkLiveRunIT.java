package io.tapstate.e2e;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Explicit, full-size live benchmark entrypoint with incremental raw evidence. */
class PipelineBenchmarkLiveRunIT {

    private static final String PREFIX = "tapstate.e2e.benchmark.";
    private static final String BASELINE = PREFIX + "baseline-jar";
    private static final String CANDIDATE = PREFIX + "candidate-jar";
    private static final String OUTPUT = PREFIX + "output";
    private static final String GATE = PREFIX + "gate";
    private static final String TARGET = PREFIX + "target";
    private static final String PRIMARY = PREFIX + "primary";

    @Test
    void interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate() throws Exception {
        boolean requested = List.of(BASELINE, CANDIDATE, OUTPUT, GATE, TARGET, PRIMARY).stream()
                .anyMatch(property -> System.getProperty(property) != null);
        Assumptions.assumeTrue(requested,
                "no live benchmark properties supplied; this full-size run is opt-in");

        Path output = Path.of(required(OUTPUT));
        Path harnessRoot = harnessRoot();
        requireSafeOutput(output, harnessRoot);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        try {
            Map<String, Object> harness = harnessRevision(harnessRoot);
            RunConfig config = readConfig(output);
            Map<String, Object> inputs = inputs(config);
            inputs.put("harness", harness);
            Map<String, Object> environment = environment();
            report.begin(inputs, environment, workloads());

            RealBenchmarkForkDriver driver = new RealBenchmarkForkDriver();
            PipelineBenchmarkHarness.Report result = PipelineBenchmarkHarness.run(
                    config.gate(), config.baselineJar(), config.candidateJar(),
                    config.target(), config.primary(), (workload, arm, armFork, jar) -> {
                        Instant startedAt = Instant.now();
                        PipelineBenchmarkHarness.ForkResult completed = driver.run(workload, arm, armFork, jar);
                        RealBenchmarkForkDriver.Evidence evidence = driver.evidence().getLast();
                        String expectedId = workload.id() + "-" + arm + "-" + armFork;
                        if (!expectedId.equals(evidence.forkId())
                                || !expectedId.equals(completed.correctness().id())) {
                            throw new AssertionError("completed fork evidence has the wrong identity: " + expectedId);
                        }
                        report.addFork(fork(evidence, completed, startedAt));
                        return completed;
                    });
            PipelineBenchmarkComparison.Evaluation evaluation = result.evaluation();
            report.finish(evaluation(evaluation), evaluation.passed());
            if (!evaluation.passed()) {
                throw new AssertionError("live benchmark gate failed: " + evaluation.failures()
                        + "; raw evidence: " + report.output());
            }
        } catch (Exception | Error failure) {
            try {
                report.fail(failure);
            } catch (RuntimeException reportFailure) {
                failure.addSuppressed(reportFailure);
            }
            throw failure;
        }
    }

    private static RunConfig readConfig(Path output) {
        Path baseline = jar(BASELINE);
        Path candidate = jar(CANDIDATE);
        PipelineBenchmarkHarness.Gate gate = named(PipelineBenchmarkHarness.Gate.class, required(GATE));
        PipelineBenchmarkComparison.Workload target = optional(TARGET) == null ? null
                : named(PipelineBenchmarkComparison.Workload.class, required(TARGET));
        PipelineBenchmarkComparison.PrimaryMetric primary = optional(PRIMARY) == null ? null
                : named(PipelineBenchmarkComparison.PrimaryMetric.class, required(PRIMARY));
        if (gate == PipelineBenchmarkHarness.Gate.BUSINESS_OPTIMIZATION
                && (target == null || primary == null)) {
            throw new IllegalArgumentException("business optimization requires a target and primary metric");
        }
        if (gate == PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST
                && (target != null || primary != null)) {
            throw new IllegalArgumentException("observability cost takes no optimization target or primary metric");
        }
        if (!ConnectorJars.directoryNamed()) {
            throw new IllegalArgumentException("live benchmark requires tapstate.e2e.connectors-dir");
        }
        return new RunConfig(baseline, candidate, output, gate, target, primary);
    }

    private static Path harnessRoot() throws Exception {
        Command result = git(Path.of("").toAbsolutePath(), "rev-parse", "--show-toplevel");
        if (result.exitCode() != 0 || result.output().isBlank()) {
            throw new AssertionError("cannot locate the benchmark harness repository: " + result.output());
        }
        return Path.of(result.output().trim()).toAbsolutePath().normalize();
    }

    private static void requireSafeOutput(Path output, Path root) throws Exception {
        if (!output.isAbsolute() || output.getFileName() == null) {
            throw new IllegalArgumentException("benchmark output must be an absolute file path");
        }
        Path normalized = output.toAbsolutePath().normalize();
        if (!normalized.startsWith(root)) {
            return;
        }
        Command ignored = git(root, "check-ignore", "-q", "--", root.relativize(normalized).toString());
        if (ignored.exitCode() == 0) {
            return;
        }
        if (ignored.exitCode() == 1) {
            throw new IllegalArgumentException("benchmark output inside the checkout must be ignored: " + output);
        }
        throw new AssertionError("cannot check whether benchmark output is ignored: " + ignored.output());
    }

    private static Map<String, Object> harnessRevision(Path root) throws Exception {
        Command revision = git(root, "rev-parse", "HEAD");
        if (revision.exitCode() != 0 || !revision.output().trim().matches("[0-9a-f]{40,64}")) {
            throw new AssertionError("cannot identify the benchmark harness revision: " + revision.output());
        }
        Command status = git(root, "status", "--porcelain=v1", "--untracked-files=all");
        if (status.exitCode() != 0) {
            throw new AssertionError("cannot inspect benchmark harness cleanliness: " + status.output());
        }
        if (!status.output().isBlank()) {
            throw new AssertionError("benchmark harness source worktree is not clean: " + status.output());
        }
        return object("revision", revision.output().trim(), "repository", root.toString(), "clean", true);
    }

    private static Command git(Path directory, String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add("git");
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command).directory(directory.toFile())
                .redirectErrorStream(true).start();
        if (!process.waitFor(20, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("git did not answer within 20 seconds: " + command);
        }
        String output = new String(process.getInputStream().readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8);
        return new Command(process.exitValue(), output);
    }

    private static Path jar(String property) {
        Path jar = Path.of(required(property)).toAbsolutePath().normalize();
        if (!Files.isRegularFile(jar)) {
            throw new IllegalArgumentException(property + " is not a regular file: " + jar);
        }
        return jar;
    }

    private static String required(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("live benchmark requires -D" + property);
        }
        return value;
    }

    private static String optional(String property) {
        return System.getProperty(property);
    }

    private static <T extends Enum<T>> T named(Class<T> type, String value) {
        try {
            return Enum.valueOf(type, value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (IllegalArgumentException unknown) {
            throw new IllegalArgumentException("unsupported live benchmark choice " + value
                    + " for " + type.getSimpleName(), unknown);
        }
    }

    private static Map<String, Object> inputs(RunConfig config) {
        Map<String, Object> inputs = object(
                "gate", config.gate().name(),
                "target", config.target() == null ? null : config.target().name(),
                "primary", config.primary() == null ? null : config.primary().name(),
                "output", config.output().toString(),
                "apiVersion", System.getProperty("api.version"),
                "baseline", artifact(config.baselineJar()),
                "candidate", artifact(config.candidateJar()));
        Map<String, Object> connectors = new LinkedHashMap<>();
        for (String id : List.of("mysql", "postgres", "mongodb")) {
            connectors.put(id, artifact(ConnectorJars.pathFor(id)));
        }
        inputs.put("connectors", connectors);
        inputs.put("schedule", PipelineBenchmarkComparison.schedule().stream()
                .map(Enum::name).toList());
        inputs.put("expectedForks", BenchmarkWorkloadDefinitions.all().size()
                * PipelineBenchmarkComparison.schedule().size());
        return inputs;
    }

    private static Map<String, Object> artifact(Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        try {
            return object("path", absolute.toString(), "sizeBytes", Files.size(absolute),
                    "sha256", sha256(absolute));
        } catch (IOException error) {
            throw new IllegalStateException("cannot inspect benchmark artifact " + absolute, error);
        }
    }

    private static String sha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
        try (InputStream source = new DigestInputStream(Files.newInputStream(path), digest)) {
            byte[] buffer = new byte[64 * 1024];
            while (source.read(buffer) != -1) {
                // DigestInputStream updates the digest as bytes are read.
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static Map<String, Object> environment() throws Exception {
        java.lang.management.OperatingSystemMXBean operatingSystem = ManagementFactory.getOperatingSystemMXBean();
        Object totalMemory = operatingSystem instanceof com.sun.management.OperatingSystemMXBean extended
                ? extended.getTotalMemorySize() : "UNAVAILABLE";
        return object(
                "capturedAt", Instant.now().toString(),
                "host", object("os", System.getProperty("os.name"),
                        "version", System.getProperty("os.version"),
                        "architecture", System.getProperty("os.arch"),
                        "logicalProcessors", operatingSystem.getAvailableProcessors(),
                        "physicalMemoryBytes", totalMemory),
                "jdk", object("version", System.getProperty("java.version"),
                        "vendor", System.getProperty("java.vendor"),
                        "vm", System.getProperty("java.vm.name"),
                        "maxHeapBytes", Runtime.getRuntime().maxMemory()),
                "docker", dockerInfo());
    }

    private static Map<String, Object> dockerInfo() throws Exception {
        String format = "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OSType}}|{{.Architecture}}";
        Process command = new ProcessBuilder("docker", "info", "--format", format)
                .redirectErrorStream(true).start();
        if (!command.waitFor(20, TimeUnit.SECONDS)) {
            command.destroyForcibly();
            throw new AssertionError("docker info did not answer within 20 seconds");
        }
        String response = new String(command.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                .trim();
        if (command.exitValue() != 0) {
            throw new AssertionError("docker info failed: " + response);
        }
        String[] values = response.split("\\|", -1);
        if (values.length != 5) {
            throw new AssertionError("docker info returned incomplete resource metadata: " + response);
        }
        return object("serverVersion", values[0], "cpus", Long.parseLong(values[1]),
                "memoryBytes", Long.parseLong(values[2]), "osType", values[3],
                "architecture", values[4]);
    }

    private static List<Map<String, Object>> workloads() {
        List<Map<String, Object>> all = new ArrayList<>();
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            List<Map<String, Object>> phases = new ArrayList<>();
            for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases()) {
                phases.add(object("id", phase.id(), "stage", phase.stage().name(),
                        "measured", phase.measured(), "sql", phase.sql(),
                        "statementsPerBatch", phase.statementsPerBatch(),
                        "batchIntervalNanos", phase.batchInterval().toNanos(),
                        "expectedLogicalOutputChanges", phase.expectedLogicalOutputChanges(),
                        "declaredCoverage", phase.expectedLogicalCoverage(),
                        "targets", phase.targets().stream().map(target -> object(
                                "pipelineId", target.pipelineId(), "location", target.location().name(),
                                "table", target.table(), "projection", target.projection().name(),
                                "rows", target.rows(), "checksum", target.checksum())).toList()));
            }
            all.add(object("id", workload.id(), "seed", workload.seed(),
                    "database", workload.database().name(), "pipelineIds", workload.pipelineIds(),
                    "sourceChains", workload.sourceChains().stream().map(chain -> object(
                            "id", chain.id(), "pipelineId", chain.pipelineId(),
                            "sourceId", chain.sourceId(), "table", chain.table(),
                            "terminalLogicalId", chain.terminalLogicalId(),
                            "terminalRowId", chain.terminalRowId())).toList(),
                    "setupSql", workload.setupSql(), "phases", phases));
        }
        return List.copyOf(all);
    }

    private static Map<String, Object> fork(RealBenchmarkForkDriver.Evidence evidence,
            PipelineBenchmarkHarness.ForkResult result, Instant startedAt) {
        PipelineBenchmarkComparison.Fork performance = result.measurement();
        BenchmarkAckOracle.Fork correctness = result.correctness();
        long[] durations = performance.deliveryNanos();
        long[] sorted = durations.clone();
        Arrays.sort(sorted);
        if (sorted.length == 0 || sorted[0] <= 0) {
            throw new AssertionError("completed benchmark fork has no positive delivery durations");
        }
        long p99 = sorted[(int) Math.ceil(sorted.length * 0.99) - 1];
        BenchmarkResourceSampler.Summary resources = evidence.resources();
        BenchmarkMongoCommandSampler.Summary commands = evidence.mongoCommands();
        return object(
                "id", evidence.forkId(), "workload", evidence.workload().id(),
                "seed", evidence.workload().seed(), "arm", evidence.arm().name(),
                "applicationJar", evidence.applicationJar().toString(),
                "startedAt", startedAt.toString(), "completedAt", Instant.now().toString(),
                "throughputRecordsPerSecond", performance.recordsOutPerSecond(),
                "deliveryP99Nanos", p99, "deliveryNanos", Arrays.stream(durations).boxed().toList(),
                "resources", object("cpuNanos", resources.cpuNanos(),
                        "gcPauseMillis", resources.gcPauseMillis(),
                        "peakHeapBytes", resources.peakHeapBytes(),
                        "peakRssBytes", resources.peakRssBytes(),
                        "sampleCount", resources.sampleCount()),
                "mongoCommands", object("byCommand", new TreeMap<>(commands.byCommand()),
                        "byFamily", namedCounts(commands.byFamily()),
                        "elapsedMillis", commands.elapsedMillis()),
                "measuredPhaseWindows", evidence.phases().stream().map(phase -> object(
                        "id", phase.id(), "acknowledgedOutputs", phase.acknowledgedOutputs(),
                        "firstIssuedAtNanos", phase.firstIssuedAtNanos(),
                        "sourceCompletedAtNanos", phase.sourceCompletedAtNanos(),
                        "sourceIssueDurationNanos",
                        phase.sourceCompletedAtNanos() - phase.firstIssuedAtNanos(),
                        "expectedSourceChanges", phase.expectedSourceChanges(),
                        "completedAckAtNanos", phase.completedAckAtNanos(),
                        "durationNanos", phase.completedAckAtNanos() - phase.firstIssuedAtNanos(),
                        "throughputRecordsPerSecond", phase.recordsOutPerSecond(),
                        "observedDeliveries", phase.observedDeliveries(),
                        "reportedRecordsOut", phase.reportedRecordsOut(),
                        "idempotentWriteOverhead",
                        phase.reportedRecordsOut() - phase.acknowledgedOutputs())).toList(),
                "declaredSourceCoverage", evidence.declaredSourceCoverage(),
                "observedTargetCoverage", evidence.observedTargetCoverage(),
                "logicalCoverage", correctness.logicalCoverage(),
                "sourceChains", correctness.chains().stream().map(chain -> object(
                        "id", chain.id(), "authoritativeTargetAck", chain.authoritativeTargetAck(),
                        "sourceTerminals", chain.sourceTerminals().stream().map(event -> object(
                                "logicalId", event.logicalId(),
                                "sourcePosition", event.sourcePosition())).toList())).toList(),
                "checksum", evidence.checksum(),
                "correctnessChecksum", correctness.checksum(),
                "errorTotal", evidence.errorTotal());
    }

    private static Map<String, Object> evaluation(PipelineBenchmarkComparison.Evaluation evaluation) {
        Map<String, Object> workloads = new LinkedHashMap<>();
        evaluation.workloads().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(entry -> workloads.put(entry.getKey().name(), object(
                        "baseline", summary(entry.getValue().baseline()),
                        "candidate", summary(entry.getValue().candidate()))));
        return object("passed", evaluation.passed(), "failures", evaluation.failures(),
                "workloads", workloads);
    }

    private static Map<String, Object> summary(PipelineBenchmarkComparison.Summary summary) {
        return object("throughput", summary.throughput(),
                "throughputRelativeMad", summary.throughputRelativeMad(),
                "deliveryP99Nanos", summary.deliveryP99Nanos(),
                "p99RelativeMad", summary.p99RelativeMad(),
                "peakHeapBytes", summary.peakHeapBytes(),
                "peakRssBytes", summary.peakRssBytes());
    }

    private static Map<String, Object> namedCounts(Map<?, Long> counts) {
        Map<String, Object> named = new TreeMap<>();
        counts.forEach((key, count) -> named.put(String.valueOf(key), count));
        return named;
    }

    private static Map<String, Object> object(Object... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("JSON object fields come in key/value pairs");
        }
        Map<String, Object> object = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            object.put((String) pairs[index], pairs[index + 1]);
        }
        return object;
    }

    private record RunConfig(Path baselineJar, Path candidateJar, Path output,
            PipelineBenchmarkHarness.Gate gate,
            PipelineBenchmarkComparison.Workload target,
            PipelineBenchmarkComparison.PrimaryMetric primary) {}

    private record Command(int exitCode, String output) {}
}
