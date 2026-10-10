package io.tapstate.e2e;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
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
import java.util.Optional;
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
        if (System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY) != null) {
            throw new AssertionError("native clock diagnostic cannot establish a live performance gate");
        }
        if (Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY)) {
            throw new AssertionError("producer cost-stage diagnostics cannot establish a live performance gate");
        }
        if (Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY)) {
            throw new AssertionError("return method cost control cannot establish a live performance gate");
        }
        if (Boolean.getBoolean("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics")) {
            throw new AssertionError("owned JVM diagnostics cannot establish a live performance gate");
        }
        if (Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY)) {
            throw new AssertionError("return clock cost control cannot establish a live performance gate");
        }
        if (Boolean.getBoolean(BenchmarkWitnessReadGate.PROPERTY)) {
            throw new AssertionError("deferred target witness cannot establish a live performance gate");
        }
        if (Boolean.getBoolean(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY)
                || Boolean.getBoolean(BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY)) {
            throw new AssertionError("clock refusal evidence cannot establish a live performance gate");
        }
        boolean requested = List.of(BASELINE, CANDIDATE, OUTPUT, GATE, TARGET, PRIMARY).stream()
                .anyMatch(property -> System.getProperty(property) != null);
        Assumptions.assumeTrue(requested,
                "no live benchmark properties supplied; this full-size run is opt-in");
        if (Boolean.getBoolean("tapstate.e2e.benchmark.compilation-diagnostics")) {
            throw new AssertionError("compilation diagnostics cannot establish a live performance gate");
        }
        if (Boolean.getBoolean("tapstate.e2e.benchmark.thread-point-diagnostics")) {
            throw new AssertionError("thread point diagnostics cannot establish a live performance gate");
        }
        BenchmarkTargetClock.requireOperationTimeErrorBound();

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
            var steadyWorkloads = BenchmarkWorkloadDefinitions.all().stream()
                    .map(workload -> BenchmarkWorkloadDefinitions.steadyPilot(workload.id())).toList();
            inputs.put("measurementMethod", BenchmarkTargetClock.OPERATION_DATE_MEASUREMENT_METHOD);
            inputs.put("steadyProfileRows", BenchmarkWorkloadDefinitions.STEADY_PILOT_ROWS);
            inputs.put("loadDiagnosticsEnabled", Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics"));
            report.begin(inputs, environment, workloads(steadyWorkloads));

            RealBenchmarkForkDriver driver = new RealBenchmarkForkDriver();
            List<String> qualificationFailures = new ArrayList<>();
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
                        try { evidence.requireSteadyStateWindow(); }
                        catch (AssertionError unqualified) {
                            qualificationFailures.add(expectedId + ": " + unqualified.getMessage());
                        }
                        return completed;
                    }, steadyWorkloads);
            PipelineBenchmarkComparison.Evaluation evaluation = result.evaluation();
            boolean passed = evaluation.passed() && qualificationFailures.isEmpty();
            Map<String, Object> finalEvaluation = qualifiedEvaluation(evaluation, qualificationFailures);
            report.finish(finalEvaluation, passed);
            if (!passed) {
                throw new AssertionError("live benchmark gate failed: " + evaluation.failures()
                        + "; qualification failures: " + qualificationFailures
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

    static Path harnessRoot() throws Exception {
        Command result = git(Path.of("").toAbsolutePath(), "rev-parse", "--show-toplevel");
        if (result.exitCode() != 0 || result.output().isBlank()) {
            throw new AssertionError("cannot locate the benchmark harness repository: " + result.output());
        }
        return Path.of(result.output().trim()).toAbsolutePath().normalize();
    }

    static void requireSafeOutput(Path output, Path root) throws Exception {
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

    static Map<String, Object> harnessRevision(Path root) throws Exception {
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

    static Map<String, Object> artifact(Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        try {
            return object("path", absolute.toString(), "sizeBytes", Files.size(absolute),
                    "sha256", sha256(absolute));
        } catch (IOException error) {
            throw new IllegalStateException("cannot inspect benchmark artifact " + absolute, error);
        }
    }

    static String sha256(Path path) throws IOException {
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

    static Map<String, Object> environment() throws Exception {
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
                "docker", dockerInfo(),
                "testFixtures", object("mongoWiredTigerCacheGiB", SharedMongo.configuredCacheBudget()
                        .map(BigDecimal::toPlainString).orElse("SYSTEM_DEFAULT")));
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

    static List<Map<String, Object>> workloads() {
        return workloads(BenchmarkWorkloadDefinitions.all());
    }

    static List<Map<String, Object>> workloads(List<BenchmarkWorkloadDefinitions.Workload> definitions) {
        List<Map<String, Object>> all = new ArrayList<>();
        for (BenchmarkWorkloadDefinitions.Workload workload : definitions) {
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

    static Map<String, Object> fork(RealBenchmarkForkDriver.Evidence evidence,
            PipelineBenchmarkHarness.ForkResult result, Instant startedAt) {
        PipelineBenchmarkComparison.Fork performance = result.measurement();
        BenchmarkAckOracle.Fork correctness = result.correctness();
        BenchmarkAckOracle.verify(List.of(correctness), PipelineBenchmarkHarness.expectedTerminals(evidence.workload()));
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
                "runtimeEvidence", evidence.runtimeEvidence(),
                "startedAt", startedAt.toString(), "completedAt", Instant.now().toString(),
                "throughputRecordsPerSecond", performance.recordsOutPerSecond(),
                "deliveryP99Nanos", p99, "deliveryNanos", Arrays.stream(durations).boxed().toList(),
                "resources", object("cpuNanos", resources.cpuNanos(),
                        "gcCollectionMillis", resources.gcCollectionMillis(),
                        "peakHeapBytes", resources.peakHeapBytes(),
                        "peakRssBytes", resources.peakRssBytes(),
                        "sampleCount", resources.sampleCount()),
                "mongoCommands", object("byCommand", new TreeMap<>(commands.byCommand()),
                        "byFamily", namedCounts(commands.byFamily()),
                        "elapsedMillis", commands.elapsedMillis()),
                "measuredPhaseWindows", evidence.phases().stream()
                        .map(PipelineBenchmarkLiveRunIT::phaseEvidence).toList(),
                "declaredSourceCoverage", evidence.declaredSourceCoverage(),
                "observedTargetCoverage", evidence.observedTargetCoverage(),
                "logicalCoverage", correctness.logicalCoverage(),
                "terminalMetaReceipts", evidence.terminalMetaReceipts(),
                "sourceChains", correctness.chains().stream().map(chain -> object(
                        "id", chain.id(), "authoritativeTargetAck", chain.authoritativeTargetAck(),
                        "ackProofKind", chain.tableConfirmation() == null ? "SOURCE_TOKEN" : "TABLE_ORDER",
                        "tableConfirmation", tableConfirmation(chain.tableConfirmation()),
                        "sourceTerminals", chain.sourceTerminals().stream().map(event -> object(
                                "logicalId", event.logicalId(),
                                "sourcePosition", event.sourcePosition())).toList())).toList(),
                "checksum", evidence.checksum(),
                "correctnessChecksum", correctness.checksum(),
                "errorTotal", evidence.errorTotal());
    }

    static Map<String, Object> tableConfirmation(BenchmarkAckOracle.TableConfirmationProof proof) {
        if (proof == null) { return object("state", "NOT_RECORDED"); }
        return object("terminalLogicalId", proof.terminalLogicalId(), "sourceTerminalToken", proof.sourceTerminalToken(),
                "ring", proof.marker().ring(), "epoch", proof.marker().epoch(), "seq", proof.marker().seq(),
                "actualMarkerSourceToken", proof.marker().sourceToken(),
                "physicalChainId", proof.binding().physicalChain(), "actualConsumerId", proof.binding().consumer(),
                "table", proof.binding().table(), "expectedWriters", proof.binding().writers(),
                "confirmedConsumer", io.tapstate.core.common.JsonReader.parse(proof.confirmedConsumerCanonicalJson()));
    }

    static Map<String, Object> telemetryEvidence(Optional<BenchmarkJdiTelemetrySession.Evidence> recorded) {
        if (recorded.isEmpty()) { return object("state", "NOT_RECORDED"); }
        var capture = recorded.orElseThrow();
        boolean active = capture.mode() == BenchmarkJdiTelemetrySession.Mode.ACTIVE_CAPTURE;
        Map<String, Object> result = object("state", "RECORDED", "mode", capture.mode().name(),
                "performanceAcceptanceEligible", false, "artifactSha256", capture.artifactSha256(),
                "scope", "BEFORE_MEASURED_GROUP_TO_BEFORE_TERMINAL", "accounting", "INVOCATION_ENTRY_COHORTS",
                "availableFeatures", capture.available().stream().map(Enum::name).sorted().toList(),
                "unavailable", capture.unavailable().stream().map(Enum::name).sorted().toList(),
                "allBreakpointEvents", capture.events(), "observerHandlingNanos", capture.handlingNanos(),
                "observerHandlingScope", "MEASURED_GROUP_HANDLER_LOWER_BOUND",
                "measuredGroupBreakpointEvents", capture.windowBreakpointEvents(),
                "drainBreakpointEvents", capture.drainBreakpointEvents(),
                "drainHandlerNanos", capture.drainHandlingNanos(),
                "drainHandlerScope", "POST_CUTOFF_HANDLER_LOWER_BOUND", "scopedAccountingDrained", capture.fullyDrained(),
                "begin", telemetryBoundary(capture.begin()), "cutoff", telemetryBoundary(capture.cutoff()),
                "shutdown", capture.shutdown() == null ? object("state", "UNAVAILABLE") : telemetryBoundary(capture.shutdown()),
                "sinkDeltas", capture.deltas().entrySet().stream().map(entry -> object(
                        "sink", entry.getKey().name(), "coalesced", entry.getValue().coalesced(),
                        "dropped", entry.getValue().dropped(), "successes", entry.getValue().successes(),
                        "failures", entry.getValue().failures(), "timeouts", entry.getValue().timeouts())).toList());
        result.put("scopedCosts", active ? object("state", "RECORDED", "costs", capture.costs().entrySet().stream()
                .map(entry -> object("segment", entry.getKey().segment().name(), "origin", entry.getKey().origin().name(),
                        "namespace", entry.getKey().namespace().name(), "unit", entry.getKey().unit().name(),
                        "entries", entry.getValue().entries(), "normalReturns", entry.getValue().normalReturns())).toList(),
                "commands", capture.commands().entrySet().stream().map(entry -> object(
                        "segment", entry.getKey().segment().name(), "origin", entry.getKey().origin().name(),
                        "namespace", entry.getKey().wire().namespace().name(), "operation", entry.getKey().wire().operation().name(),
                        "entries", entry.getValue().entries(), "normalReturns", entry.getValue().normalReturns())).toList(),
                "callbacks", capture.callbacks().entrySet().stream().map(entry -> object(
                        "segment", entry.getKey().segment().name(), "origin", entry.getKey().origin().name(),
                        "namespace", entry.getKey().namespace().name(), "entries", entry.getValue().entries(),
                        "normalReturns", entry.getValue().normalReturns())).toList())
                : object("state", "UNAVAILABLE", "reason", "PASSIVE_JDWP"));
        return result;
    }

    private static Map<String, Object> telemetryBoundary(BenchmarkJdiTelemetrySession.Boundary boundary) {
        Map<String, Object> pending = object(
                "costs", boundary.pending().costs().entrySet().stream().map(entry -> object(
                        "segment", entry.getKey().segment().name(), "origin", entry.getKey().origin().name(),
                        "namespace", entry.getKey().namespace().name(), "unit", entry.getKey().unit().name(),
                        "open", entry.getValue())).toList(),
                "commands", boundary.pending().commands().entrySet().stream().map(entry -> object(
                        "segment", entry.getKey().segment().name(), "origin", entry.getKey().origin().name(),
                        "namespace", entry.getKey().wire().namespace().name(), "operation", entry.getKey().wire().operation().name(),
                        "open", entry.getValue())).toList(),
                "callbacks", boundary.pending().callbacks().entrySet().stream().map(entry -> object(
                        "segment", entry.getKey().segment().name(), "origin", entry.getKey().origin().name(),
                        "namespace", entry.getKey().namespace().name(), "open", entry.getValue())).toList());
        var health = boundary.health();
        return object("atNanos", boundary.atNanos(), "activeScopes", boundary.activeScopes(),
                "openCalls", boundary.openCalls(), "pending", pending,
                "health", health == null ? object("state", "UNAVAILABLE") : object(
                        "state", "RECORDED", "dispatcherObjectIdentity", health.identity(),
                        "startedAtObjectIdentity", health.epochIdentity(), "closed", health.closed(),
                        "abort", health.abort(), "latestPending", health.latestPending(), "latestSlots", health.latestSlots(),
                        "sinks", health.sinks().entrySet().stream().map(entry -> object(
                                "sink", entry.getKey().name(), "statsObjectIdentity", entry.getValue().identity(),
                                "coalesced", entry.getValue().coalesced(), "dropped", entry.getValue().dropped(),
                                "successes", entry.getValue().successes(), "failures", entry.getValue().failures(),
                                "timeouts", entry.getValue().timeouts(), "queued", entry.getValue().queued(),
                                "inFlight", entry.getValue().inFlight())).toList()));
    }

    /** Delivery timing and proof-confirmation timing remain separately reported and versioned. */
    static Map<String, Object> phaseEvidence(RealBenchmarkForkDriver.MeasuredPhase phase) {
        BenchmarkForkEnvironment.ClockAnchor anchor = phase.clockAnchor();
        var sourceIssueDuration = phase.sourceIssueDurationNanos();
        return object(
                "id", phase.id(), "acknowledgedOutputs", phase.acknowledgedOutputs(),
                "firstIssuedAtNanos", phase.firstIssuedAtNanos(),
                "sourceCompletedAtNanos", phase.sourceCompletedAtNanos(),
                "sourceIssueDurationNanos", sourceIssueDuration.isPresent() ? sourceIssueDuration.getAsLong() : null,
                "sourceIssueWindowScope", sourceIssueDuration.isPresent() ? "FULL_SOURCE_BATCH_LEDGER" : "UNAVAILABLE",
                "fullSourceFirstIssuedAtNanos", phase.sourceBatches().isEmpty()
                        ? null : phase.sourceBatches().getFirst().issuedAtNanos(),
                "expectedSourceChanges", phase.expectedSourceChanges(),
                "completedAckAtNanos", phase.completedAckAtNanos(),
                "durationNanos", phase.completedAckAtNanos() - phase.firstIssuedAtNanos(),
                "throughputRecordsPerSecond", phase.confirmationTiming().isPresent()
                        ? phase.recordsOutPerSecond() : null,
                "throughputMethod", phase.steadyOutputProfile() ? BenchmarkTargetClock.OPERATION_DATE_MEASUREMENT_METHOD : "SOURCE_ISSUE_TO_OBSERVED_TARGET_V2",
                "operationDateTimeEvidence", BenchmarkTargetClock.operationDateTimeEvidence(),
                "steadyStateEstablished", phase.steadyOutputEstablished(),
                "steadyOutputRule", "COMMON_ACTIVE_INTERVAL_TEN_PROGRESS_BINS_HALF_TREND_AT_MOST_5_PERCENT",
                "throughputWindowScope", "COMMON_ACTIVE_TARGET_INTERVAL_OPEN_START_CLOSED_END",
                "deliveryWindowNanos", phase.confirmationTiming().isPresent() ? phase.deliveryWindowNanos() : null,
                "throughputWindowCompletedDeliveries", phase.steadyOutputProfile()
                        ? phase.deliveryTimeline().orElseThrow().operationWindow().completedDeliveries() : phase.acknowledgedOutputs(),
                "outputWindowDiagnostics", phase.steadyOutputProfile()
                        ? object("fixedBins", phase.deliveryTimeline().orElseThrow().operationWindow().fixedBins(),
                                "halfTrend", Double.isFinite(phase.deliveryTimeline().orElseThrow().operationWindow().halfTrend())
                                        ? phase.deliveryTimeline().orElseThrow().operationWindow().halfTrend() : null) : null,
                "latencyCohortScope", "ALL_FIXED_MIDDLE_DELIVERIES_SOURCE_ISSUE_TO_LOCAL_OBSERVATION",
                "confirmationRecordsPerSecond", phase.confirmationRecordsPerSecond(),
                "resourceAndCommandWindowScope", "SOURCE_ISSUE_THROUGH_PROOF_CONFIRMATION",
                "targetClockQualification", phase.targetClockEvidence(),
                "operationResourceWindow", object("state", "UNQUALIFIED", "reason", "OPERATION_TIME_ERROR_BOUND_NOT_ESTABLISHED"),
                "observedDeliveries", phase.observedDeliveries(),
                "reportedRecordsOut", phase.reportedRecordsOut(),
                "confirmationTiming", phase.confirmationTiming().map(timing -> object(
                        "state", "RECORDED", "scope", "LOCAL_OBSERVER_AND_PROOF_READ_INTERVALS",
                        "sourceMarkerWaitStartedAtNanos", timing.sourceMarkerWaitStartedAtNanos(),
                        "sourceMarkerWaitCompletedAtNanos", timing.sourceMarkerWaitCompletedAtNanos(),
                        "tableConfirmationCompletedAtNanos", timing.tableConfirmationCompletedAtNanos(),
                        "sourceMarkerWaitNanos", timing.sourceMarkerWaitCompletedAtNanos() - timing.sourceMarkerWaitStartedAtNanos(),
                        "tableConfirmationNanos", timing.tableConfirmationCompletedAtNanos() - timing.sourceMarkerWaitCompletedAtNanos(),
                        "firstTargetObservedAtNanos", timing.firstTargetObservedAtNanos(),
                        "lastTargetObservedAtNanos", timing.lastTargetObservedAtNanos(),
                        "confirmationEndMinusLastTargetObservedNanos", timing.tableConfirmationCompletedAtNanos()
                                - timing.lastTargetObservedAtNanos())).orElseGet(() -> object("state", "UNAVAILABLE")),
                "idempotentWriteOverhead", phase.reportedRecordsOut() - phase.observedDeliveries(),
                "fullObservedDeliveries", phase.observedDeliveries(),
                "measurementCohortDeliveries", phase.acknowledgedOutputs(),
                "deliveryTimeline", phase.deliveryTimeline().map(timeline -> object("state", "RECORDED",
                        "scope", "LOCAL_CHANGE_STREAM_OBSERVATIONS", "firstFullObservedAtNanos", timeline.firstFullObservedAtNanos(),
                        "lastFullObservedAtNanos", timeline.lastFullObservedAtNanos(),
                        "cohortFirstObservedAtNanos", timeline.cohortFirstObservedAtNanos(),
                        "cohortLastObservedAtNanos", timeline.cohortLastObservedAtNanos(),
                        "fullDeliveryCount", timeline.fullDeliveryCount(), "cohortDeliveryCount", timeline.cohortDeliveryCount(),
                        "cohortObservedAtNanos", timeline.cohortObservedAtNanos(),
                        "fullObservedAtNanos", timeline.fullObservedAtNanos(),
                        "cohortServerOperationWallMillis", timeline.cohortServerOperationWallMillis(),
                        "cohortOperationStreams", timeline.cohortOperationStreams(),
                        "fullTimelineState", timeline.fullObservedAtNanos().isEmpty() ? "UNAVAILABLE" : "RECORDED"))
                        .orElseGet(() -> object("state", "UNAVAILABLE")),
                "firstIssuedAtUtcEarliest", anchor.earliestUtc(phase.firstIssuedAtNanos()).toString(),
                "firstIssuedAtUtcLatest", anchor.latestUtc(phase.firstIssuedAtNanos()).toString(),
                "sourceCompletedAtUtcEarliest", anchor.earliestUtc(phase.sourceCompletedAtNanos()).toString(),
                "sourceCompletedAtUtcLatest", anchor.latestUtc(phase.sourceCompletedAtNanos()).toString(),
                "completedAckAtUtcEarliest", anchor.earliestUtc(phase.completedAckAtNanos()).toString(),
                "completedAckAtUtcLatest", anchor.latestUtc(phase.completedAckAtNanos()).toString(),
                "clockAnchor", object("utc", anchor.utc().toString(),
                        "monotonicBeforeNanos", anchor.beforeNanos(),
                        "monotonicAfterNanos", anchor.afterNanos(),
                        "uncertaintyNanos", anchor.uncertaintyNanos(),
                        "uncertaintyScope", "CLOCK_READ_BRACKET_ONLY"),
                "sourceBatches", phase.sourceBatches().stream().map(batch -> object(
                        "index", batch.index(), "issuedAtNanos", batch.issuedAtNanos(),
                        "completedAtNanos", batch.completedAtNanos(),
                        "issueToCompleteNanos", batch.durationNanos(),
                        "issuedAtUtcEarliest", anchor.earliestUtc(batch.issuedAtNanos()).toString(),
                        "issuedAtUtcLatest", anchor.latestUtc(batch.issuedAtNanos()).toString(),
                        "completedAtUtcEarliest", anchor.earliestUtc(batch.completedAtNanos()).toString(),
                        "completedAtUtcLatest", anchor.latestUtc(batch.completedAtNanos()).toString())).toList(),
                "resources", resourceEvidence(phase.resources(), anchor));
    }

    static Map<String, Object> resourceEvidence(BenchmarkResourceSampler.Summary resources,
                                               BenchmarkForkEnvironment.ClockAnchor anchor) {
        return object("cpuNanos", resources.cpuNanos(),
                "gcCollectionMillis", resources.gcCollectionMillis(),
                "peakHeapBytes", resources.peakHeapBytes(), "peakRssBytes", resources.peakRssBytes(),
                "sampleCount", resources.sampleCount(),
                "sampling", resources.sampling().map(diagnostics -> samplingEvidence(diagnostics, anchor))
                        .orElseGet(() -> object("state", "NOT_RECORDED")));
    }

    private static Map<String, Object> samplingEvidence(BenchmarkResourceSampler.SamplingDiagnostics diagnostics,
                                                       BenchmarkForkEnvironment.ClockAnchor anchor) {
        return object("state", diagnostics.state(), "attemptCount", diagnostics.attemptCount(),
                "failureCount", diagnostics.failureCount(),
                "totalDurationNanos", diagnostics.totalDurationNanos(),
                "maxDurationNanos", diagnostics.maxDurationNanos(),
                "durationScope", "EXTERNAL_READ_ONLY", "retention",
                diagnostics.state().equals("COHORT_INTERVAL") ? "COHORT_INTERVAL" : "FIRST_AND_LAST",
                "retainedAttemptLimit", diagnostics.retainedAttemptLimit(),
                "omittedAttempts", diagnostics.omittedAttempts(),
                "attempts", diagnostics.retainedAttempts().stream().map(attempt -> object(
                        "index", attempt.index(), "startedAtNanos", attempt.startedAtNanos(),
                        "completedAtNanos", attempt.completedAtNanos(),
                        "durationNanos", attempt.durationNanos(), "outcome", attempt.outcome().name(),
                        "failureType", attempt.failureType(),
                        "startedAtUtcEarliest", anchor.earliestUtc(attempt.startedAtNanos()).toString(),
                        "startedAtUtcLatest", anchor.latestUtc(attempt.startedAtNanos()).toString(),
                        "completedAtUtcEarliest", anchor.earliestUtc(attempt.completedAtNanos()).toString(),
                        "completedAtUtcLatest", anchor.latestUtc(attempt.completedAtNanos()).toString(),
                        "reading", readingEvidence(attempt.reading()))).toList());
    }

    private static Map<String, Object> readingEvidence(BenchmarkProcessProbe.Snapshot reading) {
        if (reading == null) {
            return object("state", "UNAVAILABLE");
        }
        return object("state", reading.complete() ? "COMPLETE" : "INCOMPLETE",
                "cpuNanos", reading.cpuNanos().isPresent() ? reading.cpuNanos().getAsLong() : null,
                "heapUsedBytes", reading.heapUsedBytes().isPresent() ? reading.heapUsedBytes().getAsLong() : null,
                "rssBytes", reading.rssBytes().isPresent() ? reading.rssBytes().getAsLong() : null,
                "gcCollectionMillis", reading.gcCollectionMillis().isPresent()
                        ? reading.gcCollectionMillis().getAsLong() : null);
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

    static Map<String, Object> qualifiedEvaluation(PipelineBenchmarkComparison.Evaluation evaluation,
                                                 List<String> qualificationFailures) {
        Map<String, Object> result = evaluation(evaluation);
        result.put("passed", evaluation.passed() && qualificationFailures.isEmpty());
        result.put("qualificationFailures", List.copyOf(qualificationFailures));
        return result;
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
