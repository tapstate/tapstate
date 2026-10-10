package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Runs one frozen workload against one separately built server without changing that server. */
final class RealBenchmarkForkDriver implements PipelineBenchmarkHarness.ForkDriver {

    private static final Duration SIDE_CAR_WAIT = Duration.ofMinutes(3);
    private static final Duration ACK_WAIT = Duration.ofMinutes(5);
    private static final Duration DELIVERY_WAIT = Duration.ofMinutes(1);
    private static final Duration RESOURCE_INTERVAL = Duration.ofMillis(200);
    private static final Duration COUNTER_POLL = Duration.ofMillis(100);
    private static final Duration PRE_WINDOW_QUIET = Duration.ofSeconds(3);
    static final String WRITE_RETURN_DIAGNOSTICS_PROPERTY = "tapstate.e2e.benchmark.write-return-diagnostics";
    static final String WRITE_RETURN_CLOCK_CONTROL_PROPERTY = "tapstate.e2e.benchmark.write-return-clock-control";
    static final String WRITE_RETURN_METHOD_CONTROL_PROPERTY = "tapstate.e2e.benchmark.write-return-method-control";
    static final String WRITE_RETURN_COST_STAGES_PROPERTY = "tapstate.e2e.benchmark.write-return-cost-stages";
    static final String NATIVE_CLOCK_LIBRARY_PROPERTY = "tapstate.benchmark.native-clock-library";
    static final String NATIVE_COUNTER_DOMAIN_PROPERTY = "tapstate.e2e.benchmark.write-return-native-domain-diagnostics";
    static final String RETURN_COLLECTOR_CALIBRATION_PROPERTY = "tapstate.e2e.benchmark.write-return-collector-calibration";

    enum CollectorCalibration { ON, OFF }

    /** Local observation intervals distinguish data arrival from subsequent proof reads. */
    record ConfirmationTiming(long sourceMarkerWaitStartedAtNanos, long sourceMarkerWaitCompletedAtNanos,
                              long tableConfirmationCompletedAtNanos, long firstTargetObservedAtNanos,
                              long lastTargetObservedAtNanos) {
        ConfirmationTiming {
            if (sourceMarkerWaitCompletedAtNanos < sourceMarkerWaitStartedAtNanos
                    || tableConfirmationCompletedAtNanos < sourceMarkerWaitCompletedAtNanos
                    || lastTargetObservedAtNanos < firstTargetObservedAtNanos) {
                throw new IllegalArgumentException("confirmation observation intervals moved backward");
            }
        }
    }

    record DeliveryTimeline(long firstFullObservedAtNanos, long lastFullObservedAtNanos,
                            long cohortFirstObservedAtNanos, long cohortLastObservedAtNanos,
                            long fullDeliveryCount, long cohortDeliveryCount,
                            List<Long> cohortObservedAtNanos, List<Long> fullObservedAtNanos,
                            List<Long> cohortServerOperationWallMillis,
                            List<List<Long>> cohortOperationStreams) {
        DeliveryTimeline(long firstFullObservedAtNanos, long lastFullObservedAtNanos,
                         long cohortFirstObservedAtNanos, long cohortLastObservedAtNanos,
                         long fullDeliveryCount, long cohortDeliveryCount, List<Long> cohortObservedAtNanos,
                         List<Long> fullObservedAtNanos, List<Long> cohortServerOperationWallMillis) {
            this(firstFullObservedAtNanos, lastFullObservedAtNanos, cohortFirstObservedAtNanos, cohortLastObservedAtNanos,
                    fullDeliveryCount, cohortDeliveryCount, cohortObservedAtNanos, fullObservedAtNanos,
                    cohortServerOperationWallMillis, cohortServerOperationWallMillis.isEmpty() ? List.of()
                            : List.of(cohortServerOperationWallMillis));
        }
        DeliveryTimeline(long firstFullObservedAtNanos, long lastFullObservedAtNanos,
                         long cohortFirstObservedAtNanos, long cohortLastObservedAtNanos,
                         long fullDeliveryCount, long cohortDeliveryCount, List<Long> cohortObservedAtNanos) {
            this(firstFullObservedAtNanos, lastFullObservedAtNanos, cohortFirstObservedAtNanos,
                    cohortLastObservedAtNanos, fullDeliveryCount, cohortDeliveryCount, cohortObservedAtNanos, List.of());
        }
        DeliveryTimeline(long firstFullObservedAtNanos, long lastFullObservedAtNanos,
                         long cohortFirstObservedAtNanos, long cohortLastObservedAtNanos,
                         long fullDeliveryCount, long cohortDeliveryCount, List<Long> cohortObservedAtNanos,
                         List<Long> fullObservedAtNanos) {
            this(firstFullObservedAtNanos, lastFullObservedAtNanos, cohortFirstObservedAtNanos, cohortLastObservedAtNanos,
                    fullDeliveryCount, cohortDeliveryCount, cohortObservedAtNanos, fullObservedAtNanos, List.of());
        }
        DeliveryTimeline {
            cohortObservedAtNanos = List.copyOf(cohortObservedAtNanos);
            fullObservedAtNanos = List.copyOf(fullObservedAtNanos);
            cohortServerOperationWallMillis = java.util.Collections.unmodifiableList(new ArrayList<>(cohortServerOperationWallMillis));
            cohortOperationStreams = cohortOperationStreams.stream().map(List::copyOf).toList();
            if (fullDeliveryCount <= 0 || fullDeliveryCount > 192_000 || cohortDeliveryCount <= 0
                    || cohortDeliveryCount > fullDeliveryCount || cohortObservedAtNanos.size() != cohortDeliveryCount
                    || firstFullObservedAtNanos > cohortFirstObservedAtNanos
                    || lastFullObservedAtNanos < cohortLastObservedAtNanos
                    || cohortFirstObservedAtNanos > cohortLastObservedAtNanos
                    || cohortObservedAtNanos.getFirst() != cohortFirstObservedAtNanos
                    || cohortObservedAtNanos.getLast() != cohortLastObservedAtNanos) {
                throw new IllegalArgumentException("delivery timeline is incomplete, unordered, or outside its fixed profile bound");
            }
            for (int i = 1; i < cohortObservedAtNanos.size(); i++) {
                if (cohortObservedAtNanos.get(i) < cohortObservedAtNanos.get(i - 1)) {
                    throw new IllegalArgumentException("delivery timeline moved backward");
                }
            }
            if (!fullObservedAtNanos.isEmpty()) {
                if (fullObservedAtNanos.size() != fullDeliveryCount
                        || fullObservedAtNanos.getFirst() != firstFullObservedAtNanos
                        || fullObservedAtNanos.getLast() != lastFullObservedAtNanos) {
                    throw new IllegalArgumentException("full delivery timeline does not match its bounded count");
                }
                for (int i = 1; i < fullObservedAtNanos.size(); i++) {
                    if (fullObservedAtNanos.get(i) < fullObservedAtNanos.get(i - 1)) {
                        throw new IllegalArgumentException("full delivery timeline moved backward");
                    }
                }
            }
        }

        BenchmarkSteadyOutputWindow.Reading operationWindow() {
            if (cohortServerOperationWallMillis.size() != cohortObservedAtNanos.size()) {
                throw new AssertionError("operation and observed delivery cohorts differ");
            }
            if (!BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(cohortOperationStreams)
                    .equals(cohortServerOperationWallMillis)) {
                throw new AssertionError("per-target operation streams differ from their full cohort");
            }
            return BenchmarkSteadyOutputWindow.readCommonOperations(cohortOperationStreams, false);
        }
    }

    record MeasuredPhase(String id, long acknowledgedOutputs, long firstIssuedAtNanos,
                         long sourceCompletedAtNanos, long completedAckAtNanos,
                         long expectedSourceChanges, int observedDeliveries, long reportedRecordsOut,
                         BenchmarkForkEnvironment.ClockAnchor clockAnchor,
                         List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
                         BenchmarkResourceSampler.Summary resources,
                         Optional<ConfirmationTiming> confirmationTiming,
                         Optional<DeliveryTimeline> deliveryTimeline,
                         boolean steadyOutputProfile,
                         Optional<BenchmarkTargetClock.LocalWindow> operationResourceWindow,
                         Map<String, Object> targetClockEvidence) {
        MeasuredPhase(String id, long acknowledgedOutputs, long firstIssuedAtNanos, long sourceCompletedAtNanos,
                      long completedAckAtNanos, long expectedSourceChanges, int observedDeliveries, long reportedRecordsOut,
                      BenchmarkForkEnvironment.ClockAnchor clockAnchor, List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
                      BenchmarkResourceSampler.Summary resources, Optional<ConfirmationTiming> confirmationTiming,
                      Optional<DeliveryTimeline> deliveryTimeline, boolean steadyOutputProfile,
                      Optional<BenchmarkTargetClock.LocalWindow> operationResourceWindow) {
            this(id, acknowledgedOutputs, firstIssuedAtNanos, sourceCompletedAtNanos, completedAckAtNanos,
                    expectedSourceChanges, observedDeliveries, reportedRecordsOut, clockAnchor, sourceBatches,
                    resources, confirmationTiming, deliveryTimeline, steadyOutputProfile, operationResourceWindow, Map.of());
        }
        MeasuredPhase(String id, long acknowledgedOutputs, long firstIssuedAtNanos, long sourceCompletedAtNanos,
                      long completedAckAtNanos, long expectedSourceChanges, int observedDeliveries, long reportedRecordsOut,
                      BenchmarkForkEnvironment.ClockAnchor clockAnchor, List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
                      BenchmarkResourceSampler.Summary resources, Optional<ConfirmationTiming> confirmationTiming,
                      Optional<DeliveryTimeline> deliveryTimeline, boolean steadyOutputProfile) {
            this(id, acknowledgedOutputs, firstIssuedAtNanos, sourceCompletedAtNanos, completedAckAtNanos,
                    expectedSourceChanges, observedDeliveries, reportedRecordsOut, clockAnchor, sourceBatches,
                    resources, confirmationTiming, deliveryTimeline, steadyOutputProfile, Optional.empty());
        }
        MeasuredPhase(String id, long acknowledgedOutputs, long firstIssuedAtNanos,
                      long sourceCompletedAtNanos, long completedAckAtNanos,
                      long expectedSourceChanges, int observedDeliveries, long reportedRecordsOut,
                      BenchmarkForkEnvironment.ClockAnchor clockAnchor,
                      List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
                      BenchmarkResourceSampler.Summary resources, Optional<ConfirmationTiming> confirmationTiming) {
            this(id, acknowledgedOutputs, firstIssuedAtNanos, sourceCompletedAtNanos, completedAckAtNanos,
                    expectedSourceChanges, observedDeliveries, reportedRecordsOut, clockAnchor, sourceBatches,
                    resources, confirmationTiming, Optional.empty(), false, Optional.empty());
        }
        MeasuredPhase(String id, long acknowledgedOutputs, long firstIssuedAtNanos,
                      long sourceCompletedAtNanos, long completedAckAtNanos,
                      long expectedSourceChanges, int observedDeliveries, long reportedRecordsOut,
                      BenchmarkForkEnvironment.ClockAnchor clockAnchor,
                      List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
                      BenchmarkResourceSampler.Summary resources) {
            this(id, acknowledgedOutputs, firstIssuedAtNanos, sourceCompletedAtNanos, completedAckAtNanos,
                    expectedSourceChanges, observedDeliveries, reportedRecordsOut, clockAnchor, sourceBatches,
                    resources, Optional.empty(), Optional.empty(), false, Optional.empty());
        }
        MeasuredPhase {
            Objects.requireNonNull(clockAnchor, "measured clock anchor");
            sourceBatches = List.copyOf(sourceBatches);
            Objects.requireNonNull(resources, "phase resource measurements");
            Objects.requireNonNull(confirmationTiming, "confirmation timing availability");
            Objects.requireNonNull(deliveryTimeline, "delivery timeline availability");
            Objects.requireNonNull(operationResourceWindow, "operation resource window availability");
            targetClockEvidence = Map.copyOf(targetClockEvidence);
        }

        double recordsOutPerSecond() {
            if (steadyOutputProfile) {
                var timeline = deliveryTimeline.orElseThrow();
                return timeline.operationWindow().recordsPerSecond();
            }
            long duration = deliveryWindowNanos();
            if (duration <= 0 || acknowledgedOutputs <= 0) {
                throw new AssertionError("measured phase has no observed target delivery window: " + id);
            }
            return acknowledgedOutputs * 1_000_000_000.0 / duration;
        }

        long deliveryWindowNanos() {
            if (steadyOutputProfile) {
                var timeline = deliveryTimeline.orElseThrow();
                var window = timeline.operationWindow();
                return window.endedAtNanos() - window.startedAtNanos();
            }
            return confirmationTiming.orElseThrow(() -> new AssertionError(
                    "observed target delivery timing is unavailable: " + id)).lastTargetObservedAtNanos() - firstIssuedAtNanos;
        }

        double confirmationRecordsPerSecond() {
            long duration = completedAckAtNanos - firstIssuedAtNanos;
            if (duration <= 0 || acknowledgedOutputs <= 0) { throw new AssertionError("invalid confirmation window: " + id); }
            return acknowledgedOutputs * 1_000_000_000.0 / duration;
        }

        OptionalLong sourceIssueDurationNanos() {
            if (sourceBatches.isEmpty()) { return OptionalLong.empty(); }
            long duration = sourceCompletedAtNanos - sourceBatches.getFirst().issuedAtNanos();
            if (duration <= 0) { throw new AssertionError("invalid full source issue window: " + id); }
            return OptionalLong.of(duration);
        }

        boolean steadyOutputEstablished() {
            if (!steadyOutputProfile || deliveryTimeline.isEmpty() || confirmationTiming.isEmpty()) { return false; }
            var timeline = deliveryTimeline.orElseThrow();
            try { BenchmarkSteadyOutputWindow.requireSteady(timeline.operationWindow()); }
            catch (AssertionError notSteady) { return false; }
            return BenchmarkTargetClock.operationDateTimingQualified();
        }
    }

    /** Required target coverage excludes an optional terminal Nest update caused by arrival order. */
    record Evidence(String forkId, BenchmarkWorkloadDefinitions.Workload workload,
                    PipelineBenchmarkComparison.Arm arm, Path applicationJar,
                    List<MeasuredPhase> phases, BenchmarkResourceSampler.Summary resources,
                    BenchmarkMongoCommandSampler.Summary mongoCommands,
                    Map<String, Long> declaredSourceCoverage,
                    Map<String, Long> observedTargetCoverage,
                    String checksum, long errorTotal,
                    List<Map<String, Object>> terminalMetaReceipts,
                    Optional<BenchmarkJdiTelemetrySession.Evidence> telemetry,
                    BenchmarkOwnedProcessReceipt processReceipt) {
        Evidence(String forkId, BenchmarkWorkloadDefinitions.Workload workload,
                PipelineBenchmarkComparison.Arm arm, Path applicationJar,
                List<MeasuredPhase> phases, BenchmarkResourceSampler.Summary resources,
                BenchmarkMongoCommandSampler.Summary mongoCommands,
                Map<String, Long> declaredSourceCoverage, Map<String, Long> observedTargetCoverage,
                String checksum, long errorTotal, Optional<BenchmarkJdiTelemetrySession.Evidence> telemetry) {
            this(forkId, workload, arm, applicationJar, phases, resources, mongoCommands,
                    declaredSourceCoverage, observedTargetCoverage, checksum, errorTotal, List.of(), telemetry, null);
        }

        Evidence(String forkId, BenchmarkWorkloadDefinitions.Workload workload,
                PipelineBenchmarkComparison.Arm arm, Path applicationJar,
                List<MeasuredPhase> phases, BenchmarkResourceSampler.Summary resources,
                BenchmarkMongoCommandSampler.Summary mongoCommands,
                Map<String, Long> declaredSourceCoverage, Map<String, Long> observedTargetCoverage,
                String checksum, long errorTotal, List<Map<String, Object>> terminalMetaReceipts,
                Optional<BenchmarkJdiTelemetrySession.Evidence> telemetry) {
            this(forkId, workload, arm, applicationJar, phases, resources, mongoCommands,
                    declaredSourceCoverage, observedTargetCoverage, checksum, errorTotal, terminalMetaReceipts, telemetry, null);
        }

        Map<String, Object> runtimeEvidence() {
            return processReceipt == null ? Map.of("status", "UNKNOWN", "reason", "OWNED_RUNTIME_NOT_RETAINED")
                    : processReceipt.evidence();
        }

        Evidence {
            phases = List.copyOf(phases);
            declaredSourceCoverage = Map.copyOf(declaredSourceCoverage);
            observedTargetCoverage = Map.copyOf(observedTargetCoverage);
            terminalMetaReceipts = List.copyOf(terminalMetaReceipts);
            Objects.requireNonNull(telemetry, "telemetry availability");
        }

        void requireSteadyStateWindow() {
            if (!workload.pilotProfile()) { throw new AssertionError("steady output requires the predeclared load profile"); }
            if (phases.isEmpty()) { throw new AssertionError("timing qualification has no measured phases"); }
            for (var phase : phases) {
                if (Boolean.TRUE.equals(phase.targetClockEvidence().get("operationClockRefusalEvidenceEnabled"))
                        || clockRefusalEvidenceRecorded(phase.targetClockEvidence().get("targetWitnessReadReceipts"))) {
                    throw new AssertionError("clock refusal evidence cannot establish a live performance gate");
                }
                if (Boolean.TRUE.equals(phase.targetClockEvidence().get("targetWitnessDeferred"))) {
                    throw new AssertionError("deferred target witness cannot establish a live performance gate");
                }
                if (!"QUALIFIED".equals(phase.targetClockEvidence().get("state"))
                        || !(phase.targetClockEvidence().get("sampledInterior") instanceof Map<?, ?> interior)
                        || !"QUALIFIED_SAMPLED_INTERIOR".equals(interior.get("state"))) {
                    throw new AssertionError("steady output lacks its qualified outer and sampled interior clocks");
                }
                var timeline = phase.deliveryTimeline().orElseThrow(() -> new AssertionError("steady output has no complete timeline"));
                BenchmarkSteadyOutputWindow.requireSteady(timeline.operationWindow());
                BenchmarkTargetClock.requireOperationTimeErrorBound();
            }
        }
    }

    private static boolean clockRefusalEvidenceRecorded(Object receipts) {
        return receipts instanceof List<?> items && items.stream().anyMatch(item -> item instanceof Map<?, ?> receipt
                && receipt.get("readSchedule") instanceof Map<?, ?> schedule
                && schedule.containsKey("operationClockRefusalEvidence"));
    }

    private record PhaseWindow(MeasuredPhase measurement, List<Long> deliveryDurations,
                               BenchmarkResourceSampler.Summary resources,
                               BenchmarkMongoCommandSampler.Summary mongoCommands) {}

    private record ValidatedTargetClocks(List<BenchmarkTargetClock.Reading> after, Map<String, Object> outer) {}

    private final List<Evidence> evidence = new ArrayList<>();
    private final BenchmarkForkEnvironment.BootLauncher launcher;

    RealBenchmarkForkDriver() { this(BenchmarkForkEnvironment.OwnedBoot::plain); }

    RealBenchmarkForkDriver(BenchmarkForkEnvironment.BootLauncher launcher) {
        this.launcher = Objects.requireNonNull(launcher);
    }

    List<Evidence> evidence() {
        return List.copyOf(evidence);
    }

    static BenchmarkReturnClockSampler.Mode writeReturnClockMode(String value, boolean diagnostics, boolean pilot) {
        if (value == null || "false".equals(value)) { return BenchmarkReturnClockSampler.Mode.PERIODIC; }
        if (!"true".equals(value)) { throw new AssertionError("return clock control must be true or false"); }
        if (!diagnostics || !pilot) {
            throw new AssertionError("return clock cost control requires the original return diagnostic pilot");
        }
        return BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL;
    }

    static boolean writeReturnMethodControl(String value, boolean diagnostics, boolean pilot,
            BenchmarkReturnClockSampler.Mode clockMode, boolean conflictingDiagnostics) {
        if (value == null || "false".equals(value)) { return false; }
        if (!"true".equals(value)) { throw new AssertionError("return method control must be true or false"); }
        if (!diagnostics || !pilot || clockMode != BenchmarkReturnClockSampler.Mode.PERIODIC || conflictingDiagnostics) {
            throw new AssertionError("return method cost control requires one original plain diagnostic pilot without other controls");
        }
        return true;
    }

    static boolean collectWriteReturns(boolean diagnostics, boolean methodControl) {
        return diagnostics && !methodControl;
    }

    static boolean writeReturnCostStages(String value, boolean diagnostics, boolean pilot,
            BenchmarkReturnClockSampler.Mode clockMode, boolean conflictingDiagnostics) {
        if (value == null || "false".equals(value)) { return false; }
        if (!"true".equals(value)) { throw new AssertionError("return cost stages must be true or false"); }
        if (!diagnostics || !pilot || clockMode != BenchmarkReturnClockSampler.Mode.PERIODIC || conflictingDiagnostics) {
            throw new AssertionError("return cost stages require one original plain stateless B diagnostic pilot without other controls");
        }
        return true;
    }

    static List<String> returnJvmArguments(boolean methodControl, boolean costStages) {
        if (methodControl && costStages) { throw new AssertionError("disabled producer cannot collect cost stages"); }
        return costStages ? List.of("-Dtapstate.benchmark.write-return=true",
                "-Dtapstate.benchmark.write-return-cost-stages=true")
                : List.of("-Dtapstate.benchmark.write-return=" + !methodControl);
    }

    static String nativeClockLibrary(String value, boolean diagnostics, boolean pilot,
            BenchmarkReturnClockSampler.Mode clockMode, boolean conflictingDiagnostics) {
        if (value == null) { return null; }
        if (value.isEmpty() || value.length() > 512
                || value.chars().anyMatch(character -> character < 0x20 || character > 0x7e)
                || !java.nio.file.Path.of(value).isAbsolute()) {
            throw new AssertionError("native clock requires one bounded absolute library path");
        }
        if (!diagnostics || !pilot || clockMode != BenchmarkReturnClockSampler.Mode.PERIODIC || conflictingDiagnostics) {
            throw new AssertionError("native clock requires one original plain stateless B return diagnostic without other controls");
        }
        return value;
    }

    static boolean nativeCounterDomain(String value, String nativeLibrary, boolean diagnostics, boolean pilot,
            BenchmarkReturnClockSampler.Mode clockMode, boolean conflictingDiagnostics) {
        if (value == null || "false".equals(value)) { return false; }
        if (!"true".equals(value) || nativeLibrary == null || !diagnostics || !pilot
                || clockMode != BenchmarkReturnClockSampler.Mode.PERIODIC || conflictingDiagnostics) {
            throw new AssertionError("common native counter diagnostics require the isolated original native-clock return pilot");
        }
        return true;
    }

    static boolean nativeClockArmAllowed(PipelineBenchmarkComparison.Arm arm, boolean commonDomain) {
        return arm == PipelineBenchmarkComparison.Arm.B || commonDomain && arm == PipelineBenchmarkComparison.Arm.A;
    }

    static CollectorCalibration collectorCalibration(String value, String workload, PipelineBenchmarkComparison.Arm arm,
            boolean diagnostics, boolean pilot, BenchmarkReturnClockSampler.Mode clocks, String library,
            boolean plain, boolean explicitOutput, boolean conflictingControls) {
        if (value == null) { return null; }
        if (!("ON".equals(value) || "OFF".equals(value)) || !"stateless".equals(workload)
                || arm != PipelineBenchmarkComparison.Arm.B || !diagnostics || !pilot || !plain || !explicitOutput
                || clocks != BenchmarkReturnClockSampler.Mode.PERIODIC || library == null || conflictingControls) {
            throw new AssertionError("return collector calibration requires an isolated original stateless B native pilot and an explicit fork output");
        }
        nativeClockLibrary(library, diagnostics, pilot, clocks, false);
        return CollectorCalibration.valueOf(value);
    }

    static boolean calibrationReadsOwnedClock(CollectorCalibration calibration) {
        return calibration != CollectorCalibration.OFF;
    }

    static Map<String, Object> collectorOwnedClockEvidence(CollectorCalibration calibration,
            BenchmarkWriteReturnReader reader, String library, String rootRaw) {
        if (calibrationReadsOwnedClock(calibration)) { return reader.nativeClockEvidence(library, rootRaw); }
        var unavailable = new LinkedHashMap<String, Object>();
        unavailable.put("state", "UNAVAILABLE"); unavailable.put("reason", "RETURN_COLLECTOR_CALIBRATION_DISABLED_PRODUCER");
        unavailable.put("scope", "OWNED_NATIVE_CLOCK_NOT_READ");
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> unavailable.put(flag, false));
        return Map.copyOf(unavailable);
    }

    private static boolean conflictingCalibrationControls() {
        return System.getProperty(WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null
                || System.getProperty(WRITE_RETURN_COST_STAGES_PROPERTY) != null
                || System.getProperty(NATIVE_COUNTER_DOMAIN_PROPERTY) != null
                || List.of("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics", "tapstate.e2e.benchmark.compilation-diagnostics",
                        "tapstate.e2e.benchmark.thread-point-diagnostics", "tapstate.e2e.benchmark.load-diagnostics",
                        BenchmarkDualGcDiagnostics.ENABLED_PROPERTY, BenchmarkWitnessReadGate.PROPERTY,
                        BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                        BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                        "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                        "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration").stream().anyMatch(Boolean::getBoolean);
    }

    private static boolean explicitCalibrationOutput() {
        String value = System.getProperty("tapstate.e2e.benchmark-smoke.fork-output");
        return value != null && !value.isBlank() && Path.of(value).isAbsolute() && Path.of(value).getFileName() != null;
    }

    static Map<String, Object> collectorCutoffOrder(long acknowledgedAt, long resourceStarted, long resourceCompleted,
            long commandStarted, long commandCompleted, long drainStarted, long drainCompleted) {
        if (acknowledgedAt < 0 || acknowledgedAt > resourceStarted || resourceStarted > resourceCompleted
                || resourceCompleted > commandStarted || commandStarted > commandCompleted
                || commandCompleted > drainStarted || drainStarted > drainCompleted) {
            throw new AssertionError("collector calibration checkpoints must follow table ACK and precede capture drain");
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("fourWriterTableAckAtNanos", acknowledgedAt);
        result.put("resourceRead", Map.of("startedAtNanos", resourceStarted, "completedAtNanos", resourceCompleted));
        result.put("commandRead", Map.of("startedAtNanos", commandStarted, "completedAtNanos", commandCompleted));
        result.put("captureDrain", Map.of("startedAtNanos", drainStarted, "completedAtNanos", drainCompleted));
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> result.put(flag, false));
        result.put("causalOverheadQualified", false);
        return Map.copyOf(result);
    }

    static Map<String, Object> supportedRootClockEvidence(String raw, String library) {
        var root = new BenchmarkCausalClock.Identity(ProcessHandle.current().pid(),
                java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime());
        var parsed = BenchmarkNativeClockEvidence.parse(raw, root, library);
        var result = new LinkedHashMap<String, Object>(parsed);
        result.putAll(BenchmarkWriteReturnCostStages.rawEvidence(raw));
        result.put("supportedSnapshot", BenchmarkNativeReturnClock.supportedRootSnapshot(parsed));
        result.put("scope", "ACTUAL_ROOT_ONLY_COLD_NATIVE_METADATA_NO_OWNED_GETTER");
        return Map.copyOf(result);
    }

    private static Map<String, Object> collectorCommands(BenchmarkMongoCommandSampler.Summary summary) {
        var families = new TreeMap<String, Long>();
        summary.byFamily().forEach((family, count) -> families.put(family.name(), count));
        return Map.of("byCommand", new TreeMap<>(summary.byCommand()), "byFamily", families,
                "elapsedMillis", summary.elapsedMillis());
    }

    static BenchmarkMongoCommandSampler.Summary finishCollectorCommands(BenchmarkResourceSampler.Summary resources,
            BenchmarkForkEnvironment.ClockAnchor anchor, Map<String, Object> retained,
            java.util.function.Supplier<BenchmarkMongoCommandSampler.Summary> commandFinish) {
        retained.put("fullPhaseResources", java.util.Collections.unmodifiableMap(
                PipelineBenchmarkLiveRunIT.resourceEvidence(resources, anchor)));
        retained.put("fullPhaseResourceScope", "SOURCE_ISSUE_THROUGH_CAPTURE_DRAIN_AND_FINAL_RESOURCE_READ");
        return commandFinish.get();
    }

    static Map<String, Object> collectorSamplingRefusal(BenchmarkResourceSampler.SamplingFailure failure,
            BenchmarkForkEnvironment.ClockAnchor anchor) {
        var facts = new LinkedHashMap<String, Object>();
        facts.put("state", "UNKNOWN"); facts.put("stage", failure.stage().name()); facts.put("reason", failure.reason().name());
        facts.put("completedSampling", PipelineBenchmarkLiveRunIT.samplingEvidence(failure.diagnostics(), anchor));
        facts.put("actualPendingRead", failure.pendingRead().<Map<String, Object>>map(read -> Map.of(
                "state", "PENDING", "index", read.index(), "startedAtNanos", read.startedAtNanos(),
                "finalRead", read.finalRead(), "checkpointRead", read.checkpointRead()))
                .orElseGet(() -> Map.of("state", "NOT_PRESENT")));
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> facts.put(flag, false));
        facts.put("causalOverheadQualified", false);
        return Map.copyOf(facts);
    }

    static void retainCollectorCommandRefusal(Map<String, Object> retained,
            BenchmarkMongoCommandSampler.CheckpointFailure failure) {
        retained.put("commandCheckpointRefusal", failure.retainedEvidence());
    }

    static boolean admitWriteReturnMethodProtocol(String value, boolean diagnostics, boolean pilot,
            BenchmarkReturnClockSampler.Mode clockMode, boolean conflictingDiagnostics) {
        if (value == null) { return false; }
        writeReturnMethodControl(value, diagnostics, pilot, clockMode, conflictingDiagnostics);
        if (!diagnostics || !pilot || clockMode != BenchmarkReturnClockSampler.Mode.PERIODIC || conflictingDiagnostics) {
            throw new AssertionError("return method cost protocol requires one original plain diagnostic pilot without other controls");
        }
        return "true".equals(value);
    }

    @Override
    public PipelineBenchmarkHarness.ForkResult run(BenchmarkWorkloadDefinitions.Workload workload,
            PipelineBenchmarkComparison.Arm arm, int armFork, Path applicationJar) throws Exception {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(arm, "arm");
        var clockMode = writeReturnClockMode(System.getProperty(WRITE_RETURN_CLOCK_CONTROL_PROPERTY),
                Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile());
        String calibrationValue = System.getProperty(RETURN_COLLECTOR_CALIBRATION_PROPERTY);
        if (calibrationValue != null) {
            collectorCalibration(calibrationValue, workload.id(), arm,
                    Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile(), clockMode,
                    System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY),
                    "PLAIN".equals(System.getProperty("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN")),
                    explicitCalibrationOutput(), conflictingCalibrationControls());
        }
        String selectedNativeLibrary = nativeClockLibrary(System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY),
                Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile(), clockMode,
                !"stateless".equals(workload.id()) || !nativeClockArmAllowed(arm, Boolean.getBoolean(NATIVE_COUNTER_DOMAIN_PROPERTY))
                        || System.getProperty(WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null
                        || Boolean.getBoolean(WRITE_RETURN_COST_STAGES_PROPERTY)
                        || List.of("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics",
                                "tapstate.e2e.benchmark.compilation-diagnostics", "tapstate.e2e.benchmark.thread-point-diagnostics",
                                "tapstate.e2e.benchmark.load-diagnostics", BenchmarkDualGcDiagnostics.ENABLED_PROPERTY,
                                BenchmarkWitnessReadGate.PROPERTY, BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                                BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                                "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                                "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration").stream().anyMatch(Boolean::getBoolean));
        nativeCounterDomain(System.getProperty(NATIVE_COUNTER_DOMAIN_PROPERTY), selectedNativeLibrary,
                Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile(), clockMode, false);
        writeReturnCostStages(System.getProperty(WRITE_RETURN_COST_STAGES_PROPERTY),
                Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile(), clockMode,
                !"stateless".equals(workload.id()) || arm != PipelineBenchmarkComparison.Arm.B
                        || System.getProperty(WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null
                        || List.of("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics",
                                "tapstate.e2e.benchmark.compilation-diagnostics", "tapstate.e2e.benchmark.thread-point-diagnostics",
                                "tapstate.e2e.benchmark.load-diagnostics", BenchmarkDualGcDiagnostics.ENABLED_PROPERTY,
                                BenchmarkWitnessReadGate.PROPERTY, BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                                BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                                "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                                "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration").stream().anyMatch(Boolean::getBoolean));
        admitWriteReturnMethodProtocol(System.getProperty(WRITE_RETURN_METHOD_CONTROL_PROPERTY),
                Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile(), clockMode,
                !"stateless".equals(workload.id()) || arm != PipelineBenchmarkComparison.Arm.B
                        || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics")
                        || Boolean.getBoolean("tapstate.e2e.benchmark.compilation-diagnostics")
                        || Boolean.getBoolean("tapstate.e2e.benchmark.thread-point-diagnostics")
                        || Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics")
                        || Boolean.getBoolean(BenchmarkDualGcDiagnostics.ENABLED_PROPERTY)
                        || Boolean.getBoolean(BenchmarkWitnessReadGate.PROPERTY)
                        || Boolean.getBoolean(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY)
                        || Boolean.getBoolean(BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY)
                        || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.paced-calibration")
                        || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.cdc-settling-calibration")
                        || Boolean.getBoolean("tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration"));
        if (armFork < 1 || applicationJar == null) {
            throw new IllegalArgumentException("a positive fork number and application JAR are required");
        }
        String forkId = workload.id() + "-" + arm + "-" + armFork;
        if (Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY) && !workload.pilotProfile()) {
            throw new AssertionError("return diagnostics require the fixed pilot profile");
        }
        if (Boolean.getBoolean(BenchmarkWitnessReadGate.PROPERTY)
                && (!"copy".equals(workload.id()) || !workload.pilotProfile()
                || arm != PipelineBenchmarkComparison.Arm.B)) {
            throw new AssertionError("deferred target witness requires the original copy B diagnostic profile");
        }
        BenchmarkOwnedProcessReceipt ownedReceipt = null;
        Throwable primary = null;
        PipelineBenchmarkHarness.ForkResult result;
        try (BenchmarkForkEnvironment fork = BenchmarkForkEnvironment.open(workload, applicationJar, forkId, launcher)) {
            ownedReceipt = fork.ownedProcessReceipt();
            BenchmarkSourceLineage.Witness lineage = workload.database()
                    == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? BenchmarkSourceLineage.readMySql(fork.sourceSettings())
                    : BenchmarkSourceLineage.readPostgres(fork.sourceSettings());
            BenchmarkWorkloadDefinitions.Phase snapshot = workload.phases().getFirst();
            if (snapshot.stage() != BenchmarkWorkloadDefinitions.Stage.SNAPSHOT) {
                throw new AssertionError("a benchmark workload must begin with snapshot");
            }
            fork.runPhase(snapshot, true);

            String connectorId = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? "mysql" : "postgres";
            Path connectorJar = ConnectorJars.pathFor(connectorId);
            Map<String, Object> connectorConfig = workload.connectorConfig(fork.sourceSettings());
            try (CaptureSet captures = CaptureSet.open(workload, fork, connectorId, connectorJar, connectorConfig);
                 BenchmarkConnectorPositionCoverage positionCoverage = BenchmarkConnectorPositionCoverage.open(
                         workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                                 ? BenchmarkConnectorPositionCoverage.Mode.MYSQL_DEFAULT
                                 : BenchmarkConnectorPositionCoverage.Mode.POSTGRES_PGOUTPUT,
                         connectorJar, connectorConfig,
                         lineage instanceof BenchmarkSourceLineage.MySql mysql
                                 ? mysql.decoderLineage() : null);
                 BenchmarkNativeQueueProbe returnCounters = Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY)
                         ? new BenchmarkNativeQueueProbe(fork.control(), fork.server().baseUrl().toString(), "tapstate",
                                 new com.hazelcast.config.MetricsConfig().getCollectionFrequencySeconds()) : null) {
                captures.awaitPreflight(workload, fork);
                List<MeasuredPhase> measured = new ArrayList<>();
                List<Long> allDurations = new ArrayList<>();
                List<BenchmarkResourceSampler.Summary> resourceWindows = new ArrayList<>();
                List<BenchmarkMongoCommandSampler.Summary> commandWindows = new ArrayList<>();
                Map<String, Long> declaredSourceCoverage =
                        new LinkedHashMap<>(snapshot.expectedLogicalCoverage());
                BenchmarkForkEnvironment.PhaseResult terminal = null;
                List<BenchmarkAckOracle.SourceChain> chains = null;
                List<Map<String, Object>> tableReceipts = List.of();
                Map<String, Long> observedTargetCoverage;
                BenchmarkWorkloadDefinitions.Phase previousPhase = snapshot;
                int phaseIndex = 1;
                while (phaseIndex < workload.phases().size()
                        && !workload.phases().get(phaseIndex).measured()) {
                    BenchmarkWorkloadDefinitions.Phase phase = workload.phases().get(phaseIndex++);
                    if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.TERMINAL) {
                        throw new AssertionError("terminal arrived before any measured delivery");
                    }
                    fork.runPhase(phase, true);
                    mergeCoverage(declaredSourceCoverage, phase.expectedLogicalCoverage());
                    previousPhase = phase;
                }
                if (phaseIndex >= workload.phases().size()) {
                    throw new AssertionError("benchmark has no measured phase");
                }
                captures.awaitMeasurementBoundary(workload, fork, previousPhase, positionCoverage);
                long acknowledgedBoundaryAt = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
                awaitFreshObservationAfterBoundary(workload, fork.control());
                awaitQuiescentRecordsOut(workload, fork.control());
                try (TargetWatchSet targets = TargetWatchSet.open(
                        workload, workload.phases().get(phaseIndex), fork);
                     BenchmarkTableCaptureSet tables = BenchmarkTableCaptureSet.open(workload, fork)) {
                    fork.beginTelemetryCapture();
                    for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases().subList(phaseIndex,
                            workload.phases().size())) {
                        if (phase.measured()) {
                            PhaseWindow window = runMeasuredPhase(workload, fork, phase,
                                    captures, positionCoverage, targets, tables, returnCounters, acknowledgedBoundaryAt);
                            measured.add(window.measurement());
                            allDurations.addAll(window.deliveryDurations());
                            resourceWindows.add(window.resources());
                            commandWindows.add(window.mongoCommands());
                            acknowledgedBoundaryAt = window.measurement().completedAckAtNanos();
                        } else if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.TERMINAL) {
                            if (measured.isEmpty()) {
                                throw new AssertionError("terminal arrived before any measured delivery");
                            }
                            fork.cutoffTelemetryCapture();
                            targets.expectTerminal(workload, phase);
                            terminal = fork.runPhase(phase, true);
                            chains = captures.awaitTerminalAcks(workload, fork, positionCoverage, tables);
                            targets.checkpoint(phase);
                        } else {
                            throw new AssertionError("unmeasured setup followed measured SQL: " + phase.id());
                        }
                        mergeCoverage(declaredSourceCoverage, phase.expectedLogicalCoverage());
                    }
                    observedTargetCoverage = targets.observedCoverage();
                    tableReceipts = tables.receipts();
                }
                if (terminal == null || chains == null
                        || resourceWindows.isEmpty() || commandWindows.isEmpty()) {
                    throw new AssertionError("benchmark fork ended without terminal or resource evidence");
                }
                BenchmarkResourceSampler.Summary resources = summarizeResources(resourceWindows);
                BenchmarkMongoCommandSampler.Summary mongoCommands = summarizeCommands(commandWindows);
                BenchmarkSourceLineage.verifyAfterTerminalAck(lineage, fork.sourceSettings());
                String checksum = checksum(terminal.targets());
                long errorTotal = errorTotal(workload, fork.control());
                if (errorTotal != 0) {
                    throw new AssertionError("benchmark fork published " + errorTotal + " pipeline errors");
                }
                Map<String, Long> logicalCoverage = new LinkedHashMap<>(observedTargetCoverage);
                for (BenchmarkAckOracle.SourceChain chain : chains) {
                    String terminalId = chain.sourceTerminals().getFirst().logicalId();
                    if (logicalCoverage.putIfAbsent(terminalId, 1L) != null) {
                        throw new AssertionError("terminal event overlaps observed target coverage");
                    }
                }
                BenchmarkAckOracle.Fork correctness = new BenchmarkAckOracle.Fork(
                        forkId, chains, logicalCoverage, checksum, errorTotal);
                BenchmarkAckOracle.verify(List.of(correctness));
                long completed = measured.stream().mapToLong(MeasuredPhase::acknowledgedOutputs).sum();
                long duration = measured.stream().mapToLong(MeasuredPhase::deliveryWindowNanos).sum();
                if (completed != allDurations.size() || duration <= 0) {
                    throw new AssertionError("target-ACK and observed delivery counts disagree in " + forkId);
                }
                double throughput;
                if (workload.pilotProfile()) {
                    long windowEvents = measured.stream().mapToLong(phase -> {
                        var timeline = phase.deliveryTimeline().orElseThrow();
                        return timeline.operationWindow().completedDeliveries();
                    }).sum();
                    throughput = windowEvents * 1_000_000_000.0 / duration;
                } else { throughput = completed * 1_000_000_000.0 / duration; }
                long[] latencies = allDurations.stream().mapToLong(Long::longValue).toArray();
                PipelineBenchmarkComparison.Fork performance = new PipelineBenchmarkComparison.Fork(
                        arm, throughput, latencies, resources.peakHeapBytes(), resources.peakRssBytes());
                List<Map<String, Object>> receipts = new ArrayList<>();
                receipts.addAll(tableReceipts);
                // All measured ACK, resource and command windows have already closed.
                try (StoreDocuments documents = StoreDocuments.at(fork.storeUri())) {
                    Map<String, ControlPlane.PositionRead> positions = new LinkedHashMap<>();
                    for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                        ControlPlane.PositionRead position = positions.computeIfAbsent(
                                chain.pipelineId(), fork.control()::positionRead);
                        BenchmarkAckOracle.SourceChain proof = chains.stream()
                                .filter(candidate -> candidate.id().equals(chain.id())).findFirst().orElseThrow();
                        if (proof.tableConfirmation() != null) {
                            receipts.add(PipelineBenchmarkLiveRunIT.tableConfirmation(proof.tableConfirmation()));
                        } else {
                            receipts.add(BenchmarkTerminalMetaReceipt.read(documents, position, chain,
                                    proof.sourceTerminals().getFirst().sourcePosition(), positionCoverage));
                        }
                    }
                }
                ownedReceipt.verifyBeforeClose();
                Evidence run = new Evidence(forkId, workload, arm, applicationJar,
                        measured, resources, mongoCommands,
                        declaredSourceCoverage, observedTargetCoverage, checksum, errorTotal, receipts,
                        fork.finishTelemetryCapture(), ownedReceipt);
                evidence.add(run);
                result = new PipelineBenchmarkHarness.ForkResult(performance, correctness);
            }
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (ownedReceipt != null) {
                try {
                    ownedReceipt.finishAfterClose();
                    if (primary != null) {
                        System.out.println("benchmark-owned-runtime-at-failure=" + JsonWriter.write(ownedReceipt.evidence()));
                    }
                } catch (Exception | Error recording) {
                    if (primary == null) { throw recording; }
                    if (recording != primary) { primary.addSuppressed(recording); }
                }
            }
        }
        return result;
    }

    private static PhaseWindow runMeasuredPhase(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkForkEnvironment fork, BenchmarkWorkloadDefinitions.Phase phase,
            CaptureSet captures, BenchmarkConnectorPositionCoverage positionCoverage,
            TargetWatchSet targets, BenchmarkTableCaptureSet tables, BenchmarkNativeQueueProbe returnCounters,
            long acknowledgedBoundaryAt) throws Exception {
        boolean compilationDiagnostics = Boolean.getBoolean("tapstate.e2e.benchmark.compilation-diagnostics");
        if (compilationDiagnostics && !workload.pilotProfile()) {
            throw new AssertionError("compilation diagnostics require the declared steady pilot profile");
        }
        boolean threadPointDiagnostics = Boolean.getBoolean("tapstate.e2e.benchmark.thread-point-diagnostics");
        if (threadPointDiagnostics && (!workload.pilotProfile() || !"copy".equals(workload.id())
                || compilationDiagnostics || !Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics"))) {
            throw new AssertionError("thread point diagnostics require the original copy pilot with load points");
        }
        List<BenchmarkExpectedChanges.TargetPlan> plans = BenchmarkExpectedChanges.forPhase(workload, phase);
        NativeCounterBaseline counterBefore = returnCounters == null ? null
                : awaitNativeCounterBaselines(workload, fork.control(), returnCounters, acknowledgedBoundaryAt, phase.id() + "/before");
        long initialAcknowledged = counterBefore == null ? recordsOut(workload, fork.control()) : counterBefore.total();
        BenchmarkForkEnvironment.PhaseIssue issued;
        long completedAckAt;
        long sourceMarkerWaitStartedAt;
        long sourceMarkerWaitCompletedAt;
        BenchmarkResourceSampler.Summary resources;
        BenchmarkMongoCommandSampler.Summary commands;
        BenchmarkWriteReturnCapture.Result writeReturns = null;
        CollectorCalibration collectorCalibration = System.getProperty(RETURN_COLLECTOR_CALIBRATION_PROPERTY) == null
                ? null : CollectorCalibration.valueOf(System.getProperty(RETURN_COLLECTOR_CALIBRATION_PROPERTY));
        boolean calibrationOff = collectorCalibration == CollectorCalibration.OFF;
        boolean methodControl = writeReturnMethodControl(System.getProperty(WRITE_RETURN_METHOD_CONTROL_PROPERTY),
                Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile(),
                writeReturnClockMode(System.getProperty(WRITE_RETURN_CLOCK_CONTROL_PROPERTY),
                        Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), workload.pilotProfile()), false);
        boolean methodCostProtocol = Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY)
                && System.getProperty(WRITE_RETURN_METHOD_CONTROL_PROPERTY) != null;
        Map<String, Object> returnRegistrationBefore = Map.of();
        Map<String, Object> returnRegistrationAfter = Map.of();
        Map<String, Object> nativeClockBefore = Map.of();
        Map<String, Object> nativeClockAfter = Map.of();
        Map<String, Object> retainedReturnEvidence = Map.of();
        Map<String, Object> retainedSourceIssue = Map.of("state", "UNAVAILABLE", "completeSourceRoster", false);
        var collectorCalibrationEvidence = new LinkedHashMap<String, Object>();
        if (collectorCalibration != null) {
            collectorCalibrationEvidence.put("state", "IN_PROGRESS_DIAGNOSTIC_ONLY");
            collectorCalibrationEvidence.put("mode", collectorCalibration.name());
            collectorCalibrationEvidence.put("estimand", "INCREMENTAL_RETURN_COLLECTOR_WITH_COMMON_RESOURCE_COMMAND_AND_TARGET_WITNESSES");
            BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> collectorCalibrationEvidence.put(flag, false));
            collectorCalibrationEvidence.put("causalOverheadQualified", false);
        }
        BenchmarkResourceSampler.Checkpoint collectorResourceCheckpoint = null;
        BenchmarkMongoCommandSampler.Checkpoint collectorCommandCheckpoint = null;
        long captureDrainStartedAt = 0;
        long captureDrainCompletedAt = 0;
        BenchmarkForkEnvironment.ClockAnchor calibrationSourceAnchor = null;
        List<String> targetClockUris = phase.targets().stream().map(target ->
                target.location() == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                        ? fork.managedViewsUri() : fork.externalTargetUri()).distinct().toList();
        List<BenchmarkTargetClock.Reading> targetClocksBefore = workload.pilotProfile()
                ? targetClockUris.stream().map(BenchmarkTargetClock::read).toList() : List.of();
        if (!targetClocksBefore.isEmpty()) { BenchmarkTargetClock.requireSharedClock(targetClocksBefore); }
        BenchmarkTargetClock.Reading targetClockBefore = targetClocksBefore.isEmpty() ? null : targetClocksBefore.getFirst();
        Map<String, Object> interiorClockEvidence = Map.of();
        List<BenchmarkTargetClock.Reading> interiorClockReadings = List.of();
        boolean loadDiagnostics = workload.pilotProfile()
                && Boolean.getBoolean("tapstate.e2e.benchmark.load-diagnostics");
        BenchmarkNativeQueueProbe nativeProbe = loadDiagnostics
                ? new BenchmarkNativeQueueProbe(fork.control(), fork.server().baseUrl().toString(), "tapstate",
                        new com.hazelcast.config.MetricsConfig().getCollectionFrequencySeconds()) : null;
        BenchmarkUnreadSampler nativeQueues = null;
        BenchmarkUnreadSampler unread = null;
        try {
            nativeQueues = nativeProbe == null ? null : new BenchmarkUnreadSampler(() ->
                    workload.pipelineIds().stream().map(nativeProbe::read).toList());
            unread = loadDiagnostics ? new BenchmarkUnreadSampler(tables) : null;
        } catch (RuntimeException | Error failure) {
            if (nativeQueues != null) {
                try { nativeQueues.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            }
            if (nativeProbe != null) {
                try { nativeProbe.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            }
            throw failure;
        }
        Throwable phaseFailure = null;
        try (BenchmarkResourceSampler resourceSampler = workload.pilotProfile()
                ? BenchmarkResourceSampler.openForPhaseBudget(fork.server().pid(), RESOURCE_INTERVAL, ACK_WAIT)
                : BenchmarkResourceSampler.open(fork.server().pid(), RESOURCE_INTERVAL);
             BenchmarkMongoCommandSampler commandSampler = BenchmarkMongoCommandSampler.open(fork.storeUri());
             BenchmarkTargetClockSampler clockSampler = targetClockBefore == null ? null
                     : BenchmarkTargetClockSampler.open(targetClockUris.getFirst())) {
            fork.ownedProcessReceipt().recordJvmRuntime(resourceSampler.runtimeEvidence());
            if (collectorCalibration != null) {
                String rootRaw = io.tapstate.adapters.pdk.PdkBenchmarkClock.metadata();
                collectorCalibrationEvidence.put("rootBeforeRaw", BenchmarkWriteReturnCostStages.rawEvidence(rootRaw));
                collectorCalibrationEvidence.put("rootBefore", supportedRootClockEvidence(rootRaw,
                        System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY)));
                returnRegistrationBefore = resourceSampler.writeReturnReader().registrationEvidence(!calibrationOff);
                collectorCalibrationEvidence.put("registrationBefore", returnRegistrationBefore);
                nativeClockBefore = collectorOwnedClockEvidence(collectorCalibration, resourceSampler.writeReturnReader(),
                        System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY), rootRaw);
            } else if (System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY) != null) {
                nativeClockBefore = resourceSampler.writeReturnReader().nativeClockEvidence(
                        System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY), io.tapstate.adapters.pdk.PdkBenchmarkClock.metadata());
            }
            if (methodCostProtocol) {
                returnRegistrationBefore = resourceSampler.writeReturnReader().registrationEvidence(!methodControl);
            }
            if (compilationDiagnostics) {
                resourceSampler.enableCompilationDiagnostics();
            }
            if (threadPointDiagnostics) {
                resourceSampler.enableThreadPointDiagnostics();
            }
            Throwable samplingFailure = null;
            try {
                resourceSampler.start();
                commandSampler.start();
                try (BenchmarkWriteReturnCapture returnCapture = collectWriteReturns(
                        Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY), methodControl || calibrationOff)
                        ? BenchmarkWriteReturnCapture.open(resourceSampler.writeReturnReader(),
                                workload.id() + "/" + phase.id(), writeReturnClockMode(
                                        System.getProperty(WRITE_RETURN_CLOCK_CONTROL_PROPERTY), true, workload.pilotProfile()),
                                Boolean.getBoolean(WRITE_RETURN_COST_STAGES_PROPERTY)) : null) {
                    try {
                        issued = fork.issuePhase(phase, true, (current, batchIndex, issuedAt, sql) -> {
                            for (BenchmarkExpectedChanges.TargetPlan plan : plans) {
                                List<BenchmarkMongoDeliveryObserver.ExpectedChange> changes = plan.forBatch(batchIndex);
                                if (!changes.isEmpty()) {
                                    targets.expectBatch(plan.target(), phase.id(), issuedAt, changes);
                                }
                            }
                        });
                        if (collectorCalibration != null) { calibrationSourceAnchor = issued.clockAnchor(); }
                        if (Boolean.getBoolean(WRITE_RETURN_DIAGNOSTICS_PROPERTY)) { retainedSourceIssue = sourceIssueEvidence(issued); }
                        if (issued.batches().isEmpty()) {
                            throw new AssertionError("measured phase has no source batches: " + phase.id());
                        }
                        sourceMarkerWaitStartedAt = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
                        captures.awaitMeasuredSourceMarkers(workload, phase);
                        sourceMarkerWaitCompletedAt = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
                        completedAckAt = tables.awaitMeasured(workload, phase.id());
                        if (collectorCalibration != null) {
                            collectorCalibrationEvidence.put("fourWriterTableAckAtNanos", completedAckAt);
                            collectorCalibrationEvidence.put("sourceIssue", retainedSourceIssue);
                            collectorResourceCheckpoint = resourceSampler.checkpoint();
                            collectorCalibrationEvidence.put("resourceCheckpoint", Map.of(
                                    "attemptIndex", collectorResourceCheckpoint.attemptIndex(),
                                    "startedAtNanos", collectorResourceCheckpoint.startedAtNanos(),
                                    "completedAtNanos", collectorResourceCheckpoint.completedAtNanos()));
                            collectorCalibrationEvidence.put("commonResources", PipelineBenchmarkLiveRunIT.resourceEvidence(
                                    collectorResourceCheckpoint.summary(), issued.clockAnchor()));
                            collectorCommandCheckpoint = commandSampler.checkpoint();
                            collectorCalibrationEvidence.put("commandCheckpoint", Map.of(
                                    "startedAtNanos", collectorCommandCheckpoint.startedAtNanos(),
                                    "completedAtNanos", collectorCommandCheckpoint.completedAtNanos()));
                            collectorCalibrationEvidence.put("commonCommands", collectorCommands(collectorCommandCheckpoint.summary()));
                            captureDrainStartedAt = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
                            collectorCalibrationEvidence.put("captureDrain", Map.of("state", calibrationOff ? "DISABLED" : "IN_PROGRESS",
                                    "startedAtNanos", captureDrainStartedAt));
                        }
                        if (returnCapture != null) {
                            writeReturns = returnCapture.finish();
                            retainedReturnEvidence = returnCapture.retainedEvidence();
                        }
                        if (collectorCalibration != null) {
                            captureDrainCompletedAt = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
                            collectorCalibrationEvidence.put("cutoffOrder", collectorCutoffOrder(completedAckAt,
                                    collectorResourceCheckpoint.startedAtNanos(), collectorResourceCheckpoint.completedAtNanos(),
                                    collectorCommandCheckpoint.startedAtNanos(), collectorCommandCheckpoint.completedAtNanos(),
                                    captureDrainStartedAt, captureDrainCompletedAt));
                            collectorCalibrationEvidence.put("captureDrain", Map.of("state", calibrationOff ? "NOT_RUN_DISABLED_PRODUCER" : "RECORDED_DIAGNOSTIC",
                                    "startedAtNanos", captureDrainStartedAt, "completedAtNanos", captureDrainCompletedAt,
                                    "scope", "POST_ACK_CAPTURE_STOP_FINAL_CLOCK_SUMMARY_PAGES_AND_ASSEMBLY",
                                    "performanceAcceptanceEligible", false, "samplingCostQualified", false));
                        }
                    } catch (Exception | Error failure) {
                        if (returnCapture != null) {
                            System.out.println("benchmark-write-return-refusal=" + JsonWriter.write(
                                    Map.of("state", "UNKNOWN", "nativeClockBefore", nativeClockBefore,
                                            "collectorCalibration", Map.copyOf(collectorCalibrationEvidence),
                                            "capture", returnCapture.retainedEvidence(),
                                            "sourceIssue", retainedSourceIssue, "resourceSummaryAvailable", false,
                                            "performanceAcceptanceEligible", false)));
                        } else if (methodCostProtocol) {
                            System.out.println("benchmark-return-method-control-refusal=" + JsonWriter.write(Map.of(
                                    "state", "UNKNOWN", "sourceIssue", retainedSourceIssue,
                                    "registrationBefore", returnRegistrationBefore,
                                    "rawReturnReceipt", "UNAVAILABLE_DISABLED_PRODUCER",
                                    "performanceAcceptanceEligible", false, "samplingCostQualified", false)));
                        }
                        throw failure;
                    }
                }
                resources = resourceSampler.finish();
                commands = collectorCalibration == null ? commandSampler.finish()
                        : finishCollectorCommands(resources, issued.clockAnchor(), collectorCalibrationEvidence, commandSampler::finish);
                if (collectorCalibration != null) {
                    collectorCalibrationEvidence.put("fullPhaseCommands", collectorCommands(commands));
                    collectorCalibrationEvidence.put("fullPhaseScope", "SOURCE_ISSUE_THROUGH_CAPTURE_DRAIN_AND_FINAL_RESOURCE_COMMAND_READS");
                    String rootRaw = io.tapstate.adapters.pdk.PdkBenchmarkClock.metadata();
                    collectorCalibrationEvidence.put("rootAfterRaw", BenchmarkWriteReturnCostStages.rawEvidence(rootRaw));
                    var rootAfter = supportedRootClockEvidence(rootRaw, System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY));
                    collectorCalibrationEvidence.put("rootAfter", rootAfter);
                    if (!((Map<?, ?>) collectorCalibrationEvidence.get("rootBefore")).get("supportedSnapshot")
                            .equals(rootAfter.get("supportedSnapshot"))) {
                        throw new AssertionError("collector calibration root native route changed across the phase");
                    }
                    returnRegistrationAfter = resourceSampler.writeReturnReader().registrationEvidence(!calibrationOff);
                    collectorCalibrationEvidence.put("registrationAfter", returnRegistrationAfter);
                    nativeClockAfter = collectorOwnedClockEvidence(collectorCalibration, resourceSampler.writeReturnReader(),
                            System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY), rootRaw);
                    collectorCalibrationEvidence.put("state", "INCREMENTAL_RETURN_COLLECTOR_DIAGNOSTIC_ONLY");
                } else if (System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY) != null) {
                    nativeClockAfter = resourceSampler.writeReturnReader().nativeClockEvidence(
                            System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY), io.tapstate.adapters.pdk.PdkBenchmarkClock.metadata());
                }
                if (methodCostProtocol) {
                    returnRegistrationAfter = resourceSampler.writeReturnReader().registrationEvidence(!methodControl);
                }
                if (clockSampler != null) {
                    clockSampler.close();
                    interiorClockEvidence = clockSampler.evidence();
                    interiorClockReadings = clockSampler.readings();
                    BenchmarkTargetClock.validate(targetClockBefore, interiorClockReadings.getFirst());
                }
            } catch (Exception | Error failure) {
                samplingFailure = failure;
                throw failure;
            } finally {
                try {
                    resourceSampler.compilationEvidence().ifPresent(reading ->
                        System.out.println("benchmark-compilation-timeline=" + JsonWriter.write(Map.of(
                                "phase", phase.id(), "ownedPid", fork.server().pid(),
                                "intervalMillis", RESOURCE_INTERVAL.toMillis(), "evidence", reading,
                                "scope", "CUMULATIVE_APPROXIMATE_COMPILATION_ELAPSED_COUNTER_READS",
                                "performanceAcceptanceEligible", false))));
                    resourceSampler.threadPointEvidence().ifPresent(reading ->
                        System.out.println("benchmark-thread-point-timeline=" + JsonWriter.write(Map.of(
                                "phase", phase.id(), "ownedPid", fork.server().pid(),
                                "intervalMillis", RESOURCE_INTERVAL.toMillis(), "evidence", reading,
                                "scope", "CONDITIONAL_PLATFORM_THREAD_CPU_AND_POSITIVE_STACK_POINT_READS",
                                "captureRoleCoverage", "UNKNOWN", "performanceAcceptanceEligible", false))));
                } catch (RuntimeException | Error diagnosticFailure) {
                    if (samplingFailure != null) {
                        samplingFailure.addSuppressed(diagnosticFailure);
                    } else {
                        throw diagnosticFailure;
                    }
                }
            }
        } catch (BenchmarkWriteReturnReader.NativeClockRefusal failure) {
            System.out.println("benchmark-native-clock-refusal=" + JsonWriter.write(Map.of(
                    "state", "UNKNOWN", "nativeClockBefore", nativeClockBefore,
                    "nativeClockRefusal", failure.retainedEvidence(), "capture", retainedReturnEvidence,
                    "collectorCalibration", Map.copyOf(collectorCalibrationEvidence),
                    "sourceIssue", retainedSourceIssue, "performanceAcceptanceEligible", false,
                    "samplingCostQualified", false)));
            phaseFailure = failure;
            throw failure;
        } catch (BenchmarkMongoCommandSampler.CheckpointFailure failure) {
            try {
                retainCollectorCommandRefusal(collectorCalibrationEvidence, failure);
                System.out.println("benchmark-return-collector-command-refusal=" + JsonWriter.write(Map.of(
                        "state", "UNKNOWN", "nativeClockBefore", nativeClockBefore, "nativeClockAfter", nativeClockAfter,
                        "collectorCalibration", Map.copyOf(collectorCalibrationEvidence), "capture", retainedReturnEvidence,
                        "sourceIssue", retainedSourceIssue, "performanceAcceptanceEligible", false, "samplingCostQualified", false)));
            } catch (RuntimeException | Error recording) {
                if (recording != failure) { failure.addSuppressed(recording); }
            }
            phaseFailure = failure;
            throw failure;
        } catch (BenchmarkResourceSampler.SamplingFailure failure) {
            try {
                if (collectorCalibration != null) {
                    collectorCalibrationEvidence.put("resourceSamplingRefusal", collectorSamplingRefusal(failure, calibrationSourceAnchor));
                }
                if (!retainedReturnEvidence.isEmpty() || !nativeClockBefore.isEmpty() || !collectorCalibrationEvidence.isEmpty()) {
                    System.out.println("benchmark-write-return-resource-refusal=" + JsonWriter.write(Map.of(
                            "state", "UNKNOWN", "nativeClockBefore", nativeClockBefore,
                            "capture", retainedReturnEvidence, "sourceIssue", retainedSourceIssue,
                            "collectorCalibration", Map.copyOf(collectorCalibrationEvidence),
                            "resourceFailureStage", failure.stage().name(), "resourceFailureReason", failure.reason().name(),
                            "performanceAcceptanceEligible", false)));
                }
            } catch (RuntimeException | Error recording) {
                if (recording != failure) { failure.addSuppressed(recording); }
            }
            phaseFailure = failure.inPhase(phase.id());
            throw (BenchmarkResourceSampler.SamplingFailure) phaseFailure;
        } catch (Exception | Error failure) {
            if (!nativeClockBefore.isEmpty() || !collectorCalibrationEvidence.isEmpty()) {
                System.out.println("benchmark-native-clock-phase-refusal=" + JsonWriter.write(Map.of(
                        "state", "UNKNOWN", "nativeClockBefore", nativeClockBefore,
                        "nativeClockAfter", nativeClockAfter, "sourceIssue", retainedSourceIssue,
                        "collectorCalibration", Map.copyOf(collectorCalibrationEvidence),
                        "performanceAcceptanceEligible", false, "samplingCostQualified", false)));
            }
            phaseFailure = failure;
            throw failure;
        } finally {
            boolean primaryFailed = phaseFailure != null;
            if (nativeQueues != null) {
                try { nativeQueues.close(); }
                catch (RuntimeException | Error cleanup) {
                    if (phaseFailure != null) { phaseFailure.addSuppressed(cleanup); }
                    else { phaseFailure = cleanup; }
                }
            }
            if (nativeProbe != null) {
                try { nativeProbe.close(); }
                catch (RuntimeException | Error cleanup) {
                    if (phaseFailure != null) { phaseFailure.addSuppressed(cleanup); }
                    else { phaseFailure = cleanup; }
                }
            }
            if (unread != null) {
                try { unread.close(); }
                catch (RuntimeException | Error cleanup) {
                    if (phaseFailure != null) { phaseFailure.addSuppressed(cleanup); }
                    else { throw cleanup; }
                }
            }
            if (!primaryFailed && phaseFailure != null) {
                if (phaseFailure instanceof RuntimeException failure) { throw failure; }
                if (phaseFailure instanceof Error failure) { throw failure; }
                throw new AssertionError("measurement cleanup failed", phaseFailure);
            }
        }
        if (unread != null) {
            System.out.println("benchmark-unread-timeline=" + JsonWriter.write(Map.of("phase", phase.id(),
                    "intervalMillis", 200, "samples", unread.samples(), "performanceAcceptanceEligible", false,
                    "samplingScope", "EXTERNAL_SEQUENTIAL_POINT_READS_WITH_RECORDED_READ_BRACKETS")));
        }
        if (nativeQueues != null) {
            System.out.println("benchmark-native-queue-timeline=" + JsonWriter.write(Map.of("phase", phase.id(),
                    "queryIntervalMillis", 200, "samples", nativeQueues.samples(), "performanceAcceptanceEligible", false,
                    "samplingScope", "READ_ONLY_NATIVE_JOB_METRICS_WITH_DECLARED_COLLECTION_CADENCE")));
        }
        Map<String, Object> capturedForClockFailure = methodControl || calibrationOff
                ? Map.of("state", "UNAVAILABLE", "reason", calibrationOff ? "RETURN_COLLECTOR_CALIBRATION_DISABLED_PRODUCER"
                        : "RETURN_METHOD_COST_CONTROL_DISABLED_PRODUCER",
                        "registrationBefore", returnRegistrationBefore, "registrationAfter", returnRegistrationAfter)
                : retainedReturnEvidence;
        Map<String, Object> sourceForClockFailure = retainedSourceIssue;
        Map<String, Object> nativeBeforeForClockFailure = nativeClockBefore;
        Map<String, Object> nativeAfterForClockFailure = nativeClockAfter;
        List<BenchmarkTargetClock.Reading> interiorForClockCheck = interiorClockReadings;
        ValidatedTargetClocks checkedClocks = BenchmarkReturnFailureRetention.run(() -> {
            List<BenchmarkTargetClock.Reading> after = targetClockBefore == null ? List.of()
                    : targetClockUris.stream().map(BenchmarkTargetClock::read).toList();
            if (!after.isEmpty()) {
                BenchmarkTargetClock.requireSharedClock(after);
                for (int i = 0; i < targetClocksBefore.size(); i++) {
                    BenchmarkTargetClock.validate(targetClocksBefore.get(i), after.get(i));
                }
            }
            BenchmarkTargetClock.Reading firstAfter = after.isEmpty() ? null : after.getFirst();
            if (!interiorForClockCheck.isEmpty()) {
                BenchmarkTargetClock.validate(interiorForClockCheck.getLast(), firstAfter);
            }
            Map<String, Object> outer = targetClockBefore == null
                    ? Map.of("state", "UNQUALIFIED", "reason", "MULTI_TARGET_CLOCKS_NOT_YET_CALIBRATED")
                    : BenchmarkTargetClock.validate(targetClockBefore, firstAfter);
            return new ValidatedTargetClocks(after, outer);
        }, () -> capturedForClockFailure.isEmpty() ? Map.of() : Map.of(
                "state", "UNKNOWN", "capture", capturedForClockFailure, "sourceIssue", sourceForClockFailure,
                "nativeClockBefore", nativeBeforeForClockFailure, "nativeClockAfter", nativeAfterForClockFailure,
                "collectorCalibration", Map.copyOf(collectorCalibrationEvidence),
                "resources", PipelineBenchmarkLiveRunIT.resourceEvidence(resources, issued.clockAnchor()),
                "reason", "POST_CAPTURE_TARGET_CLOCK_REFUSAL", "performanceAcceptanceEligible", false,
                "samplingCostQualified", false), failureEvidence -> System.out.println(
                        "benchmark-write-return-post-capture-refusal=" + JsonWriter.write(failureEvidence)));
        List<BenchmarkTargetClock.Reading> targetClocksAfter = checkedClocks.after();
        Map<String, Object> outerClockEvidence = checkedClocks.outer();
        var clockProof = new LinkedHashMap<String, Object>(outerClockEvidence);
        if (collectorCalibration != null) {
            clockProof.put("returnCollectorCalibration", Map.copyOf(collectorCalibrationEvidence));
        }
        if (!nativeClockBefore.isEmpty() || !nativeClockAfter.isEmpty()) {
            clockProof.put("nativeClockBefore", nativeClockBefore);
            clockProof.put("nativeClockAfter", nativeClockAfter);
        }
        if (counterBefore != null) { clockProof.put("nativeCounterBaselineBefore", counterBefore.evidence()); }
        if (methodCostProtocol) {
            clockProof.put("returnProbeRegistrationBefore", returnRegistrationBefore);
            clockProof.put("returnProbeRegistrationAfter", returnRegistrationAfter);
            long fullFirstIssued = issued.batches().getFirst().issuedAtNanos();
            var costDiagnostic = new LinkedHashMap<String, Object>(Map.of(
                    "state", "RECORDED_DIAGNOSTIC", "actualProducerEnabled", !methodControl,
                    "actualPeriodicClockEnabled", !methodControl && !Boolean.getBoolean(WRITE_RETURN_CLOCK_CONTROL_PROPERTY),
                    "sourceIssue", retainedSourceIssue,
                    "driverPhaseDrain", Map.of("firstSourceIssuedAtNanos", fullFirstIssued,
                            "tableAckObservedAtNanos", completedAckAt,
                            "durationNanos", Math.subtractExact(completedAckAt, fullFirstIssued),
                            "scope", "FULL_SOURCE_FIRST_ISSUE_TO_OWN_TABLE_ACK_OBSERVATION"),
                    "endpointRole", "DIAGNOSTIC_PROXY_ONLY_NOT_ACKNOWLEDGED_WRITE_RETURN",
                    "performanceAcceptanceEligible", false, "samplingCostQualified", false));
            costDiagnostic.put("resourceReceiptScope", methodControl ? "FULL_PHASE_THROUGH_RESOURCE_FINISH_NO_RETURN_CAPTURE"
                    : "FULL_PHASE_INCLUDES_POST_ACK_RETURN_CAPTURE_CLOSE_AND_PAGE_READS");
            clockProof.put("returnMethodCostDiagnostic", Map.copyOf(costDiagnostic));
        }
        if (methodControl || calibrationOff) {
            var unavailable = Map.of("state", "UNAVAILABLE", "reason", calibrationOff
                    ? "RETURN_COLLECTOR_CALIBRATION_DISABLED_PRODUCER" : "RETURN_METHOD_COST_CONTROL_DISABLED_PRODUCER");
            var disabled = new LinkedHashMap<String, Object>(Map.of("state", calibrationOff
                    ? "RETURN_COLLECTOR_CALIBRATION_OFF" : "RETURN_METHOD_COST_CONTROL",
                    "actualProducerEnabled", false, "actualPeriodicClockEnabled", false,
                    "returnScope", unavailable, "rawReturnReceipt", unavailable,
                    "p99LatencyNanos", unavailable, "completionSpanNanos", unavailable,
                    "throughput", unavailable, "commonWindowCertificate", unavailable,
                    "performanceAcceptanceEligible", false));
            disabled.put("samplingCostQualified", false);
            disabled.put("resourceBounds", unavailable);
            disabled.put("cohortReturnProof", unavailable);
            if (calibrationOff) {
                disabled.put("ownedNativeClock", unavailable);
                BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> disabled.put(flag, false));
            }
            clockProof.put("writeReturnDiagnostics", Map.copyOf(disabled));
        }
        if (writeReturns != null) {
            try {
                BenchmarkNativeReturnClock nativeReturnClock = null;
                if (Boolean.getBoolean(NATIVE_COUNTER_DOMAIN_PROPERTY) || collectorCalibration == CollectorCalibration.ON) {
                    var owned = writeReturns.samples().getFirst().identity();
                    var root = new BenchmarkCausalClock.Identity(ProcessHandle.current().pid(),
                            java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime());
                    nativeReturnClock = new BenchmarkNativeReturnClock(owned, root,
                            System.getProperty(NATIVE_CLOCK_LIBRARY_PROPERTY), writeReturns.samples(),
                            nativeClockBefore, nativeClockAfter);
                }
                Map<String, Object> returnEvidence = new LinkedHashMap<>(BenchmarkWriteReturnPhaseEvidence.record(
                        workload, phase, issued.batches(), writeReturns, nativeReturnClock));
                if (writeReturns.clockMode() == BenchmarkReturnClockSampler.Mode.PERIODIC) {
                    var associations = BenchmarkWriteReturnExpectations.associate(
                            workload, phase, issued.batches(), writeReturns.calls());
                    var owner = writeReturns.samples().getFirst().identity();
                    BenchmarkReturnPointClock returnClock = nativeReturnClock == null
                            ? new BenchmarkCausalClock(owner, writeReturns.samples()) : nativeReturnClock;
                    var mappedReturns = BenchmarkReturnTimeBounds.map(associations, owner, returnClock);
                    returnEvidence.put("resourceBounds", BenchmarkReturnResourceBounds.evidence(resources, mappedReturns));
                    returnEvidence.put("commonWindowCertificate", BenchmarkReturnCommonWindow.evidence(mappedReturns));
                } else {
                    var unavailable = Map.of("state", "UNAVAILABLE", "reason", "FIRST_FINAL_CLOCK_COST_CONTROL",
                            "samplingCostQualified", false, "performanceAcceptanceEligible", false);
                    returnEvidence.put("resourceBounds", unavailable);
                    returnEvidence.put("commonWindowCertificate", unavailable);
                }
                long fullFirstIssued = issued.batches().getFirst().issuedAtNanos();
                returnEvidence.put("driverPhaseDrain", Map.of("state", "RECORDED_DIAGNOSTIC",
                        "firstSourceIssuedAtNanos", fullFirstIssued, "tableAckObservedAtNanos", completedAckAt,
                        "durationNanos", Math.subtractExact(completedAckAt, fullFirstIssued),
                        "scope", "FULL_SOURCE_FIRST_ISSUE_TO_OWN_TABLE_ACK_OBSERVATION",
                        "performanceAcceptanceEligible", false, "samplingCostQualified", false));
                returnEvidence.put("resourceReceiptScope", "FULL_PHASE_RETAINED_THROUGH_CAPTURE_CLOSE");
                clockProof.put("writeReturnDiagnostics", Map.copyOf(returnEvidence));
                System.out.println("benchmark-write-return-phase-evidence=" + JsonWriter.write(returnEvidence));
            } catch (RuntimeException | Error refusal) {
                System.out.println("benchmark-write-return-association-refusal=" + JsonWriter.write(Map.of(
                        "state", "UNKNOWN", "nativeClockBefore", nativeClockBefore, "nativeClockAfter", nativeClockAfter,
                        "capture", retainedReturnEvidence, "sourceIssue", retainedSourceIssue,
                        "collectorCalibration", Map.copyOf(collectorCalibrationEvidence),
                        "resources", PipelineBenchmarkLiveRunIT.resourceEvidence(resources, issued.clockAnchor()),
                        "reason", "SOURCE_ASSOCIATION_OR_TIME_BOUNDS_REFUSED", "performanceAcceptanceEligible", false)));
                throw refusal;
            }
        }
        clockProof.put("sampledInterior", interiorClockEvidence);
        boolean deferredWitness = targets.readDeferred();
        if (workload.pilotProfile()) {
            System.out.println("benchmark-target-clock=" + JsonWriter.write(Map.of("workload", workload.id(),
                    "phase", phase.id(), "calibration", Map.copyOf(clockProof),
                    "targetClockUriCount", targetClockUris.size(), "allBefore", targetClocksBefore.stream().map(BenchmarkTargetClock.Reading::evidence).toList(),
                    "allAfter", targetClocksAfter.stream().map(BenchmarkTargetClock.Reading::evidence).toList(),
                    "sampledInterior", interiorClockEvidence)));
        }
        targets.releaseAfterOwnMeasuredAck(completedAckAt);
        var targetStreams = targets.checkpointStreams(phase);
        clockProof.put("targetWitnessDeferred", deferredWitness);
        var readReceipts = targets.readSchedules();
        clockProof.put("targetWitnessReadReceipts", readReceipts);
        if (clockRefusalEvidenceRecorded(readReceipts)) {
            clockProof.put("operationClockRefusalEvidenceEnabled", true);
        }
        List<BenchmarkMongoDeliveryObserver.Delivery> deliveries = targetStreams.values().stream().flatMap(List::stream).toList();
        if (deliveries.size() != phase.expectedLogicalOutputChanges()) {
            throw new AssertionError("observed " + deliveries.size() + " deliveries for " + phase.id()
                    + ", expected " + phase.expectedLogicalOutputChanges());
        }
        fork.completePhase(phase);
        long reportedRecordsOut;
        if (returnCounters == null) {
            reportedRecordsOut = awaitRecordsOut(workload, fork.control(), initialAcknowledged, phase.expectedLogicalOutputChanges());
        } else {
            NativeCounterBaseline counterAfter = awaitNativeCounterBaselines(workload, fork.control(), returnCounters,
                    completedAckAt, phase.id() + "/after");
            reportedRecordsOut = Math.subtractExact(counterAfter.total(), initialAcknowledged);
            long nativeDelta = 0;
            for (String pipeline : workload.pipelineIds()) {
                nativeDelta = Math.addExact(nativeDelta, BenchmarkNativeCounterBaseline.delta(
                        counterBefore.snapshots().get(pipeline), counterAfter.snapshots().get(pipeline)));
            }
            if (reportedRecordsOut != nativeDelta) {
                throw new AssertionError("native and flat baseline phase deltas contradict each other");
            }
            if (reportedRecordsOut < phase.expectedLogicalOutputChanges()) {
                throw new AssertionError("fresh native counter delta has fewer rows than the complete measured phase");
            }
            clockProof.put("nativeCounterBaselineAfter", counterAfter.evidence());
            clockProof.put("reportedRecordsOutScope", "FRESH_NATIVE_AND_FLAT_CORRESPONDING_BASELINES");
        }
        Map<String, Object> targetClockEvidence = Map.copyOf(clockProof);
        long firstIssued = issued.batches().getFirst().issuedAtNanos();
        long expectedSourceChanges = phase.expectedLogicalCoverage().values().stream()
                .mapToLong(Long::longValue).sum();
        List<BenchmarkMongoDeliveryObserver.Delivery> cohort = deliveries.stream()
                .filter(delivery -> workload.inFixedCohort(delivery.key())).toList();
        long measuredCount = cohort.size();
        if (workload.pilotProfile()) {
            long expected = phase.expectedLogicalOutputChanges() / 2;
            if (measuredCount != expected || measuredCount < 10_000) {
                throw new AssertionError("the fixed middle cohort lacks its complete expected deliveries");
            }
            firstIssued = cohort.stream().mapToLong(BenchmarkMongoDeliveryObserver.Delivery::issuedAtNanos)
                    .min().orElseThrow();
        }
        ConfirmationTiming timing = new ConfirmationTiming(sourceMarkerWaitStartedAt, sourceMarkerWaitCompletedAt,
                completedAckAt, cohort.stream().mapToLong(BenchmarkMongoDeliveryObserver.Delivery::observedAtNanos)
                        .min().orElseThrow(),
                cohort.stream().mapToLong(BenchmarkMongoDeliveryObserver.Delivery::observedAtNanos)
                        .max().orElseThrow());
        List<List<Long>> operationStreams = targetStreams.values().stream()
                .map(stream -> stream.stream().filter(delivery -> workload.inFixedCohort(delivery.key()))
                        .map(BenchmarkMongoDeliveryObserver.Delivery::serverOperationWallMillis).toList())
                .filter(stream -> !stream.isEmpty()).toList();
        var timeline = new DeliveryTimeline(
                deliveries.stream().mapToLong(BenchmarkMongoDeliveryObserver.Delivery::observedAtNanos).min().orElseThrow(),
                deliveries.stream().mapToLong(BenchmarkMongoDeliveryObserver.Delivery::observedAtNanos).max().orElseThrow(),
                timing.firstTargetObservedAtNanos(), timing.lastTargetObservedAtNanos(), deliveries.size(), cohort.size(),
                cohort.stream().map(BenchmarkMongoDeliveryObserver.Delivery::observedAtNanos).sorted().toList(),
                deliveries.stream().map(BenchmarkMongoDeliveryObserver.Delivery::observedAtNanos).sorted().toList(),
                BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(operationStreams), operationStreams);
        BenchmarkSteadyOutputWindow.commonIntervalMillis(operationStreams);
        // Cached operation dates have no established mapping error; retain the full measured resource samples.
        Optional<BenchmarkTargetClock.LocalWindow> resourceWindow = Optional.empty();
        if (workload.pilotProfile()) {
            System.out.println("benchmark-pre-evaluation-output-timeline=" + JsonWriter.write(Map.ofEntries(
                    Map.entry("workload", workload.id()), Map.entry("phase", phase.id()), Map.entry("rows", workload.rows()),
                    Map.entry("sourceBatches", issued.batches().stream().map(batch -> Map.of("index", batch.index(),
                            "issuedAtNanos", batch.issuedAtNanos(), "completedAtNanos", batch.completedAtNanos())).toList()),
                    Map.entry("cohortObservedAtNanos", timeline.cohortObservedAtNanos()),
                    Map.entry("fullObservedAtNanos", timeline.fullObservedAtNanos()),
                    Map.entry("resources", PipelineBenchmarkLiveRunIT.resourceEvidence(resources, issued.clockAnchor())),
                    Map.entry("targetObserverReadCosts", targets.readCosts()),
                    Map.entry("cohortServerOperationWallMillis", cohort.stream()
                            .map(BenchmarkMongoDeliveryObserver.Delivery::serverOperationWallMillis).toList()),
                    Map.entry("cohortOperationStreams", operationStreams),
                    Map.entry("performanceAcceptanceEligible", false))));
        }
        return new PhaseWindow(new MeasuredPhase(phase.id(), measuredCount,
                firstIssued, issued.sourceCompletedAtNanos(), completedAckAt, expectedSourceChanges,
                deliveries.size(), reportedRecordsOut, issued.clockAnchor(), issued.batches(), resources,
                Optional.of(timing), Optional.of(timeline), workload.pilotProfile(), resourceWindow, targetClockEvidence),
                cohort.stream().map(BenchmarkMongoDeliveryObserver.Delivery::durationNanos).toList(),
                resources, commands);
    }

    private static Map<String, Object> sourceIssueEvidence(BenchmarkForkEnvironment.PhaseIssue issued) {
        var anchor = issued.clockAnchor();
        return Map.of("state", "RECORDED", "completeSourceRoster", true,
                "startedAtNanos", issued.startedAtNanos(), "sourceCompletedAtNanos", issued.sourceCompletedAtNanos(),
                "clockAnchor", Map.of("utc", anchor.utc().toString(), "beforeNanos", anchor.beforeNanos(),
                        "afterNanos", anchor.afterNanos()),
                "batches", issued.batches().stream().map(batch -> Map.of("index", batch.index(),
                        "issuedAtNanos", batch.issuedAtNanos(), "completedAtNanos", batch.completedAtNanos())).toList());
    }

    private record NativeCounterBaseline(long total, Map<String, Object> evidence,
                                         Map<String, BenchmarkNativeCounterBaseline.Snapshot> snapshots) {
        NativeCounterBaseline { evidence = Map.copyOf(evidence); snapshots = Map.copyOf(snapshots); }
    }

    /** The benchmark creates one fresh process and pipeline scope and performs no continuation. */
    private static NativeCounterBaseline awaitNativeCounterBaselines(BenchmarkWorkloadDefinitions.Workload workload,
            ControlPlane control, BenchmarkNativeQueueProbe probe, long acknowledgedAt, String stage) throws InterruptedException {
        var guards = new LinkedHashMap<String, BenchmarkNativeCounterBaseline>();
        workload.pipelineIds().forEach(pipeline -> guards.put(pipeline,
                new BenchmarkNativeCounterBaseline(acknowledgedAt, PRE_WINDOW_QUIET)));
        long deadline = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() + ACK_WAIT.toNanos();
        var complete = new java.util.HashSet<String>();
        try {
            while (complete.size() != guards.size()) {
                for (var entry : guards.entrySet()) {
                    if (complete.contains(entry.getKey())) { continue; }
                    var snapshot = probe.delivery(entry.getKey());
                    long flat = control.recordsOut(entry.getKey()).orElseThrow(() ->
                            new AssertionError("native baseline has no matching flat observation"));
                    if (entry.getValue().observe(snapshot, flat, io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime())) { complete.add(entry.getKey()); }
                }
                if (complete.size() == guards.size()) { break; }
                if (io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() >= deadline) { throw new AssertionError("native baseline publications did not qualify"); }
                TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            }
        } catch (RuntimeException | Error | InterruptedException failure) {
            System.out.println("benchmark-native-baseline-refusal=" + JsonWriter.write(Map.of(
                    "stage", stage, "state", "UNKNOWN", "pipelines", guards.entrySet().stream().map(entry ->
                            Map.of("pipeline", entry.getKey(), "evidence", entry.getValue().evidence())).toList(),
                    "performanceAcceptanceEligible", false)));
            throw failure;
        }
        long total = 0;
        var readings = new LinkedHashMap<String, Object>();
        var snapshots = new LinkedHashMap<String, BenchmarkNativeCounterBaseline.Snapshot>();
        for (var entry : guards.entrySet()) {
            total = Math.addExact(total, entry.getValue().total());
            readings.put(entry.getKey(), entry.getValue().evidence());
            snapshots.put(entry.getKey(), entry.getValue().snapshot());
        }
        var evidence = Map.<String, Object>of("stage", stage, "state", "RECORDED_FRESH_NATIVE_BASELINES",
                "total", total, "pipelines", Map.copyOf(readings), "performanceAcceptanceEligible", false);
        System.out.println("benchmark-native-counter-baselines=" + JsonWriter.write(evidence));
        return new NativeCounterBaseline(total, evidence, snapshots);
    }

    /** Records idempotent replay work as a cost while logical delivery remains the target-change oracle. */
    private static long awaitRecordsOut(BenchmarkWorkloadDefinitions.Workload workload,
            ControlPlane control, long before, long expected) throws InterruptedException {
        long deadline = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() + ACK_WAIT.toNanos();
        long previous = before;
        long unchangedSince = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
        while (true) {
            long after = recordsOut(workload, control);
            long delta = after - before;
            long now = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
            if (after < previous) {
                throw new AssertionError("records.out moved backward during the measured phase");
            }
            if (after != previous) {
                previous = after;
                unchangedSince = now;
            } else if (delta >= expected && now - unchangedSince >= PRE_WINDOW_QUIET.toNanos()) {
                return delta;
            }
            if (now >= deadline) {
                throw new AssertionError("records.out advanced by " + delta + " of at least " + expected
                        + " and did not settle (baseline=" + before + ", current=" + after + ")");
            }
            TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
        }
    }

    private static void awaitQuiescentRecordsOut(BenchmarkWorkloadDefinitions.Workload workload,
            ControlPlane control) throws InterruptedException {
        long deadline = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() + ACK_WAIT.toNanos();
        long previous = recordsOut(workload, control);
        long unchangedSince = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
        while (true) {
            TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            long current = recordsOut(workload, control);
            long now = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
            if (current < previous) {
                throw new AssertionError("records.out moved backward before the measured window");
            }
            if (current != previous) {
                previous = current;
                unchangedSince = now;
            } else if (now - unchangedSince >= PRE_WINDOW_QUIET.toNanos()) {
                return;
            }
            if (now >= deadline) {
                throw new AssertionError("records.out did not quiesce before measured SQL");
            }
        }
    }

    private static void awaitFreshObservationAfterBoundary(
            BenchmarkWorkloadDefinitions.Workload workload, ControlPlane control) throws InterruptedException {
        Instant boundaryCompletedAt = Instant.now();
        long deadline = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() + ACK_WAIT.toNanos();
        for (String pipelineId : workload.pipelineIds()) {
            while (!control.statusObservedAt(pipelineId).isAfter(boundaryCompletedAt)) {
                if (io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() >= deadline) {
                    throw new AssertionError("no post-boundary observation for " + pipelineId);
                }
                TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            }
        }
    }

    private static long recordsOut(BenchmarkWorkloadDefinitions.Workload workload, ControlPlane control) {
        long total = 0;
        for (String pipelineId : workload.pipelineIds()) {
            Optional<Long> value = control.recordsOut(pipelineId);
            if (value.isEmpty()) {
                throw new AssertionError("records.out is unavailable for " + pipelineId);
            }
            total = Math.addExact(total, value.get());
        }
        return total;
    }

    private static long errorTotal(BenchmarkWorkloadDefinitions.Workload workload, ControlPlane control) {
        long total = 0;
        for (String pipelineId : workload.pipelineIds()) {
            total = Math.addExact(total, control.errorCount(pipelineId).orElseThrow(() ->
                    new AssertionError("error count is unavailable for " + pipelineId)));
        }
        return total;
    }

    private static void mergeCoverage(Map<String, Long> total, Map<String, Long> phase) {
        for (Map.Entry<String, Long> entry : phase.entrySet()) {
            if (total.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
                throw new AssertionError("duplicate logical coverage key: " + entry.getKey());
            }
        }
    }

    private static String checksum(List<BenchmarkForkEnvironment.TargetResult> targets) {
        String[] values = targets.stream().map(result -> result.expectation().pipelineId() + "/"
                + result.expectation().table() + "=" + result.checksum()).toArray(String[]::new);
        Arrays.sort(values);
        return String.join(";", values);
    }

    private static BenchmarkResourceSampler.Summary summarizeResources(
            List<BenchmarkResourceSampler.Summary> windows) {
        long cpu = 0;
        long gc = 0;
        long heap = 0;
        long rss = 0;
        int samples = 0;
        for (BenchmarkResourceSampler.Summary window : windows) {
            cpu = Math.addExact(cpu, window.cpuNanos());
            gc = Math.addExact(gc, window.gcCollectionMillis());
            heap = Math.max(heap, window.peakHeapBytes());
            rss = Math.max(rss, window.peakRssBytes());
            samples = Math.addExact(samples, window.sampleCount());
        }
        return new BenchmarkResourceSampler.Summary(cpu, gc, heap, rss, samples);
    }

    private static BenchmarkMongoCommandSampler.Summary summarizeCommands(
            List<BenchmarkMongoCommandSampler.Summary> windows) {
        Map<String, Long> commands = new TreeMap<>();
        Map<BenchmarkMongoCommandSampler.Family, Long> families =
                new EnumMap<>(BenchmarkMongoCommandSampler.Family.class);
        long elapsed = 0;
        for (BenchmarkMongoCommandSampler.Summary window : windows) {
            window.byCommand().forEach((name, count) -> commands.merge(name, count, Math::addExact));
            window.byFamily().forEach((family, count) -> families.merge(family, count, Math::addExact));
            elapsed = Math.addExact(elapsed, window.elapsedMillis());
        }
        return new BenchmarkMongoCommandSampler.Summary(commands, families, elapsed);
    }

    /** One change stream per target collection stays live from first measured SQL through terminal ACK. */
    private static final class TargetWatchSet implements AutoCloseable {
        private final Map<String, BenchmarkMongoDeliveryObserver> byTarget;

        private TargetWatchSet(Map<String, BenchmarkMongoDeliveryObserver> byTarget) {
            this.byTarget = byTarget;
        }

        List<Map<String, Object>> readCosts() {
            return byTarget.values().stream().map(BenchmarkMongoDeliveryObserver::diagnosticReadCosts).toList();
        }

        List<Map<String, Object>> readSchedules() {
            return byTarget.entrySet().stream().map(entry -> Map.<String, Object>of(
                    "target", entry.getKey(), "readSchedule", entry.getValue().readScheduleEvidence())).toList();
        }

        boolean readDeferred() {
            return byTarget.values().stream().anyMatch(BenchmarkMongoDeliveryObserver::readDeferred);
        }

        void releaseAfterOwnMeasuredAck(long acknowledgedAtNanos) {
            byTarget.values().forEach(observer -> observer.releaseAfterOwnMeasuredAck(acknowledgedAtNanos));
        }

        static TargetWatchSet open(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkWorkloadDefinitions.Phase firstMeasured, BenchmarkForkEnvironment fork) {
            Map<String, BenchmarkMongoDeliveryObserver> opened = new LinkedHashMap<>();
            try {
                for (BenchmarkExpectedChanges.TargetPlan plan
                        : BenchmarkExpectedChanges.forPhase(workload, firstMeasured)) {
                    String id = targetId(plan.target());
                    BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                            plan.target(), fork.externalTargetUri(), fork.managedViewsUri(), plan.keyOf(),
                            Boolean.getBoolean(BenchmarkWitnessReadGate.PROPERTY));
                    if (opened.putIfAbsent(id, observer) != null) {
                        observer.close();
                        throw new AssertionError("duplicate benchmark target " + id);
                    }
                }
                return new TargetWatchSet(opened);
            } catch (RuntimeException | Error failure) {
                for (BenchmarkMongoDeliveryObserver observer : opened.values()) {
                    try {
                        observer.close();
                    } catch (RuntimeException | Error cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
                throw failure;
            }
        }

        void expectBatch(BenchmarkWorkloadDefinitions.TargetExpectation target, String phaseId,
                long issuedAt, List<BenchmarkMongoDeliveryObserver.ExpectedChange> changes) {
            observer(target).expectBatch(phaseId, issuedAt, changes);
        }

        void expectTerminal(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkWorkloadDefinitions.Phase terminal) {
            if (terminal.stage() != BenchmarkWorkloadDefinitions.Stage.TERMINAL) {
                throw new IllegalArgumentException("only the terminal phase may register final target changes");
            }
            for (BenchmarkWorkloadDefinitions.TargetExpectation target : terminal.targets()) {
                observer(target).expectUnmeasured(terminal.id(),
                        BenchmarkTerminalTargetChanges.forTarget(workload, target));
                observer(target).allowOptionalUnmeasured(terminal.id(),
                        BenchmarkTerminalTargetChanges.optionalForTarget(workload, target));
            }
        }

        List<BenchmarkMongoDeliveryObserver.Delivery> checkpoint(BenchmarkWorkloadDefinitions.Phase phase) {
            return checkpointStreams(phase).values().stream().flatMap(List::stream).toList();
        }

        Map<String, List<BenchmarkMongoDeliveryObserver.Delivery>> checkpointStreams(BenchmarkWorkloadDefinitions.Phase phase) {
            Map<String, List<BenchmarkMongoDeliveryObserver.Delivery>> delivered = new LinkedHashMap<>();
            for (BenchmarkWorkloadDefinitions.TargetExpectation target : phase.targets()) {
                delivered.put(targetId(target), observer(target).checkpoint(phase.id(), DELIVERY_WAIT));
            }
            return Map.copyOf(delivered);
        }

        Map<String, Long> observedCoverage() {
            Map<String, Long> result = new LinkedHashMap<>();
            for (BenchmarkMongoDeliveryObserver observer : byTarget.values()) {
                observer.observedCoverage().forEach((key, count) -> {
                    String label = "target/" + key.phaseId() + "/" + key.targetId()
                            + "/" + key.kind() + "/" + key.key();
                    result.merge(label, count, Math::addExact);
                });
            }
            return Map.copyOf(result);
        }

        private BenchmarkMongoDeliveryObserver observer(
                BenchmarkWorkloadDefinitions.TargetExpectation target) {
            BenchmarkMongoDeliveryObserver found = byTarget.get(targetId(target));
            if (found == null) {
                throw new AssertionError("benchmark target was not watched from the first measured phase: "
                        + target);
            }
            return found;
        }

        private static String targetId(BenchmarkWorkloadDefinitions.TargetExpectation target) {
            return target.pipelineId() + "/" + target.location() + "/" + target.table();
        }

        @Override
        public void close() {
            Throwable failure = null;
            for (BenchmarkMongoDeliveryObserver observer : byTarget.values()) {
                try {
                    observer.close();
                } catch (RuntimeException | Error closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (readDeferred()) {
                try {
                    System.out.println("benchmark-target-witness-final=" + JsonWriter.write(Map.of(
                            "readSchedules", readSchedules(), "performanceAcceptanceEligible", false)));
                } catch (RuntimeException | Error diagnosticFailure) {
                    if (failure == null) { failure = diagnosticFailure; }
                    else { failure.addSuppressed(diagnosticFailure); }
                }
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException error) {
                throw error;
            }
        }
    }

    private static final class CaptureSet implements AutoCloseable {
        private final Map<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> captures;

        private CaptureSet(Map<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> captures) {
            this.captures = captures;
        }

        static CaptureSet open(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkForkEnvironment fork,
                String connectorId, Path connectorJar, Map<String, Object> connectorConfig) {
            Map<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> captures =
                    new LinkedHashMap<>();
            try {
                for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                    fork.registerUnconfirmedExternalPostgresBorrower(chain.id());
                    captures.put(chain, BenchmarkTerminalCapture.open(connectorId, connectorJar,
                            connectorConfig, chain.table(), BenchmarkPreflightWrites.warmupRowId(workload, chain),
                            chain.terminalRowId(), BenchmarkBoundaryWrites.forChain(workload, chain),
                            BenchmarkMeasuredEndMarkers.forChain(workload, chain)));
                }
                return new CaptureSet(captures);
            } catch (RuntimeException | Error failure) {
                for (BenchmarkTerminalCapture capture : captures.values()) {
                    try {
                        capture.close();
                    } catch (RuntimeException | Error cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
                throw failure;
            }
        }

        void awaitPreflight(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkForkEnvironment fork) {
            for (Map.Entry<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> entry
                    : captures.entrySet()) {
                entry.getValue().awaitWarmup(attempt -> {
                    try {
                        fork.executeSource(BenchmarkPreflightWrites.sql(workload, entry.getKey(), attempt));
                    } catch (Exception failure) {
                        throw new AssertionError("cannot issue source preflight write", failure);
                    }
                }, SIDE_CAR_WAIT);
            }
        }

        void awaitMeasurementBoundary(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkForkEnvironment fork, BenchmarkWorkloadDefinitions.Phase previousPhase,
                BenchmarkConnectorPositionCoverage positionCoverage) throws Exception {
            for (Map.Entry<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> entry
                    : captures.entrySet()) {
                BenchmarkWorkloadDefinitions.SourceChain chain = entry.getKey();
                BenchmarkBoundaryWrites.Boundary boundary = BenchmarkBoundaryWrites.forChain(workload, chain);
                fork.executeOneSourceUpdate(boundary.changedSql());
                fork.executeOneSourceUpdate(boundary.restoredSql());
                String restoredToken = entry.getValue().awaitBoundary(SIDE_CAR_WAIT);
                awaitAck(fork.control(), chain, restoredToken, positionCoverage);
            }
            List<BenchmarkForkEnvironment.TargetResult> targets = fork.verifyCurrentTargets(previousPhase);
            if (targets.stream().anyMatch(target -> !target.matches())) {
                throw new AssertionError("restored boundary changed the frozen target answer");
            }
            captures.values().forEach(BenchmarkTerminalCapture::sealBoundary);
        }

        long awaitMeasuredAcks(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkWorkloadDefinitions.Phase phase, BenchmarkForkEnvironment fork,
                BenchmarkConnectorPositionCoverage positionCoverage) throws InterruptedException {
            Map<BenchmarkWorkloadDefinitions.SourceChain, String> pending = new LinkedHashMap<>();
            for (Map.Entry<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> entry
                    : captures.entrySet()) {
                if (BenchmarkMeasuredEndMarkers.forChain(workload, entry.getKey()).containsKey(phase.id())) {
                    pending.put(entry.getKey(), entry.getValue().awaitMeasuredEnd(phase.id(), SIDE_CAR_WAIT));
                }
            }
            if (pending.isEmpty()) {
                throw new AssertionError("measured phase has no final source marker: " + phase.id());
            }
            Map<BenchmarkWorkloadDefinitions.SourceChain, String> lastAcks = new LinkedHashMap<>();
            long deadline = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() + ACK_WAIT.toNanos();
            while (true) {
                pending.entrySet().removeIf(entry -> {
                    Optional<String> ack = fork.control().targetAckForIfPresent(entry.getKey());
                    lastAcks.put(entry.getKey(), ack.orElse(null));
                    return ack.filter(value -> positionCoverage.covers(value, entry.getValue())).isPresent();
                });
                if (pending.isEmpty()) {
                    return io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
                }
                if (io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() >= deadline) {
                    AssertionError incomplete = new AssertionError("target ACK did not cover measured source markers: "
                            + pending.entrySet().stream().map(entry -> entry.getKey().id()
                                    + " [source=" + positionCoverage.describe(entry.getValue())
                                    + "; lastAck=" + positionCoverage.describe(lastAcks.get(entry.getKey()))
                                    + "]").toList());
                    // Read only after this window has failed; successful measurement IO stays unchanged.
                    try (StoreDocuments documents = StoreDocuments.at(fork.storeUri())) {
                        List<Map<String, Object>> receipts = new ArrayList<>();
                        for (BenchmarkWorkloadDefinitions.SourceChain chain : pending.keySet()) {
                            receipts.add(BenchmarkTerminalMetaReceipt.readPending(documents,
                                    fork.control().positionRead(chain.pipelineId()), chain, positionCoverage::describe));
                        }
                        String captured = JsonWriter.write(Map.of("phase", phase.id(), "windowFailed", true,
                                "receipts", receipts));
                        if (captured.getBytes(StandardCharsets.UTF_8).length > 65_536) {
                            throw new AssertionError("pending ACK evidence exceeded its diagnostic byte budget");
                        }
                        System.out.println("benchmark-pending-ack-receipt=" + captured);
                    } catch (RuntimeException | AssertionError diagnosticFailure) {
                        incomplete.addSuppressed(diagnosticFailure);
                    }
                    throw incomplete;
                }
                TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            }
        }

        void awaitMeasuredSourceMarkers(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkWorkloadDefinitions.Phase phase) throws InterruptedException {
            boolean observed = false;
            for (var entry : captures.entrySet()) {
                if (BenchmarkMeasuredEndMarkers.forChain(workload, entry.getKey()).containsKey(phase.id())) {
                    entry.getValue().awaitMeasuredEnd(phase.id(), SIDE_CAR_WAIT);
                    observed = true;
                }
            }
            if (!observed) { throw new AssertionError("measured phase has no own source marker"); }
        }

        List<BenchmarkAckOracle.SourceChain> awaitTerminalAcks(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkForkEnvironment fork, BenchmarkConnectorPositionCoverage positionCoverage,
                BenchmarkTableCaptureSet tables)
                throws InterruptedException {
            List<BenchmarkAckOracle.SourceChain> result = new ArrayList<>();
            for (Map.Entry<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> entry
                    : captures.entrySet()) {
                BenchmarkWorkloadDefinitions.SourceChain chain = entry.getKey();
                BenchmarkTerminalCapture capture = entry.getValue();
                String terminal = capture.awaitTerminal(SIDE_CAR_WAIT);
                var tableProof = tables.awaitTerminal(chain, terminal);
                String ack = fork.control().targetAckForIfPresent(chain).orElse(null);
                String closedTerminal = capture.closeAndTerminal();
                if (!terminal.equals(closedTerminal)) {
                    throw new AssertionError("sidecar terminal position changed for " + chain.id());
                }
                String verifiedAck = ack;
                String verifiedTerminal = terminal;
                result.add(new BenchmarkAckOracle.SourceChain(chain.id(),
                        List.of(new BenchmarkAckOracle.TerminalEvent(chain.terminalLogicalId(), terminal)),
                        ack, (candidateAck, candidateTerminal) -> Objects.equals(verifiedAck, candidateAck)
                                && verifiedTerminal.equals(candidateTerminal), tableProof));
            }
            return List.copyOf(result);
        }

        private static String awaitAck(ControlPlane control,
                BenchmarkWorkloadDefinitions.SourceChain chain, String sourceToken,
                BenchmarkConnectorPositionCoverage positionCoverage) throws InterruptedException {
            long deadline = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() + ACK_WAIT.toNanos();
            Optional<String> lastAck = Optional.empty();
            while (true) {
                Optional<String> ack = control.targetAckForIfPresent(chain);
                lastAck = ack;
                if (ack.isPresent() && positionCoverage.covers(ack.get(), sourceToken)) {
                    return ack.get();
                }
                if (io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime() >= deadline) {
                    throw new AssertionError("target ACK did not cover the source event for " + chain.id()
                            + "; source=" + positionCoverage.describe(sourceToken)
                            + "; lastAck=" + lastAck.map(positionCoverage::describe).orElse("ABSENT"));
                }
                TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            }
        }

        @Override
        public void close() {
            Throwable failure = null;
            for (BenchmarkTerminalCapture capture : captures.values()) {
                try {
                    capture.close();
                } catch (RuntimeException | Error closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException error) {
                throw error;
            }
        }
    }
}
