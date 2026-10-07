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
            return BenchmarkSteadyOutputWindow.readCommonOperations(cohortOperationStreams);
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
                         Optional<BenchmarkTargetClock.LocalWindow> operationResourceWindow) {
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

        boolean steadyOutputEstablished() {
            if (!steadyOutputProfile || deliveryTimeline.isEmpty() || confirmationTiming.isEmpty()) { return false; }
            var timeline = deliveryTimeline.orElseThrow();
            timeline.operationWindow();
            return true;
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
                    Optional<BenchmarkJdiTelemetrySession.Evidence> telemetry) {
        Evidence(String forkId, BenchmarkWorkloadDefinitions.Workload workload,
                PipelineBenchmarkComparison.Arm arm, Path applicationJar,
                List<MeasuredPhase> phases, BenchmarkResourceSampler.Summary resources,
                BenchmarkMongoCommandSampler.Summary mongoCommands,
                Map<String, Long> declaredSourceCoverage, Map<String, Long> observedTargetCoverage,
                String checksum, long errorTotal, Optional<BenchmarkJdiTelemetrySession.Evidence> telemetry) {
            this(forkId, workload, arm, applicationJar, phases, resources, mongoCommands,
                    declaredSourceCoverage, observedTargetCoverage, checksum, errorTotal, List.of(), telemetry);
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
            for (var phase : phases) {
                var timeline = phase.deliveryTimeline().orElseThrow(() -> new AssertionError("steady output has no complete timeline"));
                timeline.operationWindow();
            }
        }
    }

    private record PhaseWindow(MeasuredPhase measurement, List<Long> deliveryDurations,
                               BenchmarkResourceSampler.Summary resources,
                               BenchmarkMongoCommandSampler.Summary mongoCommands) {}

    private final List<Evidence> evidence = new ArrayList<>();
    private final BenchmarkForkEnvironment.BootLauncher launcher;

    RealBenchmarkForkDriver() { this(BenchmarkForkEnvironment.OwnedBoot::plain); }

    RealBenchmarkForkDriver(BenchmarkForkEnvironment.BootLauncher launcher) {
        this.launcher = Objects.requireNonNull(launcher);
    }

    List<Evidence> evidence() {
        return List.copyOf(evidence);
    }

    @Override
    public PipelineBenchmarkHarness.ForkResult run(BenchmarkWorkloadDefinitions.Workload workload,
            PipelineBenchmarkComparison.Arm arm, int armFork, Path applicationJar) throws Exception {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(arm, "arm");
        if (armFork < 1 || applicationJar == null) {
            throw new IllegalArgumentException("a positive fork number and application JAR are required");
        }
        String forkId = workload.id() + "-" + arm + "-" + armFork;
        try (BenchmarkForkEnvironment fork = BenchmarkForkEnvironment.open(workload, applicationJar, forkId, launcher)) {
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
                                 ? mysql.decoderLineage() : null)) {
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
                                    captures, positionCoverage, targets, tables);
                            measured.add(window.measurement());
                            allDurations.addAll(window.deliveryDurations());
                            resourceWindows.add(window.resources());
                            commandWindows.add(window.mongoCommands());
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
                BenchmarkResourceSampler.Summary resources = workload.pilotProfile()
                        ? summarizeResources(measured.stream().map(phase -> {
                            var window = phase.operationResourceWindow().orElseThrow(() -> new AssertionError("target operation resource window is uncalibrated"));
                            return BenchmarkResourceSampler.slice(phase.resources(), window.latestStartNanos(), window.earliestEndNanos());
                        }).toList()) : summarizeResources(resourceWindows);
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
                Evidence run = new Evidence(forkId, workload, arm, applicationJar,
                        measured, resources, mongoCommands,
                        declaredSourceCoverage, observedTargetCoverage, checksum, errorTotal, receipts,
                        fork.finishTelemetryCapture());
                evidence.add(run);
                return new PipelineBenchmarkHarness.ForkResult(performance, correctness);
            }
        }
    }

    private static PhaseWindow runMeasuredPhase(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkForkEnvironment fork, BenchmarkWorkloadDefinitions.Phase phase,
            CaptureSet captures, BenchmarkConnectorPositionCoverage positionCoverage,
            TargetWatchSet targets, BenchmarkTableCaptureSet tables) throws Exception {
        List<BenchmarkExpectedChanges.TargetPlan> plans = BenchmarkExpectedChanges.forPhase(workload, phase);
        long initialAcknowledged = recordsOut(workload, fork.control());
        BenchmarkForkEnvironment.PhaseIssue issued;
        long completedAckAt;
        long sourceMarkerWaitStartedAt;
        long sourceMarkerWaitCompletedAt;
        BenchmarkResourceSampler.Summary resources;
        BenchmarkMongoCommandSampler.Summary commands;
        List<String> targetClockUris = phase.targets().stream().map(target ->
                target.location() == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                        ? fork.managedViewsUri() : fork.externalTargetUri()).distinct().toList();
        List<BenchmarkTargetClock.Reading> targetClocksBefore = workload.pilotProfile()
                ? targetClockUris.stream().map(BenchmarkTargetClock::read).toList() : List.of();
        if (!targetClocksBefore.isEmpty()) { BenchmarkTargetClock.requireSharedClock(targetClocksBefore); }
        BenchmarkTargetClock.Reading targetClockBefore = targetClocksBefore.isEmpty() ? null : targetClocksBefore.getFirst();
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
             BenchmarkMongoCommandSampler commandSampler = BenchmarkMongoCommandSampler.open(fork.storeUri())) {
            resourceSampler.start();
            commandSampler.start();
            issued = fork.issuePhase(phase, true, (current, batchIndex, issuedAt, sql) -> {
                for (BenchmarkExpectedChanges.TargetPlan plan : plans) {
                    List<BenchmarkMongoDeliveryObserver.ExpectedChange> changes = plan.forBatch(batchIndex);
                    if (!changes.isEmpty()) {
                        targets.expectBatch(plan.target(), phase.id(), issuedAt, changes);
                    }
                }
            });
            if (issued.batches().isEmpty()) {
                throw new AssertionError("measured phase has no source batches: " + phase.id());
            }
            sourceMarkerWaitStartedAt = System.nanoTime();
            captures.awaitMeasuredSourceMarkers(workload, phase);
            sourceMarkerWaitCompletedAt = System.nanoTime();
            completedAckAt = tables.awaitMeasured(workload, phase.id());
            resources = resourceSampler.finish();
            commands = commandSampler.finish();
        } catch (BenchmarkResourceSampler.SamplingFailure failure) {
            phaseFailure = failure.inPhase(phase.id());
            throw (BenchmarkResourceSampler.SamplingFailure) phaseFailure;
        } catch (Exception | Error failure) {
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
        List<BenchmarkTargetClock.Reading> targetClocksAfter = targetClockBefore == null ? List.of()
                : targetClockUris.stream().map(BenchmarkTargetClock::read).toList();
        if (!targetClocksAfter.isEmpty()) {
            BenchmarkTargetClock.requireSharedClock(targetClocksAfter);
            for (int i=0;i<targetClocksBefore.size();i++) { BenchmarkTargetClock.validate(targetClocksBefore.get(i), targetClocksAfter.get(i)); }
        }
        BenchmarkTargetClock.Reading targetClockAfter = targetClocksAfter.isEmpty() ? null : targetClocksAfter.getFirst();
        Map<String, Object> targetClockEvidence = targetClockBefore == null
                ? Map.of("state", "UNQUALIFIED", "reason", "MULTI_TARGET_CLOCKS_NOT_YET_CALIBRATED")
                : BenchmarkTargetClock.validate(targetClockBefore, targetClockAfter);
        if (workload.pilotProfile()) {
            System.out.println("benchmark-target-clock=" + JsonWriter.write(Map.of("workload", workload.id(),
                    "phase", phase.id(), "calibration", targetClockEvidence,
                    "targetClockUriCount", targetClockUris.size(), "allBefore", targetClocksBefore.stream().map(BenchmarkTargetClock.Reading::evidence).toList(),
                    "allAfter", targetClocksAfter.stream().map(BenchmarkTargetClock.Reading::evidence).toList())));
        }
        var targetStreams = targets.checkpointStreams(phase);
        List<BenchmarkMongoDeliveryObserver.Delivery> deliveries = targetStreams.values().stream().flatMap(List::stream).toList();
        if (deliveries.size() != phase.expectedLogicalOutputChanges()) {
            throw new AssertionError("observed " + deliveries.size() + " deliveries for " + phase.id()
                    + ", expected " + phase.expectedLogicalOutputChanges());
        }
        fork.completePhase(phase);
        long reportedRecordsOut = awaitRecordsOut(workload, fork.control(), initialAcknowledged,
                phase.expectedLogicalOutputChanges());
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
        long commonFirst = operationStreams.stream().mapToLong(List::getFirst).max().orElseThrow();
        long commonLast = operationStreams.stream().mapToLong(List::getLast).min().orElseThrow();
        Optional<BenchmarkTargetClock.LocalWindow> resourceWindow = targetClockBefore == null ? Optional.empty()
                : Optional.of(BenchmarkTargetClock.mapWindow(targetClockBefore, targetClockAfter,
                        commonFirst, commonLast));
        if (targetClocksBefore.size() > 1) {
            List<BenchmarkTargetClock.LocalWindow> windows = new ArrayList<>();
            for (int i=0;i<targetClocksBefore.size();i++) {
                windows.add(BenchmarkTargetClock.mapWindow(targetClocksBefore.get(i), targetClocksAfter.get(i),
                        commonFirst, commonLast));
            }
            resourceWindow = Optional.of(new BenchmarkTargetClock.LocalWindow(
                    windows.stream().mapToLong(BenchmarkTargetClock.LocalWindow::earliestStartNanos).min().orElseThrow(),
                    windows.stream().mapToLong(BenchmarkTargetClock.LocalWindow::latestStartNanos).max().orElseThrow(),
                    windows.stream().mapToLong(BenchmarkTargetClock.LocalWindow::earliestEndNanos).min().orElseThrow(),
                    windows.stream().mapToLong(BenchmarkTargetClock.LocalWindow::latestEndNanos).max().orElseThrow()));
        }
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
                Optional.of(timing), Optional.of(timeline), workload.pilotProfile(), resourceWindow),
                cohort.stream().map(BenchmarkMongoDeliveryObserver.Delivery::durationNanos).toList(),
                resources, commands);
    }

    /** Records idempotent replay work as a cost while logical delivery remains the target-change oracle. */
    private static long awaitRecordsOut(BenchmarkWorkloadDefinitions.Workload workload,
            ControlPlane control, long before, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + ACK_WAIT.toNanos();
        long previous = before;
        long unchangedSince = System.nanoTime();
        while (true) {
            long after = recordsOut(workload, control);
            long delta = after - before;
            long now = System.nanoTime();
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
        long deadline = System.nanoTime() + ACK_WAIT.toNanos();
        long previous = recordsOut(workload, control);
        long unchangedSince = System.nanoTime();
        while (true) {
            TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            long current = recordsOut(workload, control);
            long now = System.nanoTime();
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
        long deadline = System.nanoTime() + ACK_WAIT.toNanos();
        for (String pipelineId : workload.pipelineIds()) {
            while (!control.statusObservedAt(pipelineId).isAfter(boundaryCompletedAt)) {
                if (System.nanoTime() >= deadline) {
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

        static TargetWatchSet open(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkWorkloadDefinitions.Phase firstMeasured, BenchmarkForkEnvironment fork) {
            Map<String, BenchmarkMongoDeliveryObserver> opened = new LinkedHashMap<>();
            try {
                for (BenchmarkExpectedChanges.TargetPlan plan
                        : BenchmarkExpectedChanges.forPhase(workload, firstMeasured)) {
                    String id = targetId(plan.target());
                    BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                            plan.target(), fork.externalTargetUri(), fork.managedViewsUri(), plan.keyOf());
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
            long deadline = System.nanoTime() + ACK_WAIT.toNanos();
            while (true) {
                pending.entrySet().removeIf(entry -> {
                    Optional<String> ack = fork.control().targetAckForIfPresent(entry.getKey());
                    lastAcks.put(entry.getKey(), ack.orElse(null));
                    return ack.filter(value -> positionCoverage.covers(value, entry.getValue())).isPresent();
                });
                if (pending.isEmpty()) {
                    return System.nanoTime();
                }
                if (System.nanoTime() >= deadline) {
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
            long deadline = System.nanoTime() + ACK_WAIT.toNanos();
            Optional<String> lastAck = Optional.empty();
            while (true) {
                Optional<String> ack = control.targetAckForIfPresent(chain);
                lastAck = ack;
                if (ack.isPresent() && positionCoverage.covers(ack.get(), sourceToken)) {
                    return ack.get();
                }
                if (System.nanoTime() >= deadline) {
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
