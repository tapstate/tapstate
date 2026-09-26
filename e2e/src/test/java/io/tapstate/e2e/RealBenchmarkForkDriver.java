package io.tapstate.e2e;

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

    record MeasuredPhase(String id, long acknowledgedOutputs, long firstIssuedAtNanos,
                         long completedAckAtNanos, int observedDeliveries) {
        double recordsOutPerSecond() {
            long duration = completedAckAtNanos - firstIssuedAtNanos;
            if (duration <= 0 || acknowledgedOutputs <= 0) {
                throw new AssertionError("measured phase has no target-ACK window: " + id);
            }
            return acknowledgedOutputs * 1_000_000_000.0 / duration;
        }
    }

    /** Required target coverage excludes an optional terminal Nest update caused by arrival order. */
    record Evidence(String forkId, BenchmarkWorkloadDefinitions.Workload workload,
                    PipelineBenchmarkComparison.Arm arm, Path applicationJar,
                    List<MeasuredPhase> phases, BenchmarkResourceSampler.Summary resources,
                    BenchmarkMongoCommandSampler.Summary mongoCommands,
                    Map<String, Long> declaredSourceCoverage,
                    Map<String, Long> observedTargetCoverage,
                    String checksum, long errorTotal) {
        Evidence {
            phases = List.copyOf(phases);
            declaredSourceCoverage = Map.copyOf(declaredSourceCoverage);
            observedTargetCoverage = Map.copyOf(observedTargetCoverage);
        }
    }

    private record PhaseWindow(MeasuredPhase measurement, List<Long> deliveryDurations,
                               BenchmarkResourceSampler.Summary resources,
                               BenchmarkMongoCommandSampler.Summary mongoCommands) {}

    private final List<Evidence> evidence = new ArrayList<>();

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
        try (BenchmarkForkEnvironment fork = BenchmarkForkEnvironment.open(workload, applicationJar, forkId)) {
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
                        workload, workload.phases().get(phaseIndex), fork)) {
                    for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases().subList(phaseIndex,
                            workload.phases().size())) {
                        if (phase.measured()) {
                            PhaseWindow window = runMeasuredPhase(workload, fork, phase,
                                    captures, positionCoverage, targets);
                            measured.add(window.measurement());
                            allDurations.addAll(window.deliveryDurations());
                            resourceWindows.add(window.resources());
                            commandWindows.add(window.mongoCommands());
                        } else if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.TERMINAL) {
                            if (measured.isEmpty()) {
                                throw new AssertionError("terminal arrived before any measured delivery");
                            }
                            targets.expectTerminal(workload, phase);
                            terminal = fork.runPhase(phase, true);
                            chains = captures.awaitTerminalAcks(workload, fork, positionCoverage);
                            targets.checkpoint(phase);
                        } else {
                            throw new AssertionError("unmeasured setup followed measured SQL: " + phase.id());
                        }
                        mergeCoverage(declaredSourceCoverage, phase.expectedLogicalCoverage());
                    }
                    observedTargetCoverage = targets.observedCoverage();
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
                long duration = measured.stream().mapToLong(phase ->
                        phase.completedAckAtNanos() - phase.firstIssuedAtNanos()).sum();
                if (completed != allDurations.size() || duration <= 0) {
                    throw new AssertionError("target-ACK and observed delivery counts disagree in " + forkId);
                }
                double throughput = completed * 1_000_000_000.0 / duration;
                long[] latencies = allDurations.stream().mapToLong(Long::longValue).toArray();
                PipelineBenchmarkComparison.Fork performance = new PipelineBenchmarkComparison.Fork(
                        arm, throughput, latencies, resources.peakHeapBytes(), resources.peakRssBytes());
                Evidence run = new Evidence(forkId, workload, arm, applicationJar,
                        measured, resources, mongoCommands,
                        declaredSourceCoverage, observedTargetCoverage, checksum, errorTotal);
                evidence.add(run);
                return new PipelineBenchmarkHarness.ForkResult(performance, correctness);
            }
        }
    }

    private static PhaseWindow runMeasuredPhase(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkForkEnvironment fork, BenchmarkWorkloadDefinitions.Phase phase,
            CaptureSet captures, BenchmarkConnectorPositionCoverage positionCoverage,
            TargetWatchSet targets) throws Exception {
        List<BenchmarkExpectedChanges.TargetPlan> plans = BenchmarkExpectedChanges.forPhase(workload, phase);
        long initialAcknowledged = recordsOut(workload, fork.control());
        BenchmarkForkEnvironment.PhaseIssue issued;
        long completedAckAt;
        BenchmarkResourceSampler.Summary resources;
        BenchmarkMongoCommandSampler.Summary commands;
        try (BenchmarkResourceSampler resourceSampler = BenchmarkResourceSampler.open(
                fork.server().pid(), RESOURCE_INTERVAL);
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
            completedAckAt = captures.awaitMeasuredAcks(workload, phase, fork, positionCoverage);
            resources = resourceSampler.finish();
            commands = commandSampler.finish();
        }
        List<BenchmarkMongoDeliveryObserver.Delivery> deliveries = targets.checkpoint(phase);
        if (deliveries.size() != phase.expectedLogicalOutputChanges()) {
            throw new AssertionError("observed " + deliveries.size() + " deliveries for " + phase.id()
                    + ", expected " + phase.expectedLogicalOutputChanges());
        }
        fork.completePhase(phase);
        awaitRecordsOut(workload, fork.control(), initialAcknowledged,
                phase.expectedLogicalOutputChanges());
        long firstIssued = issued.batches().getFirst().issuedAtNanos();
        return new PhaseWindow(new MeasuredPhase(phase.id(), phase.expectedLogicalOutputChanges(),
                firstIssued, completedAckAt, deliveries.size()),
                deliveries.stream().map(BenchmarkMongoDeliveryObserver.Delivery::durationNanos).toList(),
                resources, commands);
    }

    private static void awaitRecordsOut(BenchmarkWorkloadDefinitions.Workload workload,
            ControlPlane control, long before, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + ACK_WAIT.toNanos();
        while (true) {
            long after = recordsOut(workload, control);
            long delta = after - before;
            if (delta == expected) {
                return;
            }
            if (delta > expected) {
                throw new AssertionError("target-ACK counter advanced by " + delta + " instead of " + expected
                        + " (baseline=" + before + ", current=" + after + ")");
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("target-ACK counter advanced by only " + delta + " of " + expected);
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
            gc = Math.addExact(gc, window.gcPauseMillis());
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
            List<BenchmarkMongoDeliveryObserver.Delivery> delivered = new ArrayList<>();
            for (BenchmarkWorkloadDefinitions.TargetExpectation target : phase.targets()) {
                delivered.addAll(observer(target).checkpoint(phase.id(), DELIVERY_WAIT));
            }
            return List.copyOf(delivered);
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
            long deadline = System.nanoTime() + ACK_WAIT.toNanos();
            while (true) {
                pending.entrySet().removeIf(entry -> fork.control()
                        .targetAckForIfPresent(entry.getKey())
                        .filter(ack -> positionCoverage.covers(ack, entry.getValue()))
                        .isPresent());
                if (pending.isEmpty()) {
                    return System.nanoTime();
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("target ACK did not cover measured source markers: "
                            + pending.keySet());
                }
                TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            }
        }

        List<BenchmarkAckOracle.SourceChain> awaitTerminalAcks(BenchmarkWorkloadDefinitions.Workload workload,
                BenchmarkForkEnvironment fork, BenchmarkConnectorPositionCoverage positionCoverage)
                throws InterruptedException {
            List<BenchmarkAckOracle.SourceChain> result = new ArrayList<>();
            for (Map.Entry<BenchmarkWorkloadDefinitions.SourceChain, BenchmarkTerminalCapture> entry
                    : captures.entrySet()) {
                BenchmarkWorkloadDefinitions.SourceChain chain = entry.getKey();
                BenchmarkTerminalCapture capture = entry.getValue();
                String terminal = capture.awaitTerminal(SIDE_CAR_WAIT);
                String ack = awaitAck(fork.control(), chain, terminal, positionCoverage);
                String closedTerminal = capture.closeAndTerminal();
                if (!terminal.equals(closedTerminal)) {
                    throw new AssertionError("sidecar terminal position changed for " + chain.id());
                }
                String verifiedAck = ack;
                String verifiedTerminal = terminal;
                result.add(new BenchmarkAckOracle.SourceChain(chain.id(),
                        List.of(new BenchmarkAckOracle.TerminalEvent(chain.terminalLogicalId(), terminal)),
                        ack, (candidateAck, candidateTerminal) -> verifiedAck.equals(candidateAck)
                                && verifiedTerminal.equals(candidateTerminal)));
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
