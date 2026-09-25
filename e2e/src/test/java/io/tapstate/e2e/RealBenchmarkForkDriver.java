package io.tapstate.e2e;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    record Evidence(String forkId, BenchmarkWorkloadDefinitions.Workload workload,
                    PipelineBenchmarkComparison.Arm arm, Path applicationJar,
                    List<MeasuredPhase> phases, BenchmarkResourceSampler.Summary resources,
                    BenchmarkMongoCommandSampler.Summary mongoCommands,
                    Map<String, Long> logicalCoverage, String checksum, long errorTotal) {
        Evidence {
            phases = List.copyOf(phases);
            logicalCoverage = Map.copyOf(logicalCoverage);
        }
    }

    private record PhaseWindow(MeasuredPhase measurement, List<Long> deliveryDurations) {}

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
                Map<String, Long> coverage = new LinkedHashMap<>(snapshot.expectedLogicalCoverage());
                BenchmarkResourceSampler.Summary resources = null;
                BenchmarkMongoCommandSampler.Summary mongoCommands = null;
                BenchmarkForkEnvironment.PhaseResult terminal = null;
                BenchmarkWorkloadDefinitions.Phase previousPhase = snapshot;
                BenchmarkResourceSampler resourceSampler = null;
                BenchmarkMongoCommandSampler commandSampler = null;
                try {
                    for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases().subList(1,
                            workload.phases().size())) {
                        if (phase.measured()) {
                            if (resourceSampler == null) {
                                // The restored source event fences every unmeasured write in its
                                // chain before any measured source SQL can be issued.
                                captures.awaitMeasurementBoundary(workload, fork, previousPhase,
                                        positionCoverage);
                                awaitFreshObservationAfterBoundary(workload, fork.control());
                                awaitQuiescentRecordsOut(workload, fork.control());
                                resourceSampler = BenchmarkResourceSampler.open(
                                        fork.server().pid(), RESOURCE_INTERVAL);
                                commandSampler = BenchmarkMongoCommandSampler.open(fork.storeUri());
                                resourceSampler.start();
                                commandSampler.start();
                            }
                            PhaseWindow window = runMeasuredPhase(workload, fork, phase);
                            measured.add(window.measurement());
                            allDurations.addAll(window.deliveryDurations());
                        } else if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.TERMINAL) {
                            if (resourceSampler == null || measured.isEmpty()) {
                                throw new AssertionError("terminal arrived before any measured delivery");
                            }
                            resources = resourceSampler.finish();
                            resourceSampler.close();
                            resourceSampler = null;
                            mongoCommands = commandSampler.finish();
                            commandSampler.close();
                            commandSampler = null;
                            terminal = fork.runPhase(phase, true);
                        } else {
                            fork.runPhase(phase, true);
                        }
                        mergeCoverage(coverage, phase.expectedLogicalCoverage());
                        previousPhase = phase;
                    }
                } finally {
                    if (resourceSampler != null) {
                        resourceSampler.close();
                    }
                    if (commandSampler != null) {
                        commandSampler.close();
                    }
                }
                if (terminal == null || resources == null || mongoCommands == null) {
                    throw new AssertionError("benchmark fork ended without terminal or resource evidence");
                }
                List<BenchmarkAckOracle.SourceChain> chains = captures.awaitTerminalAcks(
                        workload, fork, positionCoverage);
                BenchmarkSourceLineage.verifyAfterTerminalAck(lineage, fork.sourceSettings());
                String checksum = checksum(terminal.targets());
                long errorTotal = errorTotal(workload, fork.control());
                if (errorTotal != 0) {
                    throw new AssertionError("benchmark fork published " + errorTotal + " pipeline errors");
                }
                BenchmarkAckOracle.Fork correctness = new BenchmarkAckOracle.Fork(
                        forkId, chains, coverage, checksum, errorTotal);
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
                        coverage, checksum, errorTotal);
                evidence.add(run);
                return new PipelineBenchmarkHarness.ForkResult(performance, correctness);
            }
        }
    }

    private static PhaseWindow runMeasuredPhase(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkForkEnvironment fork, BenchmarkWorkloadDefinitions.Phase phase) throws Exception {
        List<BenchmarkExpectedChanges.TargetPlan> plans = BenchmarkExpectedChanges.forPhase(workload, phase)
                .stream().filter(plan -> plan.totalChanges() > 0).toList();
        List<BenchmarkMongoDeliveryObserver> observers = new ArrayList<>();
        try {
            for (BenchmarkExpectedChanges.TargetPlan plan : plans) {
                observers.add(BenchmarkMongoDeliveryObserver.open(plan.target(), fork.externalTargetUri(),
                        fork.managedViewsUri(), plan.keyOf()));
            }
            long initialAcknowledged = recordsOut(workload, fork.control());
            BenchmarkForkEnvironment.PhaseResult phaseResult = fork.runPhase(phase, true,
                    (current, batchIndex, issuedAt, sql) -> {
                        for (int target = 0; target < plans.size(); target++) {
                            List<BenchmarkMongoDeliveryObserver.ExpectedChange> changes =
                                    plans.get(target).forBatch(batchIndex);
                            if (!changes.isEmpty()) {
                                observers.get(target).expectBatch(issuedAt, changes);
                            }
                        }
                    });
            if (phaseResult.batches().isEmpty()) {
                throw new AssertionError("measured phase has no source batches: " + phase.id());
            }
            long completedAckAt = awaitRecordsOut(workload, fork.control(),
                    initialAcknowledged, phase.expectedLogicalOutputChanges());
            List<Long> durations = new ArrayList<>();
            for (BenchmarkMongoDeliveryObserver observer : observers) {
                for (BenchmarkMongoDeliveryObserver.Delivery delivery : observer.finish(DELIVERY_WAIT)) {
                    durations.add(delivery.durationNanos());
                }
            }
            if (durations.size() != phase.expectedLogicalOutputChanges()) {
                throw new AssertionError("observed " + durations.size() + " deliveries for " + phase.id()
                        + ", expected " + phase.expectedLogicalOutputChanges());
            }
            long issued = phaseResult.batches().getFirst().issuedAtNanos();
            return new PhaseWindow(new MeasuredPhase(phase.id(), phase.expectedLogicalOutputChanges(),
                    issued, completedAckAt, durations.size()), List.copyOf(durations));
        } finally {
            for (BenchmarkMongoDeliveryObserver observer : observers) {
                observer.close();
            }
        }
    }

    private static long awaitRecordsOut(BenchmarkWorkloadDefinitions.Workload workload,
            ControlPlane control, long before, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + ACK_WAIT.toNanos();
        while (true) {
            long after = recordsOut(workload, control);
            long delta = after - before;
            if (delta == expected) {
                return System.nanoTime();
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
                            chain.terminalRowId(), BenchmarkBoundaryWrites.forChain(workload, chain)));
                }
                return new CaptureSet(captures);
            } catch (RuntimeException | Error failure) {
                captures.values().forEach(BenchmarkTerminalCapture::close);
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
            while (true) {
                String ack = control.targetAckFor(chain);
                if (positionCoverage.covers(ack, sourceToken)) {
                    return ack;
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("target ACK did not cover the source event for " + chain.id());
                }
                TimeUnit.NANOSECONDS.sleep(COUNTER_POLL.toNanos());
            }
        }

        @Override
        public void close() {
            captures.values().forEach(BenchmarkTerminalCapture::close);
        }
    }
}
