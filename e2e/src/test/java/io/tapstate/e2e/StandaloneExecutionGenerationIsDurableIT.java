package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual standalone submissions keep the durable sequence while resource and counter lifetimes differ. */
@RequiresDocker
class StandaloneExecutionGenerationIsDurableIT {
    private static final String PREFIX = "tapstate.e2e.execution-identity.";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @Test
    void ordinaryPauseRestartProcessRestartAndRecreationKeepTheirOwnIdentityBoundaries() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "no execution-identity properties supplied; real lifecycle witness is opt-in");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        boolean directCounts = Boolean.getBoolean(PREFIX + "direct-counts");
        assertThat(Files.isRegularFile(jar)).isTrue();
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path output = Path.of(required("output"));
        Path harnessRoot = PipelineBenchmarkLiveRunIT.harnessRoot();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, harnessRoot);
        Map<String, Object> telemetryInputs = TelemetryMongoIdentityWitness.inputHashes(harnessRoot);
        Instant telemetryFrom = Instant.now().minusSeconds(1);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        ExecutionAdmissionStages admissions = new ExecutionAdmissionStages(report, directCounts);
        try {
            String sha = PipelineBenchmarkLiveRunIT.sha256(jar);
            report.begin(Map.of("purpose", directCounts ? "REAL_STANDALONE_LIFECYCLE_ADMISSION_AND_PHYSICAL_COUNTS"
                            : "REAL_STANDALONE_LIFECYCLE_IDENTITY",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "discoveryMode", "none",
                    "mongoTelemetryWitnessInputs", telemetryInputs),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            var workload = BenchmarkWorkloadDefinitions.byId("copy");
            String pipeline = workload.pipelineIds().getFirst();
            try (var coordination = new ExecutionCoordinationProfileStages(report, directCounts, pipeline);
                    var fork = BenchmarkForkEnvironment.open(workload, jar, "standalone-identity",
                    (uri, operator, artifact) -> {
                        if (!directCounts) { return new BenchmarkForkEnvironment.OwnedBoot(
                                RealProcessServer.start(uri, operator, artifact,
                                        List.of(TelemetryMongoIdentityWitness.HISTORY_ARGUMENT)), null); }
                        String nativeUri = coordination.openOwned(uri);
                        var observer = ExecutionAdmissionJdiSession.startWithLeaseObservation(nativeUri, operator, artifact, pipeline,
                                List.of(TelemetryMongoIdentityWitness.HISTORY_ARGUMENT));
                        admissions.bind(observer);
                        return new BenchmarkForkEnvironment.OwnedBoot(observer.server(), null, observer);
                    }, boot -> {
                        admissions.stage("before-first-start", 0, 0);
                        coordination.stage("before-first-start", Map.of("appliedPipeline", pipeline, "startRequested", false));
                        coordination.begin("first-start");
                    });
                    var client = MongoClients.create(fork.storeUri())) {
                MongoDatabase database = client.getDatabase(new ConnectionString(fork.storeUri()).getDatabase());
                var latest = new MongoObservationStore(client,
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                var telemetry = new TelemetryMongoIdentityWitness(database, latest, report, pipeline, telemetryFrom, WAIT);
                var initialPhase = fork.runPhase(workload.phases().getFirst(), true);
                var warmupPhase = fork.runPhase(workload.phases().get(1), true);
                ControlPlane control = fork.control();
                Document firstClaim = claim(database, pipeline);
                long firstGeneration = generation(firstClaim);
                var first = stored(latest, pipeline, firstGeneration, PipelineState.RUNNING);
                first = activeCounter(latest, pipeline, first.scope().orElseThrow(),
                        workload.phases().getFirst().expectedLogicalOutputChanges()
                                + workload.phases().get(1).expectedLogicalOutputChanges() - 1);
                OutputCounter firstCounter = outputCounter(first.observation());
                assertThat(firstCounter.value()).isPositive();
                var firstCumulative = CumulativeMetricWitness.running(latest, pipeline, first.scope().orElseThrow(),
                        WAIT, null, report, "initial-start");
                String incarnation = first.scope().orElseThrow().pipelineIncarnationId();
                record(report, "initial-start", first, firstClaim);
                telemetry.capture("initial-start", first, control, fork.server().baseUrl(), true);
                admissions.stage("first-start", 1, 1);
                coordination.stage("first-start", readiness(first, Map.of("targetPhases",
                        List.of(targetReadiness(initialPhase), targetReadiness(warmupPhase)))));
                coordination.begin("running-ticks");
                if (directCounts) { awaitTicks(latest, pipeline, firstGeneration, PipelineState.RUNNING); }
                admissions.stage("running-ticks", 0, 0);
                coordination.stage("running-ticks", readiness(stored(latest, pipeline, firstGeneration, PipelineState.RUNNING),
                        Map.of("distinctObservationTimestampsRequired", 3)));

                coordination.begin("pause-and-paused-ticks");
                control.lifecycle(pipeline, LifecycleVerb.PAUSE);
                Await.until("the actual control state to become PAUSED", WAIT,
                        () -> control.state(pipeline).filter(PipelineState.PAUSED::equals).isPresent(),
                        () -> "status=" + control.state(pipeline) + ", metrics=" + control.metrics(pipeline));
                latest.readStored(pipeline).ifPresent(value -> System.out.printf(
                        "standalone-pause-read state=%s scope=%s facts=%s%n",
                        value.observation().state(), value.scope(), value.observation().facts().stream()
                                .map(fact -> Map.of("name", fact.name(), "points", fact.points().size())).toList()));
                var paused = stored(latest, pipeline, firstGeneration, PipelineState.PAUSED);
                assertThat(paused.scope()).isEqualTo(first.scope());
                counterIfKnown(paused.observation()).ifPresent(counter -> {
                    assertThat(counter.startTime()).isEqualTo(firstCounter.startTime());
                    assertThat(counter.value()).isGreaterThanOrEqualTo(firstCounter.value());
                });
                assertThat(generation(claim(database, pipeline))).isEqualTo(firstGeneration);
                CumulativeMetricWitness.paused(report, "pause", firstCumulative, CumulativeMetricWitness.capture(paused));
                record(report, "pause", paused, claim(database, pipeline));
                telemetry.capture("pause", paused, control, fork.server().baseUrl(), false);
                if (directCounts) { awaitTicks(latest, pipeline, firstGeneration, PipelineState.PAUSED); }
                admissions.stage("pause-and-paused-ticks", 0, 0);
                coordination.stage("pause-and-paused-ticks", readiness(stored(latest, pipeline, firstGeneration, PipelineState.PAUSED),
                        Map.of("distinctObservationTimestampsRequired", 3)));

                coordination.begin("ordinary-resume");
                long resumedAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                control.lifecycle(pipeline, LifecycleVerb.RESUME);
                delivered(client, fork.externalTargetUri(), resumedAmount);
                var resumed = activeCounter(latest, pipeline, first.scope().orElseThrow(), firstCounter.value());
                assertThat(outputCounter(resumed.observation()).startTime()).isEqualTo(firstCounter.startTime());
                assertThat(generation(claim(database, pipeline))).isEqualTo(firstGeneration);
                var resumedCumulative = CumulativeMetricWitness.running(latest, pipeline, resumed.scope().orElseThrow(),
                        WAIT, firstCumulative, report, "ordinary-resume");
                CumulativeMetricWitness.continued(report, "ordinary-resume", firstCumulative, resumedCumulative, false);
                record(report, "ordinary-resume", resumed, claim(database, pipeline));
                telemetry.capture("ordinary-resume", resumed, control, fork.server().baseUrl(), true);
                admissions.stage("ordinary-resume", 0, 0);
                coordination.stage("ordinary-resume", readiness(resumed, Map.of("targetExpectedAmount", resumedAmount,
                        "targetActualAmount", targetAmount(client, fork.externalTargetUri()))));

                coordination.begin("stop");
                control.stop(pipeline, false);
                var stopped = stored(latest, pipeline, firstGeneration, PipelineState.STOPPED);
                assertThat(stopped.scope()).isEqualTo(first.scope());
                long resumedRecords = outputCounter(resumed.observation()).value();
                var finalCounter = outputCounter(stopped.observation());
                assertThat(finalCounter.startTime()).isEqualTo(firstCounter.startTime());
                assertThat(finalCounter.value()).isGreaterThanOrEqualTo(resumedRecords);
                var stoppedCumulative = CumulativeMetricWitness.capture(stopped);
                CumulativeMetricWitness.continued(report, "stop", resumedCumulative, stoppedCumulative, false);
                record(report, "stop", stopped, claim(database, pipeline));
                telemetry.capture("stop", stopped, control, fork.server().baseUrl(), false);
                admissions.stage("stop", 0, 0);
                coordination.stage("stop", readiness(stopped, Map.of()));
                coordination.begin("stop-start");
                control.lifecycle(pipeline, LifecycleVerb.START);
                var restarted = stored(latest, pipeline, firstGeneration + 1, PipelineState.RUNNING);
                assertThat(restarted.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(incarnation);
                long restartedAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                delivered(client, fork.externalTargetUri(), restartedAmount);
                restarted = activeCounter(latest, pipeline, restarted.scope().orElseThrow(), 0);
                var restartCounter = outputCounter(restarted.observation());
                assertThat(restartCounter.startTime()).isAfter(firstCounter.startTime());
                assertThat(generation(claim(database, pipeline))).isEqualTo(firstGeneration + 1);
                var restartedCumulative = CumulativeMetricWitness.running(latest, pipeline, restarted.scope().orElseThrow(),
                        WAIT, null, report, "stop-start");
                CumulativeMetricWitness.reset(report, "stop-start", stoppedCumulative, restartedCumulative);
                record(report, "stop-start", restarted, claim(database, pipeline));
                telemetry.capture("stop-start", restarted, control, fork.server().baseUrl(), true);
                admissions.stage("stop-start", 1, 1);
                coordination.stage("stop-start", readiness(restarted, Map.of("targetExpectedAmount", restartedAmount,
                        "targetActualAmount", targetAmount(client, fork.externalTargetUri()))));
                long beforeProcessGeneration = firstGeneration + 1;

                coordination.begin("stop-before-process-restart");
                control.stop(pipeline, false);
                var beforeProcessStop = stored(latest, pipeline, beforeProcessGeneration, PipelineState.STOPPED);
                telemetry.capture("stop-before-process-restart", beforeProcessStop, control, fork.server().baseUrl(), false);
                var beforeProcessCumulative = CumulativeMetricWitness.capture(beforeProcessStop);
                CumulativeMetricWitness.continued(report, "stop-before-process-restart", restartedCumulative,
                        beforeProcessCumulative, false);
                admissions.stage("stop-before-process-restart", 0, 0);
                coordination.stage("stop-before-process-restart", readiness(beforeProcessStop, Map.of()));
                coordination.begin("first-process-shutdown");
                admissions.shutdown("first-process-shutdown");
                coordination.stage("first-process-shutdown", Map.of("ownedVmDeathAndDisconnectVerified", true));
                fork.closeBoot();
                assertThat(generation(claim(database, pipeline))).isEqualTo(beforeProcessGeneration);
                coordination.begin("new-process-boot-without-start");
                String restoredNativeUri = coordination.nativeUri(fork.storeUri());
                var nextObserver = directCounts ? ExecutionAdmissionJdiSession.startWithLeaseObservation(
                        restoredNativeUri, new ConnectionString(fork.operatorStateUri()).getDatabase(), jar, pipeline,
                        List.of(TelemetryMongoIdentityWitness.HISTORY_ARGUMENT)) : null;
                try (var next = nextObserver == null
                        ? new BenchmarkForkEnvironment.OwnedBoot(RealProcessServer.start(restoredNativeUri,
                                new ConnectionString(fork.operatorStateUri()).getDatabase(), jar,
                                List.of(TelemetryMongoIdentityWitness.HISTORY_ARGUMENT)), null)
                        : new BenchmarkForkEnvironment.OwnedBoot(nextObserver.server(), null, nextObserver)) {
                    if (nextObserver != null) { admissions.bind(nextObserver); }
                    ControlPlane restored = new ControlPlane(next.server().baseUrl());
                    restored.login("benchmark", "benchmark-password");
                    assertThat(generation(claim(database, pipeline))).isEqualTo(beforeProcessGeneration);
                    if (directCounts) { awaitTicks(latest, pipeline, beforeProcessGeneration, PipelineState.STOPPED); }
                    admissions.stage("new-process-boot-without-start", 0, 0);
                    coordination.stage("new-process-boot-without-start", readiness(stored(latest, pipeline,
                            beforeProcessGeneration, PipelineState.STOPPED),
                            Map.of("restoredControlLoginCompleted", true, "startRequested", false)));
                    coordination.begin("new-process-start");
                    restored.lifecycle(pipeline, LifecycleVerb.START);
                    var afterProcessRestart = stored(latest, pipeline, beforeProcessGeneration + 1, PipelineState.RUNNING);
                    assertThat(afterProcessRestart.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(incarnation);
                    long processRestartAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                    fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                    delivered(client, fork.externalTargetUri(), processRestartAmount);
                    afterProcessRestart = activeCounter(latest, pipeline, afterProcessRestart.scope().orElseThrow(), 0);
                    assertThat(outputCounter(afterProcessRestart.observation()).startTime())
                            .isAfter(restartCounter.startTime());
                    var afterProcessCumulative = CumulativeMetricWitness.running(latest, pipeline,
                            afterProcessRestart.scope().orElseThrow(), WAIT, null, report, "process-restart-start");
                    CumulativeMetricWitness.reset(report, "process-restart-start", beforeProcessCumulative, afterProcessCumulative);
                    record(report, "process-restart-start", afterProcessRestart, claim(database, pipeline));
                    telemetry.capture("process-restart-start", afterProcessRestart, restored, next.server().baseUrl(), true);
                    admissions.stage("new-process-start", 1, 1);
                    coordination.stage("new-process-start", readiness(afterProcessRestart,
                            Map.of("targetExpectedAmount", processRestartAmount,
                                    "targetActualAmount", targetAmount(client, fork.externalTargetUri()))));

                    coordination.begin("stop-before-recreate");
                    restored.stop(pipeline, false);
                    var beforeRecreateStop = stored(latest, pipeline, beforeProcessGeneration + 1, PipelineState.STOPPED);
                    telemetry.capture("stop-before-recreate", beforeRecreateStop, restored, next.server().baseUrl(), false);
                    var retainedCursor = telemetry.beforeRecreation(restored, next.server().baseUrl());
                    var beforeRecreateCumulative = CumulativeMetricWitness.capture(beforeRecreateStop);
                    CumulativeMetricWitness.continued(report, "stop-before-recreate", afterProcessCumulative,
                            beforeRecreateCumulative, false);
                    admissions.stage("stop-before-recreate", 0, 0);
                    coordination.stage("stop-before-recreate", readiness(beforeRecreateStop, Map.of()));
                    coordination.begin("delete-recreate-without-start");
                    restored.deleteArtifact(pipeline, restored.contentHash(pipeline));
                    assertThat(restored.artifact(pipeline)).isEmpty();
                    assertThat(generation(claim(database, pipeline))).isEqualTo(beforeProcessGeneration + 1);
                    restored.apply(workload.resources(fork.sourceSettings(),
                            fork.externalTargetUri()));
                    telemetry.recreatedBeforeStart(retainedCursor, restored, next.server().baseUrl());
                    admissions.stage("delete-recreate-without-start", 0, 0);
                    coordination.stage("delete-recreate-without-start", Map.of("deletedArtifactAbsenceVerified", true,
                            "recreatedResourcesApplied", true, "startRequested", false));
                    coordination.begin("recreate-start");
                    restored.lifecycle(pipeline, LifecycleVerb.START);
                    var recreated = stored(latest, pipeline, beforeProcessGeneration + 2, PipelineState.RUNNING);
                    assertThat(recreated.scope().orElseThrow().pipelineIncarnationId()).isNotEqualTo(incarnation);
                    long recreatedAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                    fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                    delivered(client, fork.externalTargetUri(), recreatedAmount);
                    recreated = activeCounter(latest, pipeline, recreated.scope().orElseThrow(), 0);
                    assertThat(outputCounter(recreated.observation()).startTime())
                            .isAfter(outputCounter(afterProcessRestart.observation()).startTime());
                    var recreatedCumulative = CumulativeMetricWitness.running(latest, pipeline, recreated.scope().orElseThrow(),
                            WAIT, null, report, "delete-recreate-start");
                    CumulativeMetricWitness.reset(report, "delete-recreate-start", beforeRecreateCumulative, recreatedCumulative);
                    record(report, "delete-recreate-start", recreated, claim(database, pipeline));
                    telemetry.capture("delete-recreate-start", recreated, restored, next.server().baseUrl(), true);
                    admissions.stage("recreate-start", 1, 1);
                    coordination.stage("recreate-start", readiness(recreated, Map.of("targetExpectedAmount", recreatedAmount,
                            "targetActualAmount", targetAmount(client, fork.externalTargetUri()))));
                    coordination.begin("final-stop");
                    restored.stop(pipeline, false);
                    var finalStop = stored(latest, pipeline, beforeProcessGeneration + 2, PipelineState.STOPPED);
                    telemetry.capture("final-stop", finalStop, restored, next.server().baseUrl(), false);
                    CumulativeMetricWitness.continued(report, "final-stop", recreatedCumulative,
                            CumulativeMetricWitness.capture(finalStop), false);
                    admissions.stage("final-stop", 0, 0);
                    coordination.stage("final-stop", readiness(finalStop, Map.of()));
                    coordination.begin("second-process-shutdown");
                    admissions.shutdown("second-process-shutdown");
                    coordination.stage("second-process-shutdown", Map.of("ownedVmDeathAndDisconnectVerified", true));
                }
                coordination.requireComplete();
            }
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            assertThat(TelemetryMongoIdentityWitness.inputHashes(harnessRoot)).isEqualTo(telemetryInputs);
            report.completeDiagnostic(Map.of("correctness", "REAL_LIFECYCLE_IDENTITIES_MATCHED_DURABLE_GENERATIONS",
                    "performanceAcceptanceEligible", false,
                    "unverified", directCounts
                            ? List.of("SNAPSHOT_REBUILD_RESUME", "ALL_TELEMETRY_SURFACE_IDENTITIES")
                            : List.of("SNAPSHOT_REBUILD_RESUME", "FAILED_CAS_NO_JOB",
                                    "COMMAND_LEVEL_ADVANCE_COUNTS", "ALL_TELEMETRY_SURFACE_IDENTITIES")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); } catch (RuntimeException writeFailure) { failure.addSuppressed(writeFailure); }
            throw failure;
        }
    }

    private static void awaitTicks(MongoObservationStore latest, String pipeline,
            long generation, PipelineState state) {
        java.util.Set<java.time.Instant> observed = new java.util.HashSet<>();
        Await.until("three genuine observation timestamps while " + state, WAIT, () -> {
            latest.readStored(pipeline).filter(value -> value.scope().map(scope ->
                    scope.executionGeneration() == generation).orElse(false)
                    && value.observation().state() == state).ifPresent(value ->
                            observed.add(value.observation().observedAt()));
            return observed.size() >= 3 && !observed.contains(null);
        }, () -> "distinct timestamps=" + observed.size());
    }

    private static Document claim(MongoDatabase database, String pipeline) {
        Document value = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline)).first();
        assertThat(value).as("the actual standalone submission wrote its durable generation").isNotNull();
        for (String field : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil")) {
            assertThat(value.containsKey(field)).as("standalone acquired no claim or lease field %s", field).isFalse();
        }
        return value;
    }

    private static long generation(Document claim) {
        Object raw = claim.get("executionGeneration");
        assertThat(raw instanceof Long || raw instanceof Integer).as("the durable generation is an integer")
                .isTrue();
        long value = ((Number) raw).longValue();
        assertThat(value).isPositive();
        return value;
    }

    private static long targetAmount(MongoClient client, String targetUri) {
        Document row = client.getDatabase(new ConnectionString(targetUri).getDatabase())
                .getCollection("bench_copy_orders").find(new Document("id", 1L)).first();
        assertThat(row).as("the known source row reached the real target").isNotNull();
        return ((Number) row.get("amount")).longValue();
    }

    private static void delivered(MongoClient client, String targetUri, long expectedAmount) {
        Await.until("the new lifecycle CDC change to reach the target", WAIT,
                () -> targetAmount(client, targetUri) == expectedAmount,
                () -> "target amount=" + targetAmount(client, targetUri) + ", expected=" + expectedAmount);
    }

    private static ObservationStore.Stored stored(MongoObservationStore store, String pipeline,
            long generation, PipelineState state) {
        return Await.answered(pipeline + " generation " + generation + " state " + state, WAIT,
                () -> store.readStored(pipeline).filter(value -> value.scope().map(scope ->
                        scope.executionGeneration() == generation).orElse(false)
                        && value.observation().state() == state));
    }

    private static ObservationStore.Stored activeCounter(MongoObservationStore store, String pipeline,
            ObservationStore.Scope scope, long previousValue) {
        try {
            return Await.answered("a real output counter to advance for " + pipeline + " scope " + scope, WAIT,
                    () -> store.readStored(pipeline).filter(value -> value.scope().equals(Optional.of(scope))
                            && value.observation().state() == PipelineState.RUNNING
                            && counterIfKnown(value.observation()).filter(counter -> counter.value() > previousValue)
                                    .isPresent()));
        } catch (AssertionError failure) {
            String last = store.readStored(pipeline).map(value -> "scope=" + value.scope()
                    + ", state=" + value.observation().state() + ", observedAt=" + value.observation().observedAt()
                    + ", counter=" + counterIfKnown(value.observation()) + ", expectedGreaterThan=" + previousValue)
                    .orElse("no stored observation");
            throw new AssertionError(failure.getMessage() + "; last=" + last, failure);
        }
    }

    private record OutputCounter(long value, Instant startTime) { }

    private static Optional<OutputCounter> counterIfKnown(Observation observation) {
        var points = observation.facts().stream().filter(fact -> fact.name().endsWith(".pipeline.records"))
                .flatMap(fact -> fact.points().stream())
                .filter(point -> "out".equals(point.attributes().get("direction"))).toList();
        if (points.isEmpty()) { return Optional.empty(); }
        var starts = points.stream().map(MetricPoint::startTime).distinct().toList();
        assertThat(starts).as("all output table and operation counters share their real accumulation start")
                .hasSize(1).doesNotContainNull();
        return Optional.of(new OutputCounter(points.stream().mapToLong(MetricPoint::value).sum(), starts.getFirst()));
    }

    private static OutputCounter outputCounter(Observation observation) {
        return counterIfKnown(observation).orElseThrow(() -> new AssertionError("output counter is not wired"));
    }

    private static Map<String, Object> readiness(ObservationStore.Stored stored, Map<String, Object> target) {
        var scope = stored.scope().orElseThrow();
        Map<String, Object> evidence = new LinkedHashMap<>(target);
        evidence.put("state", stored.observation().state().name());
        evidence.put("incarnation", scope.pipelineIncarnationId());
        evidence.put("executionGeneration", scope.executionGeneration());
        evidence.put("observedAt", stored.observation().observedAt().toString());
        var counter = counterIfKnown(stored.observation());
        evidence.put("counterState", counter.isPresent() ? "RECORDED" : "UNAVAILABLE");
        counter.ifPresent(value -> {
            evidence.put("counterStartTime", value.startTime().toString());
            evidence.put("recordsOut", value.value());
        });
        return evidence;
    }

    private static Map<String, Object> targetReadiness(BenchmarkForkEnvironment.PhaseResult phase) {
        return Map.of("phase", phase.phase().id(), "targets", phase.targets().stream().map(target ->
                Map.of("rows", target.rows(), "checksum", target.checksum(), "matches", target.matches())).toList());
    }

    private static void record(BenchmarkLiveReport report, String action,
            ObservationStore.Stored stored, Document claim) {
        var scope = stored.scope().orElseThrow();
        assertThat(scope.executionGeneration()).isEqualTo(generation(claim));
        var counter = counterIfKnown(stored.observation());
        var details = new LinkedHashMap<String, Object>(Map.of("action", action, "generation", scope.executionGeneration(),
                "incarnation", scope.pipelineIncarnationId(), "state", stored.observation().state().name(),
                "observedAt", stored.observation().observedAt().toString(),
                "durableClusterId", claim.getString("clusterId"), "leaseFieldsAbsent", true));
        details.put("counterState", counter.isPresent() ? "RECORDED" : "UNAVAILABLE");
        counter.ifPresent(value -> {
            details.put("counterStartTime", value.startTime().toString());
            details.put("recordsOut", value.value());
        });
        report.addFork(details);
        System.out.printf("standalone-identity action=%s generation=%d state=%s counter=%s%n",
                action, scope.executionGeneration(), stored.observation().state(), counter);
    }

    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("execution identity witness requires -D" + PREFIX + name);
        }
        return value;
    }
}
