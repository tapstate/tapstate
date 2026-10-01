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
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        ExecutionAdmissionStages admissions = new ExecutionAdmissionStages(report, directCounts);
        try {
            String sha = PipelineBenchmarkLiveRunIT.sha256(jar);
            report.begin(Map.of("purpose", directCounts ? "REAL_STANDALONE_LIFECYCLE_ADMISSION_COUNTS"
                            : "REAL_STANDALONE_LIFECYCLE_IDENTITY",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "discoveryMode", "none"),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            var workload = BenchmarkWorkloadDefinitions.byId("copy");
            String pipeline = workload.pipelineIds().getFirst();
            try (var fork = BenchmarkForkEnvironment.open(workload, jar, "standalone-identity",
                    (uri, operator, artifact) -> {
                        if (!directCounts) { return BenchmarkForkEnvironment.OwnedBoot.plain(uri, operator, artifact); }
                        var observer = ExecutionAdmissionJdiSession.startWithLeaseObservation(uri, operator, artifact, pipeline);
                        admissions.bind(observer);
                        return new BenchmarkForkEnvironment.OwnedBoot(observer.server(), null, observer);
                    }, boot -> admissions.stage("before-first-start", 0, 0));
                    var client = MongoClients.create(fork.storeUri())) {
                MongoDatabase database = client.getDatabase(new ConnectionString(fork.storeUri()).getDatabase());
                var latest = new MongoObservationStore(client,
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                fork.runPhase(workload.phases().getFirst(), true);
                fork.runPhase(workload.phases().get(1), true);
                ControlPlane control = fork.control();
                Document firstClaim = claim(database, pipeline);
                long firstGeneration = generation(firstClaim);
                var first = stored(latest, pipeline, firstGeneration, PipelineState.RUNNING);
                first = activeCounter(latest, pipeline, first.scope().orElseThrow(),
                        workload.phases().getFirst().expectedLogicalOutputChanges()
                                + workload.phases().get(1).expectedLogicalOutputChanges() - 1);
                OutputCounter firstCounter = outputCounter(first.observation());
                assertThat(firstCounter.value()).isPositive();
                String incarnation = first.scope().orElseThrow().pipelineIncarnationId();
                record(report, "initial-start", first, firstClaim);
                admissions.stage("first-start", 1, 1);
                if (directCounts) { awaitTicks(latest, pipeline, firstGeneration, PipelineState.RUNNING); }
                admissions.stage("running-ticks", 0, 0);

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
                record(report, "pause", paused, claim(database, pipeline));
                if (directCounts) { awaitTicks(latest, pipeline, firstGeneration, PipelineState.PAUSED); }
                admissions.stage("pause-and-paused-ticks", 0, 0);

                long resumedAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                control.lifecycle(pipeline, LifecycleVerb.RESUME);
                delivered(client, fork.externalTargetUri(), resumedAmount);
                var resumed = activeCounter(latest, pipeline, first.scope().orElseThrow(), firstCounter.value());
                assertThat(outputCounter(resumed.observation()).startTime()).isEqualTo(firstCounter.startTime());
                assertThat(generation(claim(database, pipeline))).isEqualTo(firstGeneration);
                record(report, "ordinary-resume", resumed, claim(database, pipeline));
                admissions.stage("ordinary-resume", 0, 0);

                control.stop(pipeline, false);
                var stopped = stored(latest, pipeline, firstGeneration, PipelineState.STOPPED);
                assertThat(stopped.scope()).isEqualTo(first.scope());
                long resumedRecords = outputCounter(resumed.observation()).value();
                var finalCounter = outputCounter(stopped.observation());
                assertThat(finalCounter.startTime()).isEqualTo(firstCounter.startTime());
                assertThat(finalCounter.value()).isGreaterThanOrEqualTo(resumedRecords);
                record(report, "stop", stopped, claim(database, pipeline));
                admissions.stage("stop", 0, 0);
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
                record(report, "stop-start", restarted, claim(database, pipeline));
                admissions.stage("stop-start", 1, 1);
                long beforeProcessGeneration = firstGeneration + 1;

                control.stop(pipeline, false);
                stored(latest, pipeline, beforeProcessGeneration, PipelineState.STOPPED);
                admissions.stage("stop-before-process-restart", 0, 0);
                admissions.shutdown("first-process-shutdown");
                fork.closeBoot();
                assertThat(generation(claim(database, pipeline))).isEqualTo(beforeProcessGeneration);
                var nextObserver = directCounts ? ExecutionAdmissionJdiSession.startWithLeaseObservation(
                        fork.storeUri(), new ConnectionString(fork.operatorStateUri()).getDatabase(), jar, pipeline) : null;
                try (var next = nextObserver == null
                        ? BenchmarkForkEnvironment.OwnedBoot.plain(fork.storeUri(),
                                new ConnectionString(fork.operatorStateUri()).getDatabase(), jar)
                        : new BenchmarkForkEnvironment.OwnedBoot(nextObserver.server(), null, nextObserver)) {
                    if (nextObserver != null) { admissions.bind(nextObserver); }
                    ControlPlane restored = new ControlPlane(next.server().baseUrl());
                    restored.login("benchmark", "benchmark-password");
                    assertThat(generation(claim(database, pipeline))).isEqualTo(beforeProcessGeneration);
                    if (directCounts) { awaitTicks(latest, pipeline, beforeProcessGeneration, PipelineState.STOPPED); }
                    admissions.stage("new-process-boot-without-start", 0, 0);
                    restored.lifecycle(pipeline, LifecycleVerb.START);
                    var afterProcessRestart = stored(latest, pipeline, beforeProcessGeneration + 1, PipelineState.RUNNING);
                    assertThat(afterProcessRestart.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(incarnation);
                    long processRestartAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                    fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                    delivered(client, fork.externalTargetUri(), processRestartAmount);
                    afterProcessRestart = activeCounter(latest, pipeline, afterProcessRestart.scope().orElseThrow(), 0);
                    assertThat(outputCounter(afterProcessRestart.observation()).startTime())
                            .isAfter(restartCounter.startTime());
                    record(report, "process-restart-start", afterProcessRestart, claim(database, pipeline));
                    admissions.stage("new-process-start", 1, 1);

                    restored.stop(pipeline, false);
                    stored(latest, pipeline, beforeProcessGeneration + 1, PipelineState.STOPPED);
                    admissions.stage("stop-before-recreate", 0, 0);
                    restored.deleteArtifact(pipeline, restored.contentHash(pipeline));
                    assertThat(restored.artifact(pipeline)).isEmpty();
                    assertThat(generation(claim(database, pipeline))).isEqualTo(beforeProcessGeneration + 1);
                    restored.apply(workload.resources(fork.sourceSettings(),
                            fork.externalTargetUri()));
                    admissions.stage("delete-recreate-without-start", 0, 0);
                    restored.lifecycle(pipeline, LifecycleVerb.START);
                    var recreated = stored(latest, pipeline, beforeProcessGeneration + 2, PipelineState.RUNNING);
                    assertThat(recreated.scope().orElseThrow().pipelineIncarnationId()).isNotEqualTo(incarnation);
                    long recreatedAmount = targetAmount(client, fork.externalTargetUri()) + 1;
                    fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                    delivered(client, fork.externalTargetUri(), recreatedAmount);
                    recreated = activeCounter(latest, pipeline, recreated.scope().orElseThrow(), 0);
                    assertThat(outputCounter(recreated.observation()).startTime())
                            .isAfter(outputCounter(afterProcessRestart.observation()).startTime());
                    record(report, "delete-recreate-start", recreated, claim(database, pipeline));
                    admissions.stage("recreate-start", 1, 1);
                    restored.stop(pipeline, false);
                    stored(latest, pipeline, beforeProcessGeneration + 2, PipelineState.STOPPED);
                    admissions.stage("final-stop", 0, 0);
                    admissions.shutdown("second-process-shutdown");
                }
            }
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            report.completeDiagnostic(Map.of("correctness", "REAL_LIFECYCLE_IDENTITIES_MATCHED_DURABLE_GENERATIONS",
                    "performanceAcceptanceEligible", false,
                    "unverified", directCounts
                            ? List.of("SNAPSHOT_REBUILD_RESUME", "PHYSICAL_MONGO_COMMAND_COUNTS",
                                    "ALL_TELEMETRY_SURFACE_IDENTITIES")
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
