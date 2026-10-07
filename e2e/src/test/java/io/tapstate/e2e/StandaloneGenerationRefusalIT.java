package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** An actual store rejection cannot submit an unfenced standalone Job. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.generation-refusal.jar", matches = ".+")
class StandaloneGenerationRefusalIT {
    private static final String PREFIX = "tapstate.e2e.generation-refusal.";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @Test
    void aRealRejectedGenerationLeavesTheOldAuthorityAndSubmitsNoJob() throws Exception {
        Path jar = Path.of(required("jar")).toRealPath();
        String sha = required("sha256");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path output = Path.of(required("output")).toAbsolutePath();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        var workload = BenchmarkWorkloadDefinitions.byId("copy");
        String pipeline = workload.pipelineIds().getFirst();
        var observer = new AtomicReference<ExecutionAdmissionJdiSession>();
        var profile = new AtomicReference<NativeCoordinationProfile>();
        Throwable primary = null;
        try {
            report.begin(Map.of("purpose", "REAL_STANDALONE_GENERATION_REFUSAL_NO_SUBMISSION",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "pipelineId", pipeline),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            try (var fork = BenchmarkForkEnvironment.open(workload, jar, "generation-refusal", (uri, operator, artifact) -> {
                var ownedProfile = NativeCoordinationProfile.openOwned(uri);
                profile.set(ownedProfile);
                var observed = ExecutionAdmissionJdiSession.startWithLeaseObservation(
                        ownedProfile.nativeUri(), operator, artifact, pipeline);
                observer.set(observed);
                return new BenchmarkForkEnvironment.OwnedBoot(observed.server(), null, observed);
            }); var client = MongoClients.create(fork.storeUri())) {
                var initial = fork.runPhase(workload.phases().getFirst(), true);
                var warmup = fork.runPhase(workload.phases().get(1), true);
                assertThat(initial.targets()).isNotEmpty().allSatisfy(target -> assertThat(target.matches()).isTrue());
                assertThat(warmup.targets()).isNotEmpty().allSatisfy(target -> assertThat(target.matches()).isTrue());
                MongoDatabase database = client.getDatabase(new ConnectionString(fork.storeUri()).getDatabase());
                var latest = new MongoObservationStore(client, database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                fork.control().stop(pipeline, false);
                Await.until("the original real Job is stopped before refusing its successor", WAIT,
                        () -> fork.control().state(pipeline).filter(PipelineState.STOPPED::equals).isPresent()
                                && latest.readStored(pipeline).filter(value -> value.observation().state() == PipelineState.STOPPED).isPresent(),
                        () -> fork.control().state(pipeline).toString());
                Document old = authority(database, pipeline);
                assertThat(((Number) old.get("executionGeneration")).longValue()).isEqualTo(1L);
                var before = observer.get().boundary();
                assertThat(before.drained()).isTrue();
                assertThat(before.counts().get(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE))
                        .isEqualTo(new ExecutionAdmissionJdiSession.Counts(1, 1, 0, 0));
                assertThat(before.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB))
                        .isEqualTo(new ExecutionAdmissionJdiSession.Counts(1, 1, 0, 0));
                noLease(before);
                var normalCommands = profile.get().boundary("normal-start-and-stop");
                record(report, Map.of("action", "actual-warm-stopped-submission-baseline",
                        "authority", old.toJson(), "advance", count(before.counts().get(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE)),
                        "submit", count(before.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB)),
                        "methodBindings", bindings(before), "leaseCounts", leaseCounts(before),
                        "physicalCommands", normalCommands.evidence(), "ownedPid", observer.get().server().pid()));
                Document description = database.listCollections().filter(new Document("name", MongoStorePort.WORKLOAD_CLAIMS)).first();
                assertThat(description).isNotNull();
                Document options = description.get("options", Document.class);
                assertThat(options).isNotNull();
                Document validator = new Document("$or", List.of(new Document("resourceId", new Document("$ne", pipeline)),
                        new Document("executionGeneration", new Document("$lte", 1L))));
                boolean installed = false;
                Throwable refusalFailure = null;
                try {
                    installed = true;
                    database.runCommand(new Document("collMod", MongoStorePort.WORKLOAD_CLAIMS).append("validator", validator)
                            .append("validationLevel", "strict").append("validationAction", "error"));
                    assertThat(database.listCollections().filter(new Document("name", MongoStorePort.WORKLOAD_CLAIMS))
                            .first().get("options", Document.class).get("validator")).isEqualTo(validator);
                    long deadline = System.nanoTime() + WAIT.toNanos();
                    fork.control().lifecycle(pipeline, LifecycleVerb.START);
                    Await.until("the actual server reports its coded generation refusal", remaining(deadline),
                            () -> fork.control().failureCode(pipeline).filter("actuation.execution-generation-unavailable"::equals).isPresent(),
                            () -> "state=" + fork.control().state(pipeline) + ", failure=" + fork.control().failureCode(pipeline));
                    var failed = observer.get().boundary();
                    assertThat(failed.drained()).isTrue();
                    var advance = failed.counts().get(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE);
                    var submit = failed.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB);
                    assertThat(advance.entries() - before.counts().get(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE).entries()).isEqualTo(1);
                    assertThat(advance.normalReturns()).isEqualTo(1);
                    assertThat(advance.exceptionalExits()).isEqualTo(1);
                    assertThat(submit).as("the failed admission never entered the actual submitJob method").isEqualTo(before.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB));
                    noLease(failed);
                    Document refusedAuthority = authority(database, pipeline);
                    assertThat(refusedAuthority.get("_id")).isEqualTo(old.get("_id"));
                    assertThat(refusedAuthority.get("executionGeneration")).isEqualTo(old.get("executionGeneration"));
                    var physical = profile.get().boundary("rejected-successor");
                    assertThat(physical.count(NativeCoordinationProfile.Family.ADVANCE_STANDALONE)).isEqualTo(1);
                    var rejected = physical.operations().stream()
                            .filter(operation -> operation.family() == NativeCoordinationProfile.Family.ADVANCE_STANDALONE).toList();
                    assertThat(rejected).hasSize(1);
                    assertThat(rejected.getFirst().errorCode()).isEqualTo(121L);
                    assertThat(rejected.getFirst().upsert()).isFalse();
                    Document id = old.get("_id", Document.class);
                    assertThat(rejected.getFirst().key()).isEqualTo(new NativeCoordinationProfile.Key(id.getString("clusterId"),
                            id.getString("resourceType"), id.getString("resourceId")));
                    for (var family : List.of(NativeCoordinationProfile.Family.ACQUIRE, NativeCoordinationProfile.Family.RENEW,
                            NativeCoordinationProfile.Family.RELEASE, NativeCoordinationProfile.Family.ADVANCE_UNDER_CLAIM)) {
                        assertThat(physical.count(family)).isZero();
                    }
                    var failure = latest.readStored(pipeline).orElseThrow().observation().failure();
                    assertThat(failure).isNotNull();
                    assertThat(failure.code()).isEqualTo("actuation.execution-generation-unavailable");
                    assertThat(failure.params()).isEqualTo(Map.of("pipeline", pipeline));
                    record(report, Map.ofEntries(Map.entry("action", "actual-generation-refused-without-submission"),
                            Map.entry("authority", refusedAuthority.toJson()), Map.entry("physicalCommands", physical.evidence()),
                            Map.entry("advance", count(advance)), Map.entry("submit", count(submit)),
                            Map.entry("methodBindings", bindings(failed)), Map.entry("leaseCounts", leaseCounts(failed)),
                            Map.entry("codedFailure", Map.of("code", failure.code(), "params", failure.params())),
                            Map.entry("ownedPid", observer.get().server().pid()), Map.entry("serverOutput", observer.get().server().output().toString())));
                    fork.control().stop(pipeline, false);
                } catch (Exception | Error failure) { refusalFailure = failure; throw failure; }
                finally {
                    if (installed) {
                        try {
                            database.runCommand(new Document("collMod", MongoStorePort.WORKLOAD_CLAIMS)
                                    .append("validator", options.get("validator", new Document()))
                                    .append("validationLevel", options.get("validationLevel", "strict"))
                                    .append("validationAction", options.get("validationAction", "error")));
                            Document restoredOptions = database.listCollections()
                                    .filter(new Document("name", MongoStorePort.WORKLOAD_CLAIMS)).first().get("options", Document.class);
                            assertThat(restoredOptions.get("validator", new Document())).isEqualTo(options.get("validator", new Document()));
                            assertThat(restoredOptions.get("validationLevel", "strict")).isEqualTo(options.get("validationLevel", "strict"));
                            assertThat(restoredOptions.get("validationAction", "error")).isEqualTo(options.get("validationAction", "error"));
                            record(report, Map.of("action", "actual-owned-validator-restored", "options", restoredOptions.toJson()));
                        } catch (RuntimeException | Error cleanup) {
                            if (refusalFailure != null) { refusalFailure.addSuppressed(cleanup); } else { throw cleanup; }
                        }
                    }
                }
                var terminal = observer.get().shutdownAndFinish();
                assertThat(terminal.fullyDrained()).isTrue();
                noLease(terminal);
                assertThat(terminal.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB)).isEqualTo(before.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB));
                record(report, Map.of("action", "actual-owned-terminal-no-new-submit", "ownedPid", observer.get().server().pid(),
                        "ownedVmDeath", terminal.ownedVmDeath(), "ownedVmDisconnected", terminal.ownedVmDisconnected(),
                        "advance", count(terminal.counts().get(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE)),
                        "submit", count(terminal.counts().get(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB))));
            }
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        } catch (Exception | Error failure) {
            primary = failure;
            try { report.fail(failure); } catch (RuntimeException | Error evidence) { failure.addSuppressed(evidence); }
            throw failure;
        } finally {
            if (profile.get() != null) {
                try { profile.get().close(); }
                catch (RuntimeException | Error cleanup) {
                    if (primary != null) { primary.addSuppressed(cleanup); }
                    else {
                        try { report.fail(cleanup); } catch (RuntimeException | Error evidence) { cleanup.addSuppressed(evidence); }
                        throw cleanup;
                    }
                }
            }
        }
        report.completeDiagnostic(Map.of("generationRefusalNoJobQualified", true, "acceptanceEvaluated", false,
                "performanceAcceptanceEligible", false));
    }

    private static Map<String, Object> leaseCounts(ExecutionAdmissionJdiSession.Boundary boundary) {
        Map<String, Object> values = new LinkedHashMap<>();
        boundary.leaseCounts().forEach((target, count) -> values.put(target.name(), count(count)));
        return Map.copyOf(values);
    }

    private static void record(BenchmarkLiveReport report, Map<String, Object> value) {
        assertThat(JsonWriter.write(value).getBytes(StandardCharsets.UTF_8).length)
                .as("each complete refusal evidence stage stays bounded").isLessThanOrEqualTo(2 * 1024 * 1024);
        report.addFork(value);
    }

    private static Document authority(MongoDatabase database, String pipeline) {
        List<Document> values = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline)).limit(2).into(new ArrayList<>());
        assertThat(values).hasSize(1);
        Document value = values.getFirst();
        assertThat(value).doesNotContainKeys("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil");
        return new Document(value);
    }

    private static void noLease(ExecutionAdmissionJdiSession.Boundary boundary) {
        assertThat(boundary.leaseObservationEnabled()).isTrue();
        assertThat(boundary.leasesDrained()).isTrue();
        for (var count : boundary.leaseCounts().values()) { assertThat(count).isEqualTo(new ExecutionAdmissionJdiSession.Counts(0, 0, 0, 0)); }
    }

    private static Map<String, Long> count(ExecutionAdmissionJdiSession.Counts count) {
        return Map.of("entries", count.entries(), "normalReturns", count.normalReturns(),
                "exceptionalExits", count.exceptionalExits(), "inFlight", count.inFlight());
    }

    private static Map<String, Object> bindings(ExecutionAdmissionJdiSession.Boundary boundary) {
        Map<String, Object> values = new LinkedHashMap<>();
        boundary.bindings().forEach((target, binding) -> values.put(target.name(), Map.of("type", binding.type(),
                "method", binding.method(), "signature", binding.signature(), "codeSha256", binding.methodSha256(),
                "origin", binding.artifactOrigin(), "loader", binding.loaderIdentity())));
        return Map.copyOf(values);
    }

    private static Duration remaining(long deadline) {
        long left = deadline - System.nanoTime();
        assertThat(left).as("the original refusal window remains").isPositive();
        return Duration.ofNanos(left);
    }

    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new AssertionError("missing generation refusal input " + name); }
        return value;
    }
}
