package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** A real paused process loss supplies a positive account before a reset in the restored JVM. */
@RequiresDocker
class NativeFailureAccountResetIT {
    private static final String PREFIX = "tapstate.e2e.native-failure-account.";
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final int MAX_RECORDS = 512, MAX_BYTES = 2 * 1024 * 1024;
    private static final String ERRORS = "tapstate.pipeline.errors";
    private static final String CODE = LifecycleError.PAUSED_JOB_MISSING.code();

    @Test
    void aPositiveRestoredFailureAccountIsEvictedByStopStartInTheSameJvm() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "sha256", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "the native failure account witness needs named immutable inputs");
        Path jar = Path.of(required("jar")).toRealPath();
        String sha = required("sha256");
        assertThat(sha).matches("[0-9a-f]{64}");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path output = Path.of(required("output"));
        Path root = PipelineBenchmarkLiveRunIT.harnessRoot();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, root);
        boolean firstCurrentWindow = Boolean.getBoolean(PREFIX + "first-current-window");
        Map<String, Object> inputs = inputHashes(root);
        Map<String, Object> connectors = new LinkedHashMap<>();
        for (String name : List.of("mysql", "postgres", "mongodb")) {
            connectors.put(name, PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(name)));
        }
        int port;
        try (ServerSocket reserved = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            port = reserved.getLocalPort();
        }
        URI scrape = URI.create("http://127.0.0.1:" + port + "/metrics");
        var workload = BenchmarkWorkloadDefinitions.byId("copy");
        String pipeline = workload.pipelineIds().getFirst();
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        try {
            report.begin(Map.of("purpose", "NATIVE_POSITIVE_FAILURE_ACCOUNT_RESET",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "expectedJarSha256", sha,
                    "harness", inputs, "connectors", connectors, "prometheusEndpoint", scrape.toString(),
                    "historySampleInterval", "PT2S", "performanceAcceptanceEligible", false,
                    "firstCurrentWindowRequested", firstCurrentWindow),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            try (var fork = BenchmarkForkEnvironment.open(workload, jar, "positive-failure-reset");
                    var mongo = MongoClients.create(fork.storeUri())) {
                var snapshot = fork.runPhase(workload.phases().getFirst(), true);
                var warmup = fork.runPhase(workload.phases().get(1), true);
                assertThat(snapshot.targets()).isNotEmpty().allSatisfy(target -> assertThat(target.matches()).isTrue());
                assertThat(warmup.targets()).isNotEmpty().allSatisfy(target -> assertThat(target.matches()).isTrue());
                report.addFork(Map.of("action", "actual-first-process-copy-targets", "snapshot",
                        snapshot.targets().stream().map(target -> Map.of("rows", target.rows(),
                                "checksum", target.checksum(), "matches", target.matches())).toList(),
                        "warmup", warmup.targets().stream().map(target -> Map.of("rows", target.rows(),
                                "checksum", target.checksum(), "matches", target.matches())).toList()));
                MongoDatabase database = mongo.getDatabase(new ConnectionString(fork.storeUri()).getDatabase());
                var desired = new MongoDesiredStore(database.getCollection(MongoStorePort.PIPELINE_DESIRED));
                var actual = new MongoStateStore(database.getCollection(MongoStorePort.PIPELINE_STATE));
                var latest = new MongoObservationStore(mongo,
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                fork.control().lifecycle(pipeline, LifecycleVerb.PAUSE);
                Await.until("real desired, actual and latest PAUSED agreement", WAIT, () ->
                        desired.read(pipeline).filter(value -> value.targetState() == PipelineState.PAUSED).isPresent()
                                && actual.read(pipeline).filter(value ->
                                        StateJson.parse(value.stateJson()) == PipelineState.PAUSED).isPresent()
                                && latest.readStored(pipeline).filter(value ->
                                        value.observation().state() == PipelineState.PAUSED).isPresent(),
                        () -> "desired=" + desired.read(pipeline) + ", actual=" + actual.read(pipeline));
                var paused = latest.readStored(pipeline).orElseThrow();
                assertThat(paused.observation().failure()).isNull();
                var old = authority(database, pipeline, paused);
                assertThat(fork.server().isAlive()).isTrue();
                Path firstOutput = fork.server().output();
                long firstPid = fork.server().pid();
                fork.server().kill();
                assertThat(fork.server().isAlive()).isFalse();
                assertThat(desired.read(pipeline).orElseThrow().targetState()).isEqualTo(PipelineState.PAUSED);
                report.addFork(Map.of("action", "owned-plain-paused-process-loss", "ownedPid", firstPid,
                        "output", firstOutput.toString(), "incarnation", old.incarnation(),
                        "generation", old.generation(), "firstJvmHeapCleanupClaimed", false));

                String operator = new ConnectionString(fork.operatorStateUri()).getDatabase();
                try (var observer = NativeTelemetryIdentityJdiSession.start(fork.storeUri(), operator, jar, sha,
                        pipeline, List.of("--tapstate.metrics.export.prometheus.host=127.0.0.1",
                                "--tapstate.metrics.export.prometheus.port=" + port,
                                "--tapstate.metrics.history.sample-interval=PT2S"))) {
                    observer.recordAuthority(old);
                    var control = new ControlPlane(observer.server().baseUrl());
                    control.login("benchmark", "benchmark-password");
                    Await.until("the genuinely missing paused job to publish its coded failure", WAIT, () ->
                            control.state(pipeline).filter(PipelineState.FAILED::equals).isPresent()
                                    && control.failureCode(pipeline).filter(CODE::equals).isPresent()
                                    && desired.read(pipeline).filter(value ->
                                            value.targetState() == PipelineState.PAUSED).isPresent()
                                    && actual.read(pipeline).filter(value ->
                                            StateJson.parse(value.stateJson()) == PipelineState.FAILED).isPresent()
                                    && latest.readStored(pipeline).filter(value ->
                                            value.observation().state() == PipelineState.FAILED
                                                    && value.observation().failure() != null
                                                    && CODE.equals(value.observation().failure().code())).isPresent(),
                            () -> "state=" + control.state(pipeline) + ", failure=" + control.failureCode(pipeline));
                    var failed = latest.readStored(pipeline).orElseThrow();
                    assertThat(failed.scope()).isEqualTo(paused.scope());
                    assertThat(failed.observation().observedAt()).isAfter(paused.observation().observedAt());
                    assertThat(authority(database, pipeline, failed).scope()).isEqualTo(old.scope());
                    HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
                    var status = get(http, control, observer.server().baseUrl().resolve(
                            "/api/pipelines/" + pipeline + "/status"));
                    assertThat(status.get("state")).isEqualTo("FAILED");
                    assertThat(object(status.get("failure"))).containsEntry("code", CODE)
                            .containsEntry("params", Map.of("pipeline", pipeline));
                    report.addFork(Map.of("action", "actual-paused-loss-status", "response", status,
                            "desired", desired.read(pipeline).orElseThrow().targetState().name()));
                    var positive = collect(observer, report, http, scrape, "positive-restored-failure",
                            NativeTelemetryIdentityJdiSession.BindingProfile.RESTORED_BEFORE_FIRST_START, stage ->
                            positiveAccount(stage.records(), old).isPresent()
                                    && scopedLog(stage.records(), old).isPresent()
                                    && matchingPoint(stage.records(), stage.body(), old, ERRORS, true,
                                            PipelineState.FAILED).isPresent());
                    Map<String, Object> oldAccountRecord = positiveAccount(positive.records(), old).orElseThrow();
                    Map<String, Object> oldAccount = object(oldAccountRecord.get("account"));
                    Map<String, Object> oldLog = scopedLog(positive.records(), old).orElseThrow();
                    var oldMatch = matchingPoint(positive.records(), positive.body(), old, ERRORS, true,
                            PipelineState.FAILED).orElseThrow();
                    Map<String, Object> oldProduced = oldMatch.produced();
                    assertThat(accountMatchesPoint(oldAccount, oldMatch.point()))
                            .as("the real account count/start reached the actual SDK error point").isTrue();
                    assertThat(namedCode(oldProduced.get("namedBackstop"))).isTrue();
                    var failureLogs = logs(http, control, observer.server().baseUrl(), pipeline);
                    assertThat(lines(failureLogs)).anyMatch(line -> sameLog(line, oldLog));
                    report.addFork(Map.of("action", "positive-account-and-current-log", "account", oldAccount,
                            "logResponse", failureLogs, "oldScope", old.scope(), "ownedPid", observer.server().pid()));

                    control.stop(pipeline, false);
                    Await.until("the restored failed execution to stop without changing its scope", WAIT, () ->
                            control.state(pipeline).filter(PipelineState.STOPPED::equals).isPresent()
                                    && latest.readStored(pipeline).filter(value ->
                                            value.observation().state() == PipelineState.STOPPED
                                                    && value.scope().equals(failed.scope())).isPresent(),
                            () -> "latest=" + latest.readStored(pipeline));
                    assertThat(authority(database, pipeline, latest.readStored(pipeline).orElseThrow()).scope())
                            .as("STOP retains the actual coordination generation").isEqualTo(old.scope());
                    var stopBoundary = observer.boundary("stop-completed",
                            NativeTelemetryIdentityJdiSession.BindingProfile.RESTORED_BEFORE_FIRST_START);
                    report.addFork(stopBoundary.evidence());
                    assertThat(stopBoundary.unverified()).isEmpty();
                    assertThat(stopBoundary.decodedAndAuthorityBound()).isTrue();
                    assertThat(folderCleared(stopBoundary.records())
                            || exporterCleared(stopBoundary.records(), oldProduced))
                            .as("STOP does not evict a positive predecessor before START").isFalse();
                    var stopped = collect(observer, report, http, scrape, "same-jvm-stop-retained",
                            NativeTelemetryIdentityJdiSession.BindingProfile.RESTORED_BEFORE_FIRST_START, stage -> {
                        assertThat(folderCleared(stage.records()) || exporterCleared(stage.records(), oldProduced))
                                .as("the STOP retention window cannot contain a positive predecessor eviction").isFalse();
                        return retainedAccount(stage.records(), old, oldAccountRecord).isPresent()
                                    && matchingPoint(stage.records(), stage.body(), old, ERRORS, true,
                                            PipelineState.STOPPED).filter(match ->
                                            Objects.equals(match.produced().get("receiver"), oldProduced.get("receiver"))
                                                    && namedCode(match.produced().get("namedBackstop"))
                                                    && accountMatchesPoint(oldAccount, match.point())).isPresent();
                    });
                    var stoppedAccount = retainedAccount(stopped.records(), old, oldAccountRecord).orElseThrow();
                    var stopMatch = matchingPoint(stopped.records(), stopped.body(), old, ERRORS, true,
                            PipelineState.STOPPED).orElseThrow();
                    assertThat(stopMatch.produced().get("receiver")).isEqualTo(oldProduced.get("receiver"));
                    assertThat(accountMatchesPoint(object(stoppedAccount.get("account")), stopMatch.point())).isTrue();
                    var stoppedLogs = logs(http, control, observer.server().baseUrl(), pipeline);
                    assertThat(lines(stoppedLogs)).anyMatch(line -> sameLog(line, oldLog));
                    report.addFork(Map.of("action", "same-restored-jvm-stop-retained",
                            "oldScope", old.scope(), "account", object(stoppedAccount.get("account")),
                            "includedFrame", stopMatch.frame(), "sdkPoint", stopMatch.point(),
                            "scrapeLine", stopMatch.line(), "currentLogResponse", stoppedLogs,
                            "ownedPid", observer.server().pid()));
                    assertThat(authority(database, pipeline, latest.readStored(pipeline).orElseThrow()).scope())
                            .isEqualTo(old.scope());
                    // Close the STOP window before any new execution is requested.
                    var retainedBoundary = observer.boundary("stop-retained-before-start",
                            NativeTelemetryIdentityJdiSession.BindingProfile.RESTORED_BEFORE_FIRST_START);
                    report.addFork(retainedBoundary.evidence());
                    assertThat(retainedBoundary.unverified()).isEmpty();
                    assertThat(retainedBoundary.decodedAndAuthorityBound()).isTrue();
                    assertThat(folderCleared(retainedBoundary.records())
                            || exporterCleared(retainedBoundary.records(), oldProduced)).isFalse();
                    observer.requireFullBindingsBeforeStart();
                    Map<String, Object> windowAdmission = null;
                    long resetDeadline = firstCurrentWindow ? System.nanoTime() + WAIT.toNanos() : 0;
                    if (firstCurrentWindow) {
                        var oldLatest = latest.readStored(pipeline).orElseThrow();
                        assertPositiveOldLatest(oldLatest, old, oldAccount);
                        var artifactBefore = database.getCollection(MongoStorePort.ARTIFACTS)
                                .find(new Document("_id", pipeline)).first();
                        var artifact = control.artifact(pipeline).orElseThrow();
                        assertThat(artifactBefore).isNotNull();
                        assertThat(artifactBefore.getString("contentHash")).isEqualTo(artifact.contentHash());
                        var held = observer.armFirstAdmissionReturn(old, resetDeadline);
                        try (held) {
                            control.lifecycle(pipeline, LifecycleVerb.START);
                            windowAdmission = held.awaitEntry();
                            qualifyFirstCurrentWindow(observer, held, windowAdmission, report, database, latest,
                                    desired, http, control, pipeline, old, oldLatest, oldAccount, artifactBefore, resetDeadline);
                        } finally { report.addFork(held.evidence()); }
                    } else { control.lifecycle(pipeline, LifecycleVerb.START); }
                    var reset = Await.answered("a real new RUNNING execution after STOP/START",
                            firstCurrentWindow ? remainingWindow(resetDeadline) : WAIT, () ->
                            latest.readStored(pipeline).filter(value -> value.scope().isPresent()
                                    && value.scope().orElseThrow().executionGeneration() > old.generation()
                                    && value.observation().state() == PipelineState.RUNNING
                                    && value.observation().failure() == null));
                    var next = authority(database, pipeline, reset);
                    assertThat(next.incarnation()).isEqualTo(old.incarnation());
                    assertThat(next.clusterId()).isEqualTo(old.clusterId());
                    assertThat(next.coordinationId()).isEqualTo(old.coordinationId());
                    assertThat(next.generation()).isEqualTo(Math.addExact(old.generation(), 1));
                    observer.recordAuthority(next);
                    issueResetEvent(fork, mongo, warmup, report, next);
                    var cleared = collect(observer, report, http, scrape, "same-jvm-reset", stage ->
                            resetAccount(stage.records(), next, oldAccountRecord).isPresent()
                                    && folderCleared(stage.records()) && exporterCleared(stage.records(), oldProduced)
                                    && matchingPoint(stage.records(), stage.body(), next,
                                            "tapstate.pipeline.records", false, PipelineState.RUNNING)
                                            .filter(match -> Objects.equals(match.produced().get("receiver"),
                                                    oldProduced.get("receiver"))).isPresent()
                                    && !hasErrorSeries(stage.body(), pipeline));
                    Map<String, Object> newAccountRecord = resetAccount(
                            cleared.records(), next, oldAccountRecord).orElseThrow();
                    Map<String, Object> newProduced = matchingPoint(cleared.records(), cleared.body(), next,
                            "tapstate.pipeline.records", false, PipelineState.RUNNING).orElseThrow().produced();
                    assertThat(newProduced.get("receiver")).isEqualTo(oldProduced.get("receiver"));
                    assertThat(namedCode(newProduced.get("namedBackstop"))).isFalse();
                    assertThat(newProduced.get("metrics") instanceof List<?> metrics && metrics.stream()
                            .map(NativeFailureAccountResetIT::object).noneMatch(metric -> ERRORS.equals(metric.get("name"))))
                            .as("the current SDK return contains no inherited error instrument").isTrue();
                    assertThat(observer.capturedJob(next)).isPresent();
                    if (firstCurrentWindow) {
                        var actualJob = observer.capturedJob(next).orElseThrow();
                        var admitted = Objects.requireNonNull(windowAdmission);
                        Map<String, Object> submitted = cleared.records().stream().filter(record -> good(record, "SUBMIT")
                                && Objects.equals(record.get("admissionObject"), admitted.get("admissionObject"))
                                && Objects.equals(record.get("ownershipReceiver"), admitted.get("receiver"))
                                && Objects.equals(record.get("scope"), next.scope())).findFirst().orElseThrow();
                        assertThat(submitted.get("fence")).isEqualTo(admitted.get("fence"));
                        assertThat(submitted.get("job")).isEqualTo(actualJob.job());
                        assertThat(submitted.get("executionJobObject")).isEqualTo(actualJob.executionObjectId());
                        assertThat(submitted.get("proxy")).isEqualTo(actualJob.proxyId());
                        assertThat(((Number) submitted.get("entryOrder")).longValue())
                                .isGreaterThan(((Number) admitted.get("returnOrder")).longValue());
                        report.addFork(Map.of("action", "first-current-window-followed-by-real-same-admission-submit",
                                "admissionObject", admitted.get("admissionObject"), "submission", submitted,
                                "currentScope", next.scope(), "currentPublication", reset.observation().observedAt().toString(),
                                "claimedQualified", false, "preSubmitJobClaimed", false));
                    }
                    var resetLogs = logs(http, control, observer.server().baseUrl(), pipeline);
                    assertThat(lines(resetLogs)).noneMatch(line -> sameLog(line, oldLog));
                    report.addFork(Map.of("action", "same-restored-jvm-reset-qualified",
                            "oldScope", old.scope(), "newScope", next.scope(),
                            "oldAccount", oldAccount, "newAccount", object(newAccountRecord.get("account")),
                            "currentLogResponse", resetLogs, "ownedPid", observer.server().pid()));
                    control.stop(pipeline, false);
                    var terminal = observer.shutdownAndFinish();
                    report.addFork(terminal.evidence());
                    assertThat(terminal.unverified()).isEmpty();
                    assertThat(terminal.decodedAndAuthorityBound()).isTrue();
                    assertThat(terminal.ownedVmDeath()).isTrue();
                    assertThat(terminal.ownedVmDisconnected()).isTrue();
                }
            }
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            assertThat(inputHashes(root)).isEqualTo(inputs);
            for (var input : connectors.entrySet()) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(input.getKey())))
                        .isEqualTo(input.getValue());
            }
            report.completeDiagnostic(Map.of("correctness", "POSITIVE_FAILURE_ACCOUNT_RESET_QUALIFIED",
                    "performanceAcceptanceEligible", false, "firstCurrentWindowQualified", firstCurrentWindow,
                    "unverified", List.of(
                            "RECREATE_FAILURE_ACCOUNT_CLEANUP", "CLUSTER_EMITTING_MEMBER",
                            "OTHER_TELEMETRY_LIFECYCLE_SURFACES", "ALL_TELEMETRY_SURFACE_IDENTITIES")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); } catch (RuntimeException reporting) { failure.addSuppressed(reporting); }
            throw failure;
        }
    }

    private static Duration remainingWindow(long deadline) {
        long left = deadline - System.nanoTime();
        assertThat(left).as("the admission window and first current frame share the original reset deadline").isPositive();
        return Duration.ofNanos(left);
    }
    private static void assertPositiveOldLatest(ObservationStore.Stored value,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt old, Map<String, Object> account) {
        assertThat(value.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(old.incarnation());
        assertThat(value.scope().orElseThrow().executionGeneration()).isEqualTo(old.generation());
        assertThat(value.observation().state()).isEqualTo(PipelineState.STOPPED);
        assertThat(object(account.get("scope"))).isEqualTo(old.scope());
        assertThat(((Number) account.get("token")).longValue()).isPositive();
        long count = ((Number) object(account.get("counts")).get(CODE)).longValue();
        assertThat(count).isPositive();
        assertThat(value.observation().facts().stream().filter(fact -> ERRORS.equals(fact.name())
                && fact.type() == io.tapstate.core.lifecycle.MetricType.COUNTER && "{error}".equals(fact.unit()))
                .flatMap(fact -> fact.points().stream()).anyMatch(point -> CODE.equals(point.attributes().get("code"))
                        && Long.valueOf(count).equals(point.value()) && point.startTime() != null
                        && point.startTime().toString().equals(account.get("countingSince"))))
                .as("the retained physical predecessor has its actual positive error count and start").isTrue();
    }
    private static void qualifyFirstCurrentWindow(NativeTelemetryIdentityJdiSession observer,
            NativeTelemetryIdentityJdiSession.AdmissionReturnPark held, Map<String, Object> admission,
            BenchmarkLiveReport report, MongoDatabase database, MongoObservationStore latest,
            MongoDesiredStore desired, HttpClient http, ControlPlane control, String pipeline,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt old, ObservationStore.Stored oldLatest,
            Map<String, Object> oldAccount, Document artifactBefore, long deadline) throws Exception {
        observer.check(); assertThat(held.held()).isTrue(); remainingWindow(deadline);
        Map<String, Object> fence = object(admission.get("fence"));
        long generation = ((Number) fence.get("executionGeneration")).longValue();
        assertThat(generation).isEqualTo(Math.addExact(old.generation(), 1));
        assertThat(admission.get("allowed")).isEqualTo(true);
        assertThat(admission.get("admissionStatus")).isEqualTo("STANDALONE");
        List<Document> actualRows = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("clusterId", old.clusterId()).append("resourceType", "PIPELINE_ACTUATION")
                        .append("resourceId", pipeline)).limit(2).into(new ArrayList<>());
        assertThat(actualRows).as("one exact scoped standalone coordination row").hasSize(1);
        Document authority = actualRows.getFirst();
        Document id = authority.get("_id", Document.class);
        assertThat(id).isNotNull();
        assertThat(((Number) authority.get("executionGeneration")).longValue()).isEqualTo(generation);
        assertThat(JsonWriter.write(authority.get("_id"))).isEqualTo(old.coordinationId());
        assertThat(id.getString("clusterId")).isEqualTo(old.clusterId());
        assertThat(id.getString("resourceId")).isEqualTo(pipeline);
        assertThat(id.getString("resourceType")).isEqualTo("PIPELINE_ACTUATION");
        for (String lease : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil")) {
            assertThat(authority.containsKey(lease)).as("standalone admission creates no %s", lease).isFalse();
        }
        assertThat(database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", pipeline)).first())
                .isEqualTo(artifactBefore);
        assertThat(artifactBefore.getString("pipelineIncarnationId")).isEqualTo(old.incarnation());
        assertThat(artifactBefore.getString("contentHash")).isNotBlank();
        var intent = desired.read(pipeline).orElseThrow();
        assertThat(intent.targetState()).isEqualTo(PipelineState.RUNNING);
        var physicalBefore = latest.readStored(pipeline).orElseThrow();
        assertPositiveOldLatest(physicalBefore, old, oldAccount);
        Map<String, Object> replies = new LinkedHashMap<>();
        for (String face : List.of("status", "metrics", "snapshot", "explain")) {
            observer.check(); assertThat(held.held()).as("each negative read happens before submit is released").isTrue();
            long left = Math.min(remainingWindow(deadline).toNanos(), held.remainingNanos());
            var request = HttpRequest.newBuilder(observer.server().baseUrl().resolve("/api/pipelines/" + pipeline + "/" + face))
                    .timeout(Duration.ofNanos(Math.min(left, Duration.ofSeconds(20).toNanos())))
                    .header("Authorization", "Bearer " + control.credential()).GET().build();
            var reply = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(reply.body().getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_BYTES);
            assertThat(reply.statusCode()).isEqualTo(404);
            Map<String, Object> body = object(JsonReader.parse(reply.body()));
            assertThat(body.get("code")).isEqualTo(MonitorError.NO_OBSERVATION.code());
            assertThat(object(body.get("params"))).isEqualTo(Map.of("pipeline", pipeline));
            replies.put(face, Map.of("httpStatus", reply.statusCode(), "body", body));
            observer.check(); assertThat(held.held()).isTrue();
        }
        assertThat(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(new Document("_id", id)).first())
                .as("exact standalone authority remains stable across all four reads").isEqualTo(authority);
        assertThat(database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", pipeline)).first())
                .isEqualTo(artifactBefore);
        assertThat(desired.read(pipeline)).contains(intent);
        var physicalAfter = latest.readStored(pipeline).orElseThrow();
        assertPositiveOldLatest(physicalAfter, old, oldAccount);
        observer.check(); assertThat(held.held()).isTrue(); remainingWindow(deadline);
        Map<String, Object> evidence = Map.of("action", "actual-admitted-pre-submit-first-current-window", "admission", admission,
                "oldScope", old.scope(), "newDurableGeneration", generation, "coordination", authority.toJson(),
                "artifact", artifactBefore.toJson(), "oldPositiveReads", Map.of(
                        "initialObservedAt", oldLatest.observation().observedAt().toString(),
                        "beforeObservedAt", physicalBefore.observation().observedAt().toString(),
                        "afterObservedAt", physicalAfter.observation().observedAt().toString(),
                        "accountToken", oldAccount.get("token"), "counts", oldAccount.get("counts"),
                        "countingSince", oldAccount.get("countingSince")),
                "replies", Map.copyOf(replies), "newJobAvailable", false, "pendingReconcileClaimed", false);
        assertThat(JsonWriter.write(evidence).getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_BYTES);
        report.addFork(evidence);
    }

    private record Stage(List<Map<String, Object>> records, String body) { }

    private static void issueResetEvent(BenchmarkForkEnvironment fork, MongoClient mongo,
            BenchmarkForkEnvironment.PhaseResult warmup, BenchmarkLiveReport report,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt owner) throws Exception {
        assertThat(warmup.targets()).hasSize(1);
        var expected = warmup.targets().getFirst().expectation();
        assertThat(expected.location()).isEqualTo(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO);
        assertThat(expected.projection()).isEqualTo(BenchmarkWorkloadDefinitions.Projection.COPY);
        var target = mongo.getDatabase(new ConnectionString(fork.externalTargetUri()).getDatabase())
                .getCollection(expected.table());
        Document projection = new Document("_id", 0).append("id", 1).append("amount", 1)
                .append("payload", 1).append("marker", 1);
        Document before = target.find(new Document("id", 1L)).projection(projection).first();
        assertThat(before).as("the real warmup target contains the chosen source row").isNotNull();
        assertThat(before.get("amount")).isInstanceOf(Number.class);
        long previous = ((Number) before.get("amount")).longValue();
        long changed = Math.addExact(previous, 1);
        String sql = "UPDATE " + expected.table() + " SET amount = " + changed
                + " WHERE id = 1 AND amount = " + previous;
        fork.executeOneSourceUpdate(sql);
        report.addFork(Map.of("action", "real-reset-source-update-issued", "sql", sql, "rowId", 1,
                "targetBefore", before, "expectedAmount", changed, "scope", owner.scope()));
        Document delivered = Await.answered("the one post-START MySQL change reaches its real Mongo row", WAIT, () ->
                Optional.ofNullable(target.find(new Document("id", 1L)).projection(projection).first())
                        .filter(row -> row.get("amount") instanceof Number amount && amount.longValue() == changed));
        assertThat(delivered.get("id")).isEqualTo(before.get("id"));
        assertThat(delivered.get("payload")).isEqualTo(before.get("payload"));
        assertThat(delivered.get("marker")).isEqualTo(before.get("marker"));
        report.addFork(Map.of("action", "real-reset-source-update-delivered", "rowId", 1,
                "targetAfter", delivered, "scope", owner.scope()));
    }

    private static Stage collect(NativeTelemetryIdentityJdiSession observer, BenchmarkLiveReport report,
            HttpClient http, URI scrape, String phase, Predicate<Stage> ready) throws Exception {
        return collect(observer, report, http, scrape, phase, NativeTelemetryIdentityJdiSession.BindingProfile.FULL, ready);
    }

    private static Stage collect(NativeTelemetryIdentityJdiSession observer, BenchmarkLiveReport report,
            HttpClient http, URI scrape, String phase, NativeTelemetryIdentityJdiSession.BindingProfile profile,
            Predicate<Stage> ready) throws Exception {
        List<Map<String, Object>> records = new ArrayList<>();
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() - deadline < 0) {
            observer.check();
            long left = deadline - System.nanoTime();
            assertThat(left).isPositive();
            String body = response(http, HttpRequest.newBuilder(scrape).timeout(Duration.ofNanos(
                    Math.min(left, Duration.ofSeconds(20).toNanos()))).GET().build());
            assertThat(body).doesNotContain("pipelineIncarnationId", "executionGeneration",
                    "pipeline_incarnation_id", "pipeline_execution_generation");
            var boundary = observer.boundary(phase, profile);
            report.addFork(boundary.evidence());
            assertThat(boundary.unverified()).as("decoder gaps cannot qualify a zero or empty account").isEmpty();
            records.addAll(boundary.records());
            assertThat(records.size()).isLessThanOrEqualTo(MAX_RECORDS);
            Stage stage = new Stage(List.copyOf(records), body);
            if (ready.test(stage)) {
                assertThat(boundary.decodedAndAuthorityBound()).isTrue();
                report.addFork(Map.of("action", "actual-fresh-prometheus-response", "phase", phase,
                        "body", body, "bodySha256", digest(body.getBytes(StandardCharsets.UTF_8))));
                return stage;
            }
            java.util.concurrent.TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new AssertionError("UNVERIFIED: the real positive telemetry stage did not qualify: " + phase);
    }

    private static NativeTelemetryIdentityJdiSession.AuthorityReceipt authority(MongoDatabase database,
            String pipeline, ObservationStore.Stored stored) {
        var scope = stored.scope().orElseThrow();
        Document artifact = database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", pipeline)).first();
        List<Document> claims = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(
                new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                .limit(2).into(new ArrayList<>());
        assertThat(artifact).isNotNull();
        assertThat(claims).hasSize(1);
        var claim = claims.getFirst();
        assertThat(artifact.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId());
        assertThat(claim.get("executionGeneration") instanceof Long
                || claim.get("executionGeneration") instanceof Integer).isTrue();
        long generation = ((Number) claim.get("executionGeneration")).longValue();
        assertThat(generation).isPositive().isEqualTo(scope.executionGeneration());
        return new NativeTelemetryIdentityJdiSession.AuthorityReceipt(pipeline, claim.getString("clusterId"),
                scope.pipelineIncarnationId(), generation, JsonWriter.write(claim.get("_id")), Instant.now().toString());
    }

    private static Optional<Map<String, Object>> positiveAccount(List<Map<String, Object>> records,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt owner) {
        return records.stream().filter(record -> good(record, "PREPARE")
                && Boolean.TRUE.equals(record.get("nonNullFailure"))).filter(record -> {
                    Map<String, Object> account = object(record.get("account"));
                    return owner.scope().equals(account.get("scope")) && CODE.equals(account.get("currentFailureCode"))
                            && account.get("token") instanceof Number token && token.longValue() > 0
                            && account.get("countingSince") instanceof String
                            && object(account.get("counts")).get(CODE) instanceof Number count && count.longValue() > 0
                            && namedCode(account.get("named"));
                }).findFirst();
    }
    private static Optional<Map<String, Object>> resetAccount(List<Map<String, Object>> records,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt owner, Map<String, Object> predecessor) {
        Map<String, Object> old = object(predecessor.get("account"));
        return records.stream().filter(record -> good(record, "PREPARE")
                && Objects.equals(record.get("receiver"), predecessor.get("receiver"))).filter(record -> {
                    Map<String, Object> account = object(record.get("account"));
                    return owner.scope().equals(account.get("scope")) && account.get("currentFailureCode") == null
                            && object(account.get("counts")).isEmpty() && !namedCode(account.get("named"))
                            && account.get("token") instanceof Number token && token.longValue() > 0
                            && !Objects.equals(account.get("token"), old.get("token"))
                            && account.get("countingSince") instanceof String since
                            && Instant.parse(since).isAfter(Instant.parse((String) old.get("countingSince")));
                }).findFirst();
    }
    private static Optional<Map<String, Object>> retainedAccount(List<Map<String, Object>> records,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt owner, Map<String, Object> predecessor) {
        Map<String, Object> old = object(predecessor.get("account"));
        return records.stream().filter(record -> good(record, "PREPARE")
                && Objects.equals(record.get("receiver"), predecessor.get("receiver"))).filter(record -> {
                    Map<String, Object> account = object(record.get("account"));
                    return owner.scope().equals(account.get("scope"))
                            && Objects.equals(account.get("token"), old.get("token"))
                            && Objects.equals(account.get("countingSince"), old.get("countingSince"))
                            && Objects.equals(account.get("counts"), old.get("counts"))
                            && namedCode(account.get("named"));
                }).findFirst();
    }
    private static Optional<Map<String, Object>> scopedLog(List<Map<String, Object>> records,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt owner) {
        return records.stream().filter(record -> good(record, "LOG") && owner.scope().equals(record.get("scope"))
                && "WARN".equals(record.get("level")) && record.get("message") instanceof String message
                && message.contains(CODE)).findFirst();
    }
    private static boolean folderCleared(List<Map<String, Object>> records) {
        return records.stream().anyMatch(record -> good(record, "FOLDER_FORGET")
                && namedCode(record.get("before")) && !namedCode(record.get("after")));
    }
    private static boolean exporterCleared(List<Map<String, Object>> records, Map<String, Object> predecessor) {
        return records.stream().anyMatch(record -> good(record, "EXPORT_FORGET")
                && Objects.equals(record.get("receiver"), predecessor.get("receiver"))
                && namedCode(record.get("beforeNamed")) && !namedCode(record.get("afterNamed"))
                && "ABSENT".equals(record.get("cachedAfter")));
    }
    private static boolean namedCode(Object value) {
        Object names = object(value).get(ERRORS);
        return names instanceof List<?> list && list.stream().anyMatch(name -> CODE.equals(name)
                || name instanceof Map<?, ?> attributes && CODE.equals(attributes.get("code")));
    }
    private static boolean good(Map<String, Object> record, String target) {
        return target.equals(record.get("target")) && Boolean.TRUE.equals(record.get("normalReturn"))
                && !record.containsKey("decoderStatus");
    }
    private static boolean accountMatchesPoint(Map<String, Object> account, Map<String, Object> point) {
        long count = ((Number) object(account.get("counts")).get(CODE)).longValue();
        long start = nanos((String) account.get("countingSince"));
        return CODE.equals(object(point.get("attributes")).get("code"))
                && point.get("value") instanceof Number value && value.longValue() == count
                && point.get("startEpochNanos") instanceof Number since && since.longValue() == start;
    }

    private record PointMatch(Map<String, Object> produced, Map<String, Object> frame,
            Map<String, Object> point, String line) { }

    private static Optional<PointMatch> matchingPoint(List<Map<String, Object>> records, String body,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt owner, String instrument, boolean error,
            PipelineState state) {
        for (Map<String, Object> produced : records) {
            if (!good(produced, "PRODUCE")) { continue; }
            for (Object rawFrame : (List<?>) produced.get("includedFrames")) {
                Map<String, Object> frame = object(rawFrame);
                if (!owner.scope().equals(frame.get("scope")) || !state.name().equals(frame.get("state"))
                        || frame.containsKey("decoderStatus")
                        || List.of("OFFER", "VISIBLE").stream().anyMatch(target -> records.stream().noneMatch(record ->
                                good(record, target) && Objects.equals(record.get("receiver"), produced.get("receiver"))
                                        && Boolean.TRUE.equals(record.get(target.equals("OFFER") ? "accepted" : "included"))
                                        && List.of("scope", "state", "observedAt", "facts").stream().allMatch(key ->
                                                Objects.equals(record.get(key), frame.get(key)))))) { continue; }
                for (Object rawMetric : (List<?>) produced.get("metrics")) {
                    Map<String, Object> metric = object(rawMetric);
                    if (!instrument.equals(metric.get("name")) || !"LONG_SUM".equals(metric.get("type"))
                            || !Boolean.TRUE.equals(metric.get("monotonic"))
                            || !"CUMULATIVE".equals(metric.get("temporality"))) { continue; }
                    for (Object rawPoint : (List<?>) metric.get("points")) {
                        Map<String, Object> point = object(rawPoint), attributes = object(point.get("attributes"));
                        if (!owner.pipelineId().equals(attributes.get("tapstate.pipeline.id"))
                                || !(point.get("value") instanceof Number number) || number.longValue() <= 0
                                || !(error ? CODE.equals(attributes.get("code")) : "out".equals(attributes.get("direction")))
                                || !factMatches(frame, metric, point)) { continue; }
                        String name = instrument.replace('.', '_') + "_total";
                        for (String line : body.lines().filter(row -> row.startsWith(name + "{")).toList()) {
                            if (attributes.entrySet().stream().allMatch(attribute -> label(line, name,
                                    attribute.getKey().replace('.', '_'), attribute.getValue().toString()))
                                    && new java.math.BigDecimal(line.substring(line.indexOf('}') + 1).trim().split("\\s+")[0])
                                    .compareTo(java.math.BigDecimal.valueOf(number.longValue())) == 0) {
                                return Optional.of(new PointMatch(produced, frame, point, line));
                            }
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }
    private static boolean factMatches(Map<String, Object> frame, Map<String, Object> metric, Map<String, Object> actual) {
        for (Object rawFact : (List<?>) frame.get("facts")) {
            Map<String, Object> fact = object(rawFact);
            if (!metric.get("name").equals(fact.get("name")) || !metric.get("unit").equals(fact.get("unit"))
                    || !"COUNTER".equals(fact.get("type"))) { continue; }
            for (Object rawPoint : (List<?>) fact.get("points")) {
                Map<String, Object> point = object(rawPoint);
                if (Objects.equals(point.get("attributes"), actual.get("attributes"))
                        && Objects.equals(point.get("value"), actual.get("value"))
                        && point.get("startTime") instanceof String start && point.get("observedAt") instanceof String at
                        && actual.get("startEpochNanos") instanceof Number actualStart
                        && actual.get("epochNanos") instanceof Number actualAt
                        && nanos(start) == actualStart.longValue() && nanos(at) == actualAt.longValue()) { return true; }
            }
        }
        return false;
    }
    private static boolean hasErrorSeries(String body, String pipeline) {
        return body.lines().filter(line -> line.startsWith("tapstate_pipeline_errors_total{")).anyMatch(line ->
                label(line, "tapstate_pipeline_errors_total", "tapstate_pipeline_id", pipeline));
    }
    private static boolean label(String line, String instrument, String name, String value) {
        String escaped = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
        String label = name + "=\"" + escaped + "\"";
        return line.startsWith(instrument + "{" + label) || line.contains("," + label);
    }
    private static long nanos(String text) {
        Instant value = Instant.parse(text);
        return Math.addExact(Math.multiplyExact(value.getEpochSecond(), 1_000_000_000L), value.getNano());
    }
    private static Map<String, Object> logs(HttpClient http, ControlPlane control, URI base, String pipeline) throws Exception {
        return get(http, control, base.resolve("/api/pipelines/" + pipeline + "/logs?scope=current"));
    }
    private static List<Map<String, Object>> lines(Map<String, Object> logs) {
        assertThat(logs.get("lines")).isInstanceOf(List.class);
        return ((List<?>) logs.get("lines")).stream().map(NativeFailureAccountResetIT::object).toList();
    }
    private static boolean sameLog(Map<String, Object> line, Map<String, Object> record) {
        return line.get("timestampMillis") instanceof Number time && record.get("timestampMillis") instanceof Number captured
                && time.longValue() == captured.longValue() && Objects.equals(line.get("level"), record.get("level"))
                && Objects.equals(line.get("message"), record.get("message"));
    }
    private static Map<String, Object> get(HttpClient http, ControlPlane control, URI uri) throws Exception {
        return object(JsonReader.parse(response(http, HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + control.credential()).GET().build())));
    }
    private static String response(HttpClient http, HttpRequest request) throws Exception {
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body().getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_BYTES);
        return response.body();
    }
    private static Map<String, Object> object(Object value) {
        assertThat(value).isInstanceOf(Map.class);
        Map<String, Object> result = new LinkedHashMap<>();
        ((Map<?, ?>) value).forEach((key, item) -> { assertThat(key).isInstanceOf(String.class); result.put((String) key, item); });
        return result;
    }
    private static Map<String, Object> inputHashes(Path root) throws Exception {
        Map<String, Object> inputs = new LinkedHashMap<>();
        for (Class<?> type : List.of(NativeFailureAccountResetIT.class,
                NativeTelemetryIdentityJdiSession.class, NativeTelemetryMirror.class)) {
            inputs.put(type.getSimpleName() + "Source", PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                    "e2e/src/test/java/io/tapstate/e2e/" + type.getSimpleName() + ".java")));
            try (InputStream bytes = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                assertThat(bytes).isNotNull();
                byte[] data = bytes.readNBytes(MAX_BYTES + 1);
                assertThat(data.length).isLessThanOrEqualTo(MAX_BYTES);
                inputs.put(type.getSimpleName() + "ExecutingClassSha256", digest(data));
            }
        }
        if (Boolean.getBoolean(PREFIX + "first-current-window")) {
            try (InputStream bytes = NativeTelemetryIdentityJdiSession.class.getResourceAsStream(
                    "NativeTelemetryIdentityJdiSession$AdmissionReturnPark.class")) {
                assertThat(bytes).isNotNull(); byte[] data = bytes.readNBytes(MAX_BYTES + 1);
                assertThat(data.length).isLessThanOrEqualTo(MAX_BYTES);
                inputs.put("AdmissionReturnParkExecutingClassSha256", digest(data));
            }
        }
        inputs.put("pausedProcessLossProtocolSource", PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                "e2e/src/test/java/io/tapstate/e2e/PipelineObservabilityLiveIT.java")));
        return Map.copyOf(inputs);
    }
    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new IllegalArgumentException("witness requires " + PREFIX + name); }
        return value;
    }
}
