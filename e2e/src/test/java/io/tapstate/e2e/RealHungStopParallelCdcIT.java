package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** A native Jet teardown wait must leave another pipeline's CDC and observation moving. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.hung-stop.jar", matches = ".+")
class RealHungStopParallelCdcIT {
    private static final String PREFIX = "tapstate.e2e.hung-stop.";
    private static final String A = "hung_stop_a";
    private static final String B = "parallel_cdc_b";
    private static final Duration SETUP_WAIT = Duration.ofMinutes(2);
    private static final Duration NATIVE_WAIT = Duration.ofSeconds(45);

    private record Progress(long sampledAtNanos, long targetAmount, long recordsOut, Instant observedAt,
            PipelineState actual, PipelineState reported) {
        Map<String, Object> evidence() {
            return Map.of("sampledAtNanos", sampledAtNanos, "targetAmount", targetAmount, "recordsOut", recordsOut,
                    "observedAt", observedAt.toString(), "actual", actual.name(),
                    "reported", reported.name());
        }
    }
    private record StopReading(long sampledAtNanos, PipelineState desired, PipelineState actual,
            PipelineState reported, String pendingReason) {
        Map<String, Object> evidence() {
            return Map.of("sampledAtNanos", sampledAtNanos,
                    "desired", desired.name(), "actual", actual.name(),
                    "reported", reported.name(), "pendingReason", pendingReason);
        }
    }

    @BeforeAll
    static void requireRealConnectors() {
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aFullNativeStopWaitLeavesAnIndependentPipelineMoving() throws Exception {
        Path jar = Path.of(required("jar")).toRealPath();
        assertThat(Files.isRegularFile(jar)).as("the selected immutable application artifact").isTrue();
        Path output = Path.of(required("output"));
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        Map<String, Object> connectors = Map.of(
                "mysql", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql")),
                "mongodb", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")));
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.begin(Map.of("purpose", "REAL_HUNG_STOP_PARALLEL_CDC",
                        "application", application, "connectors", connectors,
                        "nativeWaitBudgetSeconds", 30),
                Map.of("kind", "correctness-only", "pipelineCount", 2), List.of());
        try {
            String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            Map<String, Object> sourceA = SharedMySql.settings("hung_stop_a_" + suffix);
            Map<String, Object> sourceB = SharedMySql.settings("hung_stop_b_" + suffix);
            seed(sourceA);
            seed(sourceB);
            String storeUri = SharedMongo.replicaSetUrl("hung_stop_" + suffix + "_store");
            String targetA = SharedMongo.replicaSetUrl("hung_stop_" + suffix + "_target_a");
            String targetB = SharedMongo.replicaSetUrl("hung_stop_" + suffix + "_target_b");
            String operatorDatabase = "hung_stop_" + suffix + "_operator";
            try (MongoClient store = MongoClients.create(storeUri);
                    MongoClient target = MongoClients.create(targetB);
                    HungStopJdiSession observed = HungStopJdiSession.start(storeUri, operatorDatabase, jar, A)) {
                MongoDatabase coordination = store.getDatabase(new ConnectionString(storeUri).getDatabase());
                MongoDesiredStore desired = new MongoDesiredStore(
                        coordination.getCollection(MongoStorePort.PIPELINE_DESIRED));
                MongoStateStore actual = new MongoStateStore(
                        coordination.getCollection(MongoStorePort.PIPELINE_STATE));
                ControlPlane control = new ControlPlane(observed.server().baseUrl());
                control.bootstrapAndLogin("hung-stop", "hung-stop-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(resources(sourceA, sourceB, targetA, targetB));
                control.discoverSchema("source_a", "mysql", sourceA);
                control.discoverSchema("source_b", "mysql", sourceB);
                control.lifecycle(A, io.tapstate.core.lifecycle.LifecycleVerb.START);
                control.lifecycle(B, io.tapstate.core.lifecycle.LifecycleVerb.START);
                observed.awaitBindings(SETUP_WAIT);
                awaitRunning(control, A);
                awaitRunning(control, B);
                MongoDatabase targetDatabase = target.getDatabase(new ConnectionString(targetB).getDatabase());
                MongoDatabase aTargetDatabase = store.getDatabase(new ConnectionString(targetA).getDatabase());
                Await.until("both real target rows to arrive", SETUP_WAIT,
                        () -> targetAmount(targetDatabase) == 0 && targetAmount(aTargetDatabase) == 0,
                        () -> "A target=" + targetAmount(aTargetDatabase)
                                + ", B target=" + targetAmount(targetDatabase));
                updateOne(sourceA);
                Await.until("A to deliver real CDC before its source close is armed", SETUP_WAIT,
                        () -> targetAmount(aTargetDatabase) == 1,
                        () -> "A target amount=" + targetAmount(aTargetDatabase));
                assertThat(actual.read(A).map(value -> StateJson.parse(value.stateJson())))
                        .contains(PipelineState.RUNNING);
                Progress baseline = baseline(control, actual, targetDatabase);
                observed.arm();

                control.stop(A, false);
                HungStopJdiSession.WaitEntry nativeWait = observed.awaitWaitEntry(SETUP_WAIT);
                HungStopJdiSession.HeldClose held = observed.awaitHeldClose(SETUP_WAIT);
                assertThat(held.eventThreadOnly()).as("only A's close thread is suspended").isTrue();
                List<StopReading> duringStop = new ArrayList<>();
                List<Progress> bReadings = new ArrayList<>();
                duringStop.add(stopReading(control, observed.server().baseUrl(), desired, actual));
                report.addFork(Map.of("action", "native-source-close-held",
                        "heldTaskletClose", held.evidence(),
                        "nativeWaitEntry", nativeWait.evidence(),
                        "aAtHold", duringStop.getFirst().evidence(),
                        "bBaseline", baseline.evidence()));
                Throwable bFailure = null;
                Map<String, Object> bAtFailure = Map.of("availability", "NOT_TRIGGERED");
                Map<String, Object> threadsAtFailure = Map.of("availability", "NOT_TRIGGERED");
                Duration[] stepWaits = {Duration.ofSeconds(12), Duration.ofSeconds(8), Duration.ofSeconds(5)};
                for (int step = 0; step < stepWaits.length; step++) {
                    if (step > 0) {
                        awaitOffset(nativeWait.atNanos(), Duration.ofSeconds(step == 1 ? 15 : 25));
                        duringStop.add(stopReading(control, observed.server().baseUrl(), desired, actual));
                    }
                    try {
                        Progress previous = bReadings.isEmpty() ? baseline : bReadings.getLast();
                        bReadings.add(advanceB(control, actual, sourceB, targetDatabase,
                                previous, stepWaits[step]));
                    } catch (RuntimeException | AssertionError stalled) {
                        bFailure = stalled;
                        bAtFailure = bDiagnostic(control, actual, targetDatabase);
                        threadsAtFailure = observed.threadSnapshot();
                        report.addFork(Map.of("action", "parallel-cdc-window-diagnostic",
                                "step", step, "bAtFailure", bAtFailure,
                                "threadsAtFailure", threadsAtFailure,
                                "failureType", stalled.getClass().getName()));
                        break;
                    }
                }
                HungStopJdiSession.WaitExit nativeExit = observed.awaitFullBudget(NATIVE_WAIT);
                report.addFork(Map.of("action", "native-wait-returned",
                        "nativeWaitExit", nativeExit.evidence(),
                        "heldTaskletClose", held.evidence()));
                Map<String, Object> bAfterNativeWait = bDiagnostic(control, actual, targetDatabase);
                PipelineState desiredAfterBudget = desired.read(A).orElseThrow().targetState();
                PipelineState actualAfterBudget = actual.read(A)
                        .map(value -> StateJson.parse(value.stateJson())).orElseThrow();
                PipelineState reportedAfterBudget = control.state(A).orElseThrow();
                Map<String, Object> afterBudgetWhileCloseHeld = Map.of(
                        "desired", desiredAfterBudget.name(), "actual", actualAfterBudget.name(),
                        "reported", reportedAfterBudget.name());
                Map<String, Object> evidence = new LinkedHashMap<>();
                evidence.put("action", "native-thirty-second-wait-completed");
                evidence.put("applicationSha256", application.get("sha256"));
                evidence.put("serverPid", observed.server().pid());
                evidence.put("serverOutput", observed.server().output().toString());
                evidence.put("heldTaskletClose", held.evidence());
                evidence.put("nativeWaitEntry", nativeWait.evidence());
                evidence.put("nativeWaitExit", nativeExit.evidence());
                evidence.put("bBaseline", baseline.evidence());
                evidence.put("bProgress", bReadings.stream().map(Progress::evidence).toList());
                evidence.put("bAtFailure", bAtFailure);
                evidence.put("threadsAtFailure", threadsAtFailure);
                evidence.put("bAfterNativeWait", bAfterNativeWait);
                evidence.put("bFailureType", bFailure == null ? "NONE" : bFailure.getClass().getName());
                evidence.put("aWhileWaiting", duringStop.stream().map(StopReading::evidence).toList());
                evidence.put("aAfterBudgetWhileCloseHeld", afterBudgetWhileCloseHeld);
                report.addFork(evidence);
                assertThat(nativeExit.elapsedNanos()).isGreaterThanOrEqualTo(Duration.ofSeconds(30).toNanos());
                assertThat(nativeExit.actualReturnValue()).isFalse();
                AssertionError bAssertion = null;
                try {
                    if (bFailure != null) { throw new AssertionError("B did not advance inside the native stop window", bFailure); }
                    assertThat(bReadings).hasSize(3);
                    assertThat(bReadings.getFirst().sampledAtNanos())
                            .as("B first advanced before the middle of A's actual wait")
                            .isLessThan(nativeWait.atNanos() + Duration.ofSeconds(15).toNanos());
                    assertThat(bReadings.get(1).sampledAtNanos())
                            .as("B advanced after the midpoint of A's actual wait")
                            .isGreaterThanOrEqualTo(nativeWait.atNanos() + Duration.ofSeconds(15).toNanos());
                    assertThat(bReadings.getLast().sampledAtNanos())
                            .as("B advanced late but before A's actual wait returned")
                            .isBetween(nativeWait.atNanos() + Duration.ofSeconds(25).toNanos(),
                                    nativeWait.atNanos() + Duration.ofSeconds(30).toNanos() - 1);
                    for (Progress progress : bReadings) {
                        assertThat(progress.actual()).as("B's actual state during A's native wait")
                                .isEqualTo(PipelineState.RUNNING);
                        assertThat(progress.reported()).as("B's reported state during A's native wait")
                                .isEqualTo(PipelineState.RUNNING);
                    }
                } catch (AssertionError unmet) { bAssertion = unmet; }
                AssertionError aAssertion = null;
                try {
                    for (StopReading reading : duringStop) {
                        assertThat(reading.sampledAtNanos())
                                .isLessThan(nativeWait.atNanos() + Duration.ofSeconds(30).toNanos());
                        assertThat(reading.desired()).isEqualTo(PipelineState.STOPPED);
                        assertThat(reading.actual()).as("A cannot appear stopped before its Jet close finishes")
                                .isEqualTo(PipelineState.RUNNING);
                        assertThat(reading.reported()).as("A's latest observation cannot invent STOPPED")
                                .isEqualTo(PipelineState.RUNNING);
                        assertThat(reading.pendingReason()).isEqualTo("STOP_PENDING");
                    }
                    assertThat(desiredAfterBudget).isEqualTo(PipelineState.STOPPED);
                    assertThat(actualAfterBudget)
                            .as("A is still closing after the native wait exhausted its budget")
                            .isEqualTo(PipelineState.RUNNING);
                    assertThat(reportedAfterBudget).isEqualTo(PipelineState.RUNNING);
                } catch (AssertionError unmet) { aAssertion = unmet; }
                if (bAssertion != null) {
                    if (aAssertion != null) { bAssertion.addSuppressed(aAssertion); }
                    throw bAssertion;
                }
                if (aAssertion != null) { throw aAssertion; }
                if (!nativeWait.execution().isEmpty()) {
                    Document pending = coordination.getCollection(MongoStorePort.PIPELINE_STATE)
                            .find(new Document("_id", A)).first();
                    assertThat(pending).as("A keeps its durable stop while the native close is held").isNotNull();
                    Document marker = pending.get("stopReservation", Document.class);
                    assertThat(marker).isNotNull();
                    Document subject = marker.get("subject", Document.class);
                    assertThat(subject.getString("kind")).isEqualTo("EXISTING_JOB");
                    Document oldJob = subject.get("oldJob", Document.class);
                    Map<String, Object> pinned = Map.of("jobId", ((Number) oldJob.get("jobId")).longValue(),
                            "clusterId", oldJob.getString("clusterId"), "bootId", oldJob.getString("bootId"),
                            "pipelineIncarnationId", subject.getString("pipelineIncarnationId"),
                            "executionGeneration", ((Number) subject.get("executionGeneration")).longValue());
                    assertThat(nativeWait.execution()).as("the real wait uses exactly the durable old native job")
                            .isEqualTo(pinned);
                    report.addFork(Map.of("action", "durable-stop-still-pending", "execution", pinned,
                            "sourceEpoch", marker.get("sourceEpoch"), "reservedEpoch", marker.get("reservedEpoch")));
                }
                observed.releaseClose();
                Await.until("A to finish its actual stop after releasing the native close",
                        SETUP_WAIT, () -> actual.read(A).map(value -> StateJson.parse(value.stateJson()))
                                .filter(PipelineState.STOPPED::equals).isPresent()
                                && control.state(A).filter(PipelineState.STOPPED::equals).isPresent(),
                        () -> "actual=" + actual.read(A).map(value -> StateJson.parse(value.stateJson()))
                                + ", reported=" + control.state(A));
                Document completed = coordination.getCollection(MongoStorePort.PIPELINE_STATE)
                        .find(new Document("_id", A)).first();
                assertThat(completed.containsKey("stopReservation"))
                        .as("the completed stop clears its durable reservation").isFalse();
                Progress afterStop = advanceB(control, actual, sourceB, targetDatabase,
                        bReadings.getLast(), Duration.ofSeconds(12));
                report.addFork(Map.of("action", "native-close-released-and-stop-completed",
                        "actual", PipelineState.STOPPED.name(), "reported", PipelineState.STOPPED.name(),
                        "bProgress", afterStop.evidence()));
            }
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql")))
                    .isEqualTo(connectors.get("mysql"));
            assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")))
                    .isEqualTo(connectors.get("mongodb"));
            report.completeDiagnostic(Map.of("correctness", "NATIVE_STOP_WAIT_DID_NOT_FREEZE_PARALLEL_CDC",
                    "performanceAcceptanceEligible", false,
                    "unverified", List.of("OTHER_LIFECYCLE_CAPACITY_WINDOWS", "PERFORMANCE_ACCEPTANCE")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); }
            catch (RuntimeException writeFailure) { failure.addSuppressed(writeFailure); }
            throw failure;
        }
    }

    private static Progress baseline(ControlPlane control, MongoStateStore actual, MongoDatabase target) {
        Await.until("B target, counter and observation baseline", SETUP_WAIT,
                () -> control.recordsOut(B).filter(count -> count > 0).isPresent()
                        && control.state(B).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> "state=" + control.state(B) + ", recordsOut=" + control.recordsOut(B));
        return progress(control, actual, target);
    }

    private static Progress advanceB(ControlPlane control, MongoStateStore actual,
            Map<String, Object> source, MongoDatabase target, Progress previous,
            Duration wait) throws Exception {
        long expected = Math.addExact(previous.targetAmount(), 1);
        updateOne(source);
        Await.until("B's real CDC, records-out and observation to advance while A waits", wait,
                () -> targetAmount(target) == expected
                        && control.recordsOut(B).filter(count -> count > previous.recordsOut()).isPresent()
                        && control.statusObservedAt(B).isAfter(previous.observedAt())
                        && control.state(B).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> "target=" + targetAmount(target) + ", expected=" + expected
                        + ", recordsOut=" + control.recordsOut(B)
                        + ", observedAt=" + control.statusObservedAt(B)
                        + ", state=" + control.state(B));
        return progress(control, actual, target);
    }

    private static Progress progress(ControlPlane control, MongoStateStore actual, MongoDatabase target) {
        PipelineState actualState = actual.read(B)
                .map(value -> StateJson.parse(value.stateJson())).orElseThrow();
        long amount = targetAmount(target);
        long records = control.recordsOut(B).orElseThrow();
        Instant observedAt = control.statusObservedAt(B);
        PipelineState reported = control.state(B).orElseThrow();
        return new Progress(System.nanoTime(), amount, records, observedAt, actualState, reported);
    }

    /** Keeps partial real readings explicit when the full B progress tuple did not arrive in time. */
    private static Map<String, Object> bDiagnostic(ControlPlane control, MongoStateStore actual,
            MongoDatabase target) {
        Map<String, Object> reading = new LinkedHashMap<>();
        reading.put("sampledAtNanos", System.nanoTime());
        try {
            long amount = targetAmount(target);
            if (amount == Long.MIN_VALUE) { reading.put("targetAmountAvailability", "ABSENT"); }
            else { reading.put("targetAmount", amount); }
        } catch (RuntimeException | AssertionError unavailable) {
            reading.put("targetAmountUnavailableType", unavailable.getClass().getName());
        }
        try {
            var count = control.recordsOut(B);
            if (count.isPresent()) { reading.put("recordsOut", count.get()); }
            else { reading.put("recordsOutAvailability", "UNAVAILABLE"); }
        } catch (RuntimeException | AssertionError unavailable) {
            reading.put("recordsOutUnavailableType", unavailable.getClass().getName());
        }
        try { reading.put("observedAt", control.statusObservedAt(B).toString()); }
        catch (RuntimeException | AssertionError unavailable) {
            reading.put("observedAtUnavailableType", unavailable.getClass().getName());
        }
        try {
            var checkpoint = actual.read(B);
            if (checkpoint.isPresent()) {
                reading.put("actual", StateJson.parse(checkpoint.get().stateJson()).name());
            } else { reading.put("actualAvailability", "ABSENT"); }
        } catch (RuntimeException | AssertionError unavailable) {
            reading.put("actualUnavailableType", unavailable.getClass().getName());
        }
        try {
            var status = control.state(B);
            if (status.isPresent()) { reading.put("reported", status.get().name()); }
            else { reading.put("reportedAvailability", "ABSENT"); }
        } catch (RuntimeException | AssertionError unavailable) {
            reading.put("reportedUnavailableType", unavailable.getClass().getName());
        }
        reading.put("completedAtNanos", System.nanoTime());
        return reading;
    }

    private static StopReading stopReading(ControlPlane control, URI server, MongoDesiredStore desired,
            MongoStateStore actual) throws Exception {
        PipelineState intended = desired.read(A).orElseThrow().targetState();
        PipelineState actualState = actual.read(A)
                .map(value -> StateJson.parse(value.stateJson())).orElseThrow();
        PipelineState reported = control.state(A).orElseThrow();
        Map<String, Object> explain = explanation(control, server);
        String reason = "ABSENT";
        if (explain.get("pending") != null) {
            if (!(explain.get("pending") instanceof Map<?, ?> pending)
                    || !(pending.get("reason") instanceof String actualReason)) {
                throw new AssertionError("A's pending evidence was malformed: " + explain);
            }
            reason = actualReason;
        }
        return new StopReading(System.nanoTime(), intended, actualState, reported, reason);
    }

    private static Map<String, Object> explanation(ControlPlane control, URI server) throws Exception {
        URI uri = server.resolve("/api/pipelines/" + A + "/explain");
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15))
                .header("Authorization", "Bearer " + control.credential()).GET().build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200 || !(JsonReader.parse(response.body()) instanceof Map<?, ?> body)) {
            throw new AssertionError("A's explain read did not return a current object: "
                    + response.statusCode() + " " + response.body());
        }
        Map<String, Object> result = new LinkedHashMap<>();
        body.forEach((name, value) -> result.put(String.valueOf(name), value));
        return result;
    }

    private static void awaitRunning(ControlPlane control, String pipeline) {
        Await.until(pipeline + " to run in the owned Boot", SETUP_WAIT,
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> "state=" + control.state(pipeline));
    }

    private static void awaitOffset(long origin, Duration offset) {
        Await.until("the native stop window to reach " + offset, NATIVE_WAIT,
                () -> System.nanoTime() - origin >= offset.toNanos(),
                () -> "elapsed=" + Duration.ofNanos(Math.max(0, System.nanoTime() - origin)));
    }

    private static long targetAmount(MongoDatabase database) {
        Document row = database.getCollection("orders").find(new Document("id", 1L)).first();
        return row == null ? Long.MIN_VALUE : ((Number) row.get("amount")).longValue();
    }

    private static void seed(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedMySql.connect(source);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)");
            statement.execute("INSERT INTO orders (id, amount) VALUES (1, 0)");
        }
    }

    private static void updateOne(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedMySql.connect(source);
                Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("UPDATE orders SET amount=amount+1 WHERE id=1"))
                    .as("one real source row changed").isEqualTo(1);
        }
    }

    private static Map<String, String> resources(Map<String, Object> sourceA, Map<String, Object> sourceB,
            String targetA, String targetB) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source_a.tap.yml", sourceYaml("source_a", sourceA));
        resources.put("source_b.tap.yml", sourceYaml("source_b", sourceB));
        resources.put("target_a.tap.yml", targetYaml("target_a", targetA));
        resources.put("target_b.tap.yml", targetYaml("target_b", targetB));
        resources.put("a.tap.yml", pipelineYaml(A, "source_a", "target_a"));
        resources.put("b.tap.yml", pipelineYaml(B, "source_b", "target_b"));
        return resources;
    }

    private static String sourceYaml(String id, Map<String, Object> source) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """.formatted(id, source.get("host"), source.get("port"), source.get("database"),
                source.get("username"), source.get("password"));
    }

    private static String targetYaml(String id, String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(id, uri);
    }

    private static String pipelineYaml(String id, String source, String target) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: %s
                """.formatted(id, source, target);
    }

    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("native hung-stop witness requires -D" + PREFIX + name);
        }
        return value;
    }
}
