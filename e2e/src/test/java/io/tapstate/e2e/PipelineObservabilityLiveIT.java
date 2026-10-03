package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.control.core.MonitorError;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A shipped server, real MySQL source and real Mongo target witnessed through REST, CLI and MCP.
 *
 * <p>The history interval is shortened only to keep this acceptance bounded. Its reset and gap rules
 * still come from that interval: a stop/start is kept below twice the interval, while the process is
 * deliberately absent for longer than that before it is restarted. The final target refusal uses the
 * same real Mongo connector with an unreachable address, so the coded failure is produced by the
 * running product rather than placed into a fixture.
 * A separate opt-in case loses a suspended job with its owned process and reads the resulting failure
 * through the same public clients.
 *
 * <p>Run with a packaged app and MCP sidecar, Docker, and real connector jars:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=PipelineObservabilityLiveIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class PipelineObservabilityLiveIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final String TABLE = "orders";
    private static final String PIPELINE_ID = "mysql2mongo_observed";
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(10);
    private static final Duration DELIBERATE_GAP = Duration.ofSeconds(22);
    private static final Duration REAL_CONNECTOR_WAIT = Duration.ofMinutes(2);
    private static final String MCP_BOOT_JAR_PROPERTY = "tapstate.e2e.mcp-boot-jar";
    private static final String PAUSED_LOSS_PROPERTY_PREFIX = "tapstate.e2e.paused-job-loss.";

    @TempDir
    private Path temporaryDirectory;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void realRunRestartGapAndCodedFailureAgreeAcrossRestCliAndMcp() throws Exception {
        Instant from = Instant.now().minusSeconds(5);
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            mysql.start();
            SharedMySql.grantReplication(mysql);
            seed(mysql);

            String storeUri = SharedMongo.replicaSetUrl("observability_live_store");
            String targetUri = SharedMongo.replicaSetUrl("observability_live_target");
            EndpointAddress target = EndpointAddress.uri(targetUri);
            List<String> serverSettings = List.of(
                    "--tapstate.metrics.history.sample-interval=" + SAMPLE_INTERVAL.toSeconds() + "s");

            RealProcessServer first = null;
            RealProcessServer second = null;
            try {
                first = RealProcessServer.start(storeUri, serverSettings);
                ControlPlane control = new ControlPlane(first.baseUrl());
                control.bootstrapAndLogin(USER, PASSWORD);
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                control.apply(resources(mysqlConfig, targetUri));
                control.discoverSchema("src_mysql", "mysql", mysqlConfig);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
                awaitState(control, PipelineState.RUNNING);
                awaitCustomer(mongo, target, "seeded", "the snapshot to reach the real Mongo target");

                awaitHistory(first.baseUrl(), control.credential(), from,
                        history -> pointCount(history) >= 1, "the first retained sample");
                update(mysql, "moving");
                awaitCustomer(mongo, target, "moving", "a CDC update before the restart");
                FirstResetObservationReceipt.captureIfRequested("after-first-cdc", storeUri, PIPELINE_ID, control, from, null);
                Map<String, Object> baseline;
                try {
                    baseline = awaitHistory(first.baseUrl(), control.credential(), from,
                            PipelineObservabilityLiveIT::hasKnownInitialOutputInterval,
                            "the first known output interval in the initial window");
                } catch (AssertionError failed) {
                    try {
                        FirstResetObservationReceipt.captureIfRequested("first-reset-failed", storeUri,
                                PIPELINE_ID, control, from, failed);
                    } catch (Exception | Error receiptFailure) {
                        if (failed != receiptFailure) { failed.addSuppressed(receiptFailure); }
                    }
                    throw failed;
                }
                int resetsBeforeRestart = reasonCount(baseline, "COUNTER_RESET");
                update(mysql, "moving-again");
                awaitCustomer(mongo, target, "moving-again", "a second CDC update after the counter baseline");
                Map<String, Object> moving = awaitHistory(first.baseUrl(), control.credential(), from,
                        PipelineObservabilityLiveIT::hasPositiveOutputDelta,
                        "a positive target-acknowledged output interval");
                assertThat(lagReadings(moving)).isNotEmpty();

                Map<String, Object> normal = awaitExplanation(first.baseUrl(), control.credential(),
                        kind -> !"OBSERVATION_STALE".equals(kind) && !"CODED_FAILURE".equals(kind),
                        "a current non-failure explanation");
                assertThat(normal).containsEntry("state", "RUNNING");

                FirstResetObservationReceipt.captureIfRequested("before-counter-reset", storeUri, PIPELINE_ID, control, from, null);
                control.stop(PIPELINE_ID, false);
                awaitState(control, PipelineState.STOPPED);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
                awaitState(control, PipelineState.RUNNING);
                update(mysql, "after-counter-reset");
                awaitCustomer(mongo, target, "after-counter-reset", "a CDC update after the run reset");
                FirstResetObservationReceipt.captureIfRequested("after-counter-reset", storeUri, PIPELINE_ID, control, from, null);
                try {
                    awaitHistory(first.baseUrl(), control.credential(), from,
                            history -> reasonCount(history, "COUNTER_RESET") > resetsBeforeRestart,
                            "a counter-reset segment after stop/start");
                } catch (AssertionError failed) {
                    try {
                        FirstResetObservationReceipt.captureIfRequested("counter-reset-failed", storeUri,
                                PIPELINE_ID, control, from, failed);
                    } catch (Exception | Error receiptFailure) {
                        if (failed != receiptFailure) { failed.addSuppressed(receiptFailure); }
                    }
                    throw failed;
                }

                first.close();
                first = null;
                awaitMissingSampleInterval();

                second = RealProcessServer.start(storeUri, serverSettings);
                control = new ControlPlane(second.baseUrl());
                control.login(USER, PASSWORD);
                awaitState(control, PipelineState.RUNNING);
                update(mysql, "after-server-restart");
                awaitCustomer(mongo, target, "after-server-restart", "a CDC update after server restart");
                awaitHistory(second.baseUrl(), control.credential(), from,
                        history -> startReasons(history).contains("GAP"),
                        "an explicit sample gap across process downtime");

                control.stop(PIPELINE_ID, false);
                awaitState(control, PipelineState.STOPPED);
                control.apply(Map.of("tgt_mongo.tap.yml", targetYaml(
                        "mongodb://127.0.0.1:1/broken?serverSelectionTimeoutMS=500&connectTimeoutMS=500")));
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
                Map<String, Object> coded = awaitExplanation(second.baseUrl(), control.credential(),
                        kind -> "CODED_FAILURE".equals(kind), "the real target refusal to become coded");
                assertThat(coded).containsEntry("state", "FAILED");

                // Put the comparison window firmly in the past. A future bound is clipped independently
                // by each request's serverNow, so two correct sequential readers would freeze different
                // effectiveTo values and there would be no single window to compare.
                Instant to = Instant.now().minusSeconds(1);
                Map<String, Object> restHistory = history(
                        second.baseUrl(), control.credential(), from, to);
                Map<String, Object> restExplain = explanation(second.baseUrl(), control.credential());
                McpReads mcp = readThroughMcp(second.baseUrl(), control.mintToken("read"), from, to);

                CliOnce.Run cliExplain = CliOnce.runWithPassword(PASSWORD,
                        "-c", second.baseUrl().toString(), "-u", USER, "explain", PIPELINE_ID);
                CliOnce.Run cliHistory = CliOnce.runWithPassword(PASSWORD,
                        "-c", second.baseUrl().toString(), "-u", USER,
                        "metrics", PIPELINE_ID,
                        "--from", from.toString(), "--to", to.toString(),
                        "--resolution", "raw", "--limit", "1000", "--table", TABLE);

                assertThat(cliExplain.exitCode()).isZero();
                assertThat(cliExplain.stdout()).contains("kind       CODED_FAILURE", "engine.job-failed");
                assertThat(cliHistory.exitCode()).isZero();
                assertThat(cliHistory.stdout()).contains(
                        "counter_reset", "gap", "consistency eventual", "lag.orders=");

                assertThat(explanationContract(mcp.explanation()))
                        .isEqualTo(explanationContract(restExplain));
                assertThat(historyContract(mcp.history())).isEqualTo(historyContract(restHistory));
                assertThat(startReasons(restHistory)).contains("COUNTER_RESET", "GAP");
                assertThat(lagReadings(restHistory)).isNotEmpty();

                writeEvidence(control.version(), from, to, restHistory, restExplain, mcp,
                        cliHistory, cliExplain);
            } finally {
                if (first != null) {
                    first.close();
                }
                if (second != null) {
                    second.close();
                }
            }
        }
    }

    @Test
    void aPausedJobLostWithItsProcessIsCodedAcrossRestCliAndMcp() throws Exception {
        Assumptions.assumeTrue(System.getProperty(PAUSED_LOSS_PROPERTY_PREFIX + "jar") != null
                        || System.getProperty(PAUSED_LOSS_PROPERTY_PREFIX + "output") != null,
                "no paused-job-loss properties supplied; the real process-loss witness is opt-in");
        Path jar = Path.of(pausedLossRequired("jar")).toRealPath();
        assertThat(jar).as("the selected application artifact").isRegularFile();
        Path output = Path.of(pausedLossRequired("output"));
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        Map<String, Object> connectors = Map.of(
                "mysql", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql")),
                "mongodb", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")));
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.begin(Map.of("purpose", "REAL_PAUSED_PROCESS_LOSS_PARITY",
                        "application", application, "connectors", connectors),
                Map.of("kind", "correctness-only"), List.of());
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String storeUri = SharedMongo.replicaSetUrl("observability_paused_" + suffix + "_store");
        String targetUri = SharedMongo.replicaSetUrl("observability_paused_" + suffix + "_target");
        String operatorDatabase = "observability_paused_" + suffix + "_operator";
        EndpointAddress target = EndpointAddress.uri(targetUri);
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));
                MongoEndpoints targetMongo = new MongoEndpoints();
                MongoClient storeClient = MongoClients.create(storeUri)) {
            mysql.start();
            SharedMySql.grantReplication(mysql);
            seed(mysql);
            MongoDatabase database = storeClient.getDatabase(new ConnectionString(storeUri).getDatabase());
            MongoDesiredStore desired = new MongoDesiredStore(
                    database.getCollection(MongoStorePort.PIPELINE_DESIRED));
            MongoStateStore actual = new MongoStateStore(
                    database.getCollection(MongoStorePort.PIPELINE_STATE));
            MongoObservationStore latest = new MongoObservationStore(storeClient,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            RealProcessServer first = null;
            RealProcessServer restored = null;
            try {
                first = RealProcessServer.start(storeUri, operatorDatabase, jar);
                ControlPlane control = new ControlPlane(first.baseUrl());
                control.bootstrapAndLogin(USER, PASSWORD);
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                control.apply(resources(mysqlConfig, targetUri));
                control.discoverSchema("src_mysql", "mysql", mysqlConfig);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
                awaitState(control, PipelineState.RUNNING);
                awaitCustomer(targetMongo, target, "seeded", "the real snapshot before process loss");
                update(mysql, "before-paused-loss");
                awaitCustomer(targetMongo, target, "before-paused-loss",
                        "a real CDC update before pausing the job");

                control.lifecycle(PIPELINE_ID, LifecycleVerb.PAUSE);
                awaitState(control, PipelineState.PAUSED);
                Await.until("the durable desired, actual and latest state to agree on PAUSED",
                        REAL_CONNECTOR_WAIT,
                        () -> desired.read(PIPELINE_ID)
                                        .map(value -> value.targetState() == PipelineState.PAUSED).orElse(false)
                                && actual.read(PIPELINE_ID).map(value ->
                                        StateJson.parse(value.stateJson()) == PipelineState.PAUSED).orElse(false)
                                && latest.readStored(PIPELINE_ID).map(value ->
                                        value.observation().state() == PipelineState.PAUSED).orElse(false),
                        () -> "desired=" + desired.read(PIPELINE_ID) + ", actual=" + actual.read(PIPELINE_ID)
                                + ", latest=" + latest.readStored(PIPELINE_ID));
                var paused = latest.readStored(PIPELINE_ID).orElseThrow();
                assertThat(paused.scope()).as("the paused observation still belongs to the real execution")
                        .isPresent();
                assertThat(paused.observation().failure()).as("the paused job had not already failed").isNull();
                assertThat(first.isAlive()).as("the job-bearing process was alive before the fault").isTrue();
                Path firstOutput = first.output();
                first.kill();
                assertThat(first.isAlive()).as("the process carrying the suspended job died").isFalse();
                assertThat(desired.read(PIPELINE_ID).orElseThrow().targetState())
                        .as("process loss did not rewrite desired intent").isEqualTo(PipelineState.PAUSED);

                restored = RealProcessServer.start(storeUri, operatorDatabase, jar);
                ControlPlane restarted = new ControlPlane(restored.baseUrl());
                restarted.login(USER, PASSWORD);
                Await.until("the lost paused execution to publish its coded failure", REAL_CONNECTOR_WAIT,
                        () -> restarted.state(PIPELINE_ID).filter(PipelineState.FAILED::equals).isPresent()
                                && restarted.failureCode(PIPELINE_ID)
                                        .filter(LifecycleError.PAUSED_JOB_MISSING.code()::equals).isPresent()
                                && desired.read(PIPELINE_ID)
                                        .map(value -> value.targetState() == PipelineState.PAUSED).orElse(false)
                                && actual.read(PIPELINE_ID).map(value ->
                                        StateJson.parse(value.stateJson()) == PipelineState.FAILED).orElse(false)
                                && latest.readStored(PIPELINE_ID).map(value ->
                                        value.observation().state() == PipelineState.FAILED
                                                && value.observation().failure() != null
                                                && LifecycleError.PAUSED_JOB_MISSING.code()
                                                        .equals(value.observation().failure().code())).orElse(false),
                        () -> "state=" + restarted.state(PIPELINE_ID)
                                + ", failure=" + restarted.failureCode(PIPELINE_ID)
                                + ", desired=" + desired.read(PIPELINE_ID)
                                + ", actual=" + actual.read(PIPELINE_ID));
                var failed = latest.readStored(PIPELINE_ID).orElseThrow();
                assertThat(failed.scope()).as("a missing paused job did not allocate another execution")
                        .isEqualTo(paused.scope());
                assertThat(failed.observation().observedAt()).isAfter(paused.observation().observedAt());

                Map<String, Object> restStatus = get(restored.baseUrl(), restarted.credential(),
                        "/api/pipelines/" + PIPELINE_ID + "/status");
                assertThat(restStatus.get("state")).isEqualTo(PipelineState.FAILED.name());
                assertThat(restStatus.get("failure")).isInstanceOf(Map.class);
                Map<?, ?> failure = (Map<?, ?>) restStatus.get("failure");
                assertThat(failure.get("code")).isEqualTo(LifecycleError.PAUSED_JOB_MISSING.code());
                assertThat(failure.get("params")).isEqualTo(Map.of("pipeline", PIPELINE_ID));
                assertThat(failure.get("message")).isInstanceOf(String.class).isNotEqualTo("");
                Map<String, Object> restExplain = awaitExplanation(restored.baseUrl(), restarted.credential(),
                        "CODED_FAILURE"::equals, "the actual paused-job loss to be explained");
                assertThat(restExplain).containsEntry("pipelineId", PIPELINE_ID)
                        .containsEntry("state", PipelineState.FAILED.name())
                        .containsEntry("freshness", "FRESH");
                assertThat(failureEvidence(restExplain)).isEqualTo(failure);

                Map<String, Object> mcpExplain = readExplanationThroughMcp(
                        restored.baseUrl(), restarted.mintToken("read"));
                assertThat(mcpExplain.get("pipelineId")).isEqualTo(PIPELINE_ID);
                assertThat(explanationContract(mcpExplain)).isEqualTo(explanationContract(restExplain));
                CliOnce.Run cliExplain = CliOnce.runWithPassword(PASSWORD,
                        "-c", restored.baseUrl().toString(), "-u", USER, "explain", PIPELINE_ID);
                assertThat(cliExplain.exitCode()).isZero();
                assertThat(cliExplain.stdout()).contains("kind       CODED_FAILURE", "status.failure",
                        LifecycleError.PAUSED_JOB_MISSING.code(), "pipeline=" + PIPELINE_ID,
                        (String) failure.get("message"));
                assertThat(desired.read(PIPELINE_ID).orElseThrow().targetState())
                        .as("all three readers left desired intent paused").isEqualTo(PipelineState.PAUSED);
                assertThat(actual.read(PIPELINE_ID).map(value -> StateJson.parse(value.stateJson())))
                        .contains(PipelineState.FAILED);

                Map<String, Object> evidence = new LinkedHashMap<>();
                evidence.put("applicationSha256", application.get("sha256"));
                evidence.put("initialProcessOutput", firstOutput.toString());
                evidence.put("restoredProcessOutput", restored.output().toString());
                evidence.put("desiredBeforeLoss", PipelineState.PAUSED.name());
                evidence.put("desiredAfterLoss", PipelineState.PAUSED.name());
                evidence.put("actualAfterLoss", PipelineState.FAILED.name());
                evidence.put("restStatus", restStatus);
                evidence.put("restExplain", restExplain);
                evidence.put("mcpExplain", mcpExplain);
                evidence.put("cliExplain", cliExplain.stdout());
                report.addFork(evidence);
            } finally {
                if (first != null) { first.close(); }
                if (restored != null) { restored.close(); }
            }
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql")))
                    .isEqualTo(connectors.get("mysql"));
            assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")))
                    .isEqualTo(connectors.get("mongodb"));
            report.completeDiagnostic(Map.of(
                    "correctness", "REAL_PAUSED_PROCESS_LOSS_CODED_PARITY",
                    "performanceAcceptanceEligible", false,
                    "unverified", List.of("SAME_LIVE_JVM_JOB_ONLY_CANCEL", "OTHER_CLUSTER_MODES")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); }
            catch (RuntimeException writeFailure) { failure.addSuppressed(writeFailure); }
            throw failure;
        }
    }

    private static Map<String, String> resources(Map<String, Object> mysql, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_mysql.tap.yml", sourceYaml(mysql));
        resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
        resources.put("pipeline.tap.yml", pipelineYaml());
        return resources;
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """.formatted(config.get("host"), config.get("port"), config.get("database"),
                config.get("username"), config.get("password"));
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: mysql2mongo_observed
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: tgt_mongo
                """;
    }

    private static Map<String, Object> mysqlConfig(MySQLContainer<?> mysql) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", mysql.getHost());
        config.put("port", mysql.getMappedPort(MySQLContainer.MYSQL_PORT));
        config.put("database", mysql.getDatabaseName());
        config.put("username", mysql.getUsername());
        config.put("password", mysql.getPassword());
        return config;
    }

    private static void seed(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, customer) VALUES (1, 'seeded')");
        }
    }

    private static void update(MySQLContainer<?> mysql, String value) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE orders SET customer = '" + value + "' WHERE id = 1");
        }
    }

    private static void awaitState(ControlPlane control, PipelineState expected) {
        Await.until("pipeline " + PIPELINE_ID + " to become " + expected,
                REAL_CONNECTOR_WAIT,
                () -> control.state(PIPELINE_ID).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE_ID)));
    }

    private static void awaitCustomer(
            MongoEndpoints mongo, EndpointAddress target, String expected, String what) {
        Await.until(what, REAL_CONNECTOR_WAIT,
                () -> expected.equals(customer(mongo, target)),
                () -> String.valueOf(customer(mongo, target)));
    }

    private static String customer(MongoEndpoints mongo, EndpointAddress target) {
        List<Document> documents = mongo.documents(target, TABLE);
        return Optional.ofNullable(documents.isEmpty() ? null : documents.getFirst())
                .map(document -> document.getString("customer"))
                .orElse(null);
    }

    private static Map<String, Object> awaitHistory(
            URI base, String token, Instant from,
            java.util.function.Predicate<Map<String, Object>> condition, String what) {
        AtomicReference<Map<String, Object>> last = new AtomicReference<>(Map.of());
        Await.until(what, REAL_CONNECTOR_WAIT, () -> {
            Map<String, Object> value = history(base, token, from, Instant.now().plusSeconds(1));
            last.set(value);
            return condition.test(value);
        }, () -> String.valueOf(last.get()));
        return last.get();
    }

    private static Map<String, Object> awaitExplanation(
            URI base, String token, java.util.function.Predicate<String> kind, String what) {
        AtomicReference<Map<String, Object>> last = new AtomicReference<>(Map.of());
        Await.until(what, REAL_CONNECTOR_WAIT, () -> {
            Map<String, Object> value = get(base, token, "/api/pipelines/" + PIPELINE_ID + "/explain", true);
            last.set(value);
            return value.containsKey("kind") && kind.test(String.valueOf(value.get("kind")));
        }, () -> String.valueOf(last.get()));
        return last.get();
    }

    private static Map<String, Object> history(URI base, String token, Instant from, Instant to) {
        String path = "/api/pipelines/" + PIPELINE_ID + "/metrics/history?from=" + encoded(from.toString())
                + "&to=" + encoded(to.toString()) + "&resolution=raw&limit=1000&table=" + TABLE;
        return get(base, token, path);
    }

    private static Map<String, Object> explanation(URI base, String token) {
        return get(base, token, "/api/pipelines/" + PIPELINE_ID + "/explain");
    }

    private static Map<String, Object> get(URI base, String token, String path) {
        return get(base, token, path, false);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> get(URI base, String token, String path, boolean awaitingObservation) {
        HttpRequest request = HttpRequest.newBuilder(base.resolve(path))
                .timeout(Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        try {
            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(request, HttpResponse.BodyHandlers.ofString());
            if (awaitingObservation && response.statusCode() == 404
                    && JsonReader.parse(response.body()) instanceof Map<?, ?> body
                    && MonitorError.NO_OBSERVATION.code().equals(body.get("code"))) {
                return (Map<String, Object>) body;
            }
            if (response.statusCode() != 200) {
                throw new AssertionError("GET " + path + " returned " + response.statusCode()
                        + ": " + response.body());
            }
            if (!(JsonReader.parse(response.body()) instanceof Map<?, ?> body)) {
                throw new AssertionError("GET " + path + " did not return an object: " + response.body());
            }
            return (Map<String, Object>) body;
        } catch (IOException error) {
            throw new UncheckedIOException("could not GET " + path, error);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while GETting " + path, error);
        }
    }

    private static String encoded(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> points(Map<String, Object> history) {
        return ((List<Map<String, Object>>) history.get("segments")).stream()
                .flatMap(segment -> ((List<Map<String, Object>>) segment.get("points")).stream())
                .toList();
    }

    private static int pointCount(Map<String, Object> history) {
        return points(history).size();
    }

    @SuppressWarnings("unchecked")
    private static List<String> startReasons(Map<String, Object> history) {
        return ((List<Map<String, Object>>) history.get("segments")).stream()
                .map(segment -> String.valueOf(segment.get("startReason")))
                .toList();
    }

    private static int reasonCount(Map<String, Object> history, String reason) {
        return (int) startReasons(history).stream().filter(reason::equals).count();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> lagReadings(Map<String, Object> history) {
        return points(history).stream()
                .flatMap(point -> ((List<Map<String, Object>>) point.get("lag")).stream())
                .toList();
    }

    private static boolean hasKnownInitialOutputInterval(Map<String, Object> history) {
        if (!(history.get("segments") instanceof List<?> segments)) { return false; }
        return segments.stream().anyMatch(segment -> segment instanceof Map<?, ?> window
                && "WINDOW_START".equals(window.get("startReason"))
                && window.get("points") instanceof List<?> readings
                && readings.stream().anyMatch(point -> point instanceof Map<?, ?> interval
                        && interval.get("recordsOut") instanceof Map<?, ?> output
                        && output.get("delta") instanceof Number delta
                        && Double.isFinite(delta.doubleValue()) && delta.doubleValue() >= 0
                        && interval.get("intervalStart") instanceof String start
                        && interval.get("intervalEnd") instanceof String end
                        && Instant.parse(end).isAfter(Instant.parse(start))));
    }

    private static boolean hasPositiveOutputDelta(Map<String, Object> history) {
        return points(history).stream().anyMatch(point ->
                point.get("recordsOut") instanceof Map<?, ?> rate
                        && rate.get("delta") instanceof Number delta
                        && delta.doubleValue() > 0);
    }

    private static Map<String, Object> explanationContract(Map<String, Object> value) {
        return selected(value, List.of("state", "kind", "message", "freshness",
                "evidence", "cannotSay", "next", "pending"));
    }

    private static Map<?, ?> failureEvidence(Map<String, Object> explanation) {
        if (explanation.get("evidence") instanceof List<?> readings) {
            for (Object reading : readings) {
                if (reading instanceof Map<?, ?> evidence
                        && "status".equals(evidence.get("source"))
                        && "failure".equals(evidence.get("field"))
                        && evidence.get("value") instanceof Map<?, ?> failure) {
                    return failure;
                }
            }
        }
        throw new AssertionError("coded explanation carried no status.failure evidence: " + explanation);
    }

    private static String pausedLossRequired(String name) {
        String value = System.getProperty(PAUSED_LOSS_PROPERTY_PREFIX + name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("paused process-loss witness requires -D"
                    + PAUSED_LOSS_PROPERTY_PREFIX + name);
        }
        return value;
    }

    private static Map<String, Object> historyContract(Map<String, Object> value) {
        return selected(value, List.of("from", "to", "effectiveFrom", "effectiveTo",
                "effectiveResolution", "status", "consistency", "segments", "gaps",
                "unavailable", "nextCursor"));
    }

    private static Map<String, Object> selected(Map<String, Object> value, List<String> names) {
        Map<String, Object> selected = new LinkedHashMap<>();
        names.stream().filter(value::containsKey).forEach(name -> selected.put(name, value.get(name)));
        return selected;
    }

    private McpReads readThroughMcp(URI server, String token, Instant from, Instant to) throws Exception {
        Path stderr = temporaryDirectory.resolve("observability-mcp.stderr");
        Process process = startMcp(server, token, stderr);
        try (Writer input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
                BufferedReader output = process.inputReader(StandardCharsets.UTF_8)) {
            send(input, Map.of(
                    "jsonrpc", "2.0", "id", 1, "method", "initialize",
                    "params", Map.of(
                            "protocolVersion", "2025-06-18",
                            "capabilities", Map.of(),
                            "clientInfo", Map.of("name", "observability-live", "version", "1"))));
            receive(output);
            send(input, Map.of("jsonrpc", "2.0", "method", "notifications/initialized"));

            Map<String, Object> historyArgs = new LinkedHashMap<>();
            historyArgs.put("id", PIPELINE_ID);
            historyArgs.put("from", from.toString());
            historyArgs.put("to", to.toString());
            historyArgs.put("resolution", "raw");
            historyArgs.put("limit", 1000);
            historyArgs.put("table", List.of(TABLE));
            send(input, toolCall(2, "pipeline_metrics_history", historyArgs));
            Map<String, Object> history = structured(receive(output), "pipeline_metrics_history");

            send(input, toolCall(3, "pipeline_explain", Map.of("id", PIPELINE_ID)));
            Map<String, Object> explain = structured(receive(output), "pipeline_explain");
            return new McpReads(history, explain);
        } finally {
            process.getOutputStream().close();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(process.exitValue()).isZero();
            assertThat(Files.readString(stderr)).doesNotContain(token);
        }
    }

    private Map<String, Object> readExplanationThroughMcp(URI server, String token) throws Exception {
        Path stderr = temporaryDirectory.resolve("paused-loss-mcp.stderr");
        Process process = startMcp(server, token, stderr);
        try (Writer input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
                BufferedReader output = process.inputReader(StandardCharsets.UTF_8)) {
            send(input, Map.of(
                    "jsonrpc", "2.0", "id", 1, "method", "initialize",
                    "params", Map.of(
                            "protocolVersion", "2025-06-18",
                            "capabilities", Map.of(),
                            "clientInfo", Map.of("name", "paused-loss-live", "version", "1"))));
            receive(output);
            send(input, Map.of("jsonrpc", "2.0", "method", "notifications/initialized"));
            send(input, toolCall(2, "pipeline_explain", Map.of("id", PIPELINE_ID)));
            return structured(receive(output), "pipeline_explain");
        } finally {
            try {
                process.getOutputStream().close();
            } finally {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
                assertThat(process.exitValue()).isZero();
                assertThat(Files.readString(stderr)).doesNotContain(token);
            }
        }
    }

    private static Map<String, Object> toolCall(int id, String name, Map<String, Object> arguments) {
        return Map.of(
                "jsonrpc", "2.0", "id", id, "method", "tools/call",
                "params", Map.of("name", name, "arguments", arguments));
    }

    private static Process startMcp(URI server, String token, Path stderr) throws IOException {
        Path jar = Path.of(System.getProperty(MCP_BOOT_JAR_PROPERTY));
        assertThat(jar).isRegularFile();
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", jar.toString());
        builder.environment().put("TAPSTATE_SERVER_URL", server.toString());
        builder.environment().put("TAPSTATE_TOKEN", token);
        builder.redirectError(stderr.toFile());
        return builder.start();
    }

    private static void send(Writer input, Map<String, Object> message) throws IOException {
        input.write(JsonWriter.write(message));
        input.write('\n');
        input.flush();
    }

    private static Map<?, ?> receive(BufferedReader output) throws Exception {
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                return output.readLine();
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        });
        String response = line.get(20, TimeUnit.SECONDS);
        assertThat(response).isNotBlank();
        return (Map<?, ?>) JsonReader.parse(response);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(Map<?, ?> response, String tool) {
        assertThat(response.get("result")).as(tool).isInstanceOf(Map.class);
        Map<?, ?> result = (Map<?, ?>) response.get("result");
        assertThat(result.get("isError")).as(tool).isEqualTo(false);
        assertThat(result.get("structuredContent")).as(tool).isInstanceOf(Map.class);
        return (Map<String, Object>) result.get("structuredContent");
    }

    private static void awaitMissingSampleInterval() {
        long startedAt = System.nanoTime();
        Await.until("the server process to remain absent long enough to create a missing sample interval",
                DELIBERATE_GAP.plusSeconds(2),
                () -> System.nanoTime() - startedAt >= DELIBERATE_GAP.toNanos(),
                () -> Duration.ofNanos(Math.max(0L, System.nanoTime() - startedAt)) + " elapsed");
    }

    private static void writeEvidence(String version, Instant from, Instant to,
            Map<String, Object> restHistory, Map<String, Object> restExplain, McpReads mcp,
            CliOnce.Run cliHistory, CliOnce.Run cliExplain) throws IOException {
        String buildDirectory = System.getProperty("tapstate.e2e.build-directory");
        Path evidence = Path.of(buildDirectory).resolve("observability-live-evidence.json");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("recordedAt", Instant.now().toString());
        record.put("productVersion", version);
        record.put("from", from.toString());
        record.put("to", to.toString());
        record.put("restHistory", restHistory);
        record.put("restExplain", restExplain);
        record.put("mcpHistory", mcp.history());
        record.put("mcpExplain", mcp.explanation());
        record.put("cliHistory", cliHistory.stdout());
        record.put("cliExplain", cliExplain.stdout());
        Files.createDirectories(evidence.getParent());
        Files.writeString(evidence, JsonWriter.write(record) + System.lineSeparator(), StandardCharsets.UTF_8);
    }

    private record McpReads(Map<String, Object> history, Map<String, Object> explanation) {
    }
}
