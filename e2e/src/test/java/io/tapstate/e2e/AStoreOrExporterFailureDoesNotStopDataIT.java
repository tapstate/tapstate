package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.control.core.MonitorError;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.Observation;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-process proof that failing telemetry stores and collectors leave CDC moving. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.telemetry-fault.jar", matches = ".+")
class AStoreOrExporterFailureDoesNotStopDataIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.telemetry-fault.jar";
    private static final String DATABASE = "telemetry_fault_store";
    private static final String STORE_APP = "tapstate-telemetry-fault-store";
    private static final String PIPELINE = "telemetry_fault_pipeline";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @Container
    private static final MongoDBContainer STORE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @BeforeAll
    static void requireConnectorsAndJar() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Files.isRegularFile(Path.of(System.getProperty(BOOT_JAR_PROPERTY))))
                .as("the explicitly requested boot JAR exists").isTrue();
    }

    @Test
    void aNewExecutionIsPendingWhileThePreviousStoppedObservationStillExists() throws Exception {
        String databaseName = "pending_execution_store";
        String appName = "tapstate-pending-execution-store";
        Map<String, Object> mysql = SharedMySql.settings("pending_execution_source");
        seed(mysql);
        String rawStoreUri = STORE.getReplicaSetUrl(databaseName);
        String storeUri = rawStoreUri + (rawStoreUri.contains("?") ? "&" : "?") + "appName=" + appName;
        String targetUri = SharedMongo.replicaSetUrl("pending_execution_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        String jarSha = PipelineBenchmarkLiveRunIT.sha256(jar);
        try (MongoClient admin = MongoClients.create(STORE.getReplicaSetUrl());
                MongoEndpoints targetMongo = new MongoEndpoints();
                RealProcessServer server = RealProcessServer.start(storeUri, "pending_execution_operator", jar,
                        List.of())) {
            MongoDatabase database = admin.getDatabase(databaseName);
            var latest = new MongoObservationStore(admin,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("pending-execution", "pending-execution-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(mysql, targetUri));
            control.discoverSchema("fault_source", "mysql", mysql);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("real initial snapshot delivery", WAIT,
                    () -> "before".equals(customer(targetMongo, target)),
                    () -> "target=" + customer(targetMongo, target));
            var running = Await.answered("known output and scoped initial execution", WAIT,
                    () -> latest.readStored(PIPELINE).filter(value -> value.scope().isPresent()
                            && value.observation().state() == PipelineState.RUNNING
                            && outputStart(value.observation()) != null));
            ObservationStore.Scope oldScope = running.scope().orElseThrow();
            Instant oldStart = outputStart(running.observation());
            control.stop(PIPELINE, false);
            Await.answered("the old scoped STOPPED observation to physically commit", WAIT,
                    () -> latest.readStored(PIPELINE).filter(value -> value.scope().filter(oldScope::equals).isPresent()
                            && value.observation().state() == PipelineState.STOPPED));

            int failuresBefore = logOccurrences(server.output(), "Could not write latest observation");
            ObservationStore.Scope nextScope = new ObservationStore.Scope(oldScope.pipelineIncarnationId(),
                    oldScope.executionGeneration() + 1);
            configureFault(admin, true, databaseName, appName, "pipeline_observation", "update");
            try {
                var held = latest.readStored(PIPELINE).orElseThrow();
                assertThat(held.scope()).contains(oldScope);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the actual new submission to advance its durable generation", WAIT,
                        () -> generation(database) == nextScope.executionGeneration(),
                        () -> "generation=" + generation(database));
                Await.until("a new latest write to be refused by the observation-only failpoint", WAIT,
                        () -> logOccurrences(server.output(), "Could not write latest observation") > failuresBefore,
                        () -> "newGeneration=" + generation(database));
                var physical = latest.readStored(PIPELINE).orElseThrow();
                assertThat(physical.scope()).contains(oldScope);
                assertThat(physical.observation().state()).isEqualTo(PipelineState.STOPPED);
                assertThat(outputStart(physical.observation())).isEqualTo(oldStart);
                assertThat(physical.observation()).isEqualTo(held.observation());
                for (ControlPlane.Refusal refusal : List.of(control.stateExpectingRefusal(PIPELINE),
                        control.metricsExpectingRefusal(PIPELINE))) {
                    assertThat(refusal.status()).isEqualTo(404);
                    assertThat(refusal.code()).isEqualTo(MonitorError.NO_OBSERVATION.code());
                    assertThat(refusal.params()).containsEntry("pipeline", PIPELINE);
                }
                update(mysql, "during-pending");
                Await.until("real CDC delivery while the new execution's latest is unavailable", WAIT,
                        () -> "during-pending".equals(customer(targetMongo, target)),
                        () -> "target=" + customer(targetMongo, target));
                assertThat(control.metricsExpectingRefusal(PIPELINE).code())
                        .isEqualTo(MonitorError.NO_OBSERVATION.code());
            } finally {
                configureFault(admin, false, databaseName, appName, "pipeline_observation", "update");
            }
            var current = Await.answered("recovery to a real new scoped observation", WAIT,
                    () -> latest.readStored(PIPELINE).filter(value -> value.scope().filter(nextScope::equals).isPresent()
                            && value.observation().state() == PipelineState.RUNNING
                            && outputStart(value.observation()) != null
                            && outputStart(value.observation()).isAfter(oldStart)));
            assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
            assertThat(generation(database)).isEqualTo(nextScope.executionGeneration());
            System.out.printf("pending-execution-live jarSha=%s generationFrom=%d generationTo=%d"
                            + " oldCounterStart=%s newCounterStart=%s target=%s statusRefusal=404 metricsRefusal=404%n",
                    jarSha, oldScope.executionGeneration(), nextScope.executionGeneration(), oldStart,
                    outputStart(current.observation()), customer(targetMongo, target));
            control.stop(PIPELINE, false);
        }
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(jarSha);
    }

    private static long generation(MongoDatabase database) {
        Document claim = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", PIPELINE)).first();
        return claim == null ? -1 : ((Number) claim.get("executionGeneration")).longValue();
    }

    private static Instant outputStart(Observation observation) {
        return observation.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                .flatMap(fact -> fact.points().stream())
                .filter(point -> "out".equals(point.attributes().get(MetricAttributes.DIRECTION)))
                .map(io.tapstate.core.lifecycle.MetricPoint::startTime).findFirst().orElse(null);
    }

    @Test
    void observationHistoryEventAndCollectorOutagesLeaveCdcMovingAndRecoverHonestly() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings("telemetry_fault_source_db");
        seed(mysql);
        String rawStoreUri = STORE.getReplicaSetUrl(DATABASE);
        String storeUri = rawStoreUri + (rawStoreUri.contains("?") ? "&" : "?")
                + "appName=" + STORE_APP;
        String targetUri = SharedMongo.replicaSetUrl("telemetry_fault_target_db");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        AtomicBoolean collectorDown = new AtomicBoolean();
        AtomicInteger collectorRejected = new AtomicInteger();
        AtomicInteger collectorAccepted = new AtomicInteger();
        HttpServer collector = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        collector.createContext("/v1/metrics", exchange -> {
            exchange.getRequestBody().readAllBytes();
            if (collectorDown.get()) {
                collectorRejected.incrementAndGet();
                exchange.sendResponseHeaders(503, -1);
            } else {
                collectorAccepted.incrementAndGet();
                exchange.sendResponseHeaders(200, -1);
            }
            exchange.close();
        });
        collector.start();
        String otlpEndpoint = "http://127.0.0.1:" + collector.getAddress().getPort() + "/v1/metrics";

        try (MongoClient admin = MongoClients.create(STORE.getReplicaSetUrl());
                MongoEndpoints targetMongo = new MongoEndpoints();
                RealProcessServer server = RealProcessServer.start(storeUri, jar,
                        List.of("--tapstate.metrics.history.sample-interval=1s",
                                "--tapstate.metrics.export.otlp.endpoint=" + otlpEndpoint,
                                "--tapstate.metrics.export.otlp.interval=1s"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("telemetry-fault", "telemetry-fault-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(mysql, targetUri));
            control.discoverSchema("fault_source", "mysql", mysql);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("initial target snapshot", WAIT,
                    () -> "before".equals(customer(targetMongo, target)),
                    () -> String.valueOf(customer(targetMongo, target)));
            Await.until("initial running observation", WAIT,
                    () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE)));

            configureObservationFault(admin, true);
            Instant frozen;
            try {
                Await.until("latest write failure in local server log", Duration.ofSeconds(20),
                        () -> logContains(server.output(), "Could not write latest observation"),
                        () -> "statusObservedAt=" + control.statusObservedAt(PIPELINE));
                frozen = control.statusObservedAt(PIPELINE);
                update(mysql, "during-outage");
                Await.until("target CDC while observation writes fail", WAIT,
                        () -> "during-outage".equals(customer(targetMongo, target)),
                        () -> "target=" + customer(targetMongo, target)
                                + ", actual=" + control.state(PIPELINE));
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                Instant afterDelivery = control.statusObservedAt(PIPELINE);
                Thread.sleep(2_200);
                assertThat(control.statusObservedAt(PIPELINE)).isEqualTo(afterDelivery);
                assertThat(afterDelivery).isEqualTo(frozen);
            } finally {
                configureObservationFault(admin, false);
            }

            Instant stale = frozen;
            Await.until("latest observation refresh after store recovery", WAIT,
                    () -> control.statusObservedAt(PIPELINE).isAfter(stale),
                    () -> "stale=" + stale + ", current=" + control.statusObservedAt(PIPELINE));
            assertThat(customer(targetMongo, target)).isEqualTo("during-outage");
            assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);

            int writesBeforeDelay = logOccurrences(server.output(), "Could not write latest observation");
            configureObservationDelay(admin, true);
            Instant delayed;
            try {
                long blockedAt = System.nanoTime();
                Await.until("bounded failure during a blocked store command", Duration.ofSeconds(15),
                        () -> logOccurrences(server.output(), "Could not write latest observation")
                                > writesBeforeDelay,
                        () -> "statusObservedAt=" + control.statusObservedAt(PIPELINE));
                assertThat(Duration.ofNanos(System.nanoTime() - blockedAt))
                        .isGreaterThanOrEqualTo(Duration.ofSeconds(3));
                delayed = control.statusObservedAt(PIPELINE);
                update(mysql, "during-slow-observation");
                Await.until("target CDC while observation write is blocked", WAIT,
                        () -> "during-slow-observation".equals(customer(targetMongo, target)),
                        () -> "target=" + customer(targetMongo, target)
                                + ", actual=" + control.state(PIPELINE));
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(control.statusObservedAt(PIPELINE)).isEqualTo(delayed);
            } finally {
                configureObservationDelay(admin, false);
            }
            Instant staleWhileSlow = delayed;
            Await.until("latest observation refresh after blocked command drains", WAIT,
                    () -> control.statusObservedAt(PIPELINE).isAfter(staleWhileSlow),
                    () -> "stale=" + staleWhileSlow + ", current=" + control.statusObservedAt(PIPELINE));

            Instant from = Instant.now().minusSeconds(60);
            assertThat(control.events(PIPELINE, from, Instant.now().plusSeconds(60)).get("completeness"))
                    .isEqualTo("BEST_EFFORT");

            Instant eventFaultFrom = Instant.now().minusSeconds(2);
            configureEventFault(admin, true);
            try {
                control.lifecycle(PIPELINE, LifecycleVerb.PAUSE);
                Await.until("pause while event writes fail", WAIT,
                        () -> control.state(PIPELINE).filter(PipelineState.PAUSED::equals).isPresent(),
                        () -> String.valueOf(control.state(PIPELINE)));
                control.lifecycle(PIPELINE, LifecycleVerb.RESUME);
                Await.until("resume while event writes fail", WAIT,
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> String.valueOf(control.state(PIPELINE)));
                update(mysql, "during-event-outage");
                Await.until("target CDC while event writes fail", WAIT,
                        () -> "during-event-outage".equals(customer(targetMongo, target)),
                        () -> "target=" + customer(targetMongo, target)
                                + ", actual=" + control.state(PIPELINE));
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                Await.until("event write failure in local server log", Duration.ofSeconds(20),
                        () -> logContains(server.output(), "Could not persist event telemetry"),
                        () -> "events=" + control.events(PIPELINE, eventFaultFrom,
                                Instant.now().plusSeconds(60)));
            } finally {
                configureEventFault(admin, false);
            }
            Await.until("durable event gap and restoration after recovery", WAIT,
                    () -> hasDurableGapAndRestoration(control.events(PIPELINE, eventFaultFrom,
                            Instant.now().plusSeconds(60))),
                    () -> "events=" + control.events(PIPELINE, eventFaultFrom,
                            Instant.now().plusSeconds(60)));
            Map<?, ?> eventPage = control.events(PIPELINE, eventFaultFrom, Instant.now().plusSeconds(60));
            assertThat(eventPage.get("completeness")).isEqualTo("BEST_EFFORT");
            assertThat((List<?>) eventPage.get("knownGaps")).isNotEmpty();
            assertThat(customer(targetMongo, target)).isEqualTo("during-event-outage");

            Instant historyFaultFrom = Instant.now().minusSeconds(5);
            configureHistoryFault(admin, true);
            try {
                Await.until("history append failure in local server log", Duration.ofSeconds(20),
                        () -> logContains(server.output(), "Could not write history telemetry"),
                        () -> "history=" + control.history(PIPELINE, historyFaultFrom,
                                Instant.now().plusSeconds(60)));
                Instant freshnessBefore = control.statusObservedAt(PIPELINE);
                update(mysql, "during-history-outage");
                Await.until("target CDC while history writes fail", WAIT,
                        () -> "during-history-outage".equals(customer(targetMongo, target)),
                        () -> "target=" + customer(targetMongo, target)
                                + ", actual=" + control.state(PIPELINE));
                Await.until("latest observation remains fresh during history failure", WAIT,
                        () -> control.statusObservedAt(PIPELINE).isAfter(freshnessBefore),
                        () -> String.valueOf(control.statusObservedAt(PIPELINE)));
            } finally {
                configureHistoryFault(admin, false);
            }
            Await.until("explicit history gap after append recovery", WAIT,
                    () -> hasHistoryGap(control.history(PIPELINE, historyFaultFrom,
                            Instant.now().plusSeconds(60))),
                    () -> "history=" + control.history(PIPELINE, historyFaultFrom,
                            Instant.now().plusSeconds(60)));
            assertThat(customer(targetMongo, target)).isEqualTo("during-history-outage");

            int acceptedBeforeOutage = collectorAccepted.get();
            collectorDown.set(true);
            try {
                Await.until("collector refused OTLP pushes", Duration.ofSeconds(20),
                        () -> collectorRejected.get() > 0,
                        () -> "rejected=" + collectorRejected.get());
                Instant freshnessBefore = control.statusObservedAt(PIPELINE);
                update(mysql, "during-collector-outage");
                Await.until("target CDC while collector rejects metrics", WAIT,
                        () -> "during-collector-outage".equals(customer(targetMongo, target)),
                        () -> "target=" + customer(targetMongo, target)
                                + ", actual=" + control.state(PIPELINE));
                Await.until("latest observation continues through collector outage", WAIT,
                        () -> control.statusObservedAt(PIPELINE).isAfter(freshnessBefore),
                        () -> String.valueOf(control.statusObservedAt(PIPELINE)));
            } finally {
                collectorDown.set(false);
            }
            Await.until("collector accepts pushes after recovery", Duration.ofSeconds(20),
                    () -> collectorAccepted.get() > acceptedBeforeOutage,
                    () -> "accepted=" + collectorAccepted.get()
                            + ", rejected=" + collectorRejected.get());
            assertThat(customer(targetMongo, target)).isEqualTo("during-collector-outage");
            System.out.printf("telemetry-store-outage jar=%s frozenAt=%s recoveredAt=%s target=%s%n",
                    jar, frozen, control.statusObservedAt(PIPELINE), customer(targetMongo, target));
        } finally {
            collector.stop(0);
        }
    }

    private static boolean hasHistoryGap(Map<?, ?> page) {
        if (!(page.get("gaps") instanceof List<?> gaps) || gaps.isEmpty()) {
            return false;
        }
        return gaps.stream().anyMatch(gap -> gap instanceof Map<?, ?> row
                && "SAMPLE_GAP".equals(row.get("reason")));
    }

    private static boolean hasDurableGapAndRestoration(Map<?, ?> page) {
        if (!(page.get("knownGaps") instanceof List<?> gaps) || gaps.isEmpty()
                || !(page.get("events") instanceof List<?> events)) {
            return false;
        }
        return events.stream().anyMatch(event -> event instanceof Map<?, ?> row
                && "TELEMETRY_RESTORED".equals(row.get("kind")));
    }

    private static void configureObservationFault(MongoClient admin, boolean enabled) {
        configureFault(admin, enabled, "pipeline_observation", "update");
    }

    private static void configureObservationDelay(MongoClient admin, boolean enabled) {
        Document command = new Document("configureFailPoint", "failCommand")
                .append("mode", enabled ? "alwaysOn" : "off");
        if (enabled) {
            command.append("data", new Document("failCommands", List.of("update"))
                    .append("appName", STORE_APP)
                    .append("namespace", DATABASE + ".pipeline_observation")
                    .append("blockConnection", true)
                    .append("blockTimeMS", 20_000)
                    .append("errorCode", 2));
        }
        admin.getDatabase("admin").runCommand(command);
    }

    private static void configureEventFault(MongoClient admin, boolean enabled) {
        configureFault(admin, enabled, "pipeline_events", "insert");
    }

    private static void configureHistoryFault(MongoClient admin, boolean enabled) {
        configureFault(admin, enabled, "pipeline_rate_history", "insert");
    }

    private static void configureFault(MongoClient admin, boolean enabled,
            String collection, String commandName) {
        configureFault(admin, enabled, DATABASE, STORE_APP, collection, commandName);
    }

    private static void configureFault(MongoClient admin, boolean enabled, String databaseName, String appName,
            String collection, String commandName) {
        Document command = new Document("configureFailPoint", "failCommand")
                .append("mode", enabled ? "alwaysOn" : "off");
        if (enabled) {
            command.append("data", new Document("failCommands", List.of(commandName))
                    .append("appName", appName)
                    .append("namespace", databaseName + "." + collection)
                    .append("errorCode", 2));
        }
        admin.getDatabase("admin").runCommand(command);
    }

    private static boolean logContains(Path output, String token) {
        try {
            return Files.readString(output).contains(token);
        } catch (java.io.IOException unavailable) {
            return false;
        }
    }

    private static int logOccurrences(Path output, String token) {
        try {
            return Files.readString(output).split(java.util.regex.Pattern.quote(token), -1).length - 1;
        } catch (java.io.IOException unavailable) {
            return 0;
        }
    }

    private static String customer(MongoEndpoints mongo, EndpointAddress target) {
        List<Document> rows = mongo.documents(target, "orders");
        return rows.isEmpty() ? null : rows.getFirst().getString("customer");
    }

    private static void seed(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, customer) VALUES (1, 'before')");
        }
    }

    private static void update(Map<String, Object> mysql, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE orders SET customer = '" + customer + "' WHERE id = 1");
        }
    }

    private static Map<String, String> resources(Map<String, Object> mysql, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: fault_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"),
                mysql.get("username"), mysql.get("password")));
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: fault_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: fault_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: fault_target
                """.formatted(PIPELINE));
        return resources;
    }
}
