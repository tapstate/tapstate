package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
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

    private static void configureEventFault(MongoClient admin, boolean enabled) {
        configureFault(admin, enabled, "pipeline_events", "insert");
    }

    private static void configureHistoryFault(MongoClient admin, boolean enabled) {
        configureFault(admin, enabled, "pipeline_rate_history", "insert");
    }

    private static void configureFault(MongoClient admin, boolean enabled,
            String collection, String commandName) {
        Document command = new Document("configureFailPoint", "failCommand")
                .append("mode", enabled ? "alwaysOn" : "off");
        if (enabled) {
            command.append("data", new Document("failCommands", List.of(commandName))
                    .append("appName", STORE_APP)
                    .append("namespace", DATABASE + "." + collection)
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
