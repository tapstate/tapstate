package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Measures REST pause and resume over a real snapshot-and-CDC connector run. */
class RealLifecycleCallLatencyIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.lifecycle-latency.jar";
    private static final String PIPELINE = "lifecycle_latency";
    private static final String SOURCE = "lifecycle_latency_source";
    private static final String TARGET = "lifecycle_latency_target";
    private static final String TABLE = "orders";
    private static final int CYCLES = 5;
    private static final Duration WAIT = Duration.ofMinutes(2);

    @BeforeAll
    static void requireDockerConnectorsAndReferenceJar() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        String configured = System.getProperty(BOOT_JAR_PROPERTY);
        Assumptions.assumeTrue(configured != null && !configured.isBlank(),
                "no -D" + BOOT_JAR_PROPERTY + ": skipping an explicit-JAR lifecycle measurement");
        assertThat(Files.isRegularFile(Path.of(configured))).as("the configured boot JAR exists").isTrue();
    }

    @Test
    void pauseAndResumeCallsAreMeasuredWhileTheRealPipelineMovesChanges() throws Exception {
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        Map<String, Object> mysql = SharedMySql.settings("lifecycle_latency_source_db");
        seed(mysql);
        String storeUri = SharedMongo.replicaSetUrl("lifecycle_latency_store");
        String targetUri = SharedMongo.replicaSetUrl("lifecycle_latency_target_db");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                RealProcessServer server = RealProcessServer.start(storeUri, jar)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("lifecycle-latency", "lifecycle-latency-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(mysql, targetUri));
            control.discoverSchema(SOURCE, "mysql", mysql);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            awaitCustomer(mongo, target, "snapshot", "the initial snapshot");
            update(mysql, "cdc-before-cycles");
            awaitCustomer(mongo, target, "cdc-before-cycles", "CDC before the timed calls");

            List<Call> pauses = new ArrayList<>();
            List<Call> resumes = new ArrayList<>();
            String previous = "cdc-before-cycles";
            for (int i = 1; i <= CYCLES; i++) {
                pauses.add(measure(control, LifecycleVerb.PAUSE, PipelineState.PAUSED, i));
                String pausedValue = "changed-while-paused-" + i;
                update(mysql, pausedValue);
                assertThat(customer(mongo, target))
                        .as("the paused pipeline has not delivered the new CDC value")
                        .isEqualTo(previous);

                resumes.add(measure(control, LifecycleVerb.RESUME, PipelineState.RUNNING, i));
                awaitCustomer(mongo, target, pausedValue, "CDC after resume " + i);
                previous = pausedValue;
            }

            System.out.printf("lifecycle-call-fixture jar=%s version=%s source=mysql:8.0"
                            + " target=mongo:7.0 readMode=snapshot_and_cdc cycles=%d%n",
                    jar, control.version(), CYCLES);
            report("pause", pauses);
            report("resume", resumes);
        }
    }

    private static Call measure(ControlPlane control, LifecycleVerb verb,
            PipelineState expected, int cycle) {
        long started = System.nanoTime();
        control.lifecycle(PIPELINE, verb);
        long returned = System.nanoTime();
        Optional<PipelineState> firstStatus = control.state(PIPELINE);
        awaitState(control, expected);
        long settled = System.nanoTime();
        Call call = new Call(cycle, returned - started, settled - returned,
                firstStatus.orElse(null), firstStatus.isEmpty() || firstStatus.get() != expected);
        System.out.printf("lifecycle-call verb=%s cycle=%d httpMs=%.3f"
                        + " firstStatusAfterReturn=%s returnedBeforeActual=%s settledAfterReturnMs=%.3f%n",
                verb.id(), cycle, millis(call.httpNanos()), call.firstStatusAfterReturn(),
                call.returnedBeforeActual(), millis(call.settledAfterReturnNanos()));
        return call;
    }

    private static void report(String verb, List<Call> calls) {
        List<Long> sorted = calls.stream().map(Call::httpNanos).sorted().toList();
        List<Long> observed = calls.stream().map(Call::settledAfterReturnNanos).sorted().toList();
        long beforeActual = calls.stream().filter(Call::returnedBeforeActual).count();
        System.out.printf("lifecycle-call-summary verb=%s count=%d p50Ms=%.3f p95Ms=%.3f maxMs=%.3f"
                        + " observedAfterReturnP50Ms=%.3f observedAfterReturnP95Ms=%.3f"
                        + " observedAfterReturnMaxMs=%.3f returnedBeforeActual=%d%n",
                verb, calls.size(), millis(sorted.get(sorted.size() / 2)),
                millis(sorted.get(sorted.size() - 1)), millis(sorted.getLast()),
                millis(observed.get(observed.size() / 2)), millis(observed.get(observed.size() - 1)),
                millis(observed.getLast()), beforeActual);
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static void awaitState(ControlPlane control, PipelineState expected) {
        Await.until("pipeline to become " + expected, WAIT,
                () -> control.state(PIPELINE).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }

    private static void awaitCustomer(MongoEndpoints mongo, EndpointAddress target,
            String expected, String description) {
        Await.until(description, WAIT, () -> expected.equals(customer(mongo, target)),
                () -> String.valueOf(customer(mongo, target)));
    }

    private static String customer(MongoEndpoints mongo, EndpointAddress target) {
        List<Document> documents = mongo.documents(target, TABLE);
        return documents.isEmpty() ? null : documents.getFirst().getString("customer");
    }

    private static Map<String, String> resources(Map<String, Object> mysql, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """.formatted(SOURCE, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                mysql.get("username"), mysql.get("password")));
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(TARGET, targetUri));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE, TARGET));
        return resources;
    }

    private static void seed(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, customer) VALUES (1, 'snapshot')");
        }
    }

    private static void update(Map<String, Object> mysql, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE orders SET customer = '" + customer + "' WHERE id = 1");
        }
    }

    private record Call(int cycle, long httpNanos, long settledAfterReturnNanos,
            PipelineState firstStatusAfterReturn, boolean returnedBeforeActual) {}
}
