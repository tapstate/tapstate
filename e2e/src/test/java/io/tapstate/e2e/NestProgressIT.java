package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code nest progress} says what a step is waiting for, not just how many rows went by.
 *
 * <p>A thousand lines load while the orders table is held unread. The step cannot render a single document
 * without its root rows, so the target stays empty however many lines arrive; what a person needs to be told
 * is that the step is waiting on {@code orders}. A progress read that reported only row counts would show the
 * thousand lines and leave the stall unexplained. Releasing the hold then lets the step finish and say so.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dit.test=NestProgressIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestProgressIT {

    private static final String PIPELINE = "nest_progress";
    private static final String STEP = "order_doc";
    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final Duration CONVERGE = Duration.ofMinutes(3);
    private static final int LINES = 1_000;

    @BeforeAll
    static void requireDockerAndMongoConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void aStepHeldByItsRootTableNamesItAndCountsTheLinesRead(@TempDir Path temporary) throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        // Written as whole tables: the seeded shape is exactly id and seq, and the join needs order_id.
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id\n" + LongStream.rangeClosed(1, 10)
                .mapToObj(id -> id + "\n").collect(Collectors.joining()));
        FileEndpoints.replaceTable(source.resolve("items.csv"), "id,order_id\n" + LongStream.rangeClosed(1, LINES)
                .mapToObj(id -> id + "," + (1 + id % 10) + "\n").collect(Collectors.joining()));
        Holds holds = new Holds(temporary.resolve("holds"));
        holds.read("orders");
        String target = SharedMongo.replicaSetUrl("nest_progress_target");
        try (PreparedNativeStore storage = PreparedNativeStore.create(USER, PASSWORD);
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(workspace(source, holds.directory(), target));
            for (String table : List.of("orders", "items")) {
                control.discoverSchema("src_" + table, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", source.toString(), "hold", holds.directory().toString()));
            }
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            AtomicReference<CliOnce.Run> last = new AtomicReference<>();
            Await.until("progress to report every line read and the step waiting on orders", CONVERGE, () -> {
                CliOnce.Run run = progress(server);
                last.set(run);
                return run.exitCode() == 0
                        && run.stdout().contains("step " + STEP + ": loading; waiting for orders to be read")
                        && run.stdout().lines().anyMatch(line -> line.trim().startsWith("items ")
                                && line.contains(" " + LINES));
            }, () -> last.get() == null ? "no run yet" : last.get().stdout() + last.get().stderr());
            assertThat(mongo.documents(EndpointAddress.uri(target), "orders"))
                    .as("no document can go out while its root rows are held").isEmpty();
            assertThat(last.get().stdout()).contains("orders  (root)");

            holds.releaseRead("orders");
            Await.until("the step to finish its load and say so", CONVERGE, () -> {
                CliOnce.Run run = progress(server);
                last.set(run);
                return run.stdout().contains("step " + STEP + ": load complete; applying changes as they arrive");
            }, () -> last.get().stdout() + last.get().stderr());
            assertThat(mongo.documents(EndpointAddress.uri(target), "orders")).hasSize(10);
        } finally {
            holds.releaseRead("orders");
        }
    }

    private static CliOnce.Run progress(ServerHandle server) {
        return CliOnce.runSession(PASSWORD, "nest progress " + PIPELINE + " " + STEP + "\nexit\n",
                "-c", server.baseUrl().toString(), "-u", USER);
    }

    private static Map<String, String> workspace(Path source, Path holds, String target) {
        Map<String, String> resources = new LinkedHashMap<>();
        for (String table : List.of("orders", "items")) {
            resources.put("src_" + table + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_%s
                    connector: %s
                    config: { uri: "%s", hold: "%s" }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(table, E2eConnectorJar.CONNECTOR_ID, source, holds, table));
        }
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: nest_progress_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: %s
                    type: nest
                    from: { o: orders, i: items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: %s
                  sync:
                    - source: nest_progress_target
                """.formatted(PIPELINE, STEP, STEP));
        return resources;
    }
}
