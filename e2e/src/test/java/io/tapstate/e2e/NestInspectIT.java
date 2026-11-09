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
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code nest inspect} reads what the step holds, not what the target shows.
 *
 * <p>Three orders, six lines, one customer, and one line pointing at an order that does not exist. Inspecting
 * a real order lists its two lines as parts and renders the customer it points at; inspecting the missing
 * order's key reports the stray line held under a root row that has not arrived. That second answer is the
 * one that discriminates: the target holds no document for the missing order at all, so an inspect that read
 * the target - or that only rendered documents - would have nothing to say about it.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dit.test=NestInspectIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestInspectIT {

    private static final String PIPELINE = "nest_inspect";
    private static final String STEP = "order_doc";
    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final Duration CONVERGE = Duration.ofMinutes(3);
    private static final long MISSING_ORDER = 99L;

    @BeforeAll
    static void requireDockerAndMongoConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void anOrderListsItsLinesAndAnOrphansParentKeyReportsItHeld(@TempDir Path temporary) throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Path holds = Files.createDirectory(temporary.resolve("holds"));
        EndpointAddress address = EndpointAddress.uri(source.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(address, "orders", List.of(order(1), order(2), order(3)));
        files.seed(address, "items", List.of(item(1, 1), item(2, 1), item(3, 2), item(4, 2), item(5, 3), item(6, 3),
                item(7, MISSING_ORDER)));
        files.seed(address, "customers", List.of(Map.of("id", 1L, "name", "Ann")));
        String target = SharedMongo.replicaSetUrl("nest_inspect_target");
        try (PreparedNativeStore storage = PreparedNativeStore.create(USER, PASSWORD);
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(workspace(source, holds, target));
            for (String table : List.of("orders", "items", "customers")) {
                control.discoverSchema("src_" + table, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", source.toString(), "hold", holds.toString()));
            }
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("every order to reach the target with its two lines and its customer", CONVERGE, () -> {
                List<Document> orders = mongo.documents(EndpointAddress.uri(target), "orders");
                return orders.size() == 3 && orders.stream().allMatch(order ->
                        order.getList("items", Document.class, List.of()).size() == 2
                                && order.get("customer") instanceof Document);
            }, () -> "target=" + mongo.documents(EndpointAddress.uri(target), "orders")
                    + ", state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));

            CliOnce.Run order = inspect(server, "1");
            assertThat(order.exitCode()).as("stdout:%n%s%nstderr:%n%s", order.stdout(), order.stderr()).isZero();
            assertThat(order.stdout())
                    .contains("root key id=1", "root row: present, order ")
                    .contains("[1]", "[2]")
                    .contains("document, as the step last rendered it:")
                    .contains("\"customer\"", "\"Ann\"");
            assertThat(order.stdout().lines().filter(line -> line.trim().startsWith("items ")).count())
                    .as("the order's own two lines, and no other order's").isEqualTo(2);

            CliOnce.Run orphan = inspect(server, String.valueOf(MISSING_ORDER));
            assertThat(orphan.exitCode()).as("stdout:%n%s%nstderr:%n%s", orphan.stdout(), orphan.stderr()).isZero();
            assertThat(orphan.stdout())
                    .contains("root row: not arrived; what is below waits for it")
                    .contains("[7]")
                    .contains("held")
                    .contains("document: none while the root row is absent");
            assertThat(mongo.documents(EndpointAddress.uri(target), "orders"))
                    .as("the target has nothing for the missing order, which is why inspect reads the state")
                    .noneMatch(document -> Long.valueOf(MISSING_ORDER).equals(document.get("id")));

            CliOnce.Run nothing = inspect(server, "404");
            assertThat(nothing.exitCode()).isNotZero();
            assertThat(nothing.stdout() + nothing.stderr()).contains("holds nothing under root key 404");
        }
    }

    private static CliOnce.Run inspect(ServerHandle server, String key) {
        return CliOnce.runSession(PASSWORD, "nest inspect " + PIPELINE + " " + STEP + " " + key + "\nexit\n",
                "-c", server.baseUrl().toString(), "-u", USER);
    }

    private static Map<String, Object> order(long id) {
        return Map.of("id", id, "customer_id", 1L);
    }

    private static Map<String, Object> item(long id, long order) {
        return Map.of("id", id, "order_id", order);
    }

    private static Map<String, String> workspace(Path source, Path holds, String target) {
        Map<String, String> resources = new LinkedHashMap<>();
        for (String table : List.of("orders", "items", "customers")) {
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
                id: nest_inspect_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_items, src_customers ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: %s
                    type: nest
                    from: { o: orders, i: items, c: customers }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                        - { from: c, on: { id: customer_id }, as: object, path: customer }
                serve:
                  from: %s
                  sync:
                    - source: nest_inspect_target
                """.formatted(PIPELINE, STEP, STEP));
        return resources;
    }
}
