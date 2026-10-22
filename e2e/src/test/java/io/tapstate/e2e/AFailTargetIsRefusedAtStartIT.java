package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A target set to refuse the rows it already holds refuses the start itself, before anything is written.
 *
 * <p>A full load into a target whose {@code on_full_load} is {@code fail} used to be accepted and fail later:
 * the start answered that the pipeline was starting, and the run went {@code FAILED} once it reached the
 * target. The start now looks at the target first, so a start that could only fail is refused with a code a
 * caller can act on, the pipeline's intent is never written, and the target holds exactly what it held.
 *
 * <p>Looking needs a target the product can read rows from. The sync case writes through the harness's own
 * connector packaged under the one browsable connector's id, for the reason the data browser's cases do; the
 * view is materialized into MongoDB through the real connector, gated like every case that needs it.
 *
 * <p>Java rather than a declarative example, and the reason is a missing word: the claim is that a start
 * is refused, and with which code, before anything runs, and the specification vocabulary can say what a
 * pipeline did once it ran but not that a verb was refused.
 */
class AFailTargetIsRefusedAtStartIT {

    private static final String TABLE = "orders";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "fail_at_start";
    private static final List<Long> UNRELATED_IDS = List.of(101L, 102L, 103L, 104L, 105L);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aSyncTargetSetToFailThatHoldsRowsRefusesTheStartAndKeepsThem(@TempDir Path directory) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("fail_at_start_sync_store");
        EndpointAddress source = EndpointAddress.uri(Files.createDirectories(directory.resolve("src")).toString());
        EndpointAddress target = EndpointAddress.uri(Files.createDirectories(directory.resolve("tgt")).toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(source, TABLE, SeedRows.generated(3));
        files.seed(target, TABLE, unrelatedRows());

        try (ServerHandle server = InProcessServer.start(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            String connector = E2eConnectorJar.BROWSABLE_CONNECTOR_ID;
            control.registerConnector(connector, Files.readAllBytes(E2eConnectorJar.buildInto(directory, connector)));
            control.discoverSchema("src_file", connector, source.settings());
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_file.tap.yml", fileYaml("src_file", connector, source, true));
            resources.put("tgt_file.tap.yml", fileYaml("tgt_file", connector, target, false));
            resources.put(PIPELINE + ".tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_file
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: %s
                      sync:
                        - source: tgt_file
                          on_full_load: fail
                    """.formatted(PIPELINE, TABLE));
            control.apply(resources);

            ControlPlane.StartAnswer answer = control.start(PIPELINE, List.of(), null);

            assertRefusedOver(answer, "target-not-empty/tgt_file/" + TABLE);
            // The intent is written by the start itself, before it answers. Not there now means never: a
            // start that wrote it and then refused would leave a converger to run what was refused.
            assertThat(documents.holds(MongoStorePort.PIPELINE_DESIRED, PIPELINE))
                    .as("the intent of a start that was refused")
                    .isFalse();
            assertThat(files.count(target, TABLE)).as("rows at the target after the refusal").isEqualTo(5);
            for (long id : UNRELATED_IDS) {
                assertThat(files.fetch(target, TABLE, Map.of("id", id))).as("the target's own row %s", id).isPresent();
            }
            assertThat(files.fetch(target, TABLE, Map.of("id", 1L))).as("a source row at the target").isEmpty();
        }
    }

    @Test
    void aViewSetToFailThatHoldsRowsRefusesTheStartAndKeepsThem(@TempDir Path directory) throws Exception {
        RealConnectorGate.require("mongodb");
        String storeUri = SharedMongo.replicaSetUrl("fail_at_start_view_store");
        EndpointAddress views = EndpointAddress.uri(SharedMongo.replicaSetUrl("fail_at_start_view_views"));

        try (ServerHandle server = InProcessServer.start(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = viewOverUnrelatedDocuments(server, mongo, directory, views, "fail");

            ControlPlane.StartAnswer answer = control.start(PIPELINE, List.of(), null);

            assertRefusedOver(answer, "target-not-empty/views/" + VIEW);
            assertThat(documents.holds(MongoStorePort.PIPELINE_DESIRED, PIPELINE))
                    .as("the intent of a start that was refused")
                    .isFalse();
            assertThat(mongo.documents(views, VIEW))
                    .as("the view's documents after the refusal")
                    .extracting(document -> ((Number) document.get("_id")).longValue())
                    .containsExactlyInAnyOrderElementsOf(UNRELATED_IDS);
        }
    }

    /**
     * The contrast with refusing: a view set to clear is not asked about, because clearing is what its setting
     * already says the full load does, and the collection ends up holding the source's rows and nothing else.
     */
    @Test
    void aViewSetToClearThatHoldsRowsStartsAndHoldsOnlyTheSourcesRows(@TempDir Path directory) throws Exception {
        RealConnectorGate.require("mongodb");
        String storeUri = SharedMongo.replicaSetUrl("fail_at_start_view_clear_store");
        EndpointAddress views = EndpointAddress.uri(SharedMongo.replicaSetUrl("fail_at_start_view_clear_views"));

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = viewOverUnrelatedDocuments(server, mongo, directory, views, "clear");

            ControlPlane.StartAnswer answer = control.start(PIPELINE, List.of(), null);

            assertThat(answer.status()).as("the start's status, answered %s", answer.body()).isEqualTo(200);
            // The source's rows arrive as text and the seeded documents carry numbers, so the set of ids is
            // the source's three exactly when the seeded five are gone and every source row has landed.
            Await.until("the view to hold exactly the source's rows",
                    () -> viewIds(mongo, views).equals(Set.of("1", "2", "3")),
                    () -> "ids=" + viewIds(mongo, views));
        }
    }

    /**
     * Seeds the view's collection with the unrelated documents, then applies a view pipeline over the source's
     * three rows whose {@code on_full_load} is {@code policy}.
     */
    private static ControlPlane viewOverUnrelatedDocuments(
            ServerHandle server, MongoEndpoints mongo, Path directory, EndpointAddress views, String policy)
            throws Exception {
        EndpointAddress source = EndpointAddress.uri(Files.createDirectories(directory.resolve("src")).toString());
        new FileEndpoints().seed(source, TABLE, SeedRows.generated(3));
        for (long id : UNRELATED_IDS) {
            mongo.insert(views, VIEW, new Document("_id", id).append("id", id).append("seq", id));
        }
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        control.discoverSchema("src_file", E2eConnectorJar.CONNECTOR_ID, source.settings());
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_file.tap.yml", fileYaml("src_file", E2eConnectorJar.CONNECTOR_ID, source, true));
        resources.put("views.tap.yml", """
                version: tapstate/v1
                kind: source
                id: views
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(views.text("uri")));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_file
                settings: { read_mode: snapshot_and_cdc }
                view:
                  id: %s
                  from: %s
                  primary_key: id
                  on_full_load: %s
                  storage: { warm: { collection: %s } }
                """.formatted(PIPELINE, VIEW, TABLE, policy, VIEW));
        control.apply(resources);
        return control;
    }

    /** The {@code id} of every document in the view, as text. */
    private static Set<String> viewIds(MongoEndpoints mongo, EndpointAddress views) {
        return mongo.documents(views, VIEW).stream()
                .map(document -> String.valueOf(document.get("id")))
                .collect(Collectors.toSet());
    }

    /** A synchronous refusal that names the target and is not a question: no answer changes it. */
    private static void assertRefusedOver(ControlPlane.StartAnswer answer, String findingKey) {
        assertThat(answer.status()).as("the start's status, answered %s", answer.body()).isEqualTo(409);
        assertThat(answer.body().get("code")).isEqualTo("lifecycle.start-blocked");
        assertThat(answer.findings())
                .filteredOn(finding -> findingKey.equals(finding.get("key")))
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.get("behavior")).isEqualTo("BLOCK");
                    assertThat(finding.get("actions")).asList().isEmpty();
                    assertThat(((Map<?, ?>) finding.get("params")).get("rows")).isEqualTo(5L);
                });
    }

    private static List<Map<String, Object>> unrelatedRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (long id : UNRELATED_IDS) {
            rows.add(Map.of("id", id, "seq", id));
        }
        return rows;
    }

    private static String fileYaml(String id, String connector, EndpointAddress address, boolean readFrom) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(id, connector, address.text("uri"))
                + (readFrom ? "mode: cdc\ntables: [ " + TABLE + " ]\n" : "");
    }
}
