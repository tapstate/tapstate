package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A start after a stop that cleared asks before its new full load goes over rows the source has since deleted.
 *
 * <p>The reported sequence: a pipeline loads five rows, is stopped with its state cleared, two rows are deleted
 * at the source while it is down, and it is started again. The new full load copies the three rows the source
 * still holds and cannot see the two it no longer does, so a target that is not cleared first keeps them, with
 * nothing anywhere saying so. The start now stops and asks, naming the target and how many rows it holds, and
 * goes ahead only with an answer: keep the rows, or clear the target first, which also records
 * {@code on_full_load: clear} on the element that writes it.
 *
 * <p>Each case changes one source row while the pipeline is down as well, so a reading taken after the start
 * can tell the new full load happened from one that has not got there yet. The sync target is written through
 * the harness's own connector packaged under the one browsable connector's id, for the reason the data
 * browser's cases do, and that connector clears a target when told to; the view is materialized into MongoDB
 * through the real connector, gated like every case that needs it.
 *
 * <p>Java rather than a declarative example, and the reason is a missing word: the specification's start
 * step takes no answers, and the claim is about the start that carries them - as well as about the start
 * refused before it, which the vocabulary cannot say either.
 */
class AStartAfterStopYAsksBeforeKeepingDeletedRowsIT {

    private static final String TABLE = "orders";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "asks_after_stop";
    private static final long SEEDED = 5;
    private static final long CHANGED_SEQ = 100;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aSyncTargetKeepsTheRowsItWasToldToKeepAndTheStartRecordsTheAnswer(@TempDir Path directory)
            throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("asks_after_stop_sync_keep_store");
        try (ServerHandle server = InProcessServer.start(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            SyncScene scene = SyncScene.loadedAndStopped(server, directory);

            Map<String, Object> question = askedOnceTheSourceDeletedRows(
                    scene.control(), scene.source(), scene.files(), "target-not-empty/tgt_file/" + TABLE);
            assertThat(scene.control().start(PIPELINE, List.of(answer(question, "keep")), hashOf(question))
                    .status()).isEqualTo(200);

            awaitTheNewFullLoad(id -> scene.files().fetch(scene.target(), TABLE, Map.of("id", id)));
            // The rows the source no longer holds stay: that is what keeping meant, said before it was chosen.
            assertThat(scene.files().count(scene.target(), TABLE)).as("rows at the target").isEqualTo(SEEDED);
            assertThat(scene.files().fetch(scene.target(), TABLE, Map.of("id", 4L))).isPresent();
            assertThat(scene.files().fetch(scene.target(), TABLE, Map.of("id", 5L))).isPresent();
            assertAnswerAudited(documents, question, "keep");
        }
    }

    @Test
    void aSyncTargetClearedByTheAnswerHoldsExactlyWhatTheSourceHolds(@TempDir Path directory) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("asks_after_stop_sync_clear_store");
        try (ServerHandle server = InProcessServer.start(storeUri)) {
            SyncScene scene = SyncScene.loadedAndStopped(server, directory);

            Map<String, Object> question = askedOnceTheSourceDeletedRows(
                    scene.control(), scene.source(), scene.files(), "target-not-empty/tgt_file/" + TABLE);
            assertThat(scene.control().start(PIPELINE, List.of(answer(question, "clear")), hashOf(question))
                    .status()).isEqualTo(200);

            // The answer is recorded on the element that writes the target, so the next new full load clears it
            // without asking again.
            assertThat(scene.control().artifact(PIPELINE).orElseThrow().canonicalForm())
                    .as("the pipeline's definition after the answer")
                    .contains("on_full_load: clear");
            Await.until("the target to hold exactly what the source holds", () ->
                            scene.files().count(scene.target(), TABLE) == SEEDED - 2
                                    && scene.files().fetch(scene.target(), TABLE, Map.of("id", 1L))
                                    .map(row -> seq(row) == CHANGED_SEQ).orElse(false),
                    () -> "rows=" + scene.files().count(scene.target(), TABLE));
            assertThat(scene.files().fetch(scene.target(), TABLE, Map.of("id", 4L))).isEmpty();
            assertThat(scene.files().fetch(scene.target(), TABLE, Map.of("id", 5L))).isEmpty();
        }
    }

    @Test
    void aViewKeepsTheRowsItWasToldToKeepAndTheStartRecordsTheAnswer(@TempDir Path directory) throws Exception {
        RealConnectorGate.require("mongodb");
        String storeUri = SharedMongo.replicaSetUrl("asks_after_stop_view_store");
        try (ServerHandle server = InProcessServer.start(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ViewScene scene = ViewScene.loadedAndStopped(server, mongo, directory,
                    SharedMongo.replicaSetUrl("asks_after_stop_view_views"));

            Map<String, Object> question = askedOnceTheSourceDeletedRows(
                    scene.control(), scene.source(), scene.files(), "target-not-empty/views/" + VIEW);
            assertThat(scene.control().start(PIPELINE, List.of(answer(question, "keep")), hashOf(question))
                    .status()).isEqualTo(200);

            awaitTheNewFullLoad(id -> viewRow(mongo, scene.views(), id));
            assertThat(mongo.count(scene.views(), VIEW)).as("documents in the view").isEqualTo(SEEDED);
            assertThat(viewRow(mongo, scene.views(), 4L)).isPresent();
            assertThat(viewRow(mongo, scene.views(), 5L)).isPresent();
            assertAnswerAudited(documents, question, "keep");
        }
    }

    @Test
    void aViewClearedByTheAnswerHoldsExactlyWhatTheSourceHolds(@TempDir Path directory) throws Exception {
        RealConnectorGate.require("mongodb");
        String storeUri = SharedMongo.replicaSetUrl("asks_after_stop_view_clear_store");
        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ViewScene scene = ViewScene.loadedAndStopped(server, mongo, directory,
                    SharedMongo.replicaSetUrl("asks_after_stop_view_clear_views"));

            Map<String, Object> question = askedOnceTheSourceDeletedRows(
                    scene.control(), scene.source(), scene.files(), "target-not-empty/views/" + VIEW);
            assertThat(scene.control().start(PIPELINE, List.of(answer(question, "clear")), hashOf(question))
                    .status()).isEqualTo(200);

            assertThat(scene.control().artifact(PIPELINE).orElseThrow().canonicalForm())
                    .as("the pipeline's definition after the answer")
                    .contains("on_full_load: clear");
            // Three documents with the changed row among them is the new full load into a cleared collection;
            // one that kept the old documents stays at five, with the deleted rows still there.
            Await.until("the view to hold exactly what the source holds", () ->
                            mongo.count(scene.views(), VIEW) == SEEDED - 2
                                    && viewRow(mongo, scene.views(), 1L)
                                    .map(row -> seq(row) == CHANGED_SEQ).orElse(false),
                    () -> "documents=" + mongo.count(scene.views(), VIEW));
            assertThat(viewRow(mongo, scene.views(), 4L)).isEmpty();
            assertThat(viewRow(mongo, scene.views(), 5L)).isEmpty();
        }
    }

    /** One sync case's endpoints and session, with the pipeline loaded once and then stopped with its state cleared. */
    private record SyncScene(ControlPlane control, FileEndpoints files, EndpointAddress source, EndpointAddress target) {

        static SyncScene loadedAndStopped(ServerHandle server, Path directory) throws Exception {
            EndpointAddress source = EndpointAddress.uri(Files.createDirectories(directory.resolve("src")).toString());
            EndpointAddress target = EndpointAddress.uri(Files.createDirectories(directory.resolve("tgt")).toString());
            FileEndpoints files = new FileEndpoints();
            files.seed(source, TABLE, SeedRows.generated(SEEDED));

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
                    """.formatted(PIPELINE, TABLE));
            control.apply(resources);
            loadThenStopClearing(control, () -> files.count(target, TABLE));
            return new SyncScene(control, files, source, target);
        }
    }

    /** One view case's endpoints and session, with the pipeline loaded once and then stopped with its state cleared. */
    private record ViewScene(ControlPlane control, FileEndpoints files, EndpointAddress source, EndpointAddress views) {

        static ViewScene loadedAndStopped(ServerHandle server, MongoEndpoints mongo, Path directory, String viewUri)
                throws Exception {
            EndpointAddress views = EndpointAddress.uri(viewUri);
            EndpointAddress source = EndpointAddress.uri(Files.createDirectories(directory.resolve("src")).toString());
            FileEndpoints files = new FileEndpoints();
            files.seed(source, TABLE, SeedRows.generated(SEEDED));

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
                    """.formatted(viewUri));
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
                      storage: { warm: { collection: %s } }
                    """.formatted(PIPELINE, VIEW, TABLE, VIEW));
            control.apply(resources);
            loadThenStopClearing(control, () -> mongo.count(views, VIEW));
            return new ViewScene(control, files, source, views);
        }
    }

    /** Starts the pipeline over an empty target, waits for every seeded row, then stops it clearing its state. */
    private static void loadThenStopClearing(ControlPlane control, java.util.function.LongSupplier rowsAtTarget) {
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        Await.until("the first full load of the seeded rows", () -> rowsAtTarget.getAsLong() == SEEDED,
                () -> "rows=" + rowsAtTarget.getAsLong());
        control.stop(PIPELINE, true);
        Await.until(PIPELINE + " to reach " + PipelineState.STOPPED,
                () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }

    /**
     * Deletes two rows and changes one at the source while the pipeline is down, starts it without an answer,
     * and returns the one question that start was stopped with: the target, the rows it holds, both answers.
     */
    private static Map<String, Object> askedOnceTheSourceDeletedRows(
            ControlPlane control, EndpointAddress source, FileEndpoints files, String findingKey) {
        files.delete(source, TABLE, Map.of("id", 4L));
        files.delete(source, TABLE, Map.of("id", 5L));
        files.update(source, TABLE, Map.of("id", 1L), Map.of("seq", CHANGED_SEQ));

        ControlPlane.StartAnswer asked = control.start(PIPELINE, List.of(), null);

        assertThat(asked.status()).as("the bare start, answered %s", asked.body()).isEqualTo(409);
        assertThat(asked.body().get("code")).isEqualTo("lifecycle.start-needs-confirmation");
        Map<String, Object> question = asked.findings().stream()
                .filter(finding -> findingKey.equals(finding.get("key")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + findingKey + " in " + asked.findings()));
        assertThat(question.get("behavior")).isEqualTo("CONFIRM");
        assertThat(((Map<?, ?>) question.get("params")).get("rows")).as("rows the question says the target holds")
                .isEqualTo(SEEDED);
        assertThat(((List<?>) question.get("actions")).stream()
                .map(action -> String.valueOf(((Map<?, ?>) action).get("id")))
                .toList())
                .as("the answers the question offers")
                .containsExactlyInAnyOrder("clear", "keep");
        assertThat(control.state(PIPELINE)).as("the pipeline while it is asked").contains(PipelineState.STOPPED);
        Map<String, Object> carried = new LinkedHashMap<>(question);
        carried.put("contentHash", asked.startChecks().get("contentHash"));
        return carried;
    }

    /** Waits for the changed row to arrive, which only the new full load carries. */
    private static void awaitTheNewFullLoad(Function<Long, Optional<Map<String, Object>>> rowById) {
        Await.until("the new full load to carry the row changed while the pipeline was down",
                () -> rowById.apply(1L).map(row -> seq(row) == CHANGED_SEQ).orElse(false),
                () -> String.valueOf(rowById.apply(1L)));
    }

    private static void assertAnswerAudited(StoreDocuments documents, Map<String, Object> question, String action) {
        List<Document> starts = documents.auditOf("pipeline.start", PIPELINE);
        assertThat(starts).as("the audit records of the pipeline's starts").isNotEmpty();
        Document detail = starts.getLast().get("detail", Document.class);
        assertThat(detail).as("what the last start recorded").isNotNull();
        assertThat(detail.getList("decisions", Document.class))
                .extracting(decision -> decision.getString("finding") + "=" + decision.getString("action"))
                .containsExactly(question.get("key") + "=" + action);
    }

    private static Map<String, Object> answer(Map<String, Object> question, String action) {
        return Map.of("finding", question.get("key"), "action", action);
    }

    private static String hashOf(Map<String, Object> question) {
        return (String) question.get("contentHash");
    }

    /**
     * The view's document for {@code id}, matched on the value's text: the harness connector declares its
     * columns as text, so what lands in the view is the text of the number the file holds.
     */
    private static Optional<Map<String, Object>> viewRow(MongoEndpoints mongo, EndpointAddress views, long id) {
        return mongo.documents(views, VIEW).stream()
                .filter(document -> String.valueOf(id).equals(String.valueOf(document.get("id"))))
                .findFirst()
                .<Map<String, Object>>map(LinkedHashMap::new);
    }

    private static long seq(Map<String, Object> row) {
        return Long.parseLong(String.valueOf(row.get("seq")));
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
