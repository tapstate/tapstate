package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A stored artifact this build cannot read is isolated from tolerant read-only listings but still makes
 * safety checks that need its references fail closed.
 *
 * <p>The unreadable row is made in the real Mongo store after its surrounding workspace is accepted over
 * HTTP. The list, deletion and live source edit are then driven over HTTP, so these cases cover both
 * inventory policies against the real stored shape. One tier is enough: the behavior under test sits
 * between the HTTP controller and the same real store used by both launchers, not at the process boundary.
 */
class UnreadableArtifactIsolationIT {

    private static final String READABLE_ID = "readable_pipeline";
    private static final String UNREADABLE_ID = "unreadable_pipeline";
    private static final String SOURCE_ID = "referenced_source";
    private static final String REFERRER_ID = "unreadable_referrer";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void anUnreadableStoredArtifactDoesNotHideAReadablePipeline() {
        String storeUri = SharedMongo.replicaSetUrl("unreadable_artifact_isolation");
        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoClient store = MongoClients.create(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.apply(Map.of(
                    "readable.tap.yml", pipeline(READABLE_ID),
                    "unreadable.tap.yml", pipeline(UNREADABLE_ID)));

            makeBodyUnreadable(store, storeUri, UNREADABLE_ID);

            assertThat(pipelineIds(control.pipelines())).containsExactly(READABLE_ID);
        }
    }

    @Test
    void anUnreadableReferencingPipelineCannotLetItsSourceBeDeleted(@TempDir Path directory)
            throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("unreadable_artifact_reference");
        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoClient store = MongoClients.create(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(referencingWorkspace());
            ControlPlane.StoredArtifact source = control.artifact(SOURCE_ID).orElseThrow();

            makeFieldUnreadable(store, storeUri, REFERRER_ID, "body.settings");

            assertThatThrownBy(() -> control.deleteArtifact(SOURCE_ID, source.contentHash()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("got 500")
                    .hasMessageContaining("io.document-unreadable");
            assertThat(control.artifact(SOURCE_ID)).contains(source);
        }
    }

    @Test
    void anUnreadableRunningPipelineCannotLetItsSourceBufferingChange(@TempDir Path directory)
            throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("unreadable_live_pipeline");
        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoClient store = MongoClients.create(storeUri)) {
            RunningPipeline running = RunningPipeline.started(server, directory);
            ControlPlane control = running.control();
            ControlPlane.StoredArtifact source = control.artifact(running.sourceId()).orElseThrow();

            makeBodyUnreadable(store, storeUri, running.pipelineId());
            String replacement = source.canonicalForm() + """
                    srs:
                      enabled: false
                    """;

            assertThatThrownBy(() -> control.apply(Map.of("source.tap.yml", replacement)))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("got 500")
                    .hasMessageContaining("io.document-unreadable");
            assertThat(control.artifact(running.sourceId())).contains(source);
        }
    }

    private static String pipeline(String id) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: []
                """.formatted(id);
    }

    private static Map<String, String> referencingWorkspace() {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "/tmp/%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, SOURCE_ID));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { type: filter, from: [orders], expr: "op != 'd'" }
                serve:
                  from: orders
                  sync:
                    - source: %s
                """.formatted(REFERRER_ID, SOURCE_ID, SOURCE_ID));
        return resources;
    }

    private static void makeBodyUnreadable(MongoClient store, String storeUri, String id) {
        makeFieldUnreadable(store, storeUri, id, "body");
    }

    private static void makeFieldUnreadable(
            MongoClient store, String storeUri, String id, String field) {
        String database = new ConnectionString(storeUri).getDatabase();
        if (database == null) {
            throw new AssertionError("the store URI names no database");
        }
        long matched = store.getDatabase(database)
                .getCollection(MongoStorePort.ARTIFACTS)
                .updateOne(
                        new Document("_id", id),
                        new Document("$set", new Document(field, "unreadable")))
                .getMatchedCount();
        assertThat(matched).as("the stored artifact selected for corruption").isEqualTo(1L);
    }

    private static List<String> pipelineIds(String body) {
        if (!(JsonReader.parse(body) instanceof Map<?, ?> response)
                || !(response.get("items") instanceof List<?> items)) {
            throw new AssertionError("the pipeline list carried no items: " + body);
        }
        List<String> ids = new ArrayList<>(items.size());
        for (Object item : items) {
            if (!(item instanceof Map<?, ?> pipeline) || !(pipeline.get("id") instanceof String id)) {
                throw new AssertionError("a pipeline list entry carried no id: " + body);
            }
            ids.add(id);
        }
        return List.copyOf(ids);
    }
}
