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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stored artifact this build cannot read does not hide an unrelated readable pipeline from the
 * control plane's list.
 *
 * <p>The unreadable row is made in the real Mongo store after both pipelines are accepted over HTTP.
 * The list is then read over HTTP as well, so a strict whole-store reconstruction fails this case before
 * it can return the readable pipeline. One tier is enough: the behavior under test sits between the HTTP
 * controller and the same real store used by both launchers, not at the process boundary.
 */
class UnreadableArtifactIsolationIT {

    private static final String READABLE_ID = "readable_pipeline";
    private static final String UNREADABLE_ID = "unreadable_pipeline";

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

    private static String pipeline(String id) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: []
                """.formatted(id);
    }

    private static void makeBodyUnreadable(MongoClient store, String storeUri, String id) {
        String database = new ConnectionString(storeUri).getDatabase();
        if (database == null) {
            throw new AssertionError("the store URI names no database");
        }
        long matched = store.getDatabase(database)
                .getCollection(MongoStorePort.ARTIFACTS)
                .updateOne(
                        new Document("_id", id),
                        new Document("$set", new Document("body", "unreadable")))
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
