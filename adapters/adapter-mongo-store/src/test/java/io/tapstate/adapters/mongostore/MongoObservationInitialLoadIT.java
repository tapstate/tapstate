package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;

/** A real BSON round trip preserves unknown counts and replaces the prior run's display facts. */
@RequiresDocker
class MongoObservationInitialLoadIT {
    @Container
    private static final MongoDBContainer MONGO = container();

    private static MongoDBContainer container() {
        var container = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"));
        String owner = System.getenv("TAPSTATE_TEST_RESOURCE_OWNER");
        if (owner != null && !owner.isBlank()) container.withLabel("io.tapstate.issue-loop.owner", owner);
        return container;
    }

    @Test
    void newRunReplacesOldFactsAndLegacyDocumentsRemainUnknown() {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var collection = client.getDatabase("initial_load_observation_it").getCollection("pipeline_observation");
            var store = new MongoObservationStore(collection);
            var at = Instant.parse("2026-10-28T09:00:00Z");
            var load = new InitialLoad("execution:p:2:at", at, "snapshot_and_cdc", Map.of(
                    "orders", new InitialLoad.Table(InitialLoad.Stage.READING, 3L, null)));
            var first = new Observation("p", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, at, List.of(), load);
            store.save(first);
            assertThat(store.read("p")).contains(first);
            var next = new Observation("p", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, at.plusSeconds(1), List.of(),
                    new InitialLoad("execution:p:3:next", at.plusSeconds(1), "snapshot_and_cdc", Map.of(
                            "orders", new InitialLoad.Table(InitialLoad.Stage.PENDING, null, null))));
            store.save(next);
            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(store.read("p")).contains(next);
            collection.replaceOne(new Document("_id", "p"), new Document("_id", "p").append("state", "RUNNING"));
            assertThat(store.read("p").orElseThrow().initialLoad()).isNull();
        }
    }
}
