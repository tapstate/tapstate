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
    void legacyWriterNeverAddsInitialLoadAndKeepsHistoricalBytesReadable() {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var collection = client.getDatabase("retained_legacy_observation_it").getCollection("pipeline_observation");
            var store = new MongoObservationStore(collection);
            var at = Instant.parse("2026-10-28T09:00:00Z");
            var oldLoad = new InitialLoad("execution:p:2:at", at, "snapshot_and_cdc", Map.of(
                    "orders", new InitialLoad.Table(InitialLoad.Stage.READING, 3L, null)));
            var first = new Observation("p", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, at, List.of(), oldLoad);
            store.save(first);
            assertThat(collection.find(new Document("_id", "p")).first()).doesNotContainKey("initialLoad");
            var historical = MongoObservationStore.toDocument(first);
            collection.replaceOne(new Document("_id", "p"), historical);
            assertThat(store.read("p")).contains(first);
            var next = new Observation("p", PipelineState.STOPPED, Map.of(), Map.of(), Map.of(), null,
                    at.plusSeconds(1), List.of(), new InitialLoad("execution:p:3:next", at.plusSeconds(1),
                    "snapshot_and_cdc", Map.of("orders", new InitialLoad.Table(InitialLoad.Stage.PENDING, null, null))));
            store.save(next);
            var retained = collection.find(new Document("_id", "p")).first();
            assertThat(retained.get("initialLoad")).isEqualTo(historical.get("initialLoad"));
            assertThat(store.read("p").orElseThrow().initialLoad()).isEqualTo(oldLoad);
            assertThat(store.read("p").orElseThrow().state()).isEqualTo(PipelineState.STOPPED);
            assertThat(collection.countDocuments()).isEqualTo(1);
        }
    }
}
