package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that delayed cleanup matches the removed resource's stored owner in one Mongo delete. */
@RequiresDocker
class MongoObservationCleanupIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void oldAndLegacyCleanupCannotDeleteARecreatedResource() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("observation_cleanup_it");
            database.drop();
            MongoObservationStore store = new MongoObservationStore(client,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            ObservationStore.Scope old = new ObservationStore.Scope("inc-old", 41);
            ObservationStore.Scope current = new ObservationStore.Scope("inc-new", 42);

            store.save(new Observation("flow", PipelineState.NEW, Map.of(), Map.of()));
            store.deleteIncarnation("flow", old.pipelineIncarnationId());
            assertThat(store.readStored("flow").orElseThrow().scope()).isEmpty();
            store.deleteLegacy("flow");
            assertThat(store.readStored("flow")).isEmpty();

            assertThat(store.saveScoped(observation(1), old)).isTrue();
            store.deleteLegacy("flow");
            assertThat(store.readStored("flow").orElseThrow().scope()).contains(old);

            assertThat(store.saveScoped(observation(2), current)).isTrue();
            store.deleteIncarnation("flow", old.pipelineIncarnationId());
            store.deleteLegacy("flow");
            assertThat(store.readStored("flow").orElseThrow())
                    .satisfies(stored -> {
                        assertThat(stored.scope()).contains(current);
                        assertThat(stored.observation().metrics()).containsEntry("recordCount", 2L);
                    });
            assertThat(database.getCollection(MongoStorePort.PIPELINE_OBSERVATION)
                    .countDocuments()).isEqualTo(1);

            store.deleteIncarnation("flow", current.pipelineIncarnationId());
            assertThat(store.readStored("flow")).isEmpty();
        }
    }

    private static Observation observation(long count) {
        return new Observation("flow", PipelineState.RUNNING, Map.of("recordCount", count),
                Map.of(), Map.of(), null, Instant.parse("2026-09-27T10:00:00Z").plusSeconds(count));
    }
}
