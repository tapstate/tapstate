package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-store witness for a cold janitor batch using artifact and execution truth. */
@RequiresDocker
class ObservationJanitorMongoIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void coldBatchesRemoveOnlyAbsentArtifactsAndOldExecutions() {
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("observation_janitor_worker_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_janitor_it");
            ArtifactStore artifacts = store.artifacts();
            ObservationStore latest = store.observations();
            WorkloadClaimStore generations = store.workloadClaims();
            artifacts.saveAll(List.<Resource>of(pipeline("current"), pipeline("stale_run")));
            String currentIncarnation = artifacts.pipelineIncarnationId("current").orElseThrow();
            String staleIncarnation = artifacts.pipelineIncarnationId("stale_run").orElseThrow();
            long currentGeneration = generations.advanceStandalone("cluster", "current").orElseThrow();
            long oldGeneration = generations.advanceStandalone("cluster", "stale_run").orElseThrow();
            generations.advanceStandalone("cluster", "stale_run").orElseThrow();
            assertThat(latest.saveScoped(observation("current", 1),
                    new ObservationStore.Scope(currentIncarnation, currentGeneration))).isTrue();
            assertThat(latest.saveScoped(observation("orphan", 2),
                    new ObservationStore.Scope("removed-incarnation", 1))).isTrue();
            assertThat(latest.saveScoped(observation("stale_run", 3),
                    new ObservationStore.Scope(staleIncarnation, oldGeneration))).isTrue();

            try (ObservationJanitor janitor = new ObservationJanitor(latest, artifacts, generations,
                    "cluster", 2, Duration.ofMinutes(1), false)) {
                janitor.runOneBatch();
                janitor.runOneBatch();

                assertThat(latest.readStored("current").orElseThrow().scope())
                        .contains(new ObservationStore.Scope(currentIncarnation, currentGeneration));
                assertThat(latest.readStored("orphan")).isEmpty();
                assertThat(latest.readStored("stale_run")).isEmpty();
                assertThat(janitor.health().scanned()).isEqualTo(3);
                assertThat(janitor.health().deleted()).isEqualTo(2);
                assertThat(janitor.health().failures()).isZero();
            }
        }
    }

    private static PipelineResource pipeline(String id) {
        return new PipelineResource(id, null, List.of(), null, null, null, null, null);
    }

    private static Observation observation(String id, long count) {
        return new Observation(id, PipelineState.RUNNING, Map.of("recordCount", count),
                Map.of(), Map.of(), null, Instant.parse("2026-09-27T10:00:00Z").plusSeconds(count));
    }
}
