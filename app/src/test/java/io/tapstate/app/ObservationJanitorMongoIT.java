package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PreExecutionFailure;
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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

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
                for (int batch = 0; batch < 5; batch++) {
                    janitor.runOneBatch();
                }

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

    @Test
    void aBoundedColdBatchKeepsAQualifiedRefusalAndDropsItOnlyAfterItsIntentChanges() {
        try (RefusalFixture f = new RefusalFixture(); ObservationJanitor janitor = f.janitor()) {
            janitor.runOneBatch();
            janitor.runOneBatch();
            assertThat(f.store.observations().readStored("refused").orElseThrow().refusal()).contains(f.receipt.owner());
            assertThat(janitor.health().scanned()).isEqualTo(1);
            assertThat(janitor.health().deleted()).isZero();
            f.store.desired().save(new DesiredState("refused", PipelineState.RUNNING, "new-intent"));
            assertThat(f.store.observations().isCurrentPreExecutionFailure(f.receipt.owner())).isFalse();
            // A full page wraps at the empty tail before the next cycle can revisit this id.
            for (int phase = 0; phase < 6; phase++) { janitor.runOneBatch(); }
            assertThat(f.store.observations().readStored("refused")).isEmpty();
            assertThat(janitor.health().deleted()).isEqualTo(1);
            assertThat(janitor.health().failures()).isZero();
            assertThat(f.store.workloadClaims().currentGeneration("cluster", "refused")).isEmpty();
        }
    }

    @Test
    void aSnapshotOfTheOldRefusalCannotDeleteTheFirstRealExecution() {
        try (RefusalFixture f = new RefusalFixture(); ObservationJanitor janitor = f.janitor()) {
            var snapshot = f.store.observations().scanManifestsAfter(Optional.empty(), 1).getFirst();
            assertThat(snapshot.refusals()).containsExactly(f.receipt.owner());
            assertThat(snapshot.scopes()).isEmpty();
            long generation = f.store.workloadClaims().advanceStandalone("cluster", "refused").orElseThrow();
            assertThat(generation).isEqualTo(1);
            var scope = new ObservationStore.Scope(f.receipt.owner().pipelineIncarnationId(), generation);
            assertThat(f.store.observations().saveScoped(observation("refused", 1), scope)).isTrue();
            assertThat(f.store.observations().deleteManifestIfUnchanged(snapshot, "refused")).isFalse();
            janitor.runOneBatch(); janitor.runOneBatch();
            assertThat(f.store.observations().readStored("refused").orElseThrow().scope()).contains(scope);
            assertThat(f.store.observations().readStored("refused").orElseThrow().refusal()).isEmpty();
            assertThat(janitor.health().deleted()).isZero();
            assertThat(janitor.health().failures()).isZero();
        }
    }

    private static final class RefusalFixture implements AutoCloseable {
        final MongoConnection connection;
        final MongoStorePort store;
        final PreExecutionFailure.Receipt receipt;
        RefusalFixture() {
            String suffix = UUID.randomUUID().toString().replace("-", "");
            connection = new MongoConnection(new MongoConnectionSettings(MONGO.getReplicaSetUrl("janitor_refusal_" + suffix),
                    null, Duration.ofSeconds(5)));
            connection.verify();
            store = new MongoStorePort(connection, "operator_refusal_" + suffix);
            Resource pipeline = new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: refused
                    source: src_x
                    view:
                      from: src_x
                      primary_key: id
                    """);
            Resource source = new DslParser().parse("""
                    version: tapstate/v1
                    kind: source
                    id: src_x
                    connector: mysql
                    config: { host: localhost, port: 3306 }
                    tables: [ orders ]
                    """);
            store.artifacts().saveAll(List.of(pipeline, source));
            Instant at = Instant.parse("2026-10-08T03:00:00Z");
            store.state().create("refused", StateJson.of(PipelineState.NEW), at);
            var desired = new DesiredState("refused", PipelineState.RUNNING, CanonicalHash.of(pipeline));
            store.desired().save(desired);
            var attempt = new PreExecutionFailure.Attempt("refused", "cluster",
                    store.artifacts().pipelineIncarnationId("refused").orElseThrow(), store.state().read("refused").orElseThrow(),
                    desired, Map.of("refused", CanonicalHash.of(pipeline), "src_x", CanonicalHash.of(source)), OptionalLong.empty(), null);
            receipt = attempt.failed(store.state().failPreExecution(attempt, at.plusSeconds(1)).orElseThrow());
            Observation refusal = new Observation("refused", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new ObservationFailure("actuation.source-schema-not-discovered", Map.of("source", "src_x")), at.plusSeconds(2), List.of());
            assertThat(store.observations().savePreExecutionFailure(refusal, receipt)).isTrue();
        }
        ObservationJanitor janitor() {
            return new ObservationJanitor(store.observations(), store.artifacts(), store.workloadClaims(), store.state(),
                    "cluster", 1, Duration.ofMinutes(1), false);
        }
        @Override public void close() { connection.close(); }
    }

    private static PipelineResource pipeline(String id) {
        return new PipelineResource(id, null, List.of(), null, null, null, null, null);
    }

    private static Observation observation(String id, long count) {
        return new Observation(id, PipelineState.RUNNING, Map.of("recordCount", count),
                Map.of(), Map.of(), null, Instant.parse("2026-09-27T10:00:00Z").plusSeconds(count));
    }
}
