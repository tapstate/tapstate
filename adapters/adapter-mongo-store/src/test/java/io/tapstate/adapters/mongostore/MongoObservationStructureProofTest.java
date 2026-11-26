package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.OperatorDatabaseBinding;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoObservationStructureProofTest {
    @Test void anInvalidatedCollationAuditCannotOverwriteTheCurrentVerdict() throws Exception {
        var database = mock(MongoDatabase.class, RETURNS_DEEP_STUBS);
        when(database.getName()).thenReturn("cache");
        var oldMetadataRead = new CountDownLatch(1);
        var finishOldAudit = new CountDownLatch(1);
        try (var domain = domain(database, new MongoStructureProof()); var worker = Executors.newSingleThreadExecutor()) {
            var readiness = MongoCacheDomain.class.getDeclaredMethod("canBatchSrsControlReadiness");
            readiness.setAccessible(true);
            var old = worker.submit(() -> {
                inspect(domain, database, () -> {
                    oldMetadataRead.countDown();
                    try { assertThat(finishOldAudit.await(5, TimeUnit.SECONDS)).isTrue(); }
                    catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                    return java.util.List.of();
                });
                return (boolean) readiness.invoke(domain);
            });
            try {
                assertThat(oldMetadataRead.await(5, TimeUnit.SECONDS)).isTrue();
                inspect(domain, database, () -> java.util.List.of(customCollation()));
                assertThat((boolean) readiness.invoke(domain)).isFalse();
                finishOldAudit.countDown();
                assertThat(old.get(3, TimeUnit.SECONDS)).isFalse();
                assertThat((boolean) readiness.invoke(domain)).isFalse();
            } finally { finishOldAudit.countDown(); }
        }
    }

    private static MongoStructureInspection.Difference customCollation() {
        return new MongoStructureInspection.Difference(SystemCollections.Database.CACHE, "cache", "cache_identity", null,
                MongoStructureInspection.Problem.COLLECTION_OPTIONS, "ordinary", "custom collation", null);
    }

    private static void inspect(MongoCacheDomain domain, MongoDatabase database,
            java.util.function.Supplier<java.util.List<MongoStructureInspection.Difference>> result) {
        var budget = mock(MongoReadBudget.class);
        when(budget.database(database)).thenReturn(database);
        try (var inspection = mockStatic(MongoStructureInspection.class)) {
            inspection.when(() -> MongoStructureInspection.inspect(eq(database), anyList(), eq(budget)))
                    .thenAnswer(ignored -> result.get());
            domain.inspectStructure(new io.tapstate.spi.store.CacheAuthScope("cluster", 1,
                    io.tapstate.spi.store.StorageBinding.Mode.OP, "cache", 1), budget);
        }
    }

    @Test void anObservationCanWriteOnlyWithItsOriginalLiveProof() throws Exception {
        var now = new AtomicLong();
        var proof = new MongoStructureProof(now::get);
        var database = mock(MongoDatabase.class, RETURNS_DEEP_STUBS);
        var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        var original = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
                new WorkloadOwner("node", "boot"), 1, 1, 1);
        var observation = mock(Observation.class);
        var failedObservation = mock(Observation.class);
        var unavailable = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "write timeout"),
                new java.io.IOException("fixture"));
        var checkpoint = CheckpointDoc.initial("flow", "RUNNING", Instant.EPOCH);
        var facts = new MongoCacheDomain.ObservationFacts(checkpoint, Map.of(), Map.of(), Map.of());

        try (var domain = domain(database, proof);
             var topology = mockStatic(MongoReadBudget.class);
             var metadata = mockConstruction(MongoCacheMetadataStructure.class);
             var readers = mockConstruction(MongoPipelineObservationRead.class,
                     (reader, ignored) -> when(reader.read(Map.of())).thenReturn(Optional.of(facts)));
             var writers = mockConstruction(MongoCachePipelineObservationStore.class, (writer, ignored) -> {
                 when(writer.updateExisting(observation, 1)).thenReturn(true);
                 when(writer.updateExisting(failedObservation, 1)).thenThrow(unavailable);
             })) {
            topology.when(() -> MongoReadBudget.supportsSnapshotReads(any())).thenReturn(true);
            assertThat(domain.readObservation(scope, original, Map.of(), () -> {})).isEmpty();
            inspect(domain, database, java.util.List::of);
            var old = domain.readObservation(scope, original, Map.of(), () -> {}).orElseThrow();
            assertThat(old.facts()).isEqualTo(facts);
            now.set(TimeUnit.SECONDS.toNanos(10) + 1);
            assertThat(old.updateExisting(observation, 1)).isFalse();

            var refreshed = domain.readObservation(scope, original, Map.of(), () -> {}).orElseThrow();
            assertThat(old.updateExisting(observation, 1)).isFalse();
            assertThat(writers.constructed()).isEmpty();
            assertThat(refreshed.updateExisting(observation, 1)).isTrue();
            assertThat(writers.constructed()).hasSize(1);
            proof.clear();
            var current = domain.readObservation(scope, original, Map.of(), () -> {}).orElseThrow();
            assertThat(refreshed.updateExisting(observation, 1)).isFalse();
            assertThat(writers.constructed()).hasSize(1);

            assertThatThrownBy(() -> current.updateExisting(failedObservation, 1)).isSameAs(unavailable);
            assertThat(current.updateExisting(observation, 1)).isFalse();
            assertThat(writers.constructed()).hasSize(2);
        }
    }

    @Test void aNonSimpleCollationKeepsObservationReadsOnTheOrdinaryPath() throws Exception {
        var database = mock(MongoDatabase.class, RETURNS_DEEP_STUBS);
        when(database.getName()).thenReturn("cache");
        var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        var original = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
                new WorkloadOwner("node", "boot"), 1, 1, 1);

        try (var domain = domain(database, new MongoStructureProof());
             var topology = mockStatic(MongoReadBudget.class);
             var metadata = mockConstruction(MongoCacheMetadataStructure.class);
             var readers = mockConstruction(MongoPipelineObservationRead.class)) {
            topology.when(() -> MongoReadBudget.supportsSnapshotReads(any())).thenReturn(true);
            assertThat(domain.readObservation(scope, original, Map.of(), () -> {})).isEmpty();
            inspect(domain, database, () -> java.util.List.of(customCollation()));
            assertThat(domain.readObservation(scope, original, Map.of(), () -> {})).isEmpty();
            assertThat(readers.constructed()).isEmpty();
        }
    }

    private static MongoCacheDomain domain(MongoDatabase database, MongoStructureProof proof) throws Exception {
        var connection = mock(MongoConnection.class, RETURNS_DEEP_STUBS);
        when(connection.database()).thenReturn(database);
        var constructor = MongoCacheDomain.class.getDeclaredConstructor(MongoConnection.class, Duration.class,
                String.class, OperatorDatabaseBinding.class);
        constructor.setAccessible(true);
        var domain = constructor.newInstance(connection, Duration.ofHours(1), "diagnostic", null);
        var field = MongoCacheDomain.class.getDeclaredField("structures");
        field.setAccessible(true);
        field.set(domain, proof);
        return domain;
    }
}
