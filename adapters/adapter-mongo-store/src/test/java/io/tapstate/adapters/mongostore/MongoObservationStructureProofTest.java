package io.tapstate.adapters.mongostore;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCursor;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoObservationStructureProofTest {
    @Test void anInvalidatedCollationAuditCannotOverwriteTheCurrentVerdict() throws Exception {
        var database = mock(MongoDatabase.class);
        var collections = mock(ListCollectionsIterable.class, RETURNS_SELF);
        when(database.listCollections()).thenReturn(collections);
        var oldMetadataRead = new CountDownLatch(1);
        var finishOldAudit = new CountDownLatch(1);
        var audits = new AtomicInteger();
        when(collections.iterator()).thenAnswer(call -> {
            var row = new Document("name", "cache_identity");
            if (audits.incrementAndGet() == 1) {
                oldMetadataRead.countDown();
                assertThat(finishOldAudit.await(5, TimeUnit.SECONDS)).isTrue();
            } else {
                row.append("options", new Document("collation", new Document("locale", "en")));
            }
            return cursor(row);
        });

        var proof = new MongoStructureProof();
        try (var domain = domain(database, proof); var worker = Executors.newSingleThreadExecutor()) {
            var readiness = MongoCacheDomain.class.getDeclaredMethod("canBatchSrsControlReadiness");
            readiness.setAccessible(true);
            var old = worker.submit(() -> (boolean) readiness.invoke(domain));
            try {
                assertThat(oldMetadataRead.await(3, TimeUnit.SECONDS)).isTrue();
                proof.clear();
                assertThat((boolean) readiness.invoke(domain)).isFalse();
                finishOldAudit.countDown();
                assertThat(old.get(3, TimeUnit.SECONDS)).isFalse();
                assertThat((boolean) readiness.invoke(domain)).isFalse();
                assertThat(audits).hasValue(2);
            } finally {
                finishOldAudit.countDown();
            }
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
        when(database.listCollections().iterator()).thenAnswer(ignored -> cursor(new Document("name", "cache_identity")
                .append("options", new Document("collation", new Document("locale", "en")))));
        var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        var original = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
                new WorkloadOwner("node", "boot"), 1, 1, 1);

        try (var domain = domain(database, new MongoStructureProof());
             var topology = mockStatic(MongoReadBudget.class);
             var metadata = mockConstruction(MongoCacheMetadataStructure.class);
             var readers = mockConstruction(MongoPipelineObservationRead.class)) {
            topology.when(() -> MongoReadBudget.supportsSnapshotReads(any())).thenReturn(true);
            assertThat(domain.readObservation(scope, original, Map.of(), () -> {})).isEmpty();
            assertThat(readers.constructed()).isEmpty();
        }
    }

    @SuppressWarnings("unchecked")
    private static MongoCursor<Document> cursor(Document row) {
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(row);
        return cursor;
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
