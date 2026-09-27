package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.DataBrowserFollows;
import io.tapstate.control.core.TelemetryCleanupReporter;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** A delayed lifecycle cleanup cannot erase the telemetry of a new artifact under the same id. */
@RequiresDocker
class DelayedTelemetryCleanupMongoIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void delayedCleanupRemovesOnlyTheDeletedOwnerAcrossRealStoresAndNodeLocalLogs() {
        String database = "delayed_telemetry_cleanup_it";
        String uri = MONGO.getReplicaSetUrl(database);
        Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant bucketStart = Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(), 300) * 300 - 300);
        List<Runnable> pending = new ArrayList<>();
        List<TelemetryCleanupReporter.Failure> failures = new ArrayList<>();
        List<String> removedCurrent = new ArrayList<>();
        RingBufferLogSink logs = new RingBufferLogSink(8, 8);
        TelemetryCleanupReporter reporter = new TelemetryCleanupReporter() {
            @Override public void failed(Failure failure) { failures.add(failure); }
            @Override public void removed(String pipelineId, String incarnationId) {
                removedCurrent.add(pipelineId + ":" + incarnationId);
            }
        };

        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                uri, null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_delayed_cleanup_it");
            ArtifactMutationService mutations = new ArtifactMutationService(
                    store.artifacts(), store.desired(), store.state(), store.observations(),
                    store.layouts(), store.meta(), store.derivedSchemas(), store.rateHistory(),
                    new AuditGate(record -> { }, Clock.systemUTC()), DataBrowserFollows.NONE,
                    pending::add, logs, store.events(), store.historyRollups(),
                    ignored -> Optional.empty(), reporter);

            String id = "recreated_flow";
            PipelineResource resource = pipeline(id);
            store.artifacts().save(resource);
            String oldIncarnation = store.artifacts().pipelineIncarnationId(id).orElseThrow();
            ObservationStore.Scope oldOwner = new ObservationStore.Scope(oldIncarnation, 1);
            publishScoped(store, logs, id, oldOwner, at, bucketStart, 1);

            mutations.delete("operator", id, CanonicalHash.of(resource));
            assertThat(store.artifacts().get(id)).isEmpty();
            assertThat(removedCurrent).containsExactly(id + ":" + oldIncarnation);
            assertThat(store.observations().readStored(id).orElseThrow().scope()).contains(oldOwner);
            assertThat(store.rateHistory().readPageVisible(id,
                    new RateHistoryStore.Visibility(oldIncarnation, false),
                    at.minusSeconds(1), at.plusSeconds(30), null, 10).entries()).hasSize(1);
            assertThat(store.events().readPage(id, oldIncarnation,
                    at.minusSeconds(1), at.plusSeconds(30), null, 10).events()).hasSize(1);
            assertThat(store.historyRollups().read(key(id,
                    HistoryRollupStore.Scope.incarnation(oldIncarnation), bucketStart))).isPresent();
            assertThat(logs.tailIncarnation(id, oldIncarnation)).hasSize(1);

            store.artifacts().save(resource);
            String newIncarnation = store.artifacts().pipelineIncarnationId(id).orElseThrow();
            assertThat(newIncarnation).isNotEqualTo(oldIncarnation);
            ObservationStore.Scope newOwner = new ObservationStore.Scope(newIncarnation, 2);
            publishScoped(store, logs, id, newOwner, at.plusSeconds(1), bucketStart, 2);

            String legacyId = "legacy_flow";
            PipelineResource legacyResource = pipeline(legacyId);
            store.artifacts().save(legacyResource);
            // Simulate an artifact written before the system-owned identity field existed.
            try (MongoClient raw = MongoClients.create(uri)) {
                raw.getDatabase(database).getCollection(MongoStorePort.ARTIFACTS).updateOne(
                        new Document("_id", legacyId),
                        new Document("$unset", new Document("pipelineIncarnationId", "")));
            }
            assertThat(store.artifacts().pipelineIncarnationId(legacyId)).isEmpty();
            publishLegacy(store, logs, legacyId, at, bucketStart);
            mutations.delete("operator", legacyId, CanonicalHash.of(legacyResource));
            assertThat(store.artifacts().get(legacyId)).isEmpty();
            assertThat(removedCurrent).containsExactly(id + ":" + oldIncarnation,
                    legacyId + ":null");
            assertThat(store.observations().readStored(legacyId).orElseThrow().scope()).isEmpty();
            assertThat(store.rateHistory().readPageVisible(legacyId,
                    new RateHistoryStore.Visibility(null, true),
                    at.minusSeconds(1), at.plusSeconds(30), null, 10).entries()).hasSize(1);
            assertThat(store.historyRollups().read(key(legacyId,
                    HistoryRollupStore.Scope.legacy(), bucketStart))).isPresent();
            assertThat(logs.tail(legacyId)).hasSize(1);
            store.artifacts().save(legacyResource);
            String recreatedLegacyIncarnation = store.artifacts().pipelineIncarnationId(legacyId).orElseThrow();
            ObservationStore.Scope recreatedLegacyOwner = new ObservationStore.Scope(
                    recreatedLegacyIncarnation, 2);
            publishScoped(store, logs, legacyId, recreatedLegacyOwner,
                    at.plusSeconds(1), bucketStart, 2);

            assertThat(pending).isNotEmpty();
            pending.forEach(Runnable::run);
            assertThat(failures).isEmpty();

            assertScopedSurvives(store, logs, id, oldIncarnation, newOwner, at, bucketStart);
            assertScopedSurvives(store, logs, legacyId, null, recreatedLegacyOwner, at, bucketStart);
            assertThat(store.rateHistory().readPageVisible(legacyId,
                    new RateHistoryStore.Visibility(null, true),
                    at.minusSeconds(1), at.plusSeconds(30), null, 10).entries()).isEmpty();
            assertThat(store.historyRollups().read(key(legacyId,
                    HistoryRollupStore.Scope.legacy(), bucketStart))).isEmpty();
            assertThat(logs.tail(legacyId)).isEmpty();
        }
    }

    private static void publishScoped(MongoStorePort store, RingBufferLogSink logs, String id,
            ObservationStore.Scope owner, Instant at, Instant bucketStart, long count) {
        assertThat(store.observations().saveScoped(observation(id, at, count), owner)).isTrue();
        store.rateHistory().appendScoped(sample(id, at, count), owner);
        store.events().append(new PipelineEvent(id + "-event-" + count, id,
                owner.pipelineIncarnationId(), owner.executionGeneration(),
                PipelineEvent.Kind.STATE_CHANGED, at, PipelineState.NEW, PipelineState.RUNNING,
                null, null, null));
        store.historyRollups().upsert(bucket(id,
                HistoryRollupStore.Scope.incarnation(owner.pipelineIncarnationId()), bucketStart, at));
        logs.append(id, new LogSink.Scope(owner.pipelineIncarnationId(), owner.executionGeneration()),
                new LogLine(at.toEpochMilli(), "INFO", "run-" + count));
    }

    private static void publishLegacy(MongoStorePort store, RingBufferLogSink logs,
            String id, Instant at, Instant bucketStart) {
        store.observations().save(observation(id, at, 1));
        store.rateHistory().append(sample(id, at, 1));
        store.historyRollups().upsert(bucket(id, HistoryRollupStore.Scope.legacy(), bucketStart, at));
        logs.append(id, new LogLine(at.toEpochMilli(), "INFO", "legacy"));
    }

    private static void assertScopedSurvives(MongoStorePort store, RingBufferLogSink logs,
            String id, String oldIncarnation, ObservationStore.Scope current,
            Instant at, Instant bucketStart) {
        assertThat(store.artifacts().pipelineIncarnationId(id)).contains(current.pipelineIncarnationId());
        assertThat(store.observations().readStored(id).orElseThrow().scope()).contains(current);
        assertThat(store.observations().read(id).orElseThrow().metrics())
                .containsEntry("recordCount", 2L);
        assertThat(store.rateHistory().readPageVisible(id,
                new RateHistoryStore.Visibility(current.pipelineIncarnationId(), false),
                at.minusSeconds(1), at.plusSeconds(30), null, 10).entries())
                .extracting(entry -> entry.sample().counters().get("recordsOut"))
                .containsExactly(2L);
        assertThat(store.events().readPage(id, current.pipelineIncarnationId(),
                at.minusSeconds(1), at.plusSeconds(30), null, 10).events()).hasSize(1);
        assertThat(store.historyRollups().read(key(id,
                HistoryRollupStore.Scope.incarnation(current.pipelineIncarnationId()), bucketStart)))
                .isPresent();
        assertThat(logs.tail(id, new LogSink.Scope(current.pipelineIncarnationId(),
                current.executionGeneration()))).extracting(LogLine::message).containsExactly("run-2");
        if (oldIncarnation != null) {
            assertThat(store.rateHistory().readPageVisible(id,
                    new RateHistoryStore.Visibility(oldIncarnation, false),
                    at.minusSeconds(1), at.plusSeconds(30), null, 10).entries()).isEmpty();
            assertThat(store.events().readPage(id, oldIncarnation,
                    at.minusSeconds(1), at.plusSeconds(30), null, 10).events()).isEmpty();
            assertThat(store.historyRollups().read(key(id,
                    HistoryRollupStore.Scope.incarnation(oldIncarnation), bucketStart))).isEmpty();
            assertThat(logs.tailIncarnation(id, oldIncarnation)).isEmpty();
        }
    }

    private static PipelineResource pipeline(String id) {
        return new PipelineResource(id, null, List.of(), null, null, null, null, null);
    }

    private static Observation observation(String id, Instant at, long count) {
        return new Observation(id, PipelineState.STOPPED, Map.of("recordCount", count),
                Map.of(), Map.of(), null, at);
    }

    private static RateSample sample(String id, Instant at, long count) {
        return new RateSample(id, at, Map.of("recordsOut", count), Map.of(), at.minusSeconds(60));
    }

    private static HistoryRollupStore.Key key(String id, HistoryRollupStore.Scope scope,
            Instant bucketStart) {
        return new HistoryRollupStore.Key(id, scope, HistoryRollupStore.Resolution.PT5M, bucketStart);
    }

    private static HistoryRollupStore.Bucket bucket(String id, HistoryRollupStore.Scope scope,
            Instant bucketStart, Instant at) {
        return new HistoryRollupStore.Bucket(key(id, scope, bucketStart),
                at, at.minusSeconds(1), at.plusSeconds(60), false, List.of(), List.of(), 0);
    }
}
