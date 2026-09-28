package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Mongo witness for bounded latest manifests, chunks, fencing, and absence-only fallback. */
@RequiresDocker
class MongoLatestObservationStorageIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void inlineCurrentIsAuthoritativeFencedAndIdempotentWhileLegacyResidueRemainsCold() {
        try (Fixture fixture = fixture("latest_inline_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            Observation legacy = observation("orders", at.minusSeconds(1), "legacy");
            fixture.store.save(legacy);
            Observation current = observation("orders", at, "current");
            ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);

            assertThat(fixture.store.saveScoped(current, run)).isTrue();
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(current, java.util.Optional.of(run)));
            assertThat(fixture.manifests.countDocuments()).isEqualTo(2);
            assertThat(fixture.chunks.countDocuments()).isZero();
            assertThat(fixture.manifests.find(new Document("_id",
                    MongoLatestObservationStorage.manifestKey("orders"))).first())
                    .satisfies(manifest -> {
                        assertThat(manifest.get("_id")).isInstanceOf(Binary.class);
                        assertThat(((Document) manifest.get("current")).getString("mode")).isEqualTo("inline");
                    });

            assertThat(fixture.store.saveScoped(current, run)).as("exact replay is idempotent").isTrue();
            assertThat(fixture.store.saveScoped(observation("orders", at, "different"), run))
                    .as("same identity and time cannot mean different payload").isFalse();
            assertThat(fixture.store.saveScoped(observation("orders", at.plusSeconds(1), "old"),
                    new ObservationStore.Scope("inc-a", 40))).isFalse();
            Observation next = observation("orders", at.plusSeconds(1), "next");
            ObservationStore.Scope nextRun = new ObservationStore.Scope("inc-a", 42);
            assertThat(fixture.store.saveScoped(next, nextRun)).isTrue();
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(next, java.util.Optional.of(nextRun)));
            assertThat(fixture.store.hasCommittedManifest("orders")).isTrue();
        }
    }

    @Test
    void chunkedCurrentRoundTripsAndMissingChunkNeverFallsBackToLegacy() {
        try (Fixture fixture = fixture("latest_chunks_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            fixture.store.save(observation("orders", at.minusSeconds(1), "legacy"));
            Observation large = observation("orders", at, "x".repeat(3 * 1024 * 1024));
            ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);

            assertThat(fixture.store.saveScoped(large, run)).isTrue();
            assertThat(fixture.chunks.countDocuments()).isGreaterThan(2);
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(large, java.util.Optional.of(run)));

            Document first = fixture.chunks.find().sort(new Document("ordinal", 1)).first();
            assertThat(first).isNotNull();
            fixture.chunks.deleteOne(new Document("_id", first.get("_id")));
            assertThatThrownBy(() -> fixture.store.readStored("orders"))
                    .isInstanceOfSatisfying(TapstateException.class, failure ->
                            assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test
    void firstManifestPublishHonorsTheScopedLegacyFence() {
        try (Fixture fixture = fixture("latest_legacy_cutover_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            Observation legacy = observation("orders", at, "legacy");
            ObservationStore.Scope legacyRun = new ObservationStore.Scope("inc-a", 42);
            fixture.manifests.insertOne(MongoObservationStore.toDocument(legacy)
                    .append("pipelineIncarnationId", legacyRun.pipelineIncarnationId())
                    .append("executionGeneration", legacyRun.executionGeneration()));

            assertThat(fixture.store.saveScoped(observation("orders", at.plusSeconds(1), "older-run"),
                    new ObservationStore.Scope("inc-a", 41))).isFalse();
            assertThat(fixture.store.saveScoped(observation("orders", at.minusSeconds(1), "older-time"),
                    legacyRun)).isFalse();
            assertThat(fixture.store.saveScoped(observation("orders", at, "different"), legacyRun)).isFalse();
            assertThat(fixture.manifests.find(new Document("_id",
                    MongoLatestObservationStorage.manifestKey("orders"))).first()).isNull();
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(legacy, java.util.Optional.of(legacyRun)));

            assertThat(fixture.store.saveScoped(legacy, legacyRun)).isTrue();
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(legacy, java.util.Optional.of(legacyRun)));
            assertThat(fixture.manifests.countDocuments()).isEqualTo(2);
        }
    }

    @Test
    void anExactlyCommittedChunkedReplayDoesNotPrepareAnotherPublication() {
        try (Fixture fixture = fixture("latest_chunk_replay_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            Observation large = observation("orders", at, "x".repeat(600 * 1024));
            ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
            assertThat(fixture.store.saveScoped(large, run)).isTrue();
            Document current = fixture.manifests.find(new Document("_id",
                    MongoLatestObservationStorage.manifestKey("orders"))).first().get("current", Document.class);
            long chunks = fixture.chunks.countDocuments();

            assertThat(fixture.store.saveScoped(large, run)).isTrue();

            assertThat(fixture.manifests.find(new Document("_id",
                    MongoLatestObservationStorage.manifestKey("orders"))).first().get("current", Document.class))
                    .isEqualTo(current);
            assertThat(fixture.chunks.countDocuments()).isEqualTo(chunks);
            assertThat(fixture.store.saveScoped(observation("orders", at, "y".repeat(600 * 1024)), run)).isFalse();
            assertThat(fixture.chunks.countDocuments()).isEqualTo(chunks);
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(large, java.util.Optional.of(run)));
        }
    }

    @Test
    void committedCutoverCannotResurrectLegacyAfterDescriptorRemovalAndRecreation() {
        try (Fixture fixture = fixture("latest_sticky_cutover_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            Observation legacy = observation("orders", at, "legacy");
            ObservationStore.Scope legacyRun = new ObservationStore.Scope("inc-legacy", 42);
            fixture.manifests.insertOne(MongoObservationStore.toDocument(legacy)
                    .append("pipelineIncarnationId", legacyRun.pipelineIncarnationId())
                    .append("executionGeneration", legacyRun.executionGeneration()));
            assertThat(fixture.store.saveScoped(observation("orders", at.plusSeconds(1), "current"),
                    new ObservationStore.Scope("inc-a", 43))).isTrue();

            fixture.store.deleteIncarnation("orders", "inc-a");

            assertThat(fixture.store.readStored("orders")).isEmpty();
            assertThat(fixture.store.deleteManifestIfUnchanged(
                    fixture.store.scanManifestsAfter(java.util.Optional.empty(), 1).getFirst())).isFalse();
            Observation recreated = observation("orders", at.plusSeconds(2), "recreated");
            ObservationStore.Scope next = new ObservationStore.Scope("inc-b", 44);
            assertThat(fixture.store.saveScoped(recreated, next)).isTrue();
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(recreated, java.util.Optional.of(next)));
            assertThat(fixture.store.deleteIfUnchanged(new ObservationStore.LatestSnapshot("orders",
                    java.util.Optional.of(legacyRun), java.util.Optional.of(at)))).isTrue();
            fixture.store.deleteIncarnation("orders", "inc-b");
            assertThat(fixture.manifests.countDocuments()).isZero();
            assertThat(fixture.store.readStored("orders")).isEmpty();
        }
    }

    @Test
    void fractionalHeaderVersionsAndExplicitNullDescriptorsFailClosed() {
        try (Fixture fixture = fixture("latest_malformed_header_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            assertThat(fixture.store.saveScoped(observation("orders", at, "current"),
                    new ObservationStore.Scope("inc", 1))).isTrue();
            Document id = new Document("_id", MongoLatestObservationStorage.manifestKey("orders"));
            Document original = fixture.manifests.find(id).first();
            for (Document invalid : java.util.List.of(new Document("formatVersion", 1.5),
                    new Document("pending", null), new Document("current", null),
                    new Document("legacyFallback", true))) {
                fixture.manifests.replaceOne(id, original);
                fixture.manifests.updateOne(id, new Document("$set", invalid));
                assertThatThrownBy(() -> fixture.store.readStored("orders"))
                        .isInstanceOfSatisfying(TapstateException.class, error ->
                                assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            }
        }
    }

    @Test
    void aDifferentOwnerAtTheSameManifestKeyCannotBeReadWrittenOrDeleted() {
        try (Fixture fixture = fixture("latest_owner_collision_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            ObservationStore.Scope run = new ObservationStore.Scope("inc", 1);
            assertThat(fixture.store.saveScoped(observation("orders", at, "current"), run)).isTrue();
            fixture.manifests.updateOne(new Document("_id", MongoLatestObservationStorage.manifestKey("orders")),
                    new Document("$set", new Document("ownerDigest", MongoLatestObservationStorage.ownerDigest("other"))));
            java.util.List<Runnable> refused = java.util.List.of(
                    () -> fixture.store.readStored("orders"),
                    () -> fixture.store.hasCommittedManifest("orders"),
                    () -> fixture.store.saveScoped(observation("orders", at.plusSeconds(1), "next"), run),
                    () -> fixture.store.save(observation("orders", at.plusSeconds(1), "legacy")),
                    () -> fixture.store.deleteIncarnation("orders", "inc"),
                    () -> fixture.store.delete("orders"));
            for (Runnable operation : refused) {
                assertThatThrownBy(operation::run).isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(fixture.manifests.countDocuments()).isEqualTo(1);
            }
        }
    }

    @Test
    void compatibilityWritesCannotReplaceCommittedAuthorityOrBypassAtomicCleanup() {
        try (Fixture fixture = fixture("latest_compatibility_fence_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            ObservationStore.Scope run = new ObservationStore.Scope("inc", 1);
            Observation current = observation("orders", at, "current");
            assertThat(fixture.store.saveScoped(current, run)).isTrue();
            fixture.store.save(observation("orders", at.plusSeconds(1), "unfenced"));
            assertThat(fixture.store.readStored("orders")).contains(
                    new ObservationStore.Stored(current, java.util.Optional.of(run)));
            fixture.store.deleteIncarnation("orders", "inc");
            assertThat(fixture.store.readStored("orders")).isEmpty();
            assertThat(fixture.manifests.countDocuments()).isEqualTo(2);
            fixture.store.deleteLegacy("orders");
            assertThat(fixture.manifests.countDocuments()).isZero();
        }
    }

    @Test
    void aFractionalLegacyGenerationCannotClaimAnExecution() {
        try (Fixture fixture = fixture("latest_malformed_legacy_it")) {
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            fixture.manifests.insertOne(MongoObservationStore.toDocument(observation("orders", at, "legacy"))
                    .append("pipelineIncarnationId", "inc").append("executionGeneration", 1.5));
            assertThatThrownBy(() -> fixture.store.readStored("orders"))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThatThrownBy(() -> fixture.store.saveScoped(observation("orders", at.plusSeconds(1), "current"),
                    new ObservationStore.Scope("inc", 2)))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test
    void restartCleanupClearsExpiredPendingRetiresItsChunksAndNeverReusesItsToken() {
        try (Fixture fixture = fixture("latest_reclaim_it")) {
            String pipelineId = "orders";
            Instant at = Instant.parse("2026-09-28T04:00:00Z");
            ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
            Binary key = MongoLatestObservationStorage.manifestKey(pipelineId);
            Binary owner = MongoLatestObservationStorage.ownerDigest(pipelineId);
            String abandonedToken = "abandoned-publication";
            Document pending = new Document("pipelineIncarnationId", run.pipelineIncarnationId())
                    .append("executionGeneration", run.executionGeneration())
                    .append("observedAt", Date.from(at))
                    .append("publicationToken", abandonedToken)
                    .append("encodingVersion", LatestObservationPayloadCodec.ENCODING_VERSION)
                    .append("publishUntil", Date.from(Instant.now().minusSeconds(60)));
            fixture.manifests.insertOne(new Document("_id", key)
                    .append("formatVersion", MongoLatestObservationStorage.FORMAT_VERSION)
                    .append("legacyFallback", true).append("legacyResidue", false)
                    .append("ownerDigest", owner).append("revision", "pending-revision")
                    .append("pending", pending));
            LatestObservationPayloadCodec.Chunk abandonedChunk =
                    new LatestObservationPayloadCodec.Chunk(0, new byte[] {1}, new byte[32]);
            Document abandoned = MongoLatestObservationStorage.chunkDocument(
                    key, owner, abandonedToken, abandonedChunk);
            fixture.chunks.insertOne(abandoned);

            MongoObservationStore restarted = new MongoObservationStore(
                    fixture.client, fixture.manifests, fixture.chunks);
            assertThat(restarted.reclaimChunks(3).scanned()).isEqualTo(2);
            assertThat(fixture.manifests.find(new Document("_id", key)).first())
                    .doesNotContainKey("pending");
            Document retired = fixture.chunks.find(new Document("_id", abandoned.get("_id"))).first();
            assertThat(retired).isNotNull();
            assertThat(retired.getString("state")).isEqualTo("RETIRED");
            assertThat(retired.getDate("deleteAfter").toInstant())
                    .isAfter(Instant.now().plusSeconds(MongoLatestObservationStorage.READ_DEADLINE_SECONDS));

            Observation large = observation(pipelineId, at, "x".repeat(3 * 1024 * 1024));
            assertThat(restarted.saveScoped(large, run)).isTrue();
            Document committed = fixture.manifests.find(new Document("_id", key)).first();
            assertThat(committed).isNotNull();
            assertThat(((Document) committed.get("current")).getString("publicationToken"))
                    .isNotEqualTo(abandonedToken);
            assertThat(restarted.readStored(pipelineId)).contains(
                    new ObservationStore.Stored(large, java.util.Optional.of(run)));

            fixture.chunks.updateOne(new Document("_id", abandoned.get("_id")),
                    new Document("$set", new Document("deleteAfter", Date.from(Instant.now().minusSeconds(1)))));
            assertThat(restarted.reclaimChunks(3).deleted()).isEqualTo(1);
            assertThat(fixture.chunks.find(new Document("_id", abandoned.get("_id"))).first()).isNull();
            assertThat(restarted.readStored(pipelineId)).contains(
                    new ObservationStore.Stored(large, java.util.Optional.of(run)));
        }
    }

    private static Observation observation(String id, Instant at, String value) {
        return new Observation(id, PipelineState.RUNNING, Map.of("records.out", 1L),
                Map.of(), Map.of("orders", value), null, at);
    }

    private static Fixture fixture(String databaseName) {
        MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        MongoDatabase database = client.getDatabase(databaseName);
        database.drop();
        MongoCollection<Document> manifests = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION);
        MongoCollection<Document> chunks = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS);
        return new Fixture(client, manifests, chunks, new MongoObservationStore(client, manifests, chunks));
    }

    private record Fixture(MongoClient client, MongoCollection<Document> manifests,
            MongoCollection<Document> chunks, MongoObservationStore store) implements AutoCloseable {
        @Override public void close() { client.close(); }
    }
}
