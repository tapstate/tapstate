package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises publication and reclamation ordering through real MongoDB conditional writes. */
@RequiresDocker
class MongoLatestObservationProtocolIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final Instant FIRST = Instant.parse("2026-09-28T10:00:00Z");
    private static final String LARGE_POSITION = "x".repeat(600 * 1024);

    @Test
    void newerInlinePublicationPreemptsAnUnexpiredChunkedPending() {
        try (Fixture fixture = fixture("latest_protocol_inline_preempt_it")) {
            String id = "flow";
            ObservationStore.Scope previousScope = scope("inc-old", 7);
            Observation previous = observation(id, FIRST, "before");
            assertThat(fixture.storage.save(previous, previousScope)).isTrue();
            Prepared older = prepare(fixture, observation(id, FIRST.plusSeconds(1), LARGE_POSITION),
                    scope("inc-old", 8));
            assertThat(fixture.pending(id).getDate("publishUntil").toInstant())
                    .isAfter(fixture.serverTime());
            assertStored(fixture, previous, previousScope);

            ObservationStore.Scope currentScope = scope("inc-new", 9);
            Observation current = observation(id, FIRST.plusSeconds(2), "after");
            assertThat(fixture.storage.save(current, currentScope)).isTrue();
            Document committed = fixture.manifest(id).get("current", Document.class);
            assertThat(fixture.manifest(id)).doesNotContainKey("pending");
            assertThat(older.publication.renew()).isFalse();
            assertThat(older.publication.promote(older.encoded)).isFalse();
            assertThat(fixture.manifest(id).get("current", Document.class)).isEqualTo(committed);
            assertStored(fixture, current, currentScope);
        }
    }

    @Test
    void newerChunkedPendingSurvivesThePreemptedWritersLateCompletion() {
        try (Fixture fixture = fixture("latest_protocol_chunk_preempt_it")) {
            String id = "flow";
            ObservationStore.Scope previousScope = scope("inc-old", 7);
            Observation previous = observation(id, FIRST, "before");
            assertThat(fixture.storage.save(previous, previousScope)).isTrue();
            Prepared older = prepare(fixture, observation(id, FIRST.plusSeconds(1), LARGE_POSITION),
                    scope("inc-old", 8));
            ObservationStore.Scope currentScope = scope("inc-new", 9);
            Observation current = observation(id, FIRST.plusSeconds(2), LARGE_POSITION);
            Prepared newer = prepare(fixture, current, currentScope);
            assertThat(newer.token).isNotEqualTo(older.token);
            assertStored(fixture, previous, previousScope);

            assertThat(older.publication.renew()).isFalse();
            assertThat(older.publication.promote(older.encoded)).isFalse();
            assertThat(fixture.pending(id).getString("publicationToken")).isEqualTo(newer.token);
            assertThat(newer.publication.renew()).isTrue();
            assertThat(newer.publication.promote(newer.encoded)).isTrue();
            assertThat(older.publication.promote(older.encoded)).isFalse();
            assertStored(fixture, current, currentScope);
        }
    }

    @Test
    void sameOrLowerGenerationCannotReplaceALivePending() {
        try (Fixture fixture = fixture("latest_protocol_pending_fence_it")) {
            String id = "flow";
            ObservationStore.Scope previousScope = scope("inc-old", 7);
            Observation previous = observation(id, FIRST, "before");
            assertThat(fixture.storage.save(previous, previousScope)).isTrue();
            ObservationStore.Scope nextScope = scope("inc-new", 9);
            Observation next = observation(id, FIRST.plusSeconds(1), LARGE_POSITION);
            Prepared pending = prepare(fixture, next, nextScope);

            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(2), "inline"), nextScope))
                    .isFalse();
            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(2), LARGE_POSITION), nextScope))
                    .isFalse();
            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(2), "different owner"),
                    scope("inc-other", 9))).isFalse();
            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(2), "lower generation"),
                    scope("inc-old", 8))).isFalse();
            assertThat(fixture.pending(id).getString("publicationToken")).isEqualTo(pending.token);
            assertStored(fixture, previous, previousScope);
            assertThat(pending.publication.promote(pending.encoded)).isTrue();
            assertStored(fixture, next, nextScope);
        }
    }

    @Test
    void clearingAnExpiredPendingFencesHeartbeatPromotionAndTokenReuse() {
        try (Fixture fixture = fixture("latest_protocol_clear_first_it")) {
            String id = "flow";
            ObservationStore.Scope previousScope = scope("inc", 1);
            Observation previous = observation(id, FIRST, "before");
            assertThat(fixture.storage.save(previous, previousScope)).isTrue();
            ObservationStore.Scope currentScope = scope("inc", 2);
            Observation current = observation(id, FIRST.plusSeconds(1), LARGE_POSITION);
            Prepared cleared = prepare(fixture, current, currentScope);
            fixture.expirePending(id);

            fixture.storage.reclaimChunks(3);
            assertThat(fixture.manifest(id)).doesNotContainKey("pending");
            assertThat(cleared.publication.renew()).isFalse();
            assertThat(cleared.publication.promote(cleared.encoded)).isFalse();
            assertStored(fixture, previous, previousScope);
            assertThat(fixture.onlyChunk(cleared.token).getString("state")).isEqualTo("RETIRED");

            Prepared retry = prepare(fixture, current, currentScope);
            assertThat(retry.token).isNotEqualTo(cleared.token);
            assertThat(retry.publication.promote(retry.encoded)).isTrue();
            assertThat(fixture.onlyChunk(retry.token).getString("state")).isEqualTo("ACTIVE");
            assertThat(fixture.onlyChunk(cleared.token).getString("state")).isEqualTo("RETIRED");
            assertStored(fixture, current, currentScope);
        }
    }

    @Test
    void clearingAnUpgradePendingDoesNotLetAnOlderWriterHideTheScopedLegacyCurrent() {
        try (Fixture fixture = fixture("latest_protocol_legacy_empty_manifest_it")) {
            String id = "flow";
            ObservationStore.Scope legacyScope = scope("inc", 42);
            Observation legacy = observation(id, FIRST, "legacy current");
            Document legacyDocument = MongoObservationStore.toDocument(legacy)
                    .append("pipelineIncarnationId", legacyScope.pipelineIncarnationId())
                    .append("executionGeneration", legacyScope.executionGeneration());
            fixture.manifests.insertOne(legacyDocument);
            Prepared abandoned = prepare(fixture, observation(id, FIRST.plusSeconds(1), LARGE_POSITION),
                    scope("inc", 43));
            assertStored(fixture, legacy, legacyScope);
            fixture.expirePending(id);
            fixture.storage.reclaimChunks(3);
            assertThat(fixture.manifest(id)).doesNotContainKeys("current", "pending");
            assertThat(abandoned.publication.renew()).isFalse();
            assertThat(abandoned.publication.promote(abandoned.encoded)).isFalse();

            ObservationStore.Scope olderScope = scope("inc", 41);
            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(2), "older inline"), olderScope))
                    .isFalse();
            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(2), LARGE_POSITION), olderScope))
                    .isFalse();
            assertThat(fixture.manifest(id)).doesNotContainKey("current");
            assertThat(fixture.manifests.find(new Document("_id", id)).first()).isEqualTo(legacyDocument);
            assertStored(fixture, legacy, legacyScope);
        }
    }

    @Test
    void aHeartbeatFencesAStaleJanitorClearAfterTheOriginalLeaseExpires() throws Exception {
        CommandGate gate = new CommandGate(MongoLatestObservationProtocolIT::isPendingClear);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (Fixture fixture = fixture("latest_protocol_heartbeat_clear_it", gate)) {
            String id = "flow";
            ObservationStore.Scope run = scope("inc", 1);
            Observation current = observation(id, FIRST, LARGE_POSITION);
            Prepared prepared = prepare(fixture, current, run);
            Instant originalDeadline = fixture.shortenPendingLease(id, 2_000);
            gate.arm();
            Future<ObservationStore.ReclaimResult> reclaim = worker.submit(
                    () -> fixture.storage.reclaimChunks(3));
            try {
                assertThat(gate.entered.await(2, TimeUnit.SECONDS))
                        .as("janitor pauses after selecting the old lease and before clearing it")
                        .isTrue();
                assertThat(prepared.publication.renew()).isTrue();
                assertThat(fixture.pending(id).getDate("publishUntil").toInstant())
                        .isAfter(originalDeadline);
                awaitServerTime(fixture, originalDeadline);
            } finally {
                gate.release();
            }
            assertThat(reclaim.get(4, TimeUnit.SECONDS).scanned()).isBetween(1L, 3L);
            assertThat(fixture.pending(id).getString("publicationToken")).isEqualTo(prepared.token);
            assertThat(fixture.onlyChunk(prepared.token).getString("state")).isEqualTo("ACTIVE");
            assertThat(prepared.publication.promote(prepared.encoded)).isTrue();
            assertStored(fixture, current, run);
        } finally {
            gate.release();
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void currentStoppedAndPendingChunksSurviveRepeatedBoundedReclamation() {
        try (Fixture fixture = fixture("latest_protocol_retained_chunks_it")) {
            String id = "flow";
            ObservationStore.Scope stoppedScope = scope("inc", 1);
            Observation stopped = new Observation(id, PipelineState.STOPPED, Map.of("records.out", 1L),
                    Map.of(), Map.of("orders", LARGE_POSITION), null, FIRST);
            assertThat(fixture.storage.save(stopped, stoppedScope)).isTrue();
            String currentToken = fixture.manifest(id).get("current", Document.class)
                    .getString("publicationToken");
            ObservationStore.Scope nextScope = scope("inc", 2);
            Observation next = observation(id, FIRST.plusSeconds(1), LARGE_POSITION);
            Prepared pending = prepare(fixture, next, nextScope);

            for (int pass = 0; pass < 8; pass++) {
                ObservationStore.ReclaimResult result = fixture.storage.reclaimChunks(3);
                assertThat(result.scanned()).isLessThanOrEqualTo(3);
                assertThat(result.deleted()).isZero();
            }
            for (String token : List.of(currentToken, pending.token)) {
                Document retained = fixture.onlyChunk(token);
                assertThat(retained.getString("state")).isEqualTo("ACTIVE");
                assertThat(retained).doesNotContainKey("deleteAfter");
            }
            assertStored(fixture, stopped, stoppedScope);
            assertThat(pending.publication.promote(pending.encoded)).isTrue();
            assertStored(fixture, next, nextScope);
        }
    }

    @Test
    void aStaleRetiredChunkDeleteCannotMatchANewPublicationToken() throws Exception {
        CommandGate gate = new CommandGate(MongoLatestObservationProtocolIT::isRetiredChunkDelete);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (Fixture fixture = fixture("latest_protocol_retired_delete_it", gate)) {
            String id = "flow";
            ObservationStore.Scope oldScope = scope("inc", 1);
            assertThat(fixture.storage.save(observation(id, FIRST, LARGE_POSITION), oldScope)).isTrue();
            String oldToken = fixture.manifest(id).get("current", Document.class)
                    .getString("publicationToken");
            assertThat(fixture.storage.save(observation(id, FIRST.plusSeconds(1), "inline"),
                    scope("inc", 2))).isTrue();
            fixture.storage.reclaimChunks(3);
            Document retired = fixture.onlyChunk(oldToken);
            assertThat(retired.getString("state")).isEqualTo("RETIRED");
            fixture.chunks.updateOne(new Document("_id", retired.get("_id")),
                    new Document("$set", new Document("deleteAfter", Date.from(Instant.EPOCH))));
            gate.arm();
            Future<ObservationStore.ReclaimResult> reclaim = worker.submit(
                    () -> fixture.storage.reclaimChunks(3));
            ObservationStore.Scope newScope = scope("inc", 3);
            Observation current = observation(id, FIRST.plusSeconds(2), LARGE_POSITION);
            String newToken;
            try {
                assertThat(gate.entered.await(2, TimeUnit.SECONDS))
                        .as("janitor pauses after selecting the retired token and before deleting it")
                        .isTrue();
                Prepared newer = prepare(fixture, current, newScope);
                newToken = newer.token;
                assertThat(newToken).isNotEqualTo(oldToken);
                assertThat(newer.publication.promote(newer.encoded)).isTrue();
            } finally {
                gate.release();
            }
            assertThat(reclaim.get(4, TimeUnit.SECONDS).deleted()).isEqualTo(1);
            assertThat(fixture.chunks.countDocuments(new Document("publicationToken", oldToken))).isZero();
            assertThat(fixture.onlyChunk(newToken).getString("state")).isEqualTo("ACTIVE");
            assertStored(fixture, current, newScope);
        } finally {
            gate.release();
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void aReaderHoldingAnOldDescriptorCompletesWhileItsChunksAreRetired() throws Exception {
        CommandGate gate = new CommandGate(MongoLatestObservationProtocolIT::isChunkGetMore);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (Fixture fixture = fixture("latest_protocol_reader_retirement_it", gate)) {
            String id = "flow";
            ObservationStore.Scope oldScope = scope("inc", 1);
            String position = "x".repeat(MongoLatestObservationStorage.READ_CHUNK_BATCH_SIZE
                    * LatestObservationPayloadCodec.CHUNK_PAYLOAD_LIMIT + 1);
            Observation old = observation(id, FIRST, position);
            assertThat(fixture.storage.save(old, oldScope)).isTrue();
            Document descriptor = fixture.manifest(id).get("current", Document.class);
            String oldToken = descriptor.getString("publicationToken");
            long chunkCount = descriptor.getLong("chunkCount");
            assertThat(chunkCount).isEqualTo(MongoLatestObservationStorage.READ_CHUNK_BATCH_SIZE + 1L);
            Instant readerWholeDeadline = fixture.serverTime()
                    .plusSeconds(MongoLatestObservationStorage.READ_DEADLINE_SECONDS);
            gate.arm();
            Future<Optional<ObservationStore.Stored>> reader = worker.submit(() -> fixture.store.readStored(id));
            ObservationStore.Scope newScope = scope("inc", 2);
            Observation current = observation(id, FIRST.plusSeconds(1), "new inline current");
            try {
                assertThat(gate.entered.await(2, TimeUnit.SECONDS))
                        .as("reader holds the old descriptor and its first batch before requesting the final chunk")
                        .isTrue();
                assertThat(fixture.storage.save(current, newScope)).isTrue();
                int reclaimLimit = Math.toIntExact(4 * chunkCount);
                ObservationStore.ReclaimResult retired = fixture.storage.reclaimChunks(reclaimLimit);
                assertThat(retired.scanned()).isEqualTo(chunkCount);
                assertThat(retired.deleted()).isZero();
                List<Document> retained = fixture.chunks.find(new Document("publicationToken", oldToken))
                        .sort(new Document("ordinal", 1)).into(new java.util.ArrayList<>());
                assertThat(retained).hasSize(Math.toIntExact(chunkCount));
                for (int ordinal = 0; ordinal < retained.size(); ordinal++) {
                    Document chunk = retained.get(ordinal);
                    assertThat(chunk.getLong("ordinal")).isEqualTo(ordinal);
                    assertThat(chunk.getString("state")).isEqualTo("RETIRED");
                    assertThat(chunk.getDate("deleteAfter").toInstant()).isAfter(readerWholeDeadline);
                    assertThat(chunk.get("payload")).isNotNull();
                }
                assertThat(fixture.storage.reclaimChunks(reclaimLimit).deleted()).isZero();
                assertThat(reader.isDone()).isFalse();
            } finally {
                gate.release();
            }
            assertThat(reader.get(4, TimeUnit.SECONDS))
                    .contains(new ObservationStore.Stored(old, Optional.of(oldScope)));
            assertStored(fixture, current, newScope);
        } finally {
            gate.release();
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void manifestSnapshotsCannotDeleteARepublishPendingRenewalOrPromotion() {
        try (Fixture fixture = fixture("latest_protocol_manifest_revision_it")) {
            String id = "flow";
            assertThat(fixture.storage.save(observation(id, FIRST, "before"), scope("inc-old", 1))).isTrue();
            ObservationStore.ManifestSnapshot old = fixture.onlyManifest();
            ObservationStore.Scope currentScope = scope("inc-new", 2);
            Observation current = observation(id, FIRST.plusSeconds(1), "after");
            assertThat(fixture.storage.save(current, currentScope)).isTrue();
            assertThat(fixture.storage.deleteManifestIfUnchanged(old)).isFalse();
            assertStored(fixture, current, currentScope);

            ObservationStore.ManifestSnapshot committed = fixture.onlyManifest();
            ObservationStore.Scope nextScope = scope("inc-new", 3);
            Observation next = observation(id, FIRST.plusSeconds(2), LARGE_POSITION);
            Prepared prepared = prepare(fixture, next, nextScope);
            assertThat(fixture.storage.deleteManifestIfUnchanged(committed)).isFalse();
            ObservationStore.ManifestSnapshot pending = fixture.onlyManifest();
            assertThat(pending.scopes()).containsExactly(currentScope, nextScope);
            assertThat(prepared.publication.renew()).isTrue();
            assertThat(fixture.storage.deleteManifestIfUnchanged(pending)).isFalse();
            ObservationStore.ManifestSnapshot renewed = fixture.onlyManifest();
            assertThat(prepared.publication.promote(prepared.encoded)).isTrue();
            assertThat(fixture.storage.deleteManifestIfUnchanged(renewed)).isFalse();
            assertStored(fixture, next, nextScope);

            assertThat(fixture.storage.deleteManifestIfUnchanged(fixture.onlyManifest())).isTrue();
            assertThat(prepared.publication.promote(prepared.encoded)).isFalse();
            assertThat(fixture.manifest(id)).isNull();
            assertThat(fixture.store.readStored(id)).isEmpty();
        }
    }

    @Test
    void manifestScansUseBoundedKeysetPagesWithoutReadingPayloadsOrLegacyDocuments() {
        List<BsonDocument> finds = new CopyOnWriteArrayList<>();
        CommandListener trace = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if ("find".equals(event.getCommandName())) {
                    finds.add(event.getCommand().clone());
                }
            }
        };
        try (Fixture fixture = fixture("latest_protocol_manifest_scan_it", trace)) {
            ObservationStore.Scope run = scope("inc", 1);
            for (String id : List.of("a", "b", "c")) {
                assertThat(fixture.storage.save(observation(id, FIRST, "small"), run)).isTrue();
            }
            fixture.manifests.insertOne(MongoObservationStore.toDocument(observation("legacy", FIRST, "old")));
            finds.clear();
            List<ObservationStore.ManifestSnapshot> all = fixture.storage.scanManifestsAfter(Optional.empty(), 3);
            List<ObservationStore.ManifestSnapshot> first = fixture.storage.scanManifestsAfter(Optional.empty(), 2);
            List<ObservationStore.ManifestSnapshot> second = fixture.storage.scanManifestsAfter(
                    Optional.of(first.get(1).cursor()), 2);
            assertThat(all).hasSize(3);
            assertThat(first).containsExactlyElementsOf(all.subList(0, 2));
            assertThat(second).containsExactly(all.get(2));
            assertThat(finds).hasSize(3);
            for (BsonDocument find : finds) {
                assertThat(find.getInt32("limit").getValue()).isBetween(2, 3);
                assertThat(find.getDocument("sort").getInt32("_id").getValue()).isEqualTo(1);
                assertThat(find.getDocument("filter").getDocument("_id").getString("$type").getValue())
                        .isEqualTo("binData");
                assertThat(find.getDocument("projection").keySet()).containsExactlyInAnyOrder(
                        "_id", "formatVersion", "ownerDigest", "revision", "legacyFallback", "legacyResidue",
                        "current.pipelineIncarnationId", "current.executionGeneration",
                        "pending.pipelineIncarnationId", "pending.executionGeneration",
                        "continuation.sourceScope", "continuation.target.scope", "continuation.baselineOrigin.scope",
                        "continuationPending.sourceScope", "continuationPending.target.scope",
                        "continuationPending.baselineOrigin.scope");
            }
            assertThat(finds.get(2).getDocument("filter").getDocument("_id").get("$gt").isBinary()).isTrue();
        }
    }

    private static Prepared prepare(Fixture fixture, Observation observation, ObservationStore.Scope scope) {
        MongoLatestObservationStorage.ManifestChunks publication = fixture.storage.publication(observation, scope);
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(observation, publication);
        assertThat(encoded.inline()).isFalse();
        assertThat(encoded.chunkCount()).isEqualTo(1);
        String token = fixture.pending(observation.pipelineId()).getString("publicationToken");
        assertThat(fixture.chunks.countDocuments(new Document("publicationToken", token))).isEqualTo(1);
        return new Prepared(publication, encoded, token);
    }

    private static void assertStored(Fixture fixture, Observation observation, ObservationStore.Scope scope) {
        assertThat(fixture.store.readStored(observation.pipelineId()))
                .contains(new ObservationStore.Stored(observation, Optional.of(scope)));
    }

    private static ObservationStore.Scope scope(String incarnation, long generation) {
        return new ObservationStore.Scope(incarnation, generation);
    }

    private static Observation observation(String id, Instant observedAt, String position) {
        return new Observation(id, PipelineState.RUNNING, Map.of("records.out", 1L),
                Map.of(), Map.of("orders", position), null, observedAt);
    }

    private static void awaitServerTime(Fixture fixture, Instant threshold) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (fixture.serverTime().isBefore(threshold)) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("MongoDB did not reach the original lease deadline");
            }
            Thread.sleep(10);
        }
    }

    private static boolean isPendingClear(CommandStartedEvent event) {
        if (!"update".equals(event.getCommandName())
                || !MongoStorePort.PIPELINE_OBSERVATION.equals(event.getCommand().getString("update").getValue())) {
            return false;
        }
        for (BsonValue raw : event.getCommand().getArray("updates")) {
            BsonDocument update = raw.asDocument();
            BsonDocument filter = update.getDocument("q");
            BsonValue operation = update.get("u");
            if (filter.containsKey("pending.publishUntil") && filter.containsKey("pending.publicationToken")
                    && operation.isDocument() && operation.asDocument().containsKey("$unset")
                    && operation.asDocument().getDocument("$unset").containsKey("pending")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isRetiredChunkDelete(CommandStartedEvent event) {
        if (!"delete".equals(event.getCommandName())
                || !MongoStorePort.PIPELINE_OBSERVATION_CHUNKS.equals(event.getCommand().getString("delete").getValue())) {
            return false;
        }
        for (BsonValue raw : event.getCommand().getArray("deletes")) {
            BsonDocument filter = raw.asDocument().getDocument("q");
            if (filter.containsKey("publicationToken") && filter.containsKey("deleteAfter")
                    && filter.containsKey("state") && "RETIRED".equals(filter.getString("state").getValue())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isChunkGetMore(CommandStartedEvent event) {
        return "getMore".equals(event.getCommandName())
                && MongoStorePort.PIPELINE_OBSERVATION_CHUNKS.equals(
                        event.getCommand().getString("collection").getValue());
    }

    private static Fixture fixture(String databaseName) {
        return fixture(databaseName, new CommandListener() { });
    }

    private static Fixture fixture(String databaseName, CommandListener listener) {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build();
        MongoClient client = MongoClients.create(settings);
        MongoDatabase database = client.getDatabase(databaseName);
        database.drop();
        MongoCollection<Document> manifests = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION);
        MongoCollection<Document> chunks = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS);
        return new Fixture(client, database, manifests, chunks,
                new MongoLatestObservationStorage(client, manifests, chunks),
                new MongoObservationStore(client, manifests, chunks));
    }

    private record Prepared(MongoLatestObservationStorage.ManifestChunks publication,
            LatestObservationPayloadCodec.Encoded encoded, String token) { }

    private record Fixture(MongoClient client, MongoDatabase database, MongoCollection<Document> manifests,
            MongoCollection<Document> chunks, MongoLatestObservationStorage storage,
            MongoObservationStore store) implements AutoCloseable {

        Document manifest(String pipelineId) {
            return manifests.find(new Document("_id", MongoLatestObservationStorage.manifestKey(pipelineId))).first();
        }

        Document pending(String pipelineId) {
            return manifest(pipelineId).get("pending", Document.class);
        }

        Document onlyChunk(String token) {
            List<Document> selected = chunks.find(new Document("publicationToken", token)).into(new java.util.ArrayList<>(2));
            assertThat(selected).hasSize(1);
            return selected.get(0);
        }

        ObservationStore.ManifestSnapshot onlyManifest() {
            List<ObservationStore.ManifestSnapshot> selected = storage.scanManifestsAfter(Optional.empty(), 2);
            assertThat(selected).hasSize(1);
            return selected.get(0);
        }

        Instant serverTime() {
            return database.runCommand(new Document("hello", 1)).getDate("localTime").toInstant();
        }

        void expirePending(String pipelineId) {
            manifests.updateOne(new Document("_id", MongoLatestObservationStorage.manifestKey(pipelineId)),
                    new Document("$set", new Document("pending.publishUntil", Date.from(Instant.EPOCH))
                            .append("revision", UUID.randomUUID().toString())));
        }

        Instant shortenPendingLease(String pipelineId, long milliseconds) {
            manifests.updateOne(new Document("_id", MongoLatestObservationStorage.manifestKey(pipelineId)),
                    List.of(new Document("$set", new Document("pending.publishUntil",
                            new Document("$dateAdd", new Document("startDate", "$$NOW")
                                    .append("unit", "millisecond").append("amount", milliseconds)))
                            .append("revision", UUID.randomUUID().toString()))));
            return pending(pipelineId).getDate("publishUntil").toInstant();
        }

        @Override public void close() { client.close(); }
    }

    private static final class CommandGate implements CommandListener {
        private final Predicate<CommandStartedEvent> matches;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean held = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);

        CommandGate(Predicate<CommandStartedEvent> matches) {
            this.matches = matches;
        }

        void arm() { armed.set(true); }
        void release() { resumed.countDown(); }

        @Override
        public void commandStarted(CommandStartedEvent event) {
            if (!armed.get() || !matches.test(event) || !held.compareAndSet(false, true)) {
                return;
            }
            entered.countDown();
            try {
                if (!resumed.await(4, TimeUnit.SECONDS)) {
                    throw new AssertionError("the held MongoDB command was not released within its deadline");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while holding the MongoDB command", interrupted);
            }
        }
    }
}
