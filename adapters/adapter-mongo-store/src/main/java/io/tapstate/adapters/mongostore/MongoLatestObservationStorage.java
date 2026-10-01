package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** MongoDB's bounded manifest and immutable-chunk representation of one logical latest observation. */
final class MongoLatestObservationStorage {

    static final int FORMAT_VERSION = 1;
    static final long PUBLISH_LEASE_SECONDS = 30;
    static final long PUBLISH_HEARTBEAT_NANOS = TimeUnit.SECONDS.toNanos(10);
    static final long IO_DEADLINE_SECONDS = 5;
    static final long READ_DEADLINE_SECONDS = 10;
    static final long READ_ATTEMPT_SECONDS = 4;
    static final long RETIRE_GRACE_SECONDS = 15;
    static final int READ_CHUNK_BATCH_SIZE = 4;
    static final WriteConcern CHUNK_WRITE_CONCERN = WriteConcern.MAJORITY.withJournal(true);
    private static final TransactionOptions CUTOVER_TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).readPreference(ReadPreference.primary())
            .writeConcern(CHUNK_WRITE_CONCERN).timeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS).build();

    private static final byte[] OWNER_DOMAIN = "tapstate/latest-owner/v1\0"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] CHUNK_ID_DOMAIN = "tapstate/latest-chunk-id/v1\0"
            .getBytes(StandardCharsets.UTF_8);
    private static final String FORMAT = "formatVersion";
    private static final String OWNER = "ownerDigest";
    private static final String CURRENT = "current";
    private static final String PENDING = "pending";
    private static final String REVISION = "revision";
    private static final String LEGACY_FALLBACK = "legacyFallback";
    private static final String LEGACY_RESIDUE = "legacyResidue";
    private static final String TOKEN = "publicationToken";
    private static final String INCARNATION = "pipelineIncarnationId";
    private static final String GENERATION = "executionGeneration";
    private static final String OBSERVED_AT = "observedAt";
    private static final String ENCODING = "encodingVersion";
    private static final String MODE = "mode";
    private static final String DIGEST = "payloadDigest";
    private static final String CHUNK_COUNT = "chunkCount";
    private static final String ENCODED_BYTES = "encodedBytes";
    private static final String INLINE_PAYLOAD = "inlinePayload";

    private static final String MANIFEST_KEY = "manifestKey";
    private static final String ORDINAL = "ordinal";
    private static final String CHUNK_DIGEST = "chunkDigest";
    private static final String CHUNK_PAYLOAD = "payload";
    private static final String STATE = "state";
    private static final String ACTIVE = "ACTIVE";
    private static final String RETIRED = "RETIRED";
    private static final String DELETE_AFTER = "deleteAfter";
    private final MongoClient client;
    private final MongoCollection<Document> manifests;
    private final MongoCollection<Document> chunks;
    private TimedCursor pendingAfter;
    private TimedCursor continuationPendingAfter;
    private TimedCursor retiredAfter;
    private Binary chunkAfter;
    private int reclaimStartPhase;

    private enum CurrentWrite { APPLIED, ABSENT, UNCOMMITTED, REFUSED }

    MongoLatestObservationStorage(MongoClient client, MongoCollection<Document> manifests,
            MongoCollection<Document> chunks) {
        this.client = Objects.requireNonNull(client, "client");
        this.manifests = Objects.requireNonNull(manifests, "manifests")
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(CHUNK_WRITE_CONCERN);
        this.chunks = Objects.requireNonNull(chunks, "chunks")
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(CHUNK_WRITE_CONCERN);
    }

    boolean save(Observation observation, ObservationStore.Scope scope) {
        Objects.requireNonNull(observation, "observation");
        Objects.requireNonNull(scope, "scope");
        Instant observedAt = Objects.requireNonNull(observation.observedAt(), "observedAt");
        if (observedAt.getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException("a scoped observation time must have millisecond precision");
        }
        String pipelineId = observation.pipelineId();
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        ManifestChunks writer = publication(observation, scope);
        try {
            LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(
                    observation, writer);
            return encoded.inline()
                    ? writeInline(observation, key, owner, scope, observedAt, encoded)
                    : writer.promote(encoded);
        } catch (StalePublication stale) {
            return false;
        }
    }

    ManifestChunks publication(Observation observation, ObservationStore.Scope scope) {
        return new ManifestChunks(observation, manifestKey(observation.pipelineId()),
                ownerDigest(observation.pipelineId()), scope, observation.observedAt());
    }

    record PreparedCurrent(Document descriptor, Document filter, ManifestChunks writer) { }

    PreparedCurrent prepareCurrent(Observation observation, ObservationStore.Scope scope) {
        Objects.requireNonNull(observation.observedAt(), "observedAt");
        if (observation.observedAt().getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException("a scoped observation time must have millisecond precision");
        }
        ObservationBsonBounds.requireHeader(observation.pipelineId(), baseDescriptor(scope, observation.observedAt()));
        ManifestChunks writer = publication(observation, scope);
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(observation, writer);
        Document descriptor = encoded.inline() ? inlineCurrent(scope, observation.observedAt(), encoded)
                : chunkedCurrent(scope, observation.observedAt(), writer.token, encoded);
        ObservationBsonBounds.requireDescriptor(observation.pipelineId(), descriptor);
        Document filter = headerFilter(writer.key, writer.owner).append("$and", List.of(
                currentFence(scope, observation.observedAt(), encoded.payloadDigest(),
                        encoded.inline() ? "inline" : "chunked", false),
                encoded.inline() ? pendingAvailableForInline(scope) : new Document(PENDING + "." + TOKEN, writer.token)
                        .append("$expr", new Document("$gt", List.of("$" + PENDING + ".publishUntil", "$$NOW")))));
        return new PreparedCurrent(descriptor, filter, writer);
    }

    void abandon(PreparedCurrent prepared) {
        if (prepared.writer().begun && !prepared.writer().replay) {
            clearOwnedPending(prepared.writer().key, prepared.writer().owner, prepared.writer().token);
        }
    }

    ObservationStore.Stored readPublicDescriptor(String pipelineId, Document descriptor) {
        return readCurrent(pipelineId, manifestKey(pipelineId), ownerDigest(pipelineId), descriptor,
                new ReadDeadline());
    }

    void saveLegacy(Observation observation) {
        String pipelineId = observation.pipelineId();
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                StoreIo.run(pipelineId, () -> {
                    try (ClientSession session = client.startSession()) {
                        session.withTransaction(() -> {
                            readHeader(session, pipelineId, key, owner);
                            try {
                                manifests.updateOne(session, headerFilter(key, owner),
                                        new Document("$setOnInsert", new Document(FORMAT, FORMAT_VERSION)
                                                .append(OWNER, owner).append(LEGACY_FALLBACK, true))
                                                .append("$set", new Document(LEGACY_RESIDUE, true)
                                                        .append(REVISION, UUID.randomUUID().toString())),
                                        new UpdateOptions().upsert(true));
                            } catch (MongoException conflict) {
                                if (duplicateKey(conflict)) {
                                    throw new FirstManifestRaced();
                                }
                                throw conflict;
                            }
                            manifests.replaceOne(session, new Document("_id", pipelineId),
                                    MongoObservationStore.toDocument(observation), new ReplaceOptions().upsert(true));
                            return null;
                        }, CUTOVER_TRANSACTION);
                    }
                });
                return;
            } catch (FirstManifestRaced raced) {
                readHeader(pipelineId, key, owner);
            }
        }
        throw new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "latest observation format transition raced a compatibility write"), null);
    }

    Optional<ObservationStore.Stored> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return readOnce(pipelineId, new ReadDeadline(), true);
    }

    private Optional<ObservationStore.Stored> readOnce(String pipelineId, ReadDeadline deadline,
            boolean mayRetryChangedManifest) {
        ReadDeadline attempt = deadline.limitedTo(READ_ATTEMPT_SECONDS);
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        Document manifest = StoreIo.call(() -> manifests.withTimeout(attempt.remainingMillis(), TimeUnit.MILLISECONDS)
                .find(new Document("_id", key)).first());
        if (manifest != null) {
            validateHeader(manifest, pipelineId, owner);
            if (manifest.containsKey(PENDING) && manifest.get(PENDING) == null) {
                throw corrupt(pipelineId, PENDING);
            }
            validatePending(manifest.get(PENDING), pipelineId);
            Object rawCurrent = manifest.get(CURRENT);
            if (manifest.containsKey(CURRENT) && rawCurrent == null) {
                throw corrupt(pipelineId, CURRENT);
            }
            if (rawCurrent != null) {
                if (!(rawCurrent instanceof Document current)) {
                    throw corrupt(pipelineId, CURRENT);
                }
                String revision = requireString(manifest.get(REVISION), pipelineId, REVISION);
                try {
                    return Optional.of(readCurrent(pipelineId, key, owner, current, attempt));
                } catch (TapstateException failed) {
                    if (mayRetryChangedManifest && (failed.code() == IoError.STORE_UNAVAILABLE
                            || manifestRevisionChanged(key, revision, deadline))) {
                        return readOnce(pipelineId, deadline, false);
                    }
                    throw failed;
                }
            }
            if (!manifest.getBoolean(LEGACY_FALLBACK)) {
                return Optional.empty();
            }
        }
        return readLegacy(pipelineId, attempt);
    }

    private boolean manifestRevisionChanged(Binary key, String revision, ReadDeadline deadline) {
        Document current = StoreIo.call(() -> manifests.withTimeout(deadline.remainingMillis(), TimeUnit.MILLISECONDS)
                .find(new Document("_id", key)).projection(new Document(REVISION, 1)).first());
        return current == null || !revision.equals(current.getString(REVISION));
    }

    void delete(String pipelineId) {
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        StoreIo.run(() -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    readHeader(session, pipelineId, key, owner);
                    manifests.deleteOne(session, new Document("_id", pipelineId));
                    manifests.deleteOne(session, headerFilter(key, owner));
                    return null;
                }, CUTOVER_TRANSACTION);
            }
        });
    }

    void deleteIncarnation(String pipelineId, String incarnationId) {
        Objects.requireNonNull(incarnationId, "incarnationId");
        if (incarnationId.isBlank()) {
            throw new IllegalArgumentException("observation incarnation must not be blank");
        }
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        readHeader(pipelineId, key, owner);
        unsetOwnedDescriptor(key, owner, CURRENT, incarnationId);
        unsetOwnedDescriptor(key, owner, PENDING, incarnationId);
        unsetPrivateIncarnation(key, owner, MongoObservationContinuation.CONTINUATION, incarnationId);
        unsetPrivateIncarnation(key, owner, MongoObservationContinuation.CONTINUATION_PENDING, incarnationId);
        deleteLegacyMatching(pipelineId, new Document("_id", pipelineId).append(INCARNATION, incarnationId));
    }

    void deleteLegacy(String pipelineId) {
        deleteLegacyMatching(pipelineId, new Document("_id", pipelineId)
                .append(INCARNATION, new Document("$exists", false))
                .append(GENERATION, new Document("$exists", false)));
    }

    boolean deleteLegacyIfUnchanged(ObservationStore.LatestSnapshot snapshot) {
        return deleteLegacyMatching(snapshot.pipelineId(), legacySnapshotFilter(snapshot)) != 0;
    }

    boolean deleteLegacyIfUnchanged(ClientSession session, ObservationStore.LatestSnapshot snapshot) {
        if (deleteLegacyMatching(session, snapshot.pipelineId(), legacySnapshotFilter(snapshot)) == 0) {
            throw MongoStopReservationWrites.fencedHandoff();
        }
        return true;
    }

    private static Document legacySnapshotFilter(ObservationStore.LatestSnapshot snapshot) {
        Document filter = new Document("_id", snapshot.pipelineId());
        if (snapshot.scope().isPresent()) {
            ObservationStore.Scope scope = snapshot.scope().orElseThrow();
            filter.append(INCARNATION, scope.pipelineIncarnationId())
                    .append(GENERATION, scope.executionGeneration());
        } else {
            filter.append(INCARNATION, new Document("$exists", false))
                    .append(GENERATION, new Document("$exists", false));
        }
        filter.append(OBSERVED_AT, snapshot.observedAt().<Object>map(Date::from)
                .orElseGet(() -> new Document("$exists", false)));
        return filter;
    }

    private long deleteLegacyMatching(String pipelineId, Document filter) {
        return StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> deleteLegacyMatching(session, pipelineId, filter), CUTOVER_TRANSACTION);
            }
        });
    }

    private long deleteLegacyMatching(ClientSession session, String pipelineId, Document filter) {
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        readHeader(session, pipelineId, key, owner);
        long deleted = manifests.deleteOne(session, filter).getDeletedCount();
        Document residue = manifests.find(session, new Document("_id", pipelineId))
                .projection(new Document("_id", 1)).first();
        if (residue == null) {
            manifests.updateOne(session, headerFilter(key, owner), new Document("$set",
                    new Document(LEGACY_RESIDUE, false).append(REVISION, UUID.randomUUID().toString())));
            manifests.deleteOne(session, headerFilter(key, owner)
                    .append(CURRENT, new Document("$exists", false))
                    .append(PENDING, new Document("$exists", false))
                    .append(MongoObservationContinuation.CONTINUATION, new Document("$exists", false))
                    .append(MongoObservationContinuation.CONTINUATION_PENDING, new Document("$exists", false))
                    .append(LEGACY_RESIDUE, false));
        }
        return deleted;
    }

    boolean hasCommittedManifest(String pipelineId) {
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        Document header = readHeader(pipelineId, key, owner);
        return header != null && !header.getBoolean(LEGACY_FALLBACK);
    }

    List<ObservationStore.ManifestSnapshot> scanManifestsAfter(Optional<String> afterCursor, int limit) {
        validateBatch(afterCursor, limit);
        Document id = new Document("$type", "binData");
        afterCursor.map(MongoLatestObservationStorage::decodeCursor)
                .ifPresent(after -> id.append("$gt", after));
        Document projection = new Document("_id", 1).append(FORMAT, 1).append(OWNER, 1)
                .append(REVISION, 1).append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1)
                .append(CURRENT + "." + INCARNATION, 1).append(CURRENT + "." + GENERATION, 1)
                .append(PENDING + "." + INCARNATION, 1).append(PENDING + "." + GENERATION, 1)
                .append(MongoObservationContinuation.CONTINUATION + ".sourceScope", 1)
                .append(MongoObservationContinuation.CONTINUATION + ".target.scope", 1)
                .append(MongoObservationContinuation.CONTINUATION + ".baselineOrigin.scope", 1)
                .append(MongoObservationContinuation.CONTINUATION_PENDING + ".sourceScope", 1)
                .append(MongoObservationContinuation.CONTINUATION_PENDING + ".target.scope", 1)
                .append(MongoObservationContinuation.CONTINUATION_PENDING + ".baselineOrigin.scope", 1);
        List<Document> page = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", id)).projection(projection).sort(new Document("_id", 1))
                .limit(limit).into(new ArrayList<>(limit)));
        List<ObservationStore.ManifestSnapshot> snapshots = new ArrayList<>(page.size());
        for (Document manifest : page) {
            Binary key = requireBinaryValue(manifest.get("_id"), "manifest", "_id");
            Binary owner = requireBinaryValue(manifest.get(OWNER), encodeCursor(key), OWNER);
            validateHeader(manifest, encodeCursor(key), owner);
            String revision = manifest.getString(REVISION);
            List<ObservationStore.Scope> scopes = new ArrayList<>(2);
            if ((manifest.containsKey(CURRENT) && manifest.get(CURRENT) == null)
                    || (manifest.containsKey(PENDING) && manifest.get(PENDING) == null)) {
                throw corrupt(encodeCursor(key), "latest manifest descriptors");
            }
            addScope(manifest.get(CURRENT), scopes, encodeCursor(key), CURRENT);
            addScope(manifest.get(PENDING), scopes, encodeCursor(key), PENDING);
            addPrivateScopes(manifest.get(MongoObservationContinuation.CONTINUATION), scopes, encodeCursor(key));
            addPrivateScopes(manifest.get(MongoObservationContinuation.CONTINUATION_PENDING), scopes, encodeCursor(key));
            snapshots.add(new ObservationStore.ManifestSnapshot(encodeCursor(key), revision, scopes));
        }
        return List.copyOf(snapshots);
    }

    boolean deleteManifestIfUnchanged(ObservationStore.ManifestSnapshot snapshot) {
        Binary key = decodeCursor(snapshot.cursor());
        Document header = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", key).append(REVISION, snapshot.revision()))
                .projection(headerProjection()).first());
        if (header == null) {
            return false;
        }
        Binary owner = requireBinaryValue(header.get(OWNER), snapshot.cursor(), OWNER);
        validateHeader(header, snapshot.cursor(), owner);
        return StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .deleteOne(headerFilter(key, owner).append(REVISION, snapshot.revision())
                        .append(LEGACY_RESIDUE, false))
                .getDeletedCount()) != 0;
    }

    boolean deleteManifestIfUnchanged(ClientSession session, ObservationStore.ManifestSnapshot snapshot, String pipelineId) {
        Binary key = decodeCursor(snapshot.cursor());
        Binary owner = ownerDigest(pipelineId);
        if (!key.equals(manifestKey(pipelineId))) { throw MongoStopReservationWrites.fencedHandoff(); }
        Document header = manifests.find(session, headerFilter(key, owner).append(REVISION, snapshot.revision()))
                .projection(headerProjection()).first();
        if (header == null) { throw MongoStopReservationWrites.fencedHandoff(); }
        validateHeader(header, pipelineId, owner);
        if (manifests.deleteOne(session, headerFilter(key, owner).append(REVISION, snapshot.revision())
                .append(LEGACY_RESIDUE, false)).getDeletedCount() == 0) {
            throw MongoStopReservationWrites.fencedHandoff();
        }
        return true;
    }

    synchronized ObservationStore.ReclaimResult reclaimChunks(int limit) {
        if (limit < 1 || limit > ObservationStore.MAX_LATEST_SCAN_BATCH) {
            throw new IllegalArgumentException("observation chunk cleanup batch must be between 1 and "
                    + ObservationStore.MAX_LATEST_SCAN_BATCH);
        }
        long scanned = 0;
        long deleted = 0;
        int[] budgets = reclaimBudgets(limit);
        int firstPhase = reclaimStartPhase;
        reclaimStartPhase = (reclaimStartPhase + 1) % budgets.length;
        for (int offset = 0; offset < budgets.length; offset++) {
            int phase = (firstPhase + offset) % budgets.length;
            ObservationStore.ReclaimResult result = switch (phase) {
                case 0 -> clearPendingCandidates(budgets[phase]);
                case 1 -> deleteRetiredCandidates(budgets[phase]);
                case 2 -> retireOrphanCandidates(budgets[phase]);
                case 3 -> clearContinuationPendingCandidates(budgets[phase]);
                default -> throw new IllegalStateException("unknown observation reclaim phase");
            };
            scanned = Math.addExact(scanned, result.scanned());
            deleted = Math.addExact(deleted, result.deleted());
        }
        return new ObservationStore.ReclaimResult(scanned, deleted);
    }

    private int[] reclaimBudgets(int limit) {
        int[] budgets = new int[4];
        for (int index = 0; index < limit; index++) {
            budgets[(reclaimStartPhase + index) % budgets.length]++;
        }
        return budgets;
    }

    private ObservationStore.ReclaimResult clearContinuationPendingCandidates(int limit) {
        if (limit == 0) { return new ObservationStore.ReclaimResult(0, 0); }
        String pending = MongoObservationContinuation.CONTINUATION_PENDING;
        Document filter = new Document("_id", new Document("$type", "binData"))
                .append(FORMAT, FORMAT_VERSION).append(pending + ".publishUntil", new Document("$type", "date"));
        if (continuationPendingAfter != null) {
            filter.append("$and", List.of(afterTimed(pending + ".publishUntil", continuationPendingAfter)));
        }
        List<Document> page = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(filter).projection(new Document(FORMAT, 1).append(OWNER, 1).append(REVISION, 1)
                        .append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1).append(pending, 1))
                .sort(new Document(pending + ".publishUntil", 1).append("_id", 1)).limit(limit)
                .into(new ArrayList<>(limit)));
        for (Document candidate : page) {
            Binary key = requireBinaryValue(candidate.get("_id"), "continuation", "_id");
            Binary owner = requireBinaryValue(candidate.get(OWNER), encodeCursor(key), OWNER);
            validateHeader(candidate, encodeCursor(key), owner);
            Document value = requireDocument(candidate.get(pending), encodeCursor(key), pending);
            Instant until = requireDate(value.get("publishUntil"), encodeCursor(key), pending + ".publishUntil");
            String token = requireString(value.get(TOKEN), encodeCursor(key), pending + "." + TOKEN);
            Document exact = headerFilter(key, owner).append(pending + "." + TOKEN, token)
                    .append(pending + ".publishUntil", Date.from(until))
                    .append("$expr", new Document("$lte", List.of("$" + pending + ".publishUntil", "$$NOW")));
            StoreIo.run(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .updateOne(exact, new Document("$unset", new Document(pending, true))
                            .append("$set", new Document(REVISION, UUID.randomUUID().toString()))));
            continuationPendingAfter = new TimedCursor(until, key);
        }
        if (page.size() < limit) { continuationPendingAfter = null; }
        return new ObservationStore.ReclaimResult(page.size(), 0);
    }

    private ObservationStore.ReclaimResult clearPendingCandidates(int limit) {
        if (limit == 0) {
            return new ObservationStore.ReclaimResult(0, 0);
        }
        Document filter = new Document("_id", new Document("$type", "binData"))
                .append(FORMAT, FORMAT_VERSION)
                .append(PENDING + ".publishUntil", new Document("$type", "date"));
        if (pendingAfter != null) {
            filter.append("$and", List.of(afterTimed(PENDING + ".publishUntil", pendingAfter)));
        }
        List<Document> page = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(filter).projection(new Document("_id", 1).append(REVISION, 1)
                        .append(FORMAT, 1).append(OWNER, 1).append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1)
                        .append(CURRENT + "." + MODE, 1)
                        .append(PENDING + "." + TOKEN, 1).append(PENDING + ".publishUntil", 1))
                .sort(new Document(PENDING + ".publishUntil", 1).append("_id", 1))
                .limit(limit).into(new ArrayList<>(limit)));
        for (Document candidate : page) {
            Binary key = requireBinaryValue(candidate.get("_id"), "manifest", "_id");
            String id = encodeCursor(key);
            Binary owner = requireBinaryValue(candidate.get(OWNER), id, OWNER);
            validateHeader(candidate, id, owner);
            Document pending = requireDocument(candidate.get(PENDING), id, PENDING);
            Date publishUntil = Date.from(requireDate(pending.get("publishUntil"), id,
                    PENDING + ".publishUntil"));
            Document exact = headerFilter(key, owner)
                    .append(REVISION, requireString(candidate.get(REVISION), id, REVISION))
                    .append(PENDING + "." + TOKEN,
                            requireString(pending.get(TOKEN), id, PENDING + "." + TOKEN))
                    .append(PENDING + ".publishUntil", publishUntil)
                    .append("$and", List.of(expiredPending()));
            StoreIo.run(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .updateOne(exact, new Document("$unset", new Document(PENDING, true))
                            .append("$set", new Document(REVISION, UUID.randomUUID().toString()))));
            pendingAfter = new TimedCursor(publishUntil.toInstant(), key);
        }
        if (page.size() < limit) {
            pendingAfter = null;
        }
        return new ObservationStore.ReclaimResult(page.size(), 0);
    }

    private ObservationStore.ReclaimResult deleteRetiredCandidates(int limit) {
        if (limit == 0) {
            return new ObservationStore.ReclaimResult(0, 0);
        }
        Document filter = new Document(STATE, RETIRED)
                .append(DELETE_AFTER, new Document("$type", "date"));
        if (retiredAfter != null) {
            filter.append("$and", List.of(afterTimed(DELETE_AFTER, retiredAfter)));
        }
        List<Document> page = StoreIo.call(() -> chunks.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(filter).projection(new Document("_id", 1).append(TOKEN, 1).append(DELETE_AFTER, 1))
                .sort(new Document(DELETE_AFTER, 1).append("_id", 1))
                .limit(limit).into(new ArrayList<>(limit)));
        long deleted = 0;
        for (Document candidate : page) {
            Binary id = requireBinaryValue(candidate.get("_id"), "chunk", "_id");
            String token = requireString(candidate.get(TOKEN), encodeCursor(id), TOKEN);
            Date deleteAfter = Date.from(requireDate(candidate.get(DELETE_AFTER), encodeCursor(id), DELETE_AFTER));
            deleted = Math.addExact(deleted, StoreIo.call(() -> chunks
                    .withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .deleteOne(new Document("_id", id).append(TOKEN, token).append(STATE, RETIRED)
                            .append(DELETE_AFTER, deleteAfter).append("$and", List.of(dueForDeletion())))
                    .getDeletedCount()));
            retiredAfter = new TimedCursor(deleteAfter.toInstant(), id);
        }
        if (page.size() < limit) {
            retiredAfter = null;
        }
        return new ObservationStore.ReclaimResult(page.size(), deleted);
    }

    private ObservationStore.ReclaimResult retireOrphanCandidates(int limit) {
        if (limit == 0) {
            return new ObservationStore.ReclaimResult(0, 0);
        }
        Document id = new Document("$type", "binData");
        if (chunkAfter != null) {
            id.append("$gt", chunkAfter);
        }
        Document active = new Document("_id", id).append(STATE, ACTIVE);
        List<Document> page = StoreIo.call(() -> chunks.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(active).projection(new Document("_id", 1).append(MANIFEST_KEY, 1)
                        .append(OWNER, 1).append(TOKEN, 1))
                .sort(new Document("_id", 1)).limit(limit).into(new ArrayList<>(limit)));
        for (Document candidate : page) {
            Binary idValue = requireBinaryValue(candidate.get("_id"), "chunk", "_id");
            Binary key = requireBinaryValue(candidate.get(MANIFEST_KEY), encodeCursor(idValue), MANIFEST_KEY);
            Binary owner = requireBinaryValue(candidate.get(OWNER), encodeCursor(idValue), OWNER);
            String token = requireString(candidate.get(TOKEN), encodeCursor(idValue), TOKEN);
            if (!manifestReferences(key, owner, token)) {
                List<Bson> retire = List.of(new Document("$set", new Document(STATE, RETIRED)
                        .append(DELETE_AFTER, new Document("$dateAdd", new Document("startDate", "$$NOW")
                                .append("unit", "second").append("amount", RETIRE_GRACE_SECONDS)))));
                StoreIo.run(() -> chunks.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                        .updateOne(new Document("_id", idValue).append(TOKEN, token)
                                .append(STATE, ACTIVE), retire));
            }
            chunkAfter = idValue;
        }
        if (page.size() < limit) {
            chunkAfter = null;
        }
        return new ObservationStore.ReclaimResult(page.size(), 0);
    }

    private static Document afterTimed(String field, TimedCursor cursor) {
        Date instant = Date.from(cursor.instant());
        return new Document("$or", List.of(
                new Document(field, new Document("$gt", instant)),
                new Document(field, instant).append("_id", new Document("$gt", cursor.id()))));
    }

    private boolean writeInline(Observation observation, Binary key, Binary owner, ObservationStore.Scope scope,
            Instant observedAt, LatestObservationPayloadCodec.Encoded encoded) {
        String pipelineId = observation.pipelineId();
        Document current = inlineCurrent(scope, observedAt, encoded);
        Document filter = headerFilter(key, owner)
                .append("$and", List.of(currentFence(scope, observedAt, encoded.payloadDigest(), "inline", false),
                        pendingAvailableForInline(scope)));
        CurrentWrite written = updateCurrentOrClassify(pipelineId, key, owner, filter, current);
        if (written == CurrentWrite.APPLIED || written == CurrentWrite.REFUSED) {
            return written == CurrentWrite.APPLIED;
        }
        try {
            if (insertFirstManifest(observation, key, owner, scope, current, null, filter)) {
                return true;
            }
        } catch (TapstateException uncertain) {
            try {
                if (classifyCurrent(pipelineId, key, owner, current) == CurrentWrite.APPLIED) {
                    return true;
                }
            } catch (RuntimeException verificationFailed) {
                uncertain.addSuppressed(verificationFailed);
            }
            throw uncertain;
        }
        return updateCurrentOrClassify(pipelineId, key, owner, filter, current) == CurrentWrite.APPLIED;
    }

    private boolean insertFirstManifest(Observation observation, Binary key, Binary owner,
            ObservationStore.Scope scope, Document current, Document pending, Document commitFilter) {
        if ((current == null) == (pending == null)) {
            throw new IllegalArgumentException("a first manifest carries either current or pending");
        }
        String pipelineId = observation.pipelineId();
        try {
            return StoreIo.call(pipelineId, () -> {
                try (ClientSession session = client.startSession()) {
                    return session.withTransaction(() -> {
                        Document existing = manifests
                                .find(session, new Document("_id", key))
                                .projection(new Document(FORMAT, 1).append(OWNER, 1)
                                        .append(REVISION, 1).append(CURRENT, 1)
                                        .append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1)).first();
                        if (existing != null) {
                            validateHeader(existing, pipelineId, owner);
                            if (pending != null || existing.containsKey(CURRENT)) {
                                throw new FirstManifestRaced();
                            }
                        } else if (current != null && "chunked".equals(current.getString(MODE))) {
                            return false;
                        }
                        boolean checkLegacy = existing == null || existing.getBoolean(LEGACY_FALLBACK);
                        Document legacy = checkLegacy ? manifests
                                .find(session, new Document("_id", pipelineId)).first() : null;
                        if (!legacyAllows(legacy, observation, scope)) {
                            return false;
                        }
                        if (legacy != null) {
                            String state = requireString(legacy.get("state"), pipelineId, "state");
                            UpdateResult fenced = manifests
                                    .updateOne(session, legacyFence(legacy).append("state", state),
                                            new Document("$set", new Document("state", "")));
                            if (fenced.getMatchedCount() != 1) {
                                throw new FirstManifestRaced();
                            }
                            UpdateResult restored = manifests
                                    .updateOne(session, legacyFence(legacy).append("state", ""),
                                            new Document("$set", new Document("state", state)));
                            if (restored.getMatchedCount() != 1) {
                                throw new FirstManifestRaced();
                            }
                        }
                        if (existing != null) {
                            List<Bson> update = currentUpdate(owner, current);
                            if (checkLegacy) {
                                ((Document) ((Document) update.getFirst()).get("$set"))
                                        .append(LEGACY_RESIDUE, legacy != null);
                            }
                            UpdateResult committed = manifests.updateOne(session, commitFilter, update);
                            return committed.getMatchedCount() != 0;
                        }
                        Document manifest = new Document("_id", key).append(FORMAT, FORMAT_VERSION)
                                .append(OWNER, owner).append(REVISION, UUID.randomUUID().toString())
                                .append(LEGACY_FALLBACK, current == null).append(LEGACY_RESIDUE, legacy != null);
                        if (current != null) {
                            manifest.append(CURRENT, current);
                        }
                        try {
                            manifests.insertOne(session, manifest);
                        } catch (MongoException raced) {
                            if (duplicateKey(raced)) {
                                throw new FirstManifestRaced();
                            }
                            throw raced;
                        }
                        if (pending != null) {
                            UpdateResult leased = manifests
                                    .updateOne(session, headerFilter(key, owner), List.of(
                                            new Document("$set", new Document(PENDING, pending))));
                            if (leased.getMatchedCount() != 1) {
                                throw new FirstManifestRaced();
                            }
                        }
                        return true;
                    }, CUTOVER_TRANSACTION);
                }
            });
        } catch (FirstManifestRaced raced) {
            return false;
        }
    }

    static boolean legacyAllows(Document legacy, Observation observation,
            ObservationStore.Scope incoming) {
        if (legacy == null) {
            return true;
        }
        String pipelineId = observation.pipelineId();
        Object rawIncarnation = legacy.get(INCARNATION);
        Object rawGeneration = legacy.get(GENERATION);
        if (!legacy.containsKey(INCARNATION) && !legacy.containsKey(GENERATION)) {
            return true;
        }
        if (!(rawIncarnation instanceof String incarnation) || incarnation.isBlank()) {
            throw corrupt(pipelineId, "legacy observation scope");
        }
        long storedGeneration = requirePositiveLong(rawGeneration, pipelineId, GENERATION);
        if (storedGeneration != incoming.executionGeneration()) {
            return storedGeneration < incoming.executionGeneration();
        }
        if (!incarnation.equals(incoming.pipelineIncarnationId())) {
            return false;
        }
        Instant storedAt = requireDate(legacy.get(OBSERVED_AT), pipelineId, OBSERVED_AT);
        int timeOrder = storedAt.compareTo(observation.observedAt());
        if (timeOrder != 0) {
            return timeOrder < 0;
        }
        return MongoObservationStore.toObservation(legacy).equals(observation);
    }

    static Document legacyFence(Document legacy) {
        String pipelineId = requireString(legacy.get("_id"), String.valueOf(legacy.get("_id")), "_id");
        Document filter = new Document("_id", pipelineId);
        for (String field : List.of(INCARNATION, GENERATION, OBSERVED_AT)) {
            filter.append(field, legacy.containsKey(field)
                    ? legacy.get(field) : new Document("$exists", false));
        }
        return filter;
    }

    private CurrentWrite updateCurrentOrClassify(String pipelineId, Binary key, Binary owner,
            Document filter, Document current) {
        try {
            UpdateResult result = StoreIo.call(pipelineId, () -> manifests
                    .withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .updateOne(new Document(filter).append(CURRENT, new Document("$exists", true)),
                            currentUpdate(owner, current)));
            return result.getMatchedCount() != 0 ? CurrentWrite.APPLIED
                    : classifyCurrent(pipelineId, key, owner, current);
        } catch (TapstateException uncertain) {
            try {
                if (classifyCurrent(pipelineId, key, owner, current) == CurrentWrite.APPLIED) {
                    return CurrentWrite.APPLIED;
                }
            } catch (RuntimeException verificationFailed) {
                uncertain.addSuppressed(verificationFailed);
                throw uncertain;
            }
            throw uncertain;
        }
    }

    static List<Bson> currentUpdate(Binary owner, Document current) {
        return List.of(new Document("$set", new Document(FORMAT, FORMAT_VERSION)
                        .append(OWNER, owner).append(REVISION, UUID.randomUUID().toString())
                        .append(LEGACY_FALLBACK, false)
                        .append(CURRENT, new Document("$literal", current))
                        .append(MongoObservationContinuation.CONTINUATION,
                                retainPrivate(MongoObservationContinuation.CONTINUATION, current))
                        .append(MongoObservationContinuation.CONTINUATION_PENDING,
                                retainPrivate(MongoObservationContinuation.CONTINUATION_PENDING, current))),
                new Document("$unset", PENDING));
    }

    private static Document retainPrivate(String field, Document current) {
        String root = "$" + field;
        Object incarnation = new Document("$ifNull", List.of(root + ".target.scope.pipelineIncarnationId",
                new Document("$ifNull", List.of(root + ".baselineOrigin.scope.pipelineIncarnationId",
                        new Document("$ifNull", List.of(root + ".sourceScope.pipelineIncarnationId", ""))))));
        Object maxGeneration = new Document("$max", List.of(
                new Document("$ifNull", List.of(root + ".target.scope.executionGeneration", 0L)),
                new Document("$ifNull", List.of(root + ".baselineOrigin.scope.executionGeneration", 0L)),
                new Document("$ifNull", List.of(root + ".sourceScope.executionGeneration", 0L))));
        Document retained = new Document("$and", List.of(
                new Document("$eq", List.of(incarnation, current.get(INCARNATION))),
                new Document("$lte", List.of(current.get(GENERATION), maxGeneration))));
        return new Document("$cond", List.of(retained, root, "$$REMOVE"));
    }

    private CurrentWrite classifyCurrent(String pipelineId, Binary key, Binary owner, Document expected) {
        Document found = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", key))
                .projection(new Document(FORMAT, 1).append(OWNER, 1).append(REVISION, 1).append(CURRENT, 1)
                        .append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1))
                .first());
        if (found == null) {
            return CurrentWrite.ABSENT;
        }
        validateHeader(found, pipelineId, owner);
        if (!found.containsKey(CURRENT)) {
            return CurrentWrite.UNCOMMITTED;
        }
        Document current = requireDocument(found.get(CURRENT), pipelineId, CURRENT);
        return current.equals(expected) ? CurrentWrite.APPLIED : CurrentWrite.REFUSED;
    }

    private ObservationStore.Stored readCurrent(String pipelineId, Binary key, Binary owner, Document current,
            ReadDeadline deadline) {
        ObservationStore.Scope scope = readScope(current, pipelineId, CURRENT);
        Instant observedAt = requireDate(current.get(OBSERVED_AT), pipelineId, CURRENT + "." + OBSERVED_AT);
        int encoding = requirePositiveInt(current.get(ENCODING), pipelineId, CURRENT + "." + ENCODING);
        if (!LatestObservationPayloadCodec.supportsVersion(encoding)) {
            throw corrupt(pipelineId, CURRENT + "." + ENCODING);
        }
        String mode = requireString(current.get(MODE), pipelineId, CURRENT + "." + MODE);
        byte[] digest = requireBinary(current.get(DIGEST), pipelineId, CURRENT + "." + DIGEST);
        long encodedBytes = requirePositiveLong(current.get(ENCODED_BYTES), pipelineId,
                CURRENT + "." + ENCODED_BYTES);
        Observation observation;
        deadline.requireTime();
        if ("inline".equals(mode)) {
            byte[] inline = requireBinary(current.get(INLINE_PAYLOAD), pipelineId,
                    CURRENT + "." + INLINE_PAYLOAD);
            observation = decode(pipelineId, () -> LatestObservationPayloadCodec.decodeInline(
                    inline, digest, encodedBytes, encoding));
        } else if ("chunked".equals(mode)) {
            String token = requireString(current.get(TOKEN), pipelineId, CURRENT + "." + TOKEN);
            long count = requirePositiveLong(current.get(CHUNK_COUNT), pipelineId,
                    CURRENT + "." + CHUNK_COUNT);
            observation = readChunks(pipelineId, key, owner, token, count, encodedBytes, digest, encoding, deadline);
        } else {
            throw corrupt(pipelineId, CURRENT + "." + MODE);
        }
        deadline.requireTime();
        if (!pipelineId.equals(observation.pipelineId()) || !observedAt.equals(observation.observedAt())) {
            throw corrupt(pipelineId, CURRENT);
        }
        return new ObservationStore.Stored(observation, Optional.of(scope));
    }

    private Observation readChunks(String pipelineId, Binary key, Binary owner, String token,
            long expectedCount, long expectedBytes, byte[] expectedDigest, int encoding, ReadDeadline deadline) {
        MongoCursor<Document> cursor = StoreIo.call(() -> chunks
                .withTimeout(deadline.remainingMillis(), TimeUnit.MILLISECONDS)
                .find(new Document(MANIFEST_KEY, key).append(OWNER, owner).append(TOKEN, token)
                        .append(ENCODING, encoding))
                .sort(new Document(ORDINAL, 1)).batchSize(READ_CHUNK_BATCH_SIZE).iterator());
        try (cursor) {
            Iterable<LatestObservationPayloadCodec.Chunk> iterable = () -> new Iterator<>() {
                @Override public boolean hasNext() {
                    deadline.requireTime();
                    return StoreIo.call(cursor::hasNext);
                }
                @Override public LatestObservationPayloadCodec.Chunk next() {
                    deadline.requireTime();
                    Document chunk = StoreIo.call(cursor::next);
                    if (!key.equals(requireBinaryValue(chunk.get(MANIFEST_KEY), pipelineId, MANIFEST_KEY))
                            || !owner.equals(requireBinaryValue(chunk.get(OWNER), pipelineId, OWNER))
                            || !token.equals(requireString(chunk.get(TOKEN), pipelineId, TOKEN))
                            || encoding != requirePositiveInt(chunk.get(ENCODING), pipelineId, ENCODING)) {
                        throw corrupt(pipelineId, "latest chunk envelope");
                    }
                    long ordinal = requireNonNegativeLong(chunk.get(ORDINAL), pipelineId, ORDINAL);
                    return new LatestObservationPayloadCodec.Chunk(ordinal,
                            requireBinary(chunk.get(CHUNK_PAYLOAD), pipelineId, CHUNK_PAYLOAD),
                            requireBinary(chunk.get(CHUNK_DIGEST), pipelineId, CHUNK_DIGEST));
                }
            };
            return decode(pipelineId, () -> LatestObservationPayloadCodec.decodeChunks(
                    iterable, expectedCount, expectedBytes, expectedDigest, encoding));
        }
    }

    private Optional<ObservationStore.Stored> readLegacy(String pipelineId, ReadDeadline deadline) {
        Document legacy = StoreIo.call(() -> manifests.withTimeout(deadline.remainingMillis(), TimeUnit.MILLISECONDS)
                .find(new Document("_id", pipelineId)).first());
        if (legacy == null) {
            return Optional.empty();
        }
        Object rawIncarnation = legacy.get(INCARNATION);
        Object rawGeneration = legacy.get(GENERATION);
        Optional<ObservationStore.Scope> scope;
        if (!legacy.containsKey(INCARNATION) && !legacy.containsKey(GENERATION)) {
            scope = Optional.empty();
        } else if (rawIncarnation instanceof String incarnation && !incarnation.isBlank()) {
            scope = Optional.of(new ObservationStore.Scope(incarnation,
                    requirePositiveLong(rawGeneration, pipelineId, GENERATION)));
        } else {
            throw corrupt(pipelineId, "observation scope");
        }
        return Optional.of(new ObservationStore.Stored(MongoObservationStore.toObservation(legacy), scope));
    }

    private void unsetPrivateIncarnation(Binary key, Binary owner, String field, String incarnation) {
        Document filter = headerFilter(key, owner).append("$or", List.of(
                new Document(field + ".sourceScope." + INCARNATION, incarnation),
                new Document(field + ".target.scope." + INCARNATION, incarnation),
                new Document(field + ".baselineOrigin.scope." + INCARNATION, incarnation)));
        StoreIo.run(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .updateOne(filter, new Document("$unset", new Document(field, true))
                        .append("$set", new Document(REVISION, UUID.randomUUID().toString()))));
    }

    private void unsetOwnedDescriptor(Binary key, Binary owner, String field, String incarnationId) {
        Document filter = headerFilter(key, owner)
                .append(field + "." + INCARNATION, incarnationId);
        StoreIo.run(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .updateOne(filter, new Document("$unset", new Document(field, true))
                        .append("$set", new Document(REVISION, UUID.randomUUID().toString()))));
    }

    final class ManifestChunks implements LatestObservationPayloadCodec.ChunkWriter {
        private final Observation observation;
        private final String pipelineId;
        private final Binary key;
        private final Binary owner;
        private final ObservationStore.Scope scope;
        private final Instant observedAt;
        private String token;
        private long nextHeartbeat;
        private boolean begun;
        private boolean replay;

        private ManifestChunks(Observation observation, Binary key, Binary owner,
                ObservationStore.Scope scope, Instant observedAt) {
            this.observation = observation;
            this.pipelineId = observation.pipelineId();
            this.key = key;
            this.owner = owner;
            this.scope = scope;
            this.observedAt = observedAt;
        }

        @Override
        public void begin() {
            if (begun) {
                throw new IllegalStateException("a publication lease is acquired once");
            }
            begun = true;
            Reservation reserved = reservePending(observation, key, owner, scope, observedAt);
            if (reserved == null) {
                throw new StalePublication();
            }
            token = reserved.token();
            replay = reserved.replay();
            nextHeartbeat = System.nanoTime() + PUBLISH_HEARTBEAT_NANOS;
        }

        @Override
        public void write(LatestObservationPayloadCodec.Chunk chunk) {
            if (!begun) {
                throw new IllegalStateException("chunks require a publication lease");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new StalePublication();
            }
            if (replay) {
                return;
            }
            heartbeatIfDue();
            writeChunk(pipelineId, key, owner, token, chunk);
        }

        private void heartbeatIfDue() {
            long now = System.nanoTime();
            if (now - nextHeartbeat >= 0) {
                if (!renew()) {
                    throw new StalePublication();
                }
                nextHeartbeat = now + PUBLISH_HEARTBEAT_NANOS;
            }
        }

        boolean renew() {
            if (!begun) {
                throw new IllegalStateException("a publication heartbeat requires a lease");
            }
            if (replay) {
                return false;
            }
            return heartbeatPending(key, owner, scope, observedAt, token);
        }

        boolean promote(LatestObservationPayloadCodec.Encoded encoded) {
            if (!begun) {
                throw new IllegalStateException("a chunked payload has no publication lease");
            }
            Document current = chunkedCurrent(scope, observedAt, token, encoded);
            if (replay) {
                return classifyCurrent(pipelineId, key, owner, current) == CurrentWrite.APPLIED;
            }
            heartbeatIfDue();
            Document filter = headerFilter(key, owner)
                    .append(PENDING + "." + TOKEN, token)
                    .append(PENDING + "." + INCARNATION, scope.pipelineIncarnationId())
                    .append(PENDING + "." + GENERATION, scope.executionGeneration())
                    .append(PENDING + "." + OBSERVED_AT, Date.from(observedAt))
                    .append("$and", List.of(unexpiredPending(),
                            currentFence(scope, observedAt, encoded.payloadDigest(), "chunked", false)));
            CurrentWrite outcome = updateCurrentOrClassify(pipelineId, key, owner, filter, current);
            boolean promoted = outcome == CurrentWrite.APPLIED;
            if (outcome == CurrentWrite.UNCOMMITTED) {
                try {
                    promoted = insertFirstManifest(observation, key, owner, scope, current, null, filter);
                } catch (TapstateException uncertain) {
                    if (classifyCurrent(pipelineId, key, owner, current) != CurrentWrite.APPLIED) {
                        throw uncertain;
                    }
                    promoted = true;
                }
                if (!promoted) {
                    promoted = classifyCurrent(pipelineId, key, owner, current) == CurrentWrite.APPLIED;
                }
            }
            if (!promoted) {
                clearOwnedPending(key, owner, token);
            }
            return promoted;
        }
    }

    private void clearOwnedPending(Binary key, Binary owner, String token) {
        StoreIo.run(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .updateOne(headerFilter(key, owner).append(PENDING + "." + TOKEN, token),
                        new Document("$unset", new Document(PENDING, true))
                                .append("$set", new Document(REVISION, UUID.randomUUID().toString()))));
    }

    private Reservation reservePending(Observation observation, Binary key, Binary owner,
            ObservationStore.Scope scope, Instant observedAt) {
        String pipelineId = observation.pipelineId();
        String candidate = newPublicationToken();
        Document same = new Document(CURRENT + "." + INCARNATION, scope.pipelineIncarnationId())
                .append(CURRENT + "." + GENERATION, scope.executionGeneration())
                .append(CURRENT + "." + OBSERVED_AT, Date.from(observedAt));
        Document filter = headerFilter(key, owner).append("$or", List.of(same,
                new Document("$and", List.of(currentFence(scope, observedAt, null, null, true),
                        pendingAvailable(scope)))));
        Document pending = baseDescriptor(scope, observedAt)
                .append(TOKEN, candidate)
                .append(ENCODING, LatestObservationPayloadCodec.ENCODING_VERSION)
                .append("publishUntil", leaseUntilExpression());
        Document exact = new Document("$and", List.of(
                new Document("$eq", List.of("$" + CURRENT + "." + INCARNATION, scope.pipelineIncarnationId())),
                new Document("$eq", List.of("$" + CURRENT + "." + GENERATION, scope.executionGeneration())),
                new Document("$eq", List.of("$" + CURRENT + "." + OBSERVED_AT, Date.from(observedAt)))));
        List<Bson> update = List.of(new Document("$set", new Document(
                REVISION, new Document("$cond", List.of(exact, "$" + REVISION, UUID.randomUUID().toString())))
                .append(PENDING, new Document("$cond", List.of(exact,
                        new Document("$ifNull", List.of("$" + PENDING, "$$REMOVE")), pending)))));
        Reservation reserved = reserveExistingPending(pipelineId, key, owner, scope, observedAt,
                candidate, filter, update);
        if (reserved != null) {
            return reserved;
        }
        try {
            if (insertFirstManifest(observation, key, owner, scope, null, pending, null)) {
                return new Reservation(candidate, false);
            }
        } catch (TapstateException uncertain) {
            Optional<String> confirmed = pendingToken(key, owner, scope, observedAt, candidate);
            if (confirmed.isPresent()) {
                return new Reservation(confirmed.orElseThrow(), false);
            }
            throw uncertain;
        }
        return reserveExistingPending(pipelineId, key, owner, scope, observedAt,
                candidate, filter, update);
    }

    private Reservation reserveExistingPending(String pipelineId, Binary key, Binary owner,
            ObservationStore.Scope scope, Instant observedAt, String candidate,
            Document filter, List<Bson> update) {
        try {
            Document reserved = StoreIo.call(pipelineId, () -> manifests
                    .withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .findOneAndUpdate(filter, update, new FindOneAndUpdateOptions()
                            .returnDocument(ReturnDocument.AFTER)
                            .projection(new Document(PENDING + "." + TOKEN, 1)
                                    .append(CURRENT + "." + INCARNATION, 1).append(CURRENT + "." + GENERATION, 1)
                                    .append(CURRENT + "." + OBSERVED_AT, 1).append(CURRENT + "." + MODE, 1)
                                    .append(CURRENT + "." + TOKEN, 1))));
            if (reserved != null && reserved.get(CURRENT) instanceof Document current
                    && scope.pipelineIncarnationId().equals(current.get(INCARNATION))
                    && requirePositiveLong(current.get(GENERATION), pipelineId, CURRENT + "." + GENERATION)
                            == scope.executionGeneration()
                    && Date.from(observedAt).equals(current.get(OBSERVED_AT))) {
                String mode = requireString(current.get(MODE), pipelineId, CURRENT + "." + MODE);
                if (!List.of("inline", "chunked").contains(mode)) {
                    throw corrupt(pipelineId, CURRENT + "." + MODE);
                }
                return new Reservation("chunked".equals(mode)
                        ? requireString(current.get(TOKEN), pipelineId, CURRENT + "." + TOKEN) : candidate, true);
            }
            String token = reserved == null ? null : requireString(
                    requireDocument(reserved.get(PENDING), pipelineId, PENDING).get(TOKEN),
                    pipelineId, PENDING + "." + TOKEN);
            if (token != null && !candidate.equals(token)) {
                throw corrupt(pipelineId, PENDING + "." + TOKEN);
            }
            return token == null ? null : new Reservation(token, false);
        } catch (TapstateException uncertain) {
            Optional<String> confirmed = pendingToken(key, owner, scope, observedAt, candidate);
            if (confirmed.isPresent()) {
                return new Reservation(confirmed.orElseThrow(), false);
            }
            throw uncertain;
        }
    }

    private Optional<String> pendingToken(Binary key, Binary owner, ObservationStore.Scope scope,
            Instant observedAt, String expectedToken) {
        Document filter = headerFilter(key, owner)
                .append(PENDING + "." + TOKEN, expectedToken)
                .append(PENDING + "." + INCARNATION, scope.pipelineIncarnationId())
                .append(PENDING + "." + GENERATION, scope.executionGeneration())
                .append(PENDING + "." + OBSERVED_AT, Date.from(observedAt))
                .append("$and", List.of(unexpiredPending()));
        Document found = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(filter).projection(new Document(PENDING + "." + TOKEN, 1)).first());
        if (found == null) {
            return Optional.empty();
        }
        Document pending = requireDocument(found.get(PENDING), encodeCursor(key), PENDING);
        return Optional.of(requireString(pending.get(TOKEN), encodeCursor(key), PENDING + "." + TOKEN));
    }

    private static String newPublicationToken() {
        return UUID.randomUUID().toString();
    }

    private boolean heartbeatPending(Binary key, Binary owner, ObservationStore.Scope scope,
            Instant observedAt, String token) {
        Document filter = headerFilter(key, owner)
                .append(PENDING + "." + TOKEN, token)
                .append(PENDING + "." + INCARNATION, scope.pipelineIncarnationId())
                .append(PENDING + "." + GENERATION, scope.executionGeneration())
                .append(PENDING + "." + OBSERVED_AT, Date.from(observedAt))
                .append("$and", List.of(unexpiredPending()));
        List<Bson> update = List.of(new Document("$set",
                new Document(PENDING + ".publishUntil", leaseUntilExpression())
                        .append(REVISION, UUID.randomUUID().toString())));
        return StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .updateOne(filter, update).getMatchedCount()) != 0;
    }

    private void writeChunk(String pipelineId, Binary key, Binary owner, String token,
            LatestObservationPayloadCodec.Chunk chunk) {
        Document document = chunkDocument(key, owner, token, chunk);
        Binary id = document.get("_id", Binary.class);
        StoreIo.call(pipelineId, () -> {
            try {
                chunks.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS).insertOne(document);
                return null;
            } catch (MongoException failure) {
                try {
                    Document existing = chunks.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                            .find(new Document("_id", id)).first();
                    if (document.equals(existing)) {
                        return null;
                    }
                    if (existing != null || duplicateKey(failure)) {
                        throw corrupt(pipelineId, "latest chunk identity");
                    }
                } catch (MongoException verificationFailed) {
                    failure.addSuppressed(verificationFailed);
                }
                throw failure;
            }
        });
    }

    static Document headerFilter(Binary key, Binary owner) {
        return new Document("_id", key).append(FORMAT,
                        new Document("$eq", FORMAT_VERSION).append("$type", "int"))
                .append(OWNER, owner).append(LEGACY_FALLBACK, new Document("$type", "bool"))
                .append(LEGACY_RESIDUE, new Document("$type", "bool"))
                .append("$nor", List.of(new Document(LEGACY_FALLBACK, true)
                        .append(CURRENT, new Document("$exists", true))));
    }

    static Document currentFence(ObservationStore.Scope scope, Instant observedAt,
            byte[] exactDigest, String exactMode, boolean allowEqualUnknown) {
        List<Document> allowed = new java.util.ArrayList<>();
        allowed.add(new Document(CURRENT, new Document("$exists", false)));
        allowed.add(new Document(CURRENT + "." + GENERATION,
                new Document("$lt", scope.executionGeneration())));
        allowed.add(new Document(CURRENT + "." + INCARNATION, scope.pipelineIncarnationId())
                .append(CURRENT + "." + GENERATION, scope.executionGeneration())
                .append(CURRENT + "." + OBSERVED_AT, new Document("$lt", Date.from(observedAt))));
        if (allowEqualUnknown) {
            allowed.add(new Document(CURRENT + "." + INCARNATION, scope.pipelineIncarnationId())
                    .append(CURRENT + "." + GENERATION, scope.executionGeneration())
                    .append(CURRENT + "." + OBSERVED_AT, Date.from(observedAt)));
        } else if (exactDigest != null) {
            allowed.add(new Document(CURRENT + "." + INCARNATION, scope.pipelineIncarnationId())
                    .append(CURRENT + "." + GENERATION, scope.executionGeneration())
                    .append(CURRENT + "." + OBSERVED_AT, Date.from(observedAt))
                    .append(CURRENT + "." + MODE, exactMode)
                    .append(CURRENT + "." + DIGEST, new Binary(exactDigest)));
        }
        return new Document("$or", allowed);
    }

    private static Document pendingAvailable(ObservationStore.Scope scope) {
        return new Document("$or", List.of(
                new Document(PENDING, new Document("$exists", false)),
                new Document(PENDING + "." + GENERATION, new Document("$lt", scope.executionGeneration())),
                expiredPending()));
    }

    static Document pendingAvailableForInline(ObservationStore.Scope scope) {
        return new Document("$or", List.of(
                new Document(PENDING, new Document("$exists", false)),
                new Document(PENDING + "." + GENERATION, new Document("$lt", scope.executionGeneration())),
                expiredPending()));
    }

    private static Document expiredPending() {
        return new Document("$expr", new Document("$lte", List.of("$" + PENDING + ".publishUntil", "$$NOW")));
    }

    static Document unexpiredPending() {
        return new Document("$expr", new Document("$gt", List.of("$" + PENDING + ".publishUntil", "$$NOW")));
    }

    static Document leaseUntilExpression() {
        return new Document("$dateAdd", new Document("startDate", "$$NOW")
                .append("unit", "second").append("amount", PUBLISH_LEASE_SECONDS));
    }

    private static Document dueForDeletion() {
        return new Document(STATE, RETIRED)
                .append("$expr", new Document("$lte", List.of("$" + DELETE_AFTER, "$$NOW")));
    }

    private boolean manifestReferences(Binary key, Binary owner, String token) {
        Document manifest = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", key)).projection(new Document(FORMAT, 1).append(OWNER, 1)
                        .append(REVISION, 1).append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1)
                        .append(CURRENT + "." + TOKEN, 1)
                        .append(PENDING + "." + TOKEN, 1)
                        .append(MongoObservationContinuation.CONTINUATION + "." + TOKEN, 1)
                        .append(MongoObservationContinuation.CONTINUATION_PENDING + "." + TOKEN, 1)).first());
        if (manifest == null) {
            return false;
        }
        String id = encodeCursor(key);
        validateHeader(manifest, id, owner);
        for (String field : List.of(CURRENT, PENDING, MongoObservationContinuation.CONTINUATION,
                MongoObservationContinuation.CONTINUATION_PENDING)) {
            if (manifest.containsKey(field)) {
                Document descriptor = requireDocument(manifest.get(field), id, field);
                if (token.equals(descriptor.get(TOKEN))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Document baseDescriptor(ObservationStore.Scope scope, Instant observedAt) {
        return new Document(INCARNATION, scope.pipelineIncarnationId())
                .append(GENERATION, scope.executionGeneration())
                .append(OBSERVED_AT, Date.from(observedAt));
    }

    static Document inlineCurrent(ObservationStore.Scope scope, Instant observedAt,
            LatestObservationPayloadCodec.Encoded encoded) {
        return baseDescriptor(scope, observedAt)
                .append(MODE, "inline")
                .append(ENCODING, LatestObservationPayloadCodec.ENCODING_VERSION)
                .append(DIGEST, new Binary(encoded.payloadDigest()))
                .append(ENCODED_BYTES, encoded.encodedBytes())
                .append(INLINE_PAYLOAD, new Binary(encoded.inlinePayload()));
    }

    static Document chunkedCurrent(ObservationStore.Scope scope, Instant observedAt, String token,
            LatestObservationPayloadCodec.Encoded encoded) {
        return baseDescriptor(scope, observedAt)
                .append(MODE, "chunked")
                .append(ENCODING, LatestObservationPayloadCodec.ENCODING_VERSION)
                .append(TOKEN, token)
                .append(DIGEST, new Binary(encoded.payloadDigest()))
                .append(CHUNK_COUNT, encoded.chunkCount())
                .append(ENCODED_BYTES, encoded.encodedBytes());
    }

    static Document inlineManifestForSize(String pipelineId, ObservationStore.Scope scope,
            Instant observedAt, LatestObservationPayloadCodec.Encoded encoded) {
        return manifestForSize(pipelineId, scope, observedAt, inlineCurrent(scope, observedAt, encoded));
    }

    static Document chunkedManifestForSize(String pipelineId, ObservationStore.Scope scope,
            Instant observedAt, String token, LatestObservationPayloadCodec.Encoded encoded) {
        return manifestForSize(pipelineId, scope, observedAt,
                chunkedCurrent(scope, observedAt, token, encoded));
    }

    private static Document manifestForSize(String pipelineId, ObservationStore.Scope scope,
            Instant observedAt, Document current) {
        Binary key = manifestKey(pipelineId);
        Binary owner = ownerDigest(pipelineId);
        Document pending = baseDescriptor(scope, observedAt)
                .append(TOKEN, "A".repeat(43))
                .append(ENCODING, LatestObservationPayloadCodec.ENCODING_VERSION)
                .append("publishUntil", Date.from(observedAt.plusSeconds(PUBLISH_LEASE_SECONDS)));
        return new Document("_id", key).append(FORMAT, FORMAT_VERSION).append(OWNER, owner)
                .append(REVISION, "00000000-0000-0000-0000-000000000000")
                .append(LEGACY_FALLBACK, false).append(LEGACY_RESIDUE, true)
                .append(CURRENT, current).append(PENDING, pending);
    }

    static Document chunkDocument(Binary key, Binary owner, String token,
            LatestObservationPayloadCodec.Chunk chunk) {
        return new Document("_id", chunkId(key, owner, token, chunk.ordinal()))
                .append(MANIFEST_KEY, key)
                .append(OWNER, owner)
                .append(ENCODING, LatestObservationPayloadCodec.ENCODING_VERSION)
                .append(TOKEN, token)
                .append(ORDINAL, chunk.ordinal())
                .append(CHUNK_DIGEST, new Binary(chunk.digest()))
                .append(STATE, ACTIVE)
                .append(CHUNK_PAYLOAD, new Binary(chunk.payload()));
    }

    static void validateHeader(Document manifest, String pipelineId, Binary owner) {
        if (!(manifest.get(FORMAT) instanceof Integer version) || version != FORMAT_VERSION
                || !(manifest.get(OWNER) instanceof Binary storedOwner)
                || storedOwner.getData().length != 32 || !storedOwner.equals(owner)
                || !(manifest.get(LEGACY_FALLBACK) instanceof Boolean)
                || !(manifest.get(LEGACY_RESIDUE) instanceof Boolean)
                || !(manifest.get(REVISION) instanceof String revision) || revision.isBlank()) {
            throw corrupt(pipelineId, "latest manifest header");
        }
        if (manifest.containsKey(CURRENT) && manifest.getBoolean(LEGACY_FALLBACK)) {
            throw corrupt(pipelineId, "latest manifest authority");
        }
        for (String field : List.of(CURRENT, PENDING, MongoObservationContinuation.CONTINUATION,
                MongoObservationContinuation.CONTINUATION_PENDING)) {
            if (manifest.containsKey(field) && !(manifest.get(field) instanceof Document)) {
                throw corrupt(pipelineId, field);
            }
        }
    }

    private static void validateBatch(Optional<String> cursor, int limit) {
        Objects.requireNonNull(cursor, "cursor");
        if (limit < 1 || limit > ObservationStore.MAX_LATEST_SCAN_BATCH
                || cursor.filter(String::isBlank).isPresent()) {
            throw new IllegalArgumentException("manifest scan cursor and batch must be bounded");
        }
    }

    private static void addScope(Object raw, List<ObservationStore.Scope> scopes, String id, String field) {
        if (raw == null) {
            return;
        }
        Document descriptor = requireDocument(raw, id, field);
        ObservationStore.Scope scope = readScope(descriptor, id, field);
        if (!scopes.contains(scope)) {
            scopes.add(scope);
        }
    }

    private static void addPrivateScopes(Object raw, List<ObservationStore.Scope> scopes, String id) {
        if (raw == null) { return; }
        Document descriptor = requireDocument(raw, id, "private continuation");
        for (String field : List.of("sourceScope", "target", "baselineOrigin")) {
            Object value = descriptor.get(field);
            if (value == null) { continue; }
            Document scope = field.equals("sourceScope") ? requireDocument(value, id, field)
                    : requireDocument(requireDocument(value, id, field).get("scope"), id, field + ".scope");
            ObservationStore.Scope found = new ObservationStore.Scope(requireString(scope.get(INCARNATION), id, field),
                    requirePositiveLong(scope.get(GENERATION), id, field));
            if (!scopes.contains(found)) { scopes.add(found); }
        }
    }

    private static Document requireDocument(Object value, String id, String field) {
        if (!(value instanceof Document document)) {
            throw corrupt(id, field);
        }
        return document;
    }

    private static void validatePending(Object raw, String pipelineId) {
        if (raw == null) {
            return;
        }
        if (!(raw instanceof Document pending)) {
            throw corrupt(pipelineId, PENDING);
        }
        requireString(pending.get(TOKEN), pipelineId, PENDING + "." + TOKEN);
        readScope(pending, pipelineId, PENDING);
        requireDate(pending.get(OBSERVED_AT), pipelineId, PENDING + "." + OBSERVED_AT);
        requireDate(pending.get("publishUntil"), pipelineId, PENDING + ".publishUntil");
        if (!LatestObservationPayloadCodec.supportsVersion(
                requirePositiveInt(pending.get(ENCODING), pipelineId, PENDING + "." + ENCODING))) {
            throw corrupt(pipelineId, PENDING + "." + ENCODING);
        }
    }

    private Document readHeader(String pipelineId, Binary key, Binary owner) {
        Document header = StoreIo.call(() -> manifests.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", key)).projection(headerProjection()).first());
        if (header != null) {
            validateHeader(header, pipelineId, owner);
        }
        return header;
    }

    private Document readHeader(ClientSession session, String pipelineId, Binary key, Binary owner) {
        Document header = manifests.find(session, new Document("_id", key)).projection(headerProjection()).first();
        if (header != null) {
            validateHeader(header, pipelineId, owner);
        }
        return header;
    }

    private static Document headerProjection() {
        return new Document(FORMAT, 1).append(OWNER, 1).append(REVISION, 1)
                .append(LEGACY_FALLBACK, 1).append(LEGACY_RESIDUE, 1)
                .append(CURRENT + "." + MODE, 1).append(PENDING + "." + TOKEN, 1);
    }

    static Binary manifestKey(String pipelineId) {
        try {
            return new Binary(MessageDigest.getInstance("SHA-256")
                    .digest(pipelineId.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("the JDK has no SHA-256 implementation", impossible);
        }
    }

    static Binary ownerDigest(String pipelineId) {
        return new Binary(sha256(OWNER_DOMAIN, pipelineId.getBytes(StandardCharsets.UTF_8)));
    }

    static Binary chunkId(Binary key, Binary owner, String token, long ordinal) {
        return chunkId(key, owner, token, ordinal, LatestObservationPayloadCodec.ENCODING_VERSION);
    }

    static Binary chunkId(Binary key, Binary owner, String token, long ordinal, int encodingVersion) {
        return new Binary(sha256(CHUNK_ID_DOMAIN, owner.getData(), key.getData(), intBytes(encodingVersion),
                token.getBytes(StandardCharsets.UTF_8), longBytes(ordinal)));
    }

    private static String encodeCursor(Binary key) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(key.getData());
    }

    private static Binary decodeCursor(String cursor) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(cursor);
            if (bytes.length != 32) {
                throw new IllegalArgumentException("manifest cursor has the wrong length");
            }
            return new Binary(bytes);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("manifest scan cursor is malformed", malformed);
        }
    }

    private static byte[] sha256(byte[] domain, byte[]... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(domain);
            for (byte[] part : parts) {
                digest.update(intBytes(part.length));
                digest.update(part);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("the JDK has no SHA-256 implementation", impossible);
        }
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static boolean duplicateKey(MongoException failure) {
        return failure instanceof com.mongodb.MongoWriteException write
                ? write.getError().getCode() == 11000 : failure.getCode() == 11000;
    }

    private static ObservationStore.Scope readScope(Document descriptor, String id, String field) {
        String incarnation = requireString(descriptor.get(INCARNATION), id, field + "." + INCARNATION);
        long generation = requirePositiveLong(descriptor.get(GENERATION), id, field + "." + GENERATION);
        return new ObservationStore.Scope(incarnation, generation);
    }

    private static String requireString(Object value, String id, String field) {
        if (!(value instanceof String string) || string.isBlank()) {
            throw corrupt(id, field);
        }
        return string;
    }

    private static Instant requireDate(Object value, String id, String field) {
        if (!(value instanceof Date date)) {
            throw corrupt(id, field);
        }
        return date.toInstant();
    }

    private static int requirePositiveInt(Object value, String id, String field) {
        if (!(value instanceof Integer number) || number <= 0) {
            throw corrupt(id, field);
        }
        return number;
    }

    private static long requirePositiveLong(Object value, String id, String field) {
        if (!(value instanceof Integer || value instanceof Long) || ((Number) value).longValue() <= 0) {
            throw corrupt(id, field);
        }
        return ((Number) value).longValue();
    }

    private static long requireNonNegativeLong(Object value, String id, String field) {
        if (!(value instanceof Integer || value instanceof Long) || ((Number) value).longValue() < 0) {
            throw corrupt(id, field);
        }
        return ((Number) value).longValue();
    }

    private static byte[] requireBinary(Object value, String id, String field) {
        if (!(value instanceof Binary binary)) {
            throw corrupt(id, field);
        }
        return Arrays.copyOf(binary.getData(), binary.getData().length);
    }

    private static Binary requireBinaryValue(Object value, String id, String field) {
        if (!(value instanceof Binary binary) || binary.getData().length != 32) {
            throw corrupt(id, field);
        }
        return binary;
    }

    private static <T> T decode(String pipelineId, java.util.function.Supplier<T> decoder) {
        try {
            return decoder.get();
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException malformed) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", "latest payload"), malformed);
        }
    }

    private static TapstateException corrupt(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", field), null);
    }

    private static final class StalePublication extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private record Reservation(String token, boolean replay) {
    }

    private static final class FirstManifestRaced extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private record TimedCursor(Instant instant, Binary id) {
        private TimedCursor {
            Objects.requireNonNull(instant, "instant");
            Objects.requireNonNull(id, "id");
        }
    }

    private static final class ReadDeadline {
        private final long deadlineNanos;

        private ReadDeadline() {
            this(System.nanoTime() + TimeUnit.SECONDS.toNanos(READ_DEADLINE_SECONDS));
        }

        private ReadDeadline(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
        }

        private ReadDeadline limitedTo(long seconds) {
            requireTime();
            long candidate = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            return new ReadDeadline(Math.min(deadlineNanos, candidate));
        }

        private long remainingMillis() {
            requireTime();
            return Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
        }

        private void requireTime() {
            if (System.nanoTime() - deadlineNanos >= 0) {
                throw new TapstateException(IoError.STORE_UNAVAILABLE,
                        Map.of("detail", "latest observation read deadline exceeded"), null);
            }
        }
    }
}
