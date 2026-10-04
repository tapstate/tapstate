package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.ArtifactMutation;
import org.bson.Document;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bootstraps, refreshes and rotates the Cluster keyring stored beside encrypted Source configs. */
public final class SourceConfigKeyringStore {

    private static final String KEYRING_ID = "source-config-keyring";
    private static final int FORMAT_VERSION = 1;
    private static final int KEY_BYTES = 32;
    private static final String ACK_KIND = "source-config-keyring-node";
    private static final TransactionOptions ACK_TRANSACTION = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true))
            .build();

    private final MongoCollection<Document> systemMeta;
    private final MongoCollection<Document> workloadClaims;
    private final MongoCollection<Document> artifacts;
    private final SecureRandom random;
    private final MongoClient client;

    public SourceConfigKeyringStore(MongoDatabase database) {
        this(database, new SecureRandom());
    }

    SourceConfigKeyringStore(MongoDatabase database, SecureRandom random) {
        this(null, database, random);
    }

    SourceConfigKeyringStore(MongoClient client, MongoDatabase database) {
        this(Objects.requireNonNull(client, "client"), database, new SecureRandom());
    }

    private SourceConfigKeyringStore(MongoClient client, MongoDatabase database, SecureRandom random) {
        Objects.requireNonNull(database, "database");
        this.client = client;
        this.systemMeta = SystemCollections.SYSTEM_META.on(database)
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.workloadClaims = SystemCollections.WORKLOAD_CLAIMS.on(database)
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.artifacts = SystemCollections.ARTIFACTS.on(database)
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.random = Objects.requireNonNull(random, "random");
    }

    /** Creates the first keyring once, or loads the winner of a concurrent create. */
    public SourceConfigCipher loadOrCreateCipher() {
        // A partially restored or interrupted migration may already contain ciphertext while its
        // schema marker is older. Unknown envelopes also need the original keys, never a new ring.
        Document encryptedSource = new Document("$or", List.of(
                new Document("kind", "source"), new Document("body.kind", "source")))
                .append("body.config", new Document("$type", "string"));
        if (StoreIo.call(() -> artifacts.find(encryptedSource).projection(new Document("_id", 1)).first()) != null) {
            return loadExistingCipher();
        }
        byte[] candidate = new byte[KEY_BYTES];
        random.nextBytes(candidate);
        try {
            String keyId = SourceConfigCipher.fingerprint(candidate);
            Document key = new Document("id", keyId)
                    .append("state", "active")
                    .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(candidate));
            Document keyring = new Document("_id", KEYRING_ID)
                    .append("formatVersion", FORMAT_VERSION)
                    .append("epoch", 1L)
                    .append("activeKeyId", keyId)
                    .append("keys", List.of(key));
            try {
                systemMeta.insertOne(keyring);
            } catch (MongoException raced) {
                if (ErrorCategory.fromErrorCode(raced.getCode()) != ErrorCategory.DUPLICATE_KEY) {
                    throw StoreIo.coded(raced);
                }
            }
        } finally {
            Arrays.fill(candidate, (byte) 0);
        }
        return loadExistingCipher();
    }

    /** Loads an existing keyring without generating a replacement when it is absent or malformed. */
    public SourceConfigCipher loadExistingCipher() {
        return loadExisting().cipher();
    }

    Loaded loadExisting() {
        return decodeStored(keyringDocument());
    }

    /** Touches the active-key record in the Source transaction so activation cannot pass an uncommitted writer. */
    boolean fenceActiveWriter(ClientSession session, String keyId) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(keyId, "keyId");
        if (!session.hasActiveTransaction()) throw new IllegalStateException("Source keyring fence needs a transaction");
        UpdateResult result = systemMeta.updateOne(session,
                new Document("_id", KEYRING_ID).append("activeKeyId", keyId),
                new Document("$inc", new Document("sourceWriteFence", 1L)));
        return result.getMatchedCount() == 1 && result.getModifiedCount() == 1;
    }

    private static Loaded decodeStored(Document stored) {
        Object format = stored.get("formatVersion");
        Object epoch = stored.get("epoch");
        Object preparedValue = stored.get("preparedKeyId");
        if (!storedInteger(format) || ((Number) format).longValue() != FORMAT_VERSION
                || !storedInteger(epoch) || ((Number) epoch).longValue() < 1
                || !(stored.get("activeKeyId") instanceof String active) || active.isBlank()
                || !(stored.get("keys") instanceof List<?> entries) || entries.isEmpty()
                || (stored.containsKey("preparedKeyId") && !(preparedValue instanceof String))) {
            throw invalid();
        }
        String prepared = (String) preparedValue;

        Map<String, byte[]> decoded = new LinkedHashMap<>();
        int activeEntries = 0;
        int preparedEntries = 0;
        try {
            for (Object value : entries) {
                if (!(value instanceof Document entry)
                        || !(entry.get("id") instanceof String id)
                        || !(entry.get("state") instanceof String state)
                        || !(entry.get("material") instanceof String material)
                        || !("active".equals(state) || "read-only".equals(state) || "prepared".equals(state))
                        || decoded.containsKey(id)
                        || id.equals(active) != "active".equals(state)
                        || id.equals(prepared) != "prepared".equals(state)) {
                    throw invalid();
                }
                byte[] bytes;
                try {
                    bytes = Base64.getUrlDecoder().decode(material);
                } catch (IllegalArgumentException malformed) {
                    throw invalid();
                }
                if (bytes.length != KEY_BYTES || !id.equals(SourceConfigCipher.fingerprint(bytes))) {
                    Arrays.fill(bytes, (byte) 0);
                    throw invalid();
                }
                decoded.put(id, bytes);
                if ("active".equals(state)) activeEntries++;
                if ("prepared".equals(state)) preparedEntries++;
            }
            if (activeEntries != 1 || !decoded.containsKey(active)
                    || (prepared == null && preparedEntries != 0)
                    || (prepared != null && (preparedEntries != 1 || !decoded.containsKey(prepared)))) {
                throw invalid();
            }
            return new Loaded(((Number) epoch).longValue(), active, prepared, new SourceConfigCipher(active, decoded));
        } finally {
            decoded.values().forEach(bytes -> Arrays.fill(bytes, (byte) 0));
        }
    }

    private static boolean storedInteger(Object value) {
        return value instanceof Integer || value instanceof Long;
    }

    Loaded prepareRotation() {
        Document current = keyringDocument();
        Loaded loaded = decodeStored(current);
        if (loaded.preparedKeyId() != null) return loaded;

        byte[] candidate = new byte[KEY_BYTES];
        random.nextBytes(candidate);
        try {
            String keyId = SourceConfigCipher.fingerprint(candidate);
            Document key = new Document("id", keyId)
                    .append("state", "prepared")
                    .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(candidate));
            Document filter = new Document("_id", KEYRING_ID)
                    .append("epoch", loaded.epoch())
                    .append("activeKeyId", loaded.activeKeyId())
                    .append("preparedKeyId", new Document("$exists", false));
            UpdateResult result = StoreIo.call(() -> systemMeta.updateOne(filter,
                    new Document("$inc", new Document("epoch", 1L))
                            .append("$set", new Document("preparedKeyId", keyId))
                            .append("$push", new Document("keys", key))));
            if (result.getMatchedCount() == 0) {
                Loaded winner = loadExisting();
                if (winner.preparedKeyId() == null) throw invalid();
                return winner;
            }
            return loadExisting();
        } finally {
            Arrays.fill(candidate, (byte) 0);
        }
    }

    Loaded activatePrepared() {
        if (client == null) throw new IllegalStateException("keyring rotation needs the owning store client");
        Document current = keyringDocument();
        Loaded loaded = decodeStored(current);
        if (loaded.preparedKeyId() == null) {
            reencryptSources();
            return loaded;
        }
        if (!allLiveSessionsAcknowledged(loaded.epoch())) {
            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED, Map.of(), null);
        }

        List<Document> keys = current.getList("keys", Document.class).stream()
                .map(Document::new)
                .peek(key -> {
                    if (loaded.activeKeyId().equals(key.getString("id"))) key.put("state", "read-only");
                    if (loaded.preparedKeyId().equals(key.getString("id"))) key.put("state", "active");
                })
                .toList();
        Document replacement = new Document(current)
                .append("epoch", loaded.epoch() + 1)
                .append("activeKeyId", loaded.preparedKeyId())
                .append("keys", keys);
        replacement.remove("preparedKeyId");
        Document filter = new Document("_id", KEYRING_ID)
                .append("epoch", loaded.epoch())
                .append("activeKeyId", loaded.activeKeyId())
                .append("preparedKeyId", loaded.preparedKeyId());
        if (StoreIo.call(() -> systemMeta.replaceOne(filter, replacement)).getMatchedCount() != 1) {
            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED, Map.of(), null);
        }
        Loaded activated = loadExisting();
        reencryptSources();
        return activated;
    }

    private void reencryptSources() {
        SourceConfigKeyringHandle current = new SourceConfigKeyringHandle(this);
        EncryptedArtifactCodec codec = new EncryptedArtifactCodec(current);
        MongoArtifactStore writes = new MongoArtifactStore(client, artifacts, current);
        try (MongoCursor<Document> cursor = artifacts.find(new Document("kind", "source")).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                codec.decode(document);
                Document body = document.get("body", Document.class);
                String envelope = body == null ? null : body.getString("config");
                if (current.refresh().activeKeyId().equals(SourceConfigCipher.envelopeKeyId(envelope))) continue;
                ArtifactMutation result = writes.reencryptSource(document);
                if (result != ArtifactMutation.REPLACED) {
                    Document latest = StoreIo.call(() -> artifacts.find(
                            new Document("_id", document.get("_id"))).first());
                    if (latest == null) continue;
                    codec.decode(latest);
                    Document latestBody = latest.get("body", Document.class);
                    String currentEnvelope = latestBody == null ? null : latestBody.getString("config");
                    if (!current.refresh().activeKeyId().equals(SourceConfigCipher.envelopeKeyId(currentEnvelope))) {
                        throw new TapstateException(
                                StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED, Map.of(), null);
                    }
                }
            }
        }
    }

    private boolean allLiveSessionsAcknowledged(long epoch) {
        List<Document> live = StoreIo.call(() -> workloadClaims.aggregate(List.of(
                new Document("$match", new Document("resourceType", WorkloadClaimType.NODE_SESSION.name())
                        .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW"))))))
                .into(new java.util.ArrayList<>()));
        for (Document session : live) {
            Document id = new Document("kind", ACK_KIND)
                    .append("clusterId", session.getString("clusterId"))
                    .append("nodeId", session.getString("resourceId"));
            Document exact = new Document("_id", id)
                    .append("bootId", session.getString("ownerBootId"))
                    .append("claimGeneration", session.get("claimGeneration", Number.class).longValue())
                    .append("epoch", epoch)
                    .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
            if (StoreIo.call(() -> systemMeta.find(exact).first()) == null) return false;
        }
        return true;
    }

    private Document keyringDocument() {
        Document stored = StoreIo.call(() -> systemMeta.find(new Document("_id", KEYRING_ID)).first());
        if (stored == null) throw invalid();
        return stored;
    }

    boolean acknowledge(WorkloadClaim session, long epoch, Duration ttl) {
        requireNodeSession(session);
        Objects.requireNonNull(ttl, "ttl");
        if (epoch < 1 || ttl.toMillis() < 1) throw new IllegalArgumentException("invalid keyring lease");
        if (client == null) throw new IllegalStateException("keyring acknowledgement needs the owning store client");
        return StoreIo.call(() -> {
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    return acknowledgeOnce(session, epoch, ttl);
                } catch (MongoException error) {
                    if (attempt == 2 || !error.hasErrorLabel("TransientTransactionError")
                            || error.hasErrorLabel("UnknownTransactionCommitResult")) throw error;
                }
            }
            throw new IllegalStateException("acknowledgement attempts must return or throw");
        });
    }

    private boolean acknowledgeOnce(WorkloadClaim node, long epoch, Duration ttl) {
        try (ClientSession transaction = client.startSession()) {
            transaction.startTransaction(ACK_TRANSACTION);
            try {
                // Touch both records in the ACK transaction: a takeover/release or epoch switch
                // cannot commit between proving ownership/loading and publishing the proof.
                Document expected = new Document("_id", new Document("clusterId", node.key().clusterId())
                        .append("resourceType", WorkloadClaimType.NODE_SESSION.name())
                        .append("resourceId", node.key().resourceId()))
                        .append("ownerNodeId", node.owner().nodeId())
                        .append("ownerBootId", node.owner().bootId())
                        .append("claimGeneration", node.claimGeneration())
                        .append("executionGeneration", node.executionGeneration())
                        .append("topologyRevision", node.topologyRevision())
                        .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
                Document live = workloadClaims.findOneAndUpdate(transaction, expected,
                        new Document("$inc", new Document("sourceConfigAckFence", 1L)),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
                if (live == null) {
                    transaction.abortTransaction();
                    return false;
                }
                UpdateResult ring = systemMeta.updateOne(transaction,
                        new Document("_id", KEYRING_ID).append("epoch", epoch),
                        new Document("$inc", new Document("nodeAckFence", 1L)));
                if (ring.getMatchedCount() != 1 || ring.getModifiedCount() != 1) {
                    transaction.abortTransaction();
                    return false;
                }
                Document acknowledged = writeAcknowledgement(transaction, node, epoch, ttl, live);
                if (acknowledged == null) {
                    transaction.abortTransaction();
                    return false;
                }
            } catch (RuntimeException error) {
                try {
                    transaction.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    error.addSuppressed(abortFailure);
                }
                if (error instanceof MongoException driver
                        && ErrorCategory.fromErrorCode(driver.getCode()) == ErrorCategory.DUPLICATE_KEY) return false;
                throw error;
            }
            // An ambiguous commit is not permission to replay the transaction.
            transaction.commitTransaction();
            return true;
        }
    }

    private Document writeAcknowledgement(ClientSession transaction, WorkloadClaim session,
            long epoch, Duration ttl, Document live) {
        Document id = acknowledgementId(session);
        Document eligible = new Document("$and", List.of(
                new Document("_id", id),
                new Document("$or", List.of(
                        new Document("claimGeneration", new Document("$lt", session.claimGeneration())),
                        new Document("claimGeneration", session.claimGeneration())
                                .append("bootId", session.owner().bootId()),
                        new Document("claimGeneration", new Document("$exists", false))))));
        Document fields = new Document("kind", ACK_KIND)
                .append("clusterId", session.key().clusterId())
                .append("nodeId", session.key().resourceId())
                .append("bootId", session.owner().bootId())
                .append("claimGeneration", session.claimGeneration())
                .append("epoch", epoch)
                .append("leaseUntil", new Document("$min", List.of(live.getDate("leaseUntil"),
                        new Document("$dateAdd", new Document("startDate", "$$NOW")
                                .append("unit", "millisecond").append("amount", ttl.toMillis())))));
        return systemMeta.findOneAndUpdate(transaction, eligible, List.of(new Document("$set", fields)),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
    }

    void release(WorkloadClaim session) {
        requireNodeSession(session);
        Document expected = new Document("_id", acknowledgementId(session))
                .append("bootId", session.owner().bootId())
                .append("claimGeneration", session.claimGeneration());
        StoreIo.call(() -> systemMeta.findOneAndUpdate(expected,
                List.of(new Document("$set", new Document("leaseUntil", "$$NOW"))),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    }

    private static Document acknowledgementId(WorkloadClaim session) {
        return new Document("kind", ACK_KIND)
                .append("clusterId", session.key().clusterId())
                .append("nodeId", session.key().resourceId());
    }

    private static void requireNodeSession(WorkloadClaim session) {
        Objects.requireNonNull(session, "session");
        if (session.key().type() != WorkloadClaimType.NODE_SESSION
                || !session.key().resourceId().equals(session.owner().nodeId())) {
            throw new IllegalArgumentException("keyring acknowledgement requires its node session");
        }
    }

    private static TapstateException invalid() {
        return new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
    }

    record Loaded(long epoch, String activeKeyId, String preparedKeyId, SourceConfigCipher cipher) {
        Loaded {
            if (epoch < 1) throw new IllegalArgumentException("keyring epoch must be positive");
            Objects.requireNonNull(activeKeyId, "activeKeyId");
            Objects.requireNonNull(cipher, "cipher");
        }
    }
}
