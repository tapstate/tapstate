package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimType;
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

    private final MongoCollection<Document> systemMeta;
    private final MongoCollection<Document> workloadClaims;
    private final MongoCollection<Document> artifacts;
    private final SecureRandom random;

    public SourceConfigKeyringStore(MongoDatabase database) {
        this(database, new SecureRandom());
    }

    SourceConfigKeyringStore(MongoDatabase database, SecureRandom random) {
        Objects.requireNonNull(database, "database");
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

    private static Loaded decodeStored(Document stored) {
        Number format = stored.get("formatVersion", Number.class);
        Number epoch = stored.get("epoch", Number.class);
        String active = stored.getString("activeKeyId");
        String prepared = stored.getString("preparedKeyId");
        List<Document> entries = stored.getList("keys", Document.class);
        if (format == null || format.intValue() != FORMAT_VERSION || epoch == null || epoch.longValue() < 1
                || active == null || active.isBlank() || entries == null || entries.isEmpty()) {
            throw invalid();
        }

        Map<String, byte[]> decoded = new LinkedHashMap<>();
        int activeEntries = 0;
        int preparedEntries = 0;
        try {
            for (Document entry : entries) {
                String id = entry.getString("id");
                String state = entry.getString("state");
                String material = entry.getString("material");
                if (id == null || material == null
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
            return new Loaded(epoch.longValue(), active, prepared, new SourceConfigCipher(active, decoded));
        } finally {
            decoded.values().forEach(bytes -> Arrays.fill(bytes, (byte) 0));
        }
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
        Document current = keyringDocument();
        Loaded loaded = decodeStored(current);
        if (loaded.preparedKeyId() == null) {
            reencryptSources(loaded);
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
        reencryptSources(activated);
        return activated;
    }

    private void reencryptSources(Loaded keyring) {
        EncryptedArtifactCodec codec = new EncryptedArtifactCodec(keyring.cipher());
        try (MongoCursor<Document> cursor = artifacts.find(new Document("kind", "source")).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                Document body = document.get("body", Document.class);
                String envelope = body == null ? null : body.getString("config");
                if (keyring.activeKeyId().equals(SourceConfigCipher.envelopeKeyId(envelope))) continue;
                Resource resource = codec.decode(document);
                String replacement = codec.encode(resource).get("body", Document.class).getString("config");
                Document filter = new Document("_id", document.get("_id"))
                        .append("contentHash", document.getString("contentHash"))
                        .append("body.config", envelope);
                UpdateResult result = StoreIo.call(String.valueOf(document.get("_id")), () -> artifacts.updateOne(
                        filter, new Document("$set", new Document("body.config", replacement))));
                if (result.getMatchedCount() == 0) {
                    Document current = StoreIo.call(() -> artifacts.find(
                            new Document("_id", document.get("_id"))).first());
                    Document currentBody = current == null ? null : current.get("body", Document.class);
                    String currentEnvelope = currentBody == null ? null : currentBody.getString("config");
                    if (!keyring.activeKeyId().equals(SourceConfigCipher.envelopeKeyId(currentEnvelope))) {
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
        if (epoch < 1 || ttl.isZero() || ttl.isNegative()) throw new IllegalArgumentException("invalid keyring lease");
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
                .append("leaseUntil", new Document("$dateAdd", new Document("startDate", "$$NOW")
                        .append("unit", "millisecond").append("amount", ttl.toMillis())));
        try {
            Document acknowledged = systemMeta.findOneAndUpdate(eligible, List.of(new Document("$set", fields)),
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
            return acknowledged != null;
        } catch (MongoException raced) {
            if (ErrorCategory.fromErrorCode(raced.getCode()) == ErrorCategory.DUPLICATE_KEY) return false;
            throw StoreIo.coded(raced);
        }
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
