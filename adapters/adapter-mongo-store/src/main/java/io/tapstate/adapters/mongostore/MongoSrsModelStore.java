package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SrsModelStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Actual structural models are immutable and published under their original cache and capture context. */
public final class MongoSrsModelStore implements SrsModelStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Document PROVE_CAPTURE = new Document("$inc", new Document("fencedModelWrites", 1L));
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;

    public MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = requireActive;
    }
    @Override public MongoSrsModelStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsModelStore(client, database, scope, () -> { requireActive.run(); additional.run(); });
    }
    @Override public SrsModel publish(SrsModel model, WorkloadClaimFence fence) {
        Objects.requireNonNull(model, "model"); Objects.requireNonNull(fence, "captureFence");
        requireScope(model.key());
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())) throw claimFenced();
        Document captured = document(model);
        // Preflight the canonical typed representation before entering a retryable transaction.
        if (!decode(captured).equals(model)) throw unavailable(model.key());
        return transaction(session -> {
            requireBusiness(session);
            Document chain = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", model.key().miningChainId())).first();
            if (chain == null || !(chain.get("epoch") instanceof Long epoch) || epoch != model.key().captureEpoch()
                    || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation"))
                    || !fence.key().resourceId().equals(chain.get("captureId"))) throw unavailable(model.key());
            if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session, WorkloadClaimDocuments.live(fence), PROVE_CAPTURE)
                    .getMatchedCount() != 1) throw claimFenced();
            var models = SystemCollections.SRS_MODELS.on(database);
            Document existing = models.find(session, new Document("_id", key(model.key()))).first();
            SrsLogBounds observed = requireRing(session, model);
            if (existing != null) {
                if (!decode(existing).equals(model)) throw conflict(model.key());
                return model;
            }
            Document head = models.find(session, context(model.ringName(), model.key().captureEpoch()))
                    .sort(new Document("_id.modelVersion", -1)).limit(1).first();
            if (head == null) {
                if (model.key().modelVersion() != 0) throw unavailable(model.key());
            } else {
                SrsModel previous = decode(head);
                if (!previous.key().miningChainId().equals(model.key().miningChainId()) || !previous.key().table().equals(model.key().table())
                        || model.key().modelVersion() != previous.key().modelVersion() + 1
                        || model.ddlSequence() <= previous.ddlSequence()) throw unavailable(model.key());
            }
            if (model.ddlSequence() <= observed.largestSequence()) throw unavailable(model.key());
            models.insertOne(session, captured);
            return model;
        });
    }
    @Override public Optional<SrsModel> load(SrsModelKey key) {
        requireScope(key);
        return transaction(session -> {
            requireBusiness(session);
            return Optional.ofNullable(SystemCollections.SRS_MODELS.on(database).find(session, new Document("_id", key(key))).first())
                    .map(this::decode);
        });
    }

    boolean belongsTo(MongoClient expectedClient, String expectedDatabase) {
        return client == expectedClient && database.getName().equals(expectedDatabase);
    }

    void checkDomain(ClientSession session) { requireActive.run(); requireBusiness(session); }

    /** An empty unseen ring may be inspected, but its first event still requires a published actual model. */
    void checkRing(ClientSession session, String ring) {
        Document retained = SystemCollections.SRS_MODELS.on(database).find(session,
                new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring)).first();
        Document bounds = SystemCollections.SRS_LOG_BOUNDS.on(database).find(session, new Document("_id", MongoSrsLogStore.boundsId(ring))).first();
        if (retained == null) {
            if (bounds != null || SystemCollections.SRS_EVENTS.on(database).find(session, MongoSrsLogStore.ringRange(ring, Long.MAX_VALUE)).limit(1).first() != null) {
                throw eventUnavailable(ring, 0, 0);
            }
        } else {
            var model = decode(retained);
            if (bounds == null) throw eventUnavailable(ring, model.key().captureEpoch(), model.key().modelVersion());
        }
    }

    EventModelRange requireEvent(ClientSession session, String ring, SrsLogRecord event, boolean writing) {
        if (event.schemaVer() < 0 || event.epoch() < 1 || event.captureFence() == null) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
        var fence = event.captureFence();
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())) throw claimFenced();
        var candidates = SystemCollections.SRS_MODELS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                .append("ringName", ring).append("_id.captureContext.epoch", event.epoch()).append("_id.modelVersion", event.schemaVer()))
                .limit(2).into(new java.util.ArrayList<>());
        if (candidates.isEmpty()) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
        if (candidates.size() != 1) throw unreadable();
        var model = decode(candidates.getFirst());
        Document chain = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", model.key().miningChainId())).first();
        if (chain == null || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation"))
                || !fence.key().resourceId().equals(chain.get("captureId"))) throw claimFenced();
        if (!(chain.get("epoch") instanceof Long current) || current < 1) throw unreadable();
        // Historical reads retain their original epoch; only a new append must still own the live capture.
        if (writing && current != event.epoch()) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
        Document next = SystemCollections.SRS_MODELS.on(database).find(session, context(ring, event.epoch())
                .append("_id.modelVersion", new Document("$gt", event.schemaVer())))
                .sort(new Document("_id.modelVersion", 1)).limit(1).first();
        Long nextSequence = null;
        if (next != null) {
            var successor = decode(next);
            if (!successor.key().miningChainId().equals(model.key().miningChainId()) || !successor.key().table().equals(model.key().table())
                    || successor.ddlSequence() <= model.ddlSequence()) throw unreadable();
            if (successor.key().modelVersion() != model.key().modelVersion() + 1) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            nextSequence = successor.ddlSequence();
        }
        Document nextCapture = SystemCollections.SRS_MODELS.on(database).find(session,
                new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring)
                        .append("_id.captureContext.epoch", new Document("$gt", event.epoch())))
                .sort(new Document("_id.captureContext.epoch", 1).append("_id.modelVersion", 1)).limit(1).first();
        if (nextCapture != null) {
            var initial = decode(nextCapture);
            if (!initial.key().miningChainId().equals(model.key().miningChainId()) || !initial.key().table().equals(model.key().table())) throw unreadable();
            if (initial.key().modelVersion() != 0) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            nextSequence = nextSequence == null ? initial.ddlSequence() : Math.min(nextSequence, initial.ddlSequence());
        }
        return new EventModelRange(model.ddlSequence(), nextSequence);
    }

    record EventModelRange(long firstSequence, Long nextSequence) {
        void requireAt(long sequence, String ring, SrsLogRecord event) {
            if (sequence < firstSequence || (nextSequence != null && sequence >= nextSequence)) {
                throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            }
        }
    }

    private Document context(String ring, long epoch) {
        return new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring).append("_id.captureContext.epoch", epoch);
    }

    private static TapstateException eventUnavailable(String ring, long epoch, long version) {
        return new TapstateException(IoError.SRS_EVENT_MODEL_UNAVAILABLE,
                Map.of("ring", ring, "epoch", String.valueOf(epoch), "version", String.valueOf(version)), null);
    }

    private SrsLogBounds requireRing(ClientSession session, SrsModel model) {
        Document retained = SystemCollections.SRS_MODELS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                .append("ringName", model.ringName())).first();
        if (retained != null) {
            SrsModel previous = decode(retained);
            if (!previous.key().miningChainId().equals(model.key().miningChainId()) || !previous.key().table().equals(model.key().table())) throw conflict(model.key());
        }
        if (retained == null && SystemCollections.SRS_EVENTS.on(database).find(session,
                MongoSrsLogStore.ringRange(model.ringName(), Long.MAX_VALUE)).limit(1).first() != null) throw unavailable(model.key());
        Document filter = new Document("_id", MongoSrsLogStore.boundsId(model.ringName()));
        var bounds = SystemCollections.SRS_LOG_BOUNDS.on(database);
        Document stored = bounds.find(session, filter).first();
        if (stored == null) {
            if (retained != null || SystemCollections.SRS_EVENTS.on(database).find(session,
                    MongoSrsLogStore.ringRange(model.ringName(), Long.MAX_VALUE)).limit(1).first() != null) throw unavailable(model.key());
            bounds.insertOne(session, new Document(filter).append("largestSequence", -1L).append("trimmedThrough", -1L));
            return new SrsLogBounds(-1L, -1L);
        } else {
            if (!stored.keySet().equals(Set.of("_id", "largestSequence", "trimmedThrough"))
                    || !(stored.get("largestSequence") instanceof Long largest) || !(stored.get("trimmedThrough") instanceof Long trimmed)) throw unreadable();
            try { new SrsLogBounds(largest, trimmed); }
            catch (IllegalArgumentException invalid) { throw unreadable(); }
            if (retained == null && largest >= 0) throw unavailable(model.key());
            return new SrsLogBounds(largest, trimmed);
        }
    }
    private void requireBusiness(ClientSession session) {
        Document identity = SystemCollections.CACHE_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
        if (identity == null || !scope.clusterId().equals(identity.get("clusterId")) || !Long.valueOf(scope.bindingVersion()).equals(identity.get("bindingVersion"))
                || !scope.cacheIncarnation().equals(identity.get("cacheIncarnation")) || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))
                || !MongoCacheIdentity.READY.equals(identity.get("businessPhase"))) throw businessFenced();
        Document schema = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "business")).first();
        if (schema == null || !(schema.get("installedVersion") instanceof Integer version)) throw unreadable();
        if (version > 1) throw new TapstateException(MigrationError.DATA_NEWER_THAN_BINARY, Map.of("installed", String.valueOf(version), "supported", "1"), null);
        if (version != 1 || !MongoCacheIdentity.READY.equals(schema.get("phase"))) throw businessFenced();
        if (!(identity.get("businessPreparationId") instanceof String operation) || operation.isBlank()
                || !operation.equals(schema.get("operationId")) || !(schema.get("epoch") instanceof Long epoch) || epoch < 1) throw unreadable();
    }
    private Document document(SrsModel model) {
        Document table = MongoSchemaStore.tableDocument(model.table());
        Document row = new Document("_id", key(model.key())).append("formatVersion", 1).append("cacheIncarnation", model.key().cacheIncarnation())
                .append("ringName", model.ringName()).append("ddlSequence", model.ddlSequence()).append("schema", table);
        return row.append("contentHash", CanonicalHash.ofText(row.toJson()));
    }
    private SrsModel decode(Document row) {
        try {
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "cacheIncarnation", "ringName", "ddlSequence", "schema", "contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("ddlSequence") instanceof Long sequence)) throw unreadable();
            Document id = row.get("_id", Document.class); Document context = id.get("captureContext", Document.class);
            if (!id.keySet().equals(Set.of("chain", "table", "captureContext", "modelVersion"))
                    || !context.keySet().equals(Set.of("cacheIncarnation", "epoch"))
                    || !(context.get("epoch") instanceof Long epoch) || !(id.get("modelVersion") instanceof Long version)) throw unreadable();
            var key = new SrsModelKey(id.getString("chain"), id.getString("table"), context.getString("cacheIncarnation"), epoch, version);
            requireScope(key);
            Document table = row.get("schema", Document.class);
            var model = new SrsModel(key, row.getString("ringName"), MongoSchemaStore.toTable(table, "srs-model"), sequence);
            Document payload = new Document(row); payload.remove("contentHash");
            if (!key.cacheIncarnation().equals(row.get("cacheIncarnation")) || !table.equals(MongoSchemaStore.tableDocument(model.table()))
                    || !CanonicalHash.ofText(payload.toJson()).equals(row.get("contentHash"))) throw unreadable();
            return model;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private static Document key(SrsModelKey key) {
        return new Document("chain", key.miningChainId()).append("table", key.table())
                .append("captureContext", new Document("cacheIncarnation", key.cacheIncarnation()).append("epoch", key.captureEpoch()))
                .append("modelVersion", key.modelVersion());
    }
    private void requireScope(SrsModelKey key) {
        if (!scope.cacheIncarnation().equals(Objects.requireNonNull(key, "key").cacheIncarnation())) throw businessFenced();
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> { requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
            }
        });
    }
    private static TapstateException unavailable(SrsModelKey key) { return modelError(IoError.SRS_MODEL_UNAVAILABLE, key); }
    private static TapstateException conflict(SrsModelKey key) { return modelError(IoError.SRS_MODEL_CONFLICT, key); }
    private static TapstateException modelError(IoError code, SrsModelKey key) {
        return new TapstateException(code, Map.of("chain", key.miningChainId(), "table", key.table(), "version", String.valueOf(key.modelVersion())), null);
    }
    private static TapstateException businessFenced() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }
    private static TapstateException claimFenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "srs-model", "field", "model"), null); }
}
