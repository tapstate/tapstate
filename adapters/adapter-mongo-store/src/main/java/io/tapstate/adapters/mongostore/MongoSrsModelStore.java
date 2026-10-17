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
    private final MongoSrsModelContexts contexts;

    public MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = requireActive;
        this.contexts = new MongoSrsModelContexts(database, scope);
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
        if (!decode(captured).equals(model)) throw unavailable(model.key());
        return transaction(session -> {
            requireBusiness(session);
            Document chain = requireChain(session, model.key());
            if (!Long.valueOf(model.key().captureEpoch()).equals(chain.get("epoch"))
                    || !fence.key().resourceId().equals(chain.get("captureId"))) throw unavailable(model.key());
            if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session, WorkloadClaimDocuments.live(fence), PROVE_CAPTURE)
                    .getMatchedCount() != 1) throw claimFenced();
            Document anchor = contexts.head(chain, model.key().table());
            var control = contexts.find(session, model.key());
            var models = SystemCollections.SRS_MODELS.on(database);
            Document existing = models.find(session, new Document("_id", key(model.key()))).first();
            if (control != null) {
                contexts.requireAnchor(session, chain, control);
                if (anchor == null || !control.id().equals(anchor.get("contextId")) || control.nextId() != null
                        || !fence.equals(control.fence()) || !model.ringName().equals(control.ring())) throw unavailable(model.key());
                requireRing(session, model, false);
                SrsModel previous = requirePublishedHead(session, control);
                if (existing != null) {
                    if (!decode(existing).equals(model)) throw conflict(model.key());
                    if (model.key().modelVersion() > control.publishedVersion()) throw unavailable(model.key());
                    return model;
                }
                if (model.key().modelVersion() != control.publishedVersion() + 1 || model.ddlSequence() <= previous.ddlSequence()) throw unavailable(model.key());
                SrsLogBounds observed = requireRing(session, model, false);
                if (model.ddlSequence() <= observed.largestSequence()) throw unavailable(model.key());
                contexts.replace(session, control, contexts.advance(control, model, captured.getString("contentHash")));
            } else {
                if (existing != null || model.key().modelVersion() != 0) throw unavailable(model.key());
                MongoSrsModelContexts.Context previous = anchor == null ? null : contexts.require(session, anchor.get("contextId", Document.class));
                if (previous != null) {
                    contexts.requireAnchor(session, chain, previous); requirePublishedHead(session, previous);
                    if (!previous.ring().equals(model.ringName()) || previous.key().captureEpoch() >= model.key().captureEpoch()) throw unavailable(model.key());
                }
                SrsLogBounds observed = requireRing(session, model, previous == null);
                if (model.ddlSequence() <= observed.largestSequence()) throw unavailable(model.key());
                var initial = contexts.initial(model, fence, captured.getString("contentHash"));
                if (previous != null) contexts.replace(session, previous, contexts.close(previous, initial));
                contexts.insert(session, initial);
                contexts.setHead(session, chain, initial);
            }
            models.insertOne(session, captured);
            return model;
        });
    }
    @Override public Optional<SrsModel> load(SrsModelKey key) {
        requireScope(key);
        return transaction(session -> {
            requireBusiness(session);
            Document chain = requireChain(session, key);
            Document anchor = contexts.head(chain, key.table());
            var control = contexts.find(session, key);
            Document stored = SystemCollections.SRS_MODELS.on(database).find(session, new Document("_id", key(key))).first();
            if (control == null) {
                if (stored != null || anchor != null) throw unavailable(key);
                return Optional.empty();
            }
            contexts.requireAnchor(session, chain, control);
            if (stored == null) {
                if (key.modelVersion() <= control.publishedVersion()) throw unavailable(key);
                return Optional.empty();
            }
            SrsModel model = decode(stored); requirePublished(control, model);
            return Optional.of(model);
        });
    }

    private SrsModel requirePublishedHead(ClientSession session, MongoSrsModelContexts.Context control) {
        Document stored = SystemCollections.SRS_MODELS.on(database).find(session,
                new Document("_id", key(version(control, control.publishedVersion())))).first();
        if (stored == null) throw unavailable(control.key());
        SrsModel model = decode(stored); requirePublished(control, model);
        if (!control.publishedHash().equals(stored.get("contentHash")) || model.ddlSequence() != control.publishedSequence()) throw unreadable();
        return model;
    }
    private static SrsModelKey version(MongoSrsModelContexts.Context control, long version) {
        var key = control.key();
        return new SrsModelKey(key.miningChainId(), key.table(), key.cacheIncarnation(), key.captureEpoch(), version);
    }
    private void requirePublished(MongoSrsModelContexts.Context control, SrsModel model) {
        if (!MongoSrsModelContexts.contextId(model.key()).equals(control.id()) || !model.ringName().equals(control.ring())
                || model.key().modelVersion() > control.publishedVersion() || model.ddlSequence() < control.firstSequence()
                || (model.key().modelVersion() == 0 && model.ddlSequence() != control.firstSequence())
                || model.ddlSequence() > control.publishedSequence()
                || (model.key().modelVersion() < control.publishedVersion() && model.ddlSequence() >= control.publishedSequence())) throw unavailable(model.key());
        if (model.key().modelVersion() == control.publishedVersion()
                && (model.ddlSequence() != control.publishedSequence() || !control.publishedHash().equals(document(model).get("contentHash")))) throw unreadable();
    }
    private Document requireChain(ClientSession session, SrsModelKey key) {
        Document chain = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", key.miningChainId())).first();
        if (chain == null || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation"))) throw unavailable(key);
        if (!(chain.get("epoch") instanceof Long epoch) || epoch < 1 || !(chain.get("captureId") instanceof String capture) || capture.isBlank()
                || !(chain.get("modelWriteFence") instanceof Long revision) || revision < 0 || revision == Long.MAX_VALUE) throw unreadable();
        return chain;
    }

    boolean belongsTo(MongoClient expectedClient, String expectedDatabase) {
        return client == expectedClient && database.getName().equals(expectedDatabase);
    }

    void checkDomain(ClientSession session) { requireActive.run(); requireBusiness(session); }

    /** An unseen ring may be inspected only when no published ordering anchor or retained data contradicts it. */
    void checkRing(ClientSession session, String ring) {
        Document retained = SystemCollections.SRS_MODELS.on(database).find(session,
                new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring)).first();
        var control = contexts.firstOnRing(session, ring);
        Document bounds = SystemCollections.SRS_LOG_BOUNDS.on(database).find(session, new Document("_id", MongoSrsLogStore.boundsId(ring))).first();
        if (control == null) {
            Document known = SystemCollections.SRS_CHAINS.on(database).find(session,
                    new Document("cacheIncarnation", scope.cacheIncarnation()).append("modelContextHeads.ringName", ring)).first();
            if (known != null || retained != null || bounds != null
                    || SystemCollections.SRS_EVENTS.on(database).find(session, MongoSrsLogStore.ringRange(ring, Long.MAX_VALUE)).limit(1).first() != null) {
                throw eventUnavailable(ring, 0, 0);
            }
        } else {
            if (bounds == null || retained == null) throw eventUnavailable(ring, control.key().captureEpoch(), 0);
            try { contexts.requireAnchor(session, requireChain(session, control.key()), control); }
            catch (TapstateException missing) {
                if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, control.key().captureEpoch(), 0);
                throw missing;
            }
        }
    }

    EventModelRange requireEvent(ClientSession session, String ring, SrsLogRecord event, boolean writing) {
        try { return eventRange(session, ring, event, writing); }
        catch (TapstateException missing) {
            if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            throw missing;
        }
    }
    private EventModelRange eventRange(ClientSession session, String ring, SrsLogRecord event, boolean writing) {
        if (event.schemaVer() < 0 || event.epoch() < 1 || event.captureFence() == null) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
        var fence = event.captureFence();
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())) throw claimFenced();
        var candidates = SystemCollections.SRS_MODELS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                .append("ringName", ring).append("_id.captureContext.epoch", event.epoch()).append("_id.modelVersion", event.schemaVer()))
                .limit(2).into(new java.util.ArrayList<>());
        if (candidates.isEmpty()) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
        if (candidates.size() != 1) throw unreadable();
        SrsModel model = decode(candidates.getFirst());
        Document chain = requireChain(session, model.key());
        var control = contexts.find(session, model.key());
        if (control == null) throw unavailable(model.key());
        contexts.requireAnchor(session, chain, control);
        if (!fence.equals(control.fence())) throw claimFenced();
        if (writing && (!fence.key().resourceId().equals(chain.get("captureId"))
                || !Long.valueOf(event.epoch()).equals(chain.get("epoch")) || control.nextId() != null)) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
        requirePublished(control, model);
        Long end = null;
        if (event.schemaVer() < control.publishedVersion()) {
            if (event.schemaVer() + 1 == control.publishedVersion()) {
                end = control.publishedSequence();
            } else {
                Document next = SystemCollections.SRS_MODELS.on(database).find(session,
                        new Document("_id", key(version(control, event.schemaVer() + 1)))).first();
                if (next == null) throw unavailable(model.key());
                SrsModel successor = decode(next); requirePublished(control, successor);
                end = successor.ddlSequence();
            }
            if (end <= model.ddlSequence()) throw unreadable();
        }
        if (control.nextId() != null) {
            var next = contexts.require(session, control.nextId());
            if (!next.ring().equals(ring) || !control.nextSequence().equals(next.firstSequence())) throw unreadable();
            // The independently retained control proves this cut without retaining an unrelated schema body.
            end = end == null ? next.firstSequence() : Math.min(end, next.firstSequence());
        }
        if (writing) {
            // Capture replacement and context closure both write this root, including when claim IDs differ.
            Document expected = new Document("_id", chain.get("_id")).append("cacheIncarnation", scope.cacheIncarnation())
                    .append("epoch", event.epoch()).append("captureId", fence.key().resourceId())
                    .append("modelContextHeads", chain.get("modelContextHeads")).append("modelWriteFence", chain.get("modelWriteFence"));
            if (SystemCollections.SRS_CHAINS.on(database).updateOne(session, expected,
                    new Document("$inc", new Document("modelWriteFence", 1L))).getMatchedCount() != 1) {
                throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            }
        }
        return new EventModelRange(model.ddlSequence(), end);
    }

    record EventModelRange(long firstSequence, Long nextSequence) {
        void requireAt(long sequence, String ring, SrsLogRecord event) {
            if (sequence < firstSequence || (nextSequence != null && sequence >= nextSequence)) {
                throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            }
        }
    }

    private static TapstateException eventUnavailable(String ring, long epoch, long version) {
        return new TapstateException(IoError.SRS_EVENT_MODEL_UNAVAILABLE,
                Map.of("ring", ring, "epoch", String.valueOf(epoch), "version", String.valueOf(version)), null);
    }

    private SrsLogBounds requireRing(ClientSession session, SrsModel model, boolean firstContext) {
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
        if (firstContext && (retained != null || stored != null || contexts.firstOnRing(session, model.ringName()) != null
                || SystemCollections.SRS_CHAINS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                        .append("modelContextHeads.ringName", model.ringName())).first() != null)) throw unavailable(model.key());
        if (stored == null) {
            if (!firstContext || retained != null || SystemCollections.SRS_EVENTS.on(database).find(session,
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
