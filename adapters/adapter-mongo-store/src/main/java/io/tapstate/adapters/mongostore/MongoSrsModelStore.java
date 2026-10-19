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
    private final Runnable requireStructure;
    private final MongoSrsModelContexts contexts;

    public MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, () -> {});
    }
    MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive, Runnable requireStructure) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.requireStructure = Objects.requireNonNull(requireStructure, "requireStructure");
        this.contexts = new MongoSrsModelContexts(database, scope);
    }
    @Override public MongoSrsModelStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsModelStore(client, database, scope, () -> { requireActive.run(); additional.run(); }, requireStructure);
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
    @Override public SrsModel requireEvent(SrsModelKey expected, String ring, long sequence, WorkloadClaimFence originalCapture) {
        Objects.requireNonNull(expected, "expected"); requireScope(expected);
        if (ring == null || ring.isBlank() || sequence < 0 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid event model coordinates");
        return transaction(session -> {
            requireBusiness(session);
            try {
                var reference = eventReference(session, ring, expected.captureEpoch(), expected.modelVersion(), originalCapture, false);
                if (!expected.equals(reference.model().key())) throw eventUnavailable(ring, expected.captureEpoch(), expected.modelVersion());
                reference.range().requireAt(sequence, ring, expected.captureEpoch(), expected.modelVersion());
                return reference.model();
            } catch (TapstateException missing) {
                if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, expected.captureEpoch(), expected.modelVersion());
                throw missing;
            }
        });
    }

    @Override public Optional<Map<String, SrsModel>> publishedForNewSnapshot(String chainId, Map<String, String> tableRings) {
        if (chainId == null || chainId.isBlank()) throw new IllegalArgumentException("blank mining chain");
        Map<String, String> selected = Map.copyOf(Objects.requireNonNull(tableRings, "tableRings"));
        if (selected.isEmpty() || selected.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue().isBlank())
                || Set.copyOf(selected.values()).size() != selected.size()) throw new IllegalArgumentException("invalid snapshot table rings");
        return transaction(session -> {
            requireSnapshotResources(); requireBusiness(session);
            String firstTable = selected.keySet().iterator().next();
            Document chain = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chainId)).first();
            if (chain == null || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation"))) {
                throw unavailable(new SrsModelKey(chainId, firstTable, scope.cacheIncarnation(), 1, 0));
            }
            var checkpoint = MongoSrsChainStore.decode(chain, scope, contexts).checkpoint();
            var result = new java.util.TreeMap<String, SrsModel>();
            for (var entry : new java.util.TreeMap<>(selected).entrySet()) {
                String table = entry.getKey(); String ring = entry.getValue();
                Document anchor = contexts.head(chain, table);
                if (anchor == null) {
                    if (checkpoint != null && checkpoint.tables().containsKey(table)) throw unavailable(checkpoint.tables().get(table).model());
                    Document identity = new Document("_id.chain", chainId).append("_id.table", table)
                            .append("_id.captureContext.cacheIncarnation", scope.cacheIncarnation());
                    if (SystemCollections.SRS_MODELS.on(database).find(session, identity).limit(1).first() != null
                            || SystemCollections.SRS_MODEL_CONTEXTS.on(database).find(session, identity).limit(1).first() != null) {
                        throw unavailable(new SrsModelKey(chainId, table, scope.cacheIncarnation(), 1, 0));
                    }
                    if (SystemCollections.SRS_MODELS.on(database).find(session,
                            new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring)).limit(1).first() != null
                            || contexts.firstOnRing(session, ring) != null
                            || SystemCollections.SRS_LOG_BOUNDS.on(database).find(session, new Document("_id", MongoSrsLogStore.boundsId(ring))).first() != null
                            || SystemCollections.SRS_EVENTS.on(database).find(session, MongoSrsLogStore.ringRange(ring, Long.MAX_VALUE)).limit(1).first() != null
                            || SystemCollections.SRS_CHAINS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                                    .append("modelContextHeads.ringName", ring)).limit(1).first() != null) {
                        throw unavailable(new SrsModelKey(chainId, table, scope.cacheIncarnation(), 1, 0));
                    }
                    continue;
                }
                var control = contexts.require(session, anchor.get("contextId", Document.class));
                contexts.requireAnchor(session, chain, control);
                if (!ring.equals(control.ring()) || control.nextId() != null) throw unavailable(control.key());
                SrsModel model = requirePublishedHead(session, control);
                Document bounds = SystemCollections.SRS_LOG_BOUNDS.on(database).find(session,
                        new Document("_id", MongoSrsLogStore.boundsId(ring))).first();
                if (bounds == null) throw unavailable(model.key());
                requireBounds(bounds);
                result.put(table, model);
            }
            return result.size() == selected.size() ? Optional.of(Map.copyOf(result)) : Optional.empty();
        });
    }

    private void requireSnapshotResources() {
        var names = java.util.List.of(SystemCollections.SRS_CHAINS, SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS,
                SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS).stream().map(SystemCollections::collectionName).toList();
        var present = database.listCollections().filter(new Document("name", new Document("$in", names))).into(new java.util.ArrayList<>());
        if (present.size() != names.size() || present.stream().anyMatch(row -> !"collection".equals(row.get("type")))) throw unreadable();
    }

    @Override public Optional<SrsModel> load(SrsModelKey key) {
        requireScope(key);
        return transaction(session -> loadInSession(session, key));
    }
    private Optional<SrsModel> loadInSession(ClientSession session, SrsModelKey key) {
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
    }

    /** Snapshot decoding needs an exact structural reference, without claiming an event-time range. */
    SrsModel requireSnapshotReference(ClientSession session, SrsModelKey key) {
        requireScope(key);
        SrsModel model = loadInSession(session, key).orElseThrow(() -> unavailable(key));
        Document bounds = SystemCollections.SRS_LOG_BOUNDS.on(database).find(session, new Document("_id", MongoSrsLogStore.boundsId(model.ringName()))).first();
        if (bounds == null) throw unavailable(key);
        requireBounds(bounds);
        return model;
    }

    record ModelReference(SrsModel model, WorkloadClaimFence fence, EventModelRange range) {}

    ModelReference requireReference(ClientSession session, SrsModelKey key) {
        requireScope(key);
        SrsModel model = loadInSession(session, key).orElseThrow(() -> unavailable(key));
        var control = contexts.find(session, key);
        return new ModelReference(model, control.fence(), range(session, model, control));
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

    void beforeSession() { requireActive.run(); }
    void checkActive() { requireActive.run(); }

    void beforeAttempt(ClientSession session) { requireActive.run(); requireStructure.run(); checkDomain(session); }

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

    /** Trimming historical rows belongs to the chain's current original capture tenure. */
    void requireCaptureForTrim(ClientSession session, String ring, WorkloadClaimFence original) {
        if (original.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(original.key().clusterId())) throw claimFenced();
        checkRing(session, ring);
        var control = contexts.firstOnRing(session, ring);
        if (control == null) throw eventUnavailable(ring, 0, 0);
        Document chain = requireChain(session, control.key());
        if (!(chain.get("captureFence") instanceof Document bound)) throw unreadable();
        if (!original.key().resourceId().equals(chain.get("captureId"))
                || !original.equals(WorkloadClaimDocuments.readCapture(bound))) throw claimFenced();
    }

    EventModelRange requireEvent(ClientSession session, String ring, SrsLogRecord event, boolean writing) {
        try { return eventRange(session, ring, event, writing); }
        catch (TapstateException missing) {
            if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, event.epoch(), event.schemaVer());
            throw missing;
        }
    }
    private EventModelRange eventRange(ClientSession session, String ring, SrsLogRecord event, boolean writing) {
        return eventReference(session, ring, event.epoch(), event.schemaVer(), event.captureFence(), writing).range();
    }
    private ModelReference eventReference(ClientSession session, String ring, long epoch, long version, WorkloadClaimFence fence, boolean writing) {
        if (version < 0 || epoch < 1 || fence == null) throw eventUnavailable(ring, epoch, version);
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())) throw claimFenced();
        var candidates = SystemCollections.SRS_MODELS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                .append("ringName", ring).append("_id.captureContext.epoch", epoch).append("_id.modelVersion", version))
                .limit(2).into(new java.util.ArrayList<>());
        if (candidates.isEmpty()) throw eventUnavailable(ring, epoch, version);
        if (candidates.size() != 1) throw unreadable();
        SrsModel model = decode(candidates.getFirst());
        Document chain = requireChain(session, model.key());
        var control = contexts.find(session, model.key());
        if (control == null) throw unavailable(model.key());
        contexts.requireAnchor(session, chain, control);
        if (!fence.equals(control.fence())) throw claimFenced();
        if (writing && (!fence.key().resourceId().equals(chain.get("captureId"))
                || !Long.valueOf(epoch).equals(chain.get("epoch")) || control.nextId() != null)) throw eventUnavailable(ring, epoch, version);
        requirePublished(control, model);
        EventModelRange range = range(session, model, control);
        if (writing) {
            if (chain.getLong("modelWriteFence") >= Long.MAX_VALUE - 1) throw eventUnavailable(ring, epoch, version);
            // Capture replacement and context closure both write this root, including when claim IDs differ.
            Document expected = new Document("_id", chain.get("_id")).append("cacheIncarnation", scope.cacheIncarnation())
                    .append("epoch", epoch).append("captureId", fence.key().resourceId())
                    .append("modelContextHeads", chain.get("modelContextHeads")).append("modelWriteFence", chain.get("modelWriteFence"));
            if (SystemCollections.SRS_CHAINS.on(database).updateOne(session, expected,
                    new Document("$inc", new Document("modelWriteFence", 1L))).getMatchedCount() != 1) {
                throw eventUnavailable(ring, epoch, version);
            }
        }
        return new ModelReference(model, control.fence(), range);
    }

    private EventModelRange range(ClientSession session, SrsModel model, MongoSrsModelContexts.Context control) {
        Long end = null;
        if (model.key().modelVersion() < control.publishedVersion()) {
            if (model.key().modelVersion() + 1 == control.publishedVersion()) {
                end = control.publishedSequence();
            } else {
                Document next = SystemCollections.SRS_MODELS.on(database).find(session,
                        new Document("_id", key(version(control, model.key().modelVersion() + 1)))).first();
                if (next == null) throw unavailable(model.key());
                SrsModel successor = decode(next); requirePublished(control, successor);
                end = successor.ddlSequence();
            }
            if (end <= model.ddlSequence()) throw unreadable();
        }
        if (control.nextId() != null) {
            var next = contexts.require(session, control.nextId());
            if (!next.ring().equals(model.ringName()) || !control.nextSequence().equals(next.firstSequence())) throw unreadable();
            // The independently retained control proves this cut without retaining an unrelated schema body.
            end = end == null ? next.firstSequence() : Math.min(end, next.firstSequence());
        }
        return new EventModelRange(model.ddlSequence(), end);
    }

    record EventModelRange(long firstSequence, Long nextSequence) {
        void requireAt(long sequence, String ring, SrsLogRecord event) {
            requireAt(sequence, ring, event.epoch(), event.schemaVer());
        }
        void requireAt(long sequence, String ring, long epoch, long version) {
            if (sequence < firstSequence || (nextSequence != null && sequence >= nextSequence)) {
                throw eventUnavailable(ring, epoch, version);
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
            SrsLogBounds observed = requireBounds(stored);
            if (retained == null && observed.largestSequence() >= 0) throw unavailable(model.key());
            return observed;
        }
    }
    private static SrsLogBounds requireBounds(Document stored) {
        if (!stored.keySet().equals(Set.of("_id", "largestSequence", "trimmedThrough"))
                || !(stored.get("largestSequence") instanceof Long largest) || !(stored.get("trimmedThrough") instanceof Long trimmed)) throw unreadable();
        try { return new SrsLogBounds(largest, trimmed); }
        catch (IllegalArgumentException invalid) { throw unreadable(); }
    }
    private void requireBusiness(ClientSession session) { new MongoCacheBusinessIdentity(database, scope).requireReady(session); }
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
            SrsModelKey key = readKey(row.get("_id", Document.class));
            requireScope(key);
            Document table = row.get("schema", Document.class);
            var model = new SrsModel(key, row.getString("ringName"), MongoSchemaStore.toTable(table, "srs-model"), sequence);
            Document payload = new Document(row); payload.remove("contentHash");
            if (!key.cacheIncarnation().equals(row.get("cacheIncarnation")) || !table.equals(MongoSchemaStore.tableDocument(model.table()))
                    || !CanonicalHash.ofText(payload.toJson()).equals(row.get("contentHash"))) throw unreadable();
            return model;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    static SrsModelKey readKey(Document id) {
        try {
            Document context = id.get("captureContext", Document.class);
            if (!id.keySet().equals(Set.of("chain", "table", "captureContext", "modelVersion"))
                    || !context.keySet().equals(Set.of("cacheIncarnation", "epoch"))
                    || !(context.get("epoch") instanceof Long epoch) || !(id.get("modelVersion") instanceof Long version)) throw unreadable();
            return new SrsModelKey(id.getString("chain"), id.getString("table"), context.getString("cacheIncarnation"), epoch, version);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }

    static Document key(SrsModelKey key) {
        return new Document("chain", key.miningChainId()).append("table", key.table())
                .append("captureContext", new Document("cacheIncarnation", key.cacheIncarnation()).append("epoch", key.captureEpoch()))
                .append("modelVersion", key.modelVersion());
    }
    private void requireScope(SrsModelKey key) {
        if (!scope.cacheIncarnation().equals(Objects.requireNonNull(key, "key").cacheIncarnation())) throw businessFenced();
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                beforeSession();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> { requireActive.run(); requireStructure.run(); requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
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
