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
import io.tapstate.spi.store.SrsConsumerStore.ReadEvent;
import io.tapstate.spi.store.SrsCaptureCheckpoint;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SourceModelComparison;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SrsModelStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Actual structural models are immutable and published under their original cache and capture context. */
public final class MongoSrsModelStore implements SrsModelStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final int MAX_INITIAL_PUBLICATION_BATCH = 4;
    private static final Document PROVE_CAPTURE = new Document("$inc", new Document("fencedModelWrites", 1L));
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final Runnable requireStructure;
    private final MongoSrsModelContexts contexts;
    private final MongoSrsLease lease;

    public MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, () -> {});
    }
    MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive, Runnable requireStructure) {
        this(client, database, scope, requireActive, requireStructure, MongoSrsLease.perTransaction());
    }
    /** The capture assembly's store: a model change here invalidates the append proof. */
    MongoSrsModelStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive, Runnable requireStructure,
                       MongoSrsLease lease) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.requireStructure = Objects.requireNonNull(requireStructure, "requireStructure");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.contexts = new MongoSrsModelContexts(database, scope);
    }
    @Override public MongoSrsModelStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsModelStore(client, database, scope, () -> { requireActive.run(); additional.run(); }, requireStructure, lease);
    }
    @Override public long reclaim(String chainId,WorkloadClaimFence fence) {
        if(chainId==null || chainId.isBlank())throw new IllegalArgumentException("blank mining chain");
        Objects.requireNonNull(fence,"originalCapture");
        if(fence.key().type()!=WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId()))throw claimFenced();
        // An empty retention pass leaves the append proof valid.
        boolean changed = true;
        try {
            long deleted = transaction(session -> reclaimInSession(session, chainId, fence));
            changed = deleted > 0;
            return deleted;
        } finally { if (changed) lease.drop(); }
    }
    private long reclaimInSession(ClientSession session, String chainId, WorkloadClaimFence fence) {
            requireBusiness(session);
            // Checkpoints take the same claim before the chain; retain that write order during cleanup.
            if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,WorkloadClaimDocuments.live(fence),PROVE_CAPTURE).getMatchedCount()!=1)throw claimFenced();
            var consumers=new MongoSrsConsumerStore(client,database,scope,requireActive,requireStructure);
            var pinned=consumers.retainedModels(session,chainId);
            var chain=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chainId)).first();
            if(chain==null || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation")) || !fence.key().resourceId().equals(chain.get("captureId")))throw claimFenced();
            var state=MongoSrsChainStore.decode(chain,scope,contexts);
            var checkpoint=state.checkpoint();
            var protectedKeys=new java.util.HashSet<>(pinned);
            if(state.pendingFreshFull()!=null)protectedKeys.addAll(state.pendingFreshFull().models().values());
            if(checkpoint!=null)checkpoint.tables().values().forEach(value -> protectedKeys.add(value.model()));
            long deleted=0;
            try(var rows=SystemCollections.SRS_MODEL_CONTEXTS.on(database).find(session,new Document("_id.chain",chainId)
                    .append("cacheIncarnation",scope.cacheIncarnation()).append("nextContext",new Document("$ne",null))).batchSize(64).iterator()) {
                while(rows.hasNext() && deleted<256) {
                    var stored=rows.next();var context=contexts.require(session,stored.get("_id",Document.class));contexts.requireAnchor(session,chain,context);
                    if(context.nextId()==null || protectedKeys.stream().anyMatch(key -> key.miningChainId().equals(chainId)
                            && key.table().equals(context.key().table()) && key.captureEpoch()==context.key().captureEpoch()))continue;
                    var next=contexts.require(session,context.nextId());
                    if(!next.ring().equals(context.ring()) || !Long.valueOf(next.firstSequence()).equals(context.nextSequence()))throw unreadable();
                    var bounds=SystemCollections.SRS_LOG_BOUNDS.on(database).find(session,new Document("_id",MongoSrsLogStore.boundsId(context.ring()))).first();
                    if(bounds==null)throw unavailable(context.key());
                    if(requireBounds(bounds).trimmedThrough()<context.nextSequence()-1)continue;
                    var filter=new Document("_id.chain",chainId).append("_id.table",context.key().table())
                            .append("_id.captureContext",new Document("cacheIncarnation",scope.cacheIncarnation()).append("epoch",context.key().captureEpoch()));
                    var ids=new java.util.ArrayList<Document>();
                    for(var body:SystemCollections.SRS_MODELS.on(database).find(session,filter).limit(256-(int)deleted)) {
                        var actual=decode(body);requirePublished(context,actual);ids.add(body.get("_id",Document.class));
                    }
                    if(!ids.isEmpty())deleted+=SystemCollections.SRS_MODELS.on(database).deleteMany(session,new Document("_id",new Document("$in",ids))).getDeletedCount();
                }
            }
            if (deleted > 0) lease.drop();
            return deleted;
    }
    private long retainedModelBytes(ClientSession session, String chain) {
        long bytes = 0;
        try (var rows = SystemCollections.SRS_MODELS.on(database).find(session, new Document("_id.chain", chain)
                .append("cacheIncarnation", scope.cacheIncarnation())).batchSize(64).iterator()) {
            while (rows.hasNext()) {
                requireActive.run();
                bytes += modelBytes(rows.next());
                if (bytes > RETAINED_MODEL_BUDGET_BYTES) break;
            }
        }
        return bytes;
    }

    private static int modelBytes(Document row) {
        return row.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    private void requireBudget(ClientSession session, SrsModel proposed, Document captured, Map<String, Long> batchBytes) {
        String chain = proposed.key().miningChainId();
        long retained = batchBytes == null ? retainedModelBytes(session, chain)
                : batchBytes.computeIfAbsent(chain, ignored -> retainedModelBytes(session, chain));
        long next = retained + modelBytes(captured);
        if (next > RETAINED_MODEL_BUDGET_BYTES) throw new TapstateException(IoError.SRS_MODEL_CAPACITY,
                Map.of("chain", chain, "bytes", RETAINED_MODEL_BUDGET_BYTES), null);
        if (batchBytes != null) batchBytes.put(chain, next);
    }

    @Override public SrsModel publishInitial(SrsModelKey key, String ring, SourceTable actual, WorkloadClaimFence fence) {
        return publishObservedInitial(key, ring, actual, fence, SourceModelComparison.DECLARED_STRUCTURE);
    }
    @Override public SrsModel publishObservedInitial(SrsModelKey key, String ring, SourceTable actual,
            WorkloadClaimFence fence, SourceModelComparison comparison) {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(actual, "actual"); Objects.requireNonNull(fence, "originalCapture");
        Objects.requireNonNull(comparison, "comparison");
        requireScope(key);
        if (key.modelVersion() != 0) throw new IllegalArgumentException("an initial model has version zero");
        var proposal = transaction(session -> initialProposal(session, key, ring, actual, comparison, null));
        // Keep the proposal fixed across publication retries; a changed source context must refuse it.
        return publish(proposal, fence);
    }

    private SrsModel initialProposal(ClientSession session, SrsModelKey key, String ring, SourceTable actual,
            SourceModelComparison comparison, MongoSrsReadFacts facts) {
        requireBusiness(session, facts);
        Document chain = requireChain(session, key, facts);
        var control = contexts.find(session, key, facts);
        SourceTable proposed = new SourceTable(actual.name(), actual.fields(), actual.primaryKey(), actual.indexes());
        long first;
        if (control != null) {
            contexts.requireAnchor(session, chain, control, facts);
            if (control.publishedVersion() != 0) throw unavailable(key);
            var retained = loadInSession(session, key, facts).orElseThrow(() -> unavailable(key));
            first = retained.ddlSequence();
            if (comparison == SourceModelComparison.SAMPLED_DOCUMENT
                    && comparison.acceptsObservation(retained.table(), proposed)) proposed = retained.table();
        } else first = initialSequence(session, key, ring);
        return new SrsModel(key, ring, proposed, first);
    }

    @Override public Map<SrsModelKey, SrsModel> requireAll(Set<SrsModelKey> keys) {
        var selected = Set.copyOf(keys);
        selected.forEach(this::requireScope);
        if (selected.isEmpty()) return Map.of();
        if (selected.size() > 64) return SrsModelStore.super.requireAll(selected);
        return transaction(session -> {
            var facts = new MongoSrsReadFacts(session, database, scope, false);
            var result = new java.util.LinkedHashMap<SrsModelKey, SrsModel>();
            for (var key : selected) result.put(key, loadInSession(session, key, facts).orElseThrow(() -> unavailable(key)));
            return Map.copyOf(result);
        });
    }

    @Override public Map<SrsModelKey, SrsModel> publishObservedInitials(Map<SrsModelKey, InitialObservation> observations,
            WorkloadClaimFence fence, SourceModelComparison comparison) {
        var selected = Map.copyOf(observations);
        Objects.requireNonNull(fence, "originalCapture"); Objects.requireNonNull(comparison, "comparison");
        selected.forEach((key, observation) -> {
            requireScope(key);
            if (key.modelVersion() != 0) throw new IllegalArgumentException("an initial model has version zero");
        });
        if (selected.isEmpty()) return Map.of();
        // Keep one exact capture context per batch, and leave renewal boundaries for wide sources.
        var first = selected.keySet().iterator().next();
        if (selected.size() > MAX_INITIAL_PUBLICATION_BATCH || selected.keySet().stream().anyMatch(key ->
                !key.miningChainId().equals(first.miningChainId()) || key.captureEpoch() != first.captureEpoch()))
            return SrsModelStore.super.publishObservedInitials(selected, fence, comparison);
        var proposals = transaction(session -> {
            var facts = new MongoSrsReadFacts(session, database, scope, false);
            var result = new java.util.LinkedHashMap<SrsModelKey, SrsModel>();
            selected.forEach((key, observation) -> result.put(key,
                    initialProposal(session, key, observation.ring(), observation.table(), comparison, facts)));
            return Map.copyOf(result);
        });
        // Proposals are fixed; every retry earns a fresh write proof and capacity budget.
        return changingTheModels(() -> transaction(session -> {
            requireBusiness(session);
            var result = new java.util.LinkedHashMap<SrsModelKey, SrsModel>();
            var batchBytes = new HashMap<String, Long>();
            proposals.forEach((key, proposal) -> result.put(key, publishInSession(session, proposal, fence, batchBytes)));
            return Map.copyOf(result);
        }));
    }

    private long initialSequence(ClientSession session, SrsModelKey key, String ring) {
        Document bounds =
                SystemCollections.SRS_LOG_BOUNDS
                        .on(database)
                        .find(session, new Document("_id", MongoSrsLogStore.boundsId(ring)))
                        .first();
        long largest = bounds == null ? -1 : requireBounds(bounds).largestSequence();
        if (largest >= Long.MAX_VALUE - 1) throw unavailable(key);
        return largest + 1;
    }

    @Override public SrsModel publish(SrsModel model, WorkloadClaimFence fence) {
        Objects.requireNonNull(model, "model"); Objects.requireNonNull(fence, "captureFence");
        return changingTheModels(() -> transaction(session -> publishInSession(session, model, fence)));
    }
    /** A model change by the owner invalidates the proof so the next append validates again. */
    private <T> T changingTheModels(java.util.function.Supplier<T> change) {
        try { return change.get(); } finally { lease.drop(); }
    }
    SrsModel publishInSession(ClientSession session, SrsModel model, WorkloadClaimFence fence) {
        return publishInSession(session, model, fence, null);
    }
    private SrsModel publishInSession(ClientSession session, SrsModel model, WorkloadClaimFence fence, Map<String, Long> batchBytes) {
        lease.drop();
        if (!session.hasActiveTransaction()) throw new IllegalStateException("model publication requires its original CACHE transaction");
        Objects.requireNonNull(model, "model"); Objects.requireNonNull(fence, "captureFence");
        requireScope(model.key());
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())) throw claimFenced();
        Document captured = document(model);
        if (!decode(captured).equals(model)) throw unavailable(model.key());
        if (batchBytes == null) requireBusiness(session);
        else requireActive.run();
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
        requireBudget(session, model, captured, batchBytes);
        models.insertOne(session, captured);
        return model;
    }
    @Override public SrsModel requireEvent(SrsModelKey expected, String ring, long sequence, WorkloadClaimFence originalCapture) {
        Objects.requireNonNull(expected, "expected"); requireScope(expected);
        if (ring == null || ring.isBlank() || sequence < 0 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid event model coordinates");
        return transaction(session -> {
            requireBusiness(session);
            var reference = requireEventReference(session, expected, ring, originalCapture);
            reference.range().requireAt(sequence, ring, expected.captureEpoch(), expected.modelVersion());
            return reference.model();
        });
    }

    /** The original model and its range, read within the consumer's verification transaction. */
    ModelReference requireEventReference(ClientSession session, SrsModelKey expected, String ring, WorkloadClaimFence originalCapture) {
        requireScope(expected);
        requireActive.run();
        try {
            if (originalCapture == null) throw eventUnavailable(ring, expected.captureEpoch(), expected.modelVersion());
            if (originalCapture.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(originalCapture.key().clusterId())) throw claimFenced();
            var chain = requireChain(session, expected);
            var reference = eventReference(session, ring, expected.captureEpoch(), expected.modelVersion(), originalCapture, false, chain);
            if (!expected.equals(reference.model().key())) throw eventUnavailable(ring, expected.captureEpoch(), expected.modelVersion());
            requireActive.run();
            return reference;
        } catch (TapstateException missing) {
            if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, expected.captureEpoch(), expected.modelVersion());
            throw missing;
        }
    }

    Map<SrsModelKey, ModelReference> verifyReadBatch(ClientSession session, Document root, String chain, String table, String ring,
            List<ReadEvent> events, SrsModelKey pin) {
        var requested = new HashSet<SrsModelKey>();
        for (var event : events) {
            if (event.sequence() < 0 || event.sequence() == Long.MAX_VALUE || event.epoch() < 1 || event.epoch() == Long.MAX_VALUE
                    || event.version() < 0 || event.version() == Long.MAX_VALUE || event.capture() == null)
                throw eventUnavailable(ring, event.epoch(), event.version());
            if (event.capture().key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(event.capture().key().clusterId()))
                throw claimFenced();
            requested.add(readKey(chain, table, event));
        }
        var selection = requested.stream().map(key -> new Document("_id.captureContext.epoch", key.captureEpoch())
                    .append("_id.modelVersion", key.modelVersion())).toList();
        var filter = new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring);
        if (selection.size() == 1) filter.putAll(selection.getFirst()); else filter.append("$or", selection);
        var models = new HashMap<SrsModelKey, SrsModel>();
        for (var row : SystemCollections.SRS_MODELS.on(database).find(session, filter).limit(2 * requested.size())) {
            var model = decode(row);
            if (!requested.contains(model.key()) || !ring.equals(model.ringName()))
                throw eventUnavailable(ring, model.key().captureEpoch(), model.key().modelVersion());
            if (models.putIfAbsent(model.key(), model) != null) throw unreadable();
        }
        for (var event : events) {
            if (!models.containsKey(readKey(chain, table, event)))
                throw eventUnavailable(ring, event.epoch(), event.version());
        }
        if (pin != null && !models.containsKey(pin)) models.put(pin, requireModel(session, pin));
        var ids = new HashSet<Document>();
        for (var key : models.keySet()) {
            requireChain(root, key);
            ids.add(MongoSrsModelContexts.contextId(key));
            var head = contexts.head(root, key.table());
            if (head != null) ids.add(head.get("contextId", Document.class));
        }
        var controls = contexts.findAll(session, ids);
        var references = new HashMap<SrsModelKey, ModelReference>();
        if (pin != null) {
            var model = models.get(pin);
            readControl(root, model, controls);
            requireSnapshotBounds(session, model);
        }
        for (var event : events) {
            var key = readKey(chain, table, event);
            var model = models.get(key);
            try {
                var reference = references.get(key);
                if (reference == null) {
                    var control = readControl(root, model, controls);
                    if (!event.capture().equals(control.fence())) throw claimFenced();
                    reference = new ModelReference(model, control.fence(), range(model, control,
                            successor -> models.computeIfAbsent(successor, k -> requireModel(session, k)),
                            next -> controls.computeIfAbsent(next, id -> contexts.require(session, id))));
                    references.put(key, reference);
                }
                if (!event.capture().equals(reference.fence())) throw claimFenced();
                reference.range().requireAt(event.sequence(), ring, event.epoch(), event.version());
            } catch (TapstateException missing) {
                if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, event.epoch(), event.version());
                throw missing;
            }
        }
        return references;
    }

    private SrsModelKey readKey(String chain, String table, ReadEvent event) {
        return new SrsModelKey(chain, table, scope.cacheIncarnation(), event.epoch(), event.version());
    }

    private SrsModel requireModel(ClientSession session, SrsModelKey key) {
        return requireModel(session, key, null);
    }
    private SrsModel requireModel(ClientSession session, SrsModelKey key, MongoSrsReadFacts facts) {
        var row = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_MODELS, new Document("_id", key(key)));
        if (row == null) throw unavailable(key);
        var model = decode(row);
        if (!key.equals(model.key())) throw unavailable(key);
        return model;
    }

    private MongoSrsModelContexts.Context readControl(Document root, SrsModel model,
            Map<Document, MongoSrsModelContexts.Context> controls) {
        var key = model.key();
        var context = controls.get(MongoSrsModelContexts.contextId(key));
        var head = contexts.head(root, key.table());
        if (context == null || head == null) throw unavailable(key);
        var latest = controls.get(head.get("contextId", Document.class));
        if (latest == null) throw unavailable(key);
        contexts.requireAnchor(root, context, latest);
        requirePublished(context, model);
        return context;
    }

    void requireIncrementalSnapshot(ClientSession session, Document root, SrsModelKey key) {
        requireScope(key);
        requireChain(root, key);
        var model = requireModel(session, key);
        var ids = new HashSet<Document>();
        ids.add(MongoSrsModelContexts.contextId(key));
        var head = contexts.head(root, key.table());
        if (head != null) ids.add(head.get("contextId", Document.class));
        readControl(root, model, contexts.findAll(session, ids));
        requireSnapshotBounds(session, model);
    }

    @Override public Optional<Map<String, SrsModel>> publishedForNewSnapshot(String chainId, Map<String, String> tableRings) {
        if (chainId == null || chainId.isBlank()) throw new IllegalArgumentException("blank mining chain");
        var selected = snapshotRings(tableRings);
        return transaction(session -> {
            requireSnapshotResources(); requireBusiness(session);
            String firstTable = selected.keySet().iterator().next();
            Document chain = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chainId)).first();
            if (chain == null || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation"))) {
                throw unavailable(new SrsModelKey(chainId, firstTable, scope.cacheIncarnation(), 1, 0));
            }
            return publishedForNewSnapshot(session, chain, selected,
                    MongoSrsChainStore.decode(chain, scope, contexts).checkpoint(),
                    new MongoSrsReadFacts(session, database, scope, chain, true));
        });
    }

    Optional<Map<String, SrsModel>> startupModels(ClientSession session, Document chain, Map<String, String> tableRings,
            SrsCaptureCheckpoint checkpoint, MongoSrsReadFacts facts) {
        requireSnapshotResources();
        return publishedForNewSnapshot(session, chain, snapshotRings(tableRings), checkpoint, facts);
    }

    private static Map<String, String> snapshotRings(Map<String, String> tableRings) {
        Map<String, String> selected = Map.copyOf(Objects.requireNonNull(tableRings, "tableRings"));
        if (selected.isEmpty() || selected.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue().isBlank())
                || Set.copyOf(selected.values()).size() != selected.size()) throw new IllegalArgumentException("invalid snapshot table rings");
        return selected;
    }

    private Optional<Map<String, SrsModel>> publishedForNewSnapshot(ClientSession session, Document chain,
            Map<String, String> selected, SrsCaptureCheckpoint checkpoint, MongoSrsReadFacts facts) {
        String chainId = chain.getString("_id");
        prefetchSnapshotHeads(session, chain, selected, facts);
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
            var control = contexts.require(session, anchor.get("contextId", Document.class), facts);
            contexts.requireAnchor(session, chain, control, facts);
            if (!ring.equals(control.ring()) || control.nextId() != null) throw unavailable(control.key());
            SrsModel model = requirePublishedHead(session, control, facts);
            requireSnapshotBounds(session, model, facts);
            result.put(table, model);
        }
        return result.size() == selected.size() ? Optional.of(Map.copyOf(result)) : Optional.empty();
    }

    private void prefetchSnapshotHeads(ClientSession session, Document chain, Map<String, String> selected, MongoSrsReadFacts facts) {
        if (selected.size() < 2 || selected.size() > 64) return;
        var ids = new java.util.ArrayList<Document>();
        for (String table : selected.keySet()) {
            var head = contexts.head(chain, table);
            if (head == null) return;
            ids.add(head.get("contextId", Document.class));
        }
        if (!facts.prefetchExactIds(SystemCollections.SRS_MODEL_CONTEXTS, ids, requireActive)) return;
        var models = ids.stream().map(id -> contexts.require(session, id, facts))
                .map(control -> key(version(control, control.publishedVersion()))).toList();
        if (!facts.prefetchExactIds(SystemCollections.SRS_MODELS, models, requireActive)) return;
        facts.prefetchExactIds(SystemCollections.SRS_LOG_BOUNDS,
                selected.values().stream().map(MongoSrsLogStore::boundsId).toList(), requireActive);
    }

    private void requireSnapshotResources() {
        var names = java.util.List.of(SystemCollections.SRS_CHAINS, SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS,
                SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS).stream().map(SystemCollections::collectionName).toList();
        var present = MongoCollectionMetadata.matching(database, names);
        if (present.size() != names.size() || present.stream().anyMatch(row -> !"collection".equals(row.get("type")))) throw unreadable();
    }

    @Override public Optional<SrsModel> load(SrsModelKey key) {
        requireScope(key);
        return transaction(session -> loadInSession(session, key));
    }
    private Optional<SrsModel> loadInSession(ClientSession session, SrsModelKey key) {
        return loadInSession(session, key, null);
    }
    private Optional<SrsModel> loadInSession(ClientSession session, SrsModelKey key, MongoSrsReadFacts facts) {
            requireBusiness(session, facts);
            Document chain = requireChain(session, key, facts);
            Document anchor = contexts.head(chain, key.table());
            var control = contexts.find(session, key, facts);
            Document stored = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_MODELS, new Document("_id", key(key)));
            if (control == null) {
                if (stored != null || anchor != null) throw unavailable(key);
                return Optional.empty();
            }
            contexts.requireAnchor(session, chain, control, facts);
            if (stored == null) {
                if (key.modelVersion() <= control.publishedVersion()) throw unavailable(key);
                return Optional.empty();
            }
            SrsModel model = decode(stored); requirePublished(control, model);
            return Optional.of(model);
    }

    /** Snapshot decoding needs an exact structural reference, without claiming an event-time range. */
    SrsModel requireSnapshotReference(ClientSession session, SrsModelKey key) {
        return requireSnapshotReference(session,key,null);
    }
    private SrsModel requireSnapshotReference(ClientSession session, SrsModelKey key, MongoSrsReadFacts facts) {
        requireScope(key);
        SrsModel model = loadInSession(session, key, facts).orElseThrow(() -> unavailable(key));
        requireSnapshotBounds(session, model, facts);
        return model;
    }

    private void requireSnapshotBounds(ClientSession session, SrsModel model) {
        requireSnapshotBounds(session,model,null);
    }
    private void requireSnapshotBounds(ClientSession session, SrsModel model, MongoSrsReadFacts facts) {
        Document bounds = MongoSrsReadFacts.find(facts,session,database,SystemCollections.SRS_LOG_BOUNDS,
                new Document("_id",MongoSrsLogStore.boundsId(model.ringName())));
        if (bounds == null) throw unavailable(model.key());
        requireBounds(bounds);
    }

    /** Snapshot pins need exact structural bodies and bounds, without adopting newer models or event ranges. */
    void requireSnapshotReferences(ClientSession session, Set<SrsModelKey> keys) {
        requireSnapshotReferences(session, keys, new MongoSrsReadFacts(session, database, scope, false));
    }

    void requireSnapshotReferences(ClientSession session, Set<SrsModelKey> keys, MongoSrsReadFacts facts) {
        if (keys.size() < 2) { for (var key : keys) requireSnapshotReference(session, key, facts); return; }
        var first = keys.iterator().next();
        var current = currentReferences(session, requireChain(session, first, facts), keys, false, facts);
        if (current.isEmpty()) { for (var key : keys) requireSnapshotReference(session, key, facts); return; }
        var references = current.orElseThrow().values();
        var byId = references.stream().collect(java.util.stream.Collectors.toMap(
                reference -> MongoSrsLogStore.boundsId(reference.model().ringName()), reference -> reference.model().key()));
        if (facts.prefetchExactIds(SystemCollections.SRS_LOG_BOUNDS, byId.keySet(), requireActive)) {
            for (var reference : references) requireSnapshotBounds(session, reference.model(), facts);
            return;
        }
        // An incomplete batch still distinguishes an absent boundary from a collating invalid row.
        var found = new java.util.HashSet<SrsModelKey>();
        try (var rows = SystemCollections.SRS_LOG_BOUNDS.on(database).find(session,
                new Document("_id", new Document("$in", byId.keySet()))).batchSize(64).iterator()) {
            while (rows.hasNext()) {
                requireActive.run(); var row = rows.next(); var key = byId.get(row.get("_id"));
                if (key == null || !found.add(key)) throw unreadable();
                requireBounds(row);
            }
        }
        for (var key : byId.values()) if (!found.contains(key)) throw unavailable(key);
    }

    record ModelReference(SrsModel model, WorkloadClaimFence fence, EventModelRange range) {}

    ModelReference requireReference(ClientSession session, SrsModelKey key) {
        return requireReference(session, key, null);
    }
    ModelReference requireReference(ClientSession session, SrsModelKey key, MongoSrsReadFacts facts) {
        requireScope(key);
        SrsModel model = loadInSession(session, key, facts).orElseThrow(() -> unavailable(key));
        var control = contexts.find(session, key, facts);
        return new ModelReference(model, control.fence(), range(session, model, control, facts));
    }

    /** A current, single-context ring can share its exact model and anchor reads within this transaction. */
    Optional<Map<SrsModelKey, ModelReference>> currentReferences(ClientSession session, Document chain, Set<SrsModelKey> keys) {
        return currentReferences(session, chain, keys, false);
    }

    Optional<Map<SrsModelKey, ModelReference>> currentReferences(ClientSession session, Document chain, Set<SrsModelKey> keys,
            boolean businessChecked) {
        return currentReferences(session, chain, keys, businessChecked, null);
    }
    Optional<Map<SrsModelKey, ModelReference>> currentReferences(ClientSession session, Document chain, Set<SrsModelKey> keys,
            boolean businessChecked, MongoSrsReadFacts facts) {
        if (keys.size() < 2) return Optional.empty();
        if (!businessChecked) requireBusiness(session, facts);
        return facts == null ? loadCurrentReferences(session, chain, keys, null)
                : facts.currentReferences(session, database, keys, () -> loadCurrentReferences(session, chain, keys, facts));
    }

    private Optional<Map<SrsModelKey, ModelReference>> loadCurrentReferences(ClientSession session, Document chain,
            Set<SrsModelKey> keys, MongoSrsReadFacts facts) {
        for (var key : keys) {
            requireScope(key); requireChain(chain, key);
            var head = contexts.head(chain, key.table());
            if (head == null || !MongoSrsModelContexts.contextId(key).equals(head.get("contextId"))) return Optional.empty();
        }
        var selected = new java.util.HashMap<SrsModelKey, SrsModel>();
        var ids = keys.stream().map(MongoSrsModelStore::key).toList();
        try (var rows = SystemCollections.SRS_MODELS.on(database).find(session, new Document("_id", new Document("$in", ids)))
                .batchSize(64).iterator()) {
            while (rows.hasNext()) {
                requireActive.run(); var row = rows.next(); var model = decode(row);
                if (!keys.contains(model.key()) || selected.put(model.key(), model) != null) throw unreadable();
                if (facts != null) facts.seed(SystemCollections.SRS_MODELS, row);
            }
        }
        if (selected.size() != keys.size()) return Optional.empty();
        var rings = selected.values().stream().map(SrsModel::ringName).collect(java.util.stream.Collectors.toSet());
        if (rings.size() != keys.size()) return Optional.empty();
        // Historical or ambiguous rings use the full reference walk. No proof survives this transaction.
        var rows = SystemCollections.SRS_MODEL_CONTEXTS.on(database).find(session,
                new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", new Document("$in", rings)))
                .limit(keys.size() + 1).batchSize(64).into(new java.util.ArrayList<>());
        if (rows.size() != keys.size()) return Optional.empty();
        var controls = new java.util.HashMap<Document, MongoSrsModelContexts.Context>();
        for (var row : rows) {
            var control = contexts.decode(row);
            if (controls.put(control.id(), control) != null) throw unreadable();
        }
        var result = new java.util.HashMap<SrsModelKey, ModelReference>();
        for (var model : selected.values()) {
            requireActive.run(); var control = controls.get(MongoSrsModelContexts.contextId(model.key()));
            if (control == null || control.nextId() != null || control.publishedVersion() != model.key().modelVersion()) return Optional.empty();
            contexts.requireAnchor(chain, control, control); requirePublished(control, model);
            result.put(model.key(), new ModelReference(model, control.fence(), new EventModelRange(model.ddlSequence(), null)));
        }
        return Optional.of(Map.copyOf(result));
    }

    private SrsModel requirePublishedHead(ClientSession session, MongoSrsModelContexts.Context control) {
        return requirePublishedHead(session, control, null);
    }
    private SrsModel requirePublishedHead(ClientSession session, MongoSrsModelContexts.Context control, MongoSrsReadFacts facts) {
        Document stored = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_MODELS,
                new Document("_id", key(version(control, control.publishedVersion()))));
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
        return requireChain(session, key, null);
    }
    private Document requireChain(ClientSession session, SrsModelKey key, MongoSrsReadFacts facts) {
        Document chain = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CHAINS, new Document("_id", key.miningChainId()));
        return requireChain(chain, key);
    }
    private Document requireChain(Document chain, SrsModelKey key) {
        if (chain == null || !scope.cacheIncarnation().equals(chain.get("cacheIncarnation"))) throw unavailable(key);
        MongoSrsRecoveryControls.requireOrdinary(chain);
        if (!key.miningChainId().equals(chain.get("_id"))) throw unavailable(key);
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

    /** Proves only the original ring's isolated control and ordering anchor, without model payload IO. */
    boolean isolatedRingForInitialization(ClientSession session,String ring) {
        var context=contexts.firstOnRing(session,ring);
        var chain=SystemCollections.SRS_CHAINS.on(database).find(session,context==null
                ? new Document("cacheIncarnation",scope.cacheIncarnation()).append("modelContextHeads.ringName",ring)
                : new Document("_id",context.key().miningChainId())).first();
        if(chain==null) {
            if(context!=null)throw eventUnavailable(ring,context.key().captureEpoch(),0);
            return false;
        }
        if(MongoSrsRecoveryControls.read(chain).isEmpty())return false;
        if(context==null)throw eventUnavailable(ring,0,0);
        var original=new MongoSrsConsumerStore(client,database,scope,requireActive,requireStructure)
                .isolatedRootForRingInitialization(session,context.key().miningChainId());
        contexts.requireAnchor(session,original,context);
        return true;
    }

    /** Proves sole, current anchors and published model bodies for this batch in the caller's snapshot. */
    boolean provePublishedRings(ClientSession session, String chainId, Set<String> rings) {
        var root = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chainId)).first();
        return provePublishedRings(session, root, chainId, rings);
    }

    boolean provePublishedRings(ClientSession session, Document root, String chainId, Set<String> rings) {
        if (root == null || MongoSrsRecoveryControls.read(root).isPresent()) return false;
        var names = List.copyOf(rings);
        var selected = SystemCollections.SRS_MODEL_CONTEXTS.on(database).aggregate(session, List.of(
                new Document("$match", new Document("cacheIncarnation", scope.cacheIncarnation())
                        .append("ringName", new Document("$in", names))),
                new Document("$limit", rings.size() + 1),
                new Document("$project", new Document("row", "$$ROOT").append("matches", exactMatches(names, "$ringName")))))
                .into(new java.util.ArrayList<>());
        if (selected.size() != rings.size()) return false;
        var controls = new java.util.HashMap<SrsModelKey, MongoSrsModelContexts.Context>();
        var remaining = new HashSet<>(rings);
        for (var row : selected) {
            var control = contexts.decode(row.get("row", Document.class));
            if (!remaining.remove(control.ring()) || !List.of(control.ring()).equals(row.get("matches"))
                    || !chainId.equals(control.key().miningChainId()) || !scope.cacheIncarnation().equals(control.key().cacheIncarnation())
                    || control.nextId() != null || !Long.valueOf(control.key().captureEpoch()).equals(root.get("epoch"))) return false;
            requireChain(root, control.key());
            var head = contexts.head(root, control.key().table());
            if (head == null || !control.id().equals(head.get("contextId"))) return false;
            contexts.requireAnchor(root, control, control);
            if (controls.putIfAbsent(version(control, control.publishedVersion()), control) != null) return false;
        }
        var ids = controls.keySet().stream().map(MongoSrsModelStore::key).toList();
        for (var row : SystemCollections.SRS_MODELS.on(database).find(session, new Document("_id", new Document("$in", ids)))
                .limit(rings.size() + 1)) {
            requireActive.run();
            var model = decode(row);
            var control = controls.remove(model.key());
            if (control == null) return false;
            requirePublished(control, model);
        }
        return controls.isEmpty();
    }

    /** Verifies which requested keys match under the collection's own collation. */
    static Document exactMatches(List<String> keys, String field) {
        return new Document("$filter", new Document("input", new Document("$literal", keys))
                .append("as", "requested").append("cond", new Document("$eq", List.of("$$requested", field))));
    }

    /** An unseen ring may be inspected only when no published ordering anchor or retained data contradicts it. */
    void checkRing(ClientSession session, String ring) {
        checkRing(session, ring, null);
    }
    void checkRing(ClientSession session, String ring, MongoSrsReadFacts facts) {
        var control = contexts.firstOnRing(session, ring, facts);
        Document chain;
        if (control != null) {
            try { chain = requireChain(session, control.key(), facts); }
            catch (TapstateException missing) {
                if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, control.key().captureEpoch(), 0);
                throw missing;
            }
        }
        else {
            chain = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CHAINS,
                    new Document("cacheIncarnation", scope.cacheIncarnation()).append("modelContextHeads.ringName", ring));
            if (chain != null) MongoSrsRecoveryControls.requireOrdinary(chain);
        }
        Document retained = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_MODELS,
                new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring), true);
        Document bounds = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_LOG_BOUNDS, new Document("_id", MongoSrsLogStore.boundsId(ring)));
        if (control == null) {
            if (chain != null || retained != null || bounds != null
                    || SystemCollections.SRS_EVENTS.on(database).find(session, MongoSrsLogStore.ringRange(ring, Long.MAX_VALUE)).limit(1).first() != null) {
                throw eventUnavailable(ring, 0, 0);
            }
        } else {
            if (bounds == null || retained == null) throw eventUnavailable(ring, control.key().captureEpoch(), 0);
            try { contexts.requireAnchor(session, chain, control, facts); }
            catch (TapstateException missing) {
                if (missing.code() == IoError.SRS_MODEL_UNAVAILABLE) throw eventUnavailable(ring, control.key().captureEpoch(), 0);
                throw missing;
            }
        }
    }

    /** Trimming historical rows belongs to the chain's current original capture tenure. */
    SrsModelKey requireCaptureForTrim(ClientSession session, String ring, WorkloadClaimFence original) {
        if (original.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(original.key().clusterId())) throw claimFenced();
        checkRing(session, ring);
        var control = contexts.firstOnRing(session, ring);
        if (control == null) throw eventUnavailable(ring, 0, 0);
        Document chain = requireChain(session, control.key());
        if (!(chain.get("captureFence") instanceof Document bound)) throw unreadable();
        if (!original.key().resourceId().equals(chain.get("captureId"))
                || !original.equals(WorkloadClaimDocuments.readCapture(bound))) throw claimFenced();
        return control.key();
    }

    long confirmedTrimCut(ClientSession session,SrsModelKey identity,long requested) {
        requireScope(identity);
        return new MongoSrsConsumerStore(client,database,scope,requireActive,null)
                .limitTrimToConfirmed(session,identity.miningChainId(),identity.table(),requested);
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
        return eventReference(session,ring,epoch,version,fence,writing,null);
    }
    private ModelReference eventReference(ClientSession session, String ring, long epoch, long version, WorkloadClaimFence fence, boolean writing,Document originalRoot) {
        if (version < 0 || epoch < 1 || fence == null) throw eventUnavailable(ring, epoch, version);
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())) throw claimFenced();
        var candidates = SystemCollections.SRS_MODELS.on(database).find(session, new Document("cacheIncarnation", scope.cacheIncarnation())
                .append("ringName", ring).append("_id.captureContext.epoch", epoch).append("_id.modelVersion", version))
                .limit(2).into(new java.util.ArrayList<>());
        if (candidates.isEmpty()) throw eventUnavailable(ring, epoch, version);
        if (candidates.size() != 1) throw unreadable();
        SrsModel model = decode(candidates.getFirst());
        Document chain = originalRoot==null?requireChain(session, model.key()):requireChain(originalRoot,model.key());
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
        return range(session, model, control, null);
    }
    private EventModelRange range(ClientSession session, SrsModel model, MongoSrsModelContexts.Context control, MongoSrsReadFacts facts) {
        return range(model, control, key -> requireModel(session, key, facts), id -> contexts.require(session, id, facts));
    }

    private EventModelRange range(SrsModel model, MongoSrsModelContexts.Context control,
            Function<SrsModelKey, SrsModel> successorModel, Function<Document, MongoSrsModelContexts.Context> nextContext) {
        Long end = null;
        if (model.key().modelVersion() < control.publishedVersion()) {
            if (model.key().modelVersion() + 1 == control.publishedVersion()) {
                end = control.publishedSequence();
            } else {
                SrsModel successor = successorModel.apply(version(control, model.key().modelVersion() + 1));
                requirePublished(control, successor);
                end = successor.ddlSequence();
            }
            if (end <= model.ddlSequence()) throw unreadable();
        }
        if (control.nextId() != null) {
            var next = nextContext.apply(control.nextId());
            if (!next.ring().equals(model.ringName()) || !control.nextSequence().equals(next.firstSequence())) throw unreadable();
            // The independently retained control proves this cut without retaining an unrelated schema body.
            end = end == null ? next.firstSequence() : Math.min(end, next.firstSequence());
        }
        return new EventModelRange(model.ddlSequence(), end);
    }

    record EventModelRange(long firstSequence, Long nextSequence) {
        boolean covers(long sequence) {
            return sequence >= firstSequence && (nextSequence == null || sequence < nextSequence);
        }

        void requireAt(long sequence, String ring, SrsLogRecord event) {
            requireAt(sequence, ring, event.epoch(), event.schemaVer());
        }
        void requireAt(long sequence, String ring, long epoch, long version) {
            if (!covers(sequence)) {
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
    private void requireBusiness(ClientSession session, MongoSrsReadFacts facts) {
        if (facts == null) requireBusiness(session);
        else facts.requireBusiness(session, scope, () -> requireBusiness(session));
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
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            try {
                beforeSession();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> { MongoStructureFailureScope.beginCallback(); requireActive.run(); requireStructure.run(); requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        }));
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
