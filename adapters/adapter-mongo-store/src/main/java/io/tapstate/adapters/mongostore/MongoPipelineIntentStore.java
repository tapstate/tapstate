package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelineStateMapping;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Latest configuration intent, state mapping contention and authoring version share one transaction. */
final class MongoPipelineIntentStore implements PipelineIntentStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Set<String> HEADER = Set.of("_id", "formatVersion", "clusterId", "bindingVersion", "cacheIncarnation",
            "stateGeneration", "intentRevision", "deleted");
    private static final Set<String> BODY = Set.of("targetState", "revision", "purgeState", "assemblyRevision", "reassemble", "rebuiltAtStateEpoch");
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoCollection<Document> intents;
    private final MongoCollection<Document> mappings;
    private final MongoCollection<Document> pipelines;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> versions;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final Runnable requireActive;

    MongoPipelineIntentStore(MongoClient client, MongoDatabase database, String clusterId, MongoStorageBindingStore bindings) {
        this(client, database, clusterId, bindings, () -> {});
    }
    private MongoPipelineIntentStore(MongoClient client, MongoDatabase database, String clusterId,
            MongoStorageBindingStore bindings, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.database=Objects.requireNonNull(database,"database");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        if (!bindings.belongsTo(client, database.getName())) {
            throw new IllegalArgumentException("configuration intent and binding must share one original client and database");
        }
        this.intents = SystemCollections.PIPELINE_INTENTS.on(database);
        this.mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
        this.pipelines = SystemCollections.PIPELINES.on(database);
        this.identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database);
        this.versions = SystemCollections.CONFIGURATION_VERSION.on(database);
        this.requireActive = requireActive;
    }
    @Override public PipelineIntentStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoPipelineIntentStore(client, client.getDatabase(intents.getNamespace().getDatabaseName()), clusterId, bindings,
                () -> { requireActive.run(); additional.run(); });
    }
    @Override public Optional<PipelineIntentSnapshot> load(String pipelineId) {
        requireId(pipelineId);
        return transaction(session -> loadInTransaction(session, pipelineId));
    }
    Optional<PipelineIntentSnapshot> loadInTransaction(ClientSession session, String pipelineId) {
        return Optional.ofNullable(intents.find(session, new Document("_id", pipelineId)).first()).map(row -> decodeCurrent(session, row));
    }
    Optional<PipelineIntentSnapshot> loadRetainedInTransaction(ClientSession session, String pipelineId) {
        return Optional.ofNullable(intents.find(session, new Document("_id", pipelineId)).first()).map(this::decode);
    }
    void rebindStopped(ClientSession session, io.tapstate.spi.store.PipelineCacheRecovery recovery, String pipelineHash) {
        var target = recovery.targetScope();
        var previous = loadRetainedInTransaction(session, target.pipelineId());
        if (previous.filter(PipelineIntentSnapshot::deleted).isPresent() || pipelineHash == null || !pipelineHash.matches("[0-9a-f]{64}")) throw fenced();
        long revision = previous.map(PipelineIntentSnapshot::intentRevision).orElse(0L);
        if (revision >= Long.MAX_VALUE - 1) throw fenced();
        var stopped = header(target, revision + 1, false).append("targetState", PipelineState.STOPPED.name())
                .append("revision", pipelineHash).append("purgeState", false).append("assemblyRevision", null)
                .append("reassemble", false).append("rebuiltAtStateEpoch", null);
        if (previous.isEmpty()) intents.insertOne(session, stopped);
        else if (intents.replaceOne(session, new Document("_id", target.pipelineId()).append("intentRevision", revision), stopped).getMatchedCount() != 1) throw fenced();
    }
    @Override public Optional<PipelineIntentSnapshot> loadForActuation(String pipelineId) {
        requireId(pipelineId);
        return transaction(session -> {
            Document row = intents.find(session, new Document("_id", pipelineId)).first();
            if (row == null) return Optional.empty();
            var snapshot = decodeCurrent(session, row);
            if (snapshot.deleted()) return Optional.empty();
            Document mapping = mappings.find(session, new Document("_id", pipelineId)).first();
            var identity = MongoPipelineStateMappings.decode(pipelineId, mapping);
            if (identity.phase() != PipelineStateMapping.Phase.ACTIVE) return Optional.empty();
            if (identity.stateGeneration() != snapshot.scope().stateGeneration()) throw fenced();
            if (pipelines.find(session, new Document("_id", pipelineId)).first() == null) throw fenced();
            var prepared = MongoPipelinePreparationFacts.decode(pipelineId, mapping, clusterId);
            if (prepared.phase() != io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE) return Optional.empty();
            if (!snapshot.scope().equals(prepared.scope())) throw fenced();
            var shared=MongoPipelineSharedRecoveryFacts.read(mapping);
            if(shared.isPresent()) {
                if(snapshot.desired().targetState()==PipelineState.STOPPED && !snapshot.desired().purgeState())return Optional.of(snapshot);
                if(!MongoPipelineSharedRecoveryFacts.ready(session,database,shared.orElseThrow()))return Optional.empty();
                if(snapshot.desired().targetState()!=PipelineState.STOPPED && !snapshot.desired().purgeState())return Optional.empty();
            }
            return Optional.of(snapshot);
        });
    }

    @Override public List<String> pipelineIds() {
        return transaction(session -> intents.find(session, new Document("deleted", new Document("$ne", true)))
                .projection(new Document("_id", 1)).map(row -> {
                    if (!(row.get("_id") instanceof String id) || id.isBlank()) throw unreadable();
                    return id;
                }).into(new ArrayList<>()));
    }
    @Override public boolean compareAndSet(PipelineIntentScope scope, long expected, DesiredState desired) {
        Objects.requireNonNull(desired, "desired");
        if (!scope.pipelineId().equals(desired.pipelineId()) || desired.revision().isBlank()
                || (desired.rebuiltAtStateEpoch() != null && desired.rebuiltAtStateEpoch() < 0)) {
            throw new IllegalArgumentException("invalid Pipeline intent");
        }
        return change(scope, expected, desired);
    }
    @Override public boolean remove(PipelineIntentScope scope, long expected) { return change(scope, expected, null); }

    @Override public boolean startRecovered(io.tapstate.spi.store.CacheRecoveryStore.FullPreparation confirmation,long expected,String pipelineHash) {
        if(!(confirmation instanceof MongoCacheRecoveryStore.Permission permission))throw fenced();
        var scope=permission.recovery().targetScope();
        var desired=new DesiredState(scope.pipelineId(),PipelineState.RUNNING,pipelineHash,false,null,false,null);
        return change(scope,expected,desired,permission);
    }

    private boolean change(PipelineIntentScope scope, long expected, DesiredState desired) {
        return change(scope,expected,desired,null);
    }
    private boolean change(PipelineIntentScope scope,long expected,DesiredState desired,MongoCacheRecoveryStore.Permission permission) {
        Objects.requireNonNull(scope, "scope");
        if (expected < 0 || expected >= Long.MAX_VALUE - 1) throw new IllegalArgumentException("invalid expected intent revision");
        return transaction(session -> {
            Document current = intents.find(session, new Document("_id", scope.pipelineId())).first();
            PipelineIntentSnapshot held = current == null ? null : decodeCurrent(session, current);
            long revision = held == null ? 0 : held.intentRevision();
            if (revision != expected) return false;
            Document generation = requireScope(session, scope);
            MongoPipelineSharedRecoveryFacts.requireIntent(session,database,generation,scope,held,desired);
            var recovery=MongoCacheRecoveryFacts.read(generation);
            if(permission!=null) {
                permission.fence(session,client,database);
                if(!recovery.filter(permission.recovery()::equals).isPresent()
                        || MongoPipelinePreparationFacts.decode(scope.pipelineId(),generation,clusterId).phase()!=io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE)
                    throw fenced();
            } else if(recovery.isPresent() && desired!=null && desired.targetState()==PipelineState.RUNNING) {
                var preparation=MongoPipelinePreparationFacts.decode(scope.pipelineId(),generation,clusterId);
                var root=SystemCollections.STORAGE_RECOVERY.on(database).find(session,new Document("_id","cluster")).first();
                if(root==null)throw fenced();
                var pending=MongoStorageRecoveryCommands.pending(root);
                if(preparation.phase()!=io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE
                        || pending.filter(plan -> plan.phase()==io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING
                        && plan.manifest().affectedPipelineIds().contains(scope.pipelineId())).isPresent())
                    throw new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null);
            }
            if (held != null && !held.scope().equals(scope)) throw fenced();
            Document artifact = pipelines.find(session, new Document("_id", scope.pipelineId())).first();
            if (artifact == null) throw fenced();
            if (desired != null && !desired.revision().equals(artifact.get("contentHash"))) return false;
            if (desired == null && current != null && Boolean.TRUE.equals(current.get("deleted"))) return true;
            Document replacement = header(scope, revision + 1, desired == null);
            if (desired != null) replacement.append("targetState", desired.targetState().name()).append("revision", desired.revision())
                    .append("purgeState", desired.purgeState()).append("assemblyRevision", desired.assemblyRevision())
                    .append("reassemble", desired.reassemble()).append("rebuiltAtStateEpoch", desired.rebuiltAtStateEpoch());
            ConfigurationVersion.advance(session, versions);
            if (mappings.updateOne(session, new Document("_id", scope.pipelineId()).append("stateGeneration", scope.stateGeneration())
                            .append("phase", PipelineStateMapping.Phase.ACTIVE.name()).append("noteWriteFence", generation.getLong("noteWriteFence")),
                    new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) {
                throw new IllegalStateException("Pipeline state mapping changed within one transaction snapshot");
            }
            if (current == null) intents.insertOne(session, replacement);
            else if (intents.replaceOne(session, new Document("_id", scope.pipelineId()).append("intentRevision", expected), replacement,
                    new ReplaceOptions().upsert(false)).getMatchedCount() != 1) {
                throw new IllegalStateException("Pipeline intent changed within one transaction snapshot");
            }
            return true;
        });
    }
    static void recreate(ClientSession session,MongoDatabase database,PipelineIntentScope old,PipelineIntentScope next) {
        var table=SystemCollections.PIPELINE_INTENTS.on(database);var row=table.find(session,new Document("_id",old.pipelineId())).first();if(row==null)return;
        if(!(row.get("intentRevision") instanceof Long revision) || revision<1 || revision>=Long.MAX_VALUE-1
                || !row.equals(header(old,revision,true)))throw unreadable();
        if(table.replaceOne(session,new Document("_id",old.pipelineId()).append("intentRevision",revision),header(next,revision+1,true)).getMatchedCount()!=1)throw fenced();
    }

    void retire(ClientSession session,PipelineIntentScope original) {
        var held=loadInTransaction(session,original.pipelineId());if(held.isEmpty())return;
        var prior=held.orElseThrow();if(!prior.scope().equals(original) || prior.intentRevision()>=Long.MAX_VALUE-1)throw fenced();
        if(prior.deleted())return;
        if(intents.replaceOne(session,new Document("_id",original.pipelineId()).append("intentRevision",prior.intentRevision()),
                header(original,prior.intentRevision()+1,true)).getMatchedCount()!=1)throw fenced();
    }

    /** Only the original controlled rebuild transaction may re-scope and consume an already carried-out command. */
    long completeRebuild(ClientSession session, PipelineIntentSnapshot original, PipelineIntentScope target) {
        var current = loadInTransaction(session, original.scope().pipelineId()).orElseThrow(MongoPipelineIntentStore::fenced);
        if (!current.equals(original) || original.deleted() || original.intentRevision() >= Long.MAX_VALUE - 1
                || !target.pipelineId().equals(original.scope().pipelineId()) || target.stateGeneration() != original.scope().stateGeneration() + 1
                || !matchesCurrentCache(session, target)) throw fenced();
        var desired = original.desired(); long next = original.intentRevision() + 1;
        var replacement = header(target, next, false).append("targetState", desired.targetState().name()).append("revision", desired.revision())
                .append("purgeState", false).append("assemblyRevision", desired.assemblyRevision()).append("reassemble", false).append("rebuiltAtStateEpoch", null);
        if (intents.replaceOne(session, new Document("_id", target.pipelineId()).append("intentRevision", original.intentRevision())
                        .append("stateGeneration", original.scope().stateGeneration()), replacement).getMatchedCount() != 1)
            throw new IllegalStateException("original rebuild intent changed within one CONFIG snapshot");
        return next;
    }

    private Document requireScope(ClientSession session, PipelineIntentScope scope) {
        Document stored = identity.find(session, new Document("_id", "cluster")).first();
        if (stored == null || !MongoConfigurationDomain.LAYOUT.equals(stored.get("layoutId"))
                || !(stored.get("bindingVersion") instanceof Long binding)) throw unreadable();
        if (!clusterId.equals(scope.clusterId()) || !clusterId.equals(stored.get("clusterId")) || binding != scope.bindingVersion()) throw fenced();
        if (!matchesCurrentCache(session, scope)) throw fenced();
        Document row = mappings.find(session, new Document("_id", scope.pipelineId())).first();
        var state = MongoPipelineStateMappings.decode(scope.pipelineId(), row);
        if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() != scope.stateGeneration()) throw fenced();
        return row;
    }
    private boolean matchesCurrentCache(ClientSession session, PipelineIntentScope expected) {
        var preparation = bindings.cachePreparationForTransaction(session);
        if (preparation.isEmpty()) return false;
        var current = preparation.get().businessScope();
        return current.clusterId().equals(expected.clusterId()) && current.bindingVersion() == expected.bindingVersion()
                && current.cacheIncarnation().equals(expected.cacheIncarnation());
    }
    private PipelineIntentSnapshot decodeCurrent(ClientSession session, Document row) {
        var snapshot = decode(row);
        if (!matchesCurrentCache(session, snapshot.scope())) throw unreadable();
        return snapshot;
    }
    private static Document header(PipelineIntentScope scope, long revision, boolean deleted) {
        return new Document("_id", scope.pipelineId()).append("formatVersion", 1).append("clusterId", scope.clusterId())
                .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation())
                .append("stateGeneration", scope.stateGeneration()).append("intentRevision", revision).append("deleted", deleted);
    }
    private PipelineIntentSnapshot decode(Document row) {
        try {
            if (!(row.get("deleted") instanceof Boolean deleted) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !(row.get("intentRevision") instanceof Long revision) || !(row.get("bindingVersion") instanceof Long binding)
                    || !(row.get("stateGeneration") instanceof Long generation) || !clusterId.equals(row.get("clusterId"))) throw unreadable();
            var fields = new HashSet<>(HEADER); if (!deleted) fields.addAll(BODY);
            if (!fields.equals(row.keySet())) throw unreadable();
            var scope = new PipelineIntentScope(row.getString("clusterId"), binding, row.getString("cacheIncarnation"), row.getString("_id"), generation);
            DesiredState desired = null;
            if (!deleted) {
                if (!(row.get("purgeState") instanceof Boolean purge) || !(row.get("reassemble") instanceof Boolean reassemble)
                        || !(row.get("revision") instanceof String hash) || hash.isBlank()
                        || (row.get("assemblyRevision") != null && !(row.get("assemblyRevision") instanceof String))
                        || (row.get("rebuiltAtStateEpoch") != null && (!(row.get("rebuiltAtStateEpoch") instanceof Long epoch) || epoch < 0))) throw unreadable();
                desired = new DesiredState(scope.pipelineId(), PipelineState.valueOf(row.getString("targetState")), hash, purge,
                        row.getString("assemblyRevision"), reassemble, row.getLong("rebuiltAtStateEpoch"));
            }
            return new PipelineIntentSnapshot(scope, revision, desired);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> { requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }
    private static void requireId(String id) { if (id == null || id.isBlank()) throw new IllegalArgumentException("Pipeline id must not be blank"); }
    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "pipeline-intent", "field", "intent"), null); }
}
