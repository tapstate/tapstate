package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.function.Function;
import org.bson.Document;

/** Publishes the entire replacement scope and stopped user intents in the original CONFIG transaction. */
final class MongoCacheRecoveryStore implements CacheRecoveryStore {
    private static final TransactionOptions TX = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final String clusterId;
    private final Runnable active;

    MongoCacheRecoveryStore(MongoClient client, MongoDatabase database, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, String clusterId, Runnable active) {
        this.client = client; this.database = database; this.bindings = bindings; this.artifacts = artifacts; this.clusterId = clusterId; this.active = active;
    }
    @Override public CacheRecoveryStore guardedBy(Runnable guard) {
        Objects.requireNonNull(guard);
        return new MongoCacheRecoveryStore(client, database, bindings, artifacts, clusterId, () -> { active.run(); guard.run(); });
    }
    @Override public List<ScopeChange> inspectCluster(CacheAuthScope target) {
        return transaction(session -> inspect(session, target));
    }
    List<ScopeChange> inspect(ClientSession session, CacheAuthScope target) {
        requireTarget(session, target);
        var result = new ArrayList<ScopeChange>();
        try (var rows = SystemCollections.PIPELINES.on(database).find(session).sort(new Document("_id", 1)).iterator()) {
            while (rows.hasNext()) {
                var pipeline = rows.next();
                if (!(pipeline.get("_id") instanceof String id) || id.isBlank()) throw MongoCacheRecoveryFacts.unreadable();
                var mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", id)).first();
                var state = MongoPipelineStateMappings.decode(id, mapping);
                if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() >= Long.MAX_VALUE - 1) throw MongoCacheRecoveryFacts.refused();
                var prepared = MongoPipelinePreparationFacts.decode(id, mapping, clusterId);
                var retained = MongoCacheRecoveryFacts.read(mapping);
                if (retained.isPresent() && matches(target, retained.orElseThrow().targetScope())) {
                    var value = retained.orElseThrow();
                    result.add(new ScopeChange(value.previousScope(), value.targetScope()));
                    continue;
                }
                var intent = intents().loadRetainedInTransaction(session, id);
                if (intent.isPresent() && intent.orElseThrow().deleted()) throw MongoCacheRecoveryFacts.refused();
                var previous = prepared.scope();
                if (previous == null && retained.isPresent()) previous = retained.orElseThrow().previousScope();
                if (previous == null && retained.isEmpty() && intent.isPresent()) previous = intent.orElseThrow().scope();
                if (previous != null && (!clusterId.equals(previous.clusterId()) || target.bindingVersion() != previous.bindingVersion()
                        || matches(target, previous))) throw MongoCacheRecoveryFacts.refused();
                if (intent.isPresent() && prepared.scope() != null && !intent.orElseThrow().scope().equals(prepared.scope())) throw MongoCacheRecoveryFacts.refused();
                var next = new PipelineIntentScope(clusterId, target.bindingVersion(), target.cacheIncarnation(), id, Math.addExact(state.stateGeneration(), 1));
                result.add(new ScopeChange(previous, next));
            }
        }
        return List.copyOf(result);
    }
    @Override public List<PipelineCacheRecovery> stageCluster(StorageRecoveryCommands.Plan confirmed) {
        if (confirmed.manifest().selection().extent() != StorageRecoveryCommands.Extent.CLUSTER
                || confirmed.phase() != StorageRecoveryCommands.Phase.CONFIRMED && confirmed.phase() != StorageRecoveryCommands.Phase.RUNNING)
            throw MongoCacheRecoveryFacts.refused();
        var running = new MongoStorageRecoveryCommands(client, database, bindings, artifacts, active).startStoppedCluster(confirmed, this);
        return transaction(session -> {
            requirePlan(session, running);
            var result = new ArrayList<PipelineCacheRecovery>();
            for (String id : running.manifest().affectedPipelineIds()) {
                var summary = MongoCacheRecoveryFacts.read(SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", id)).first())
                        .orElseThrow(MongoCacheRecoveryFacts::refused);
                if (!summary.recoveryId().equals(running.operationId()) || !matches(running.originalScope(), summary.targetScope())) throw MongoCacheRecoveryFacts.refused();
                result.add(summary);
            }
            return List.copyOf(result);
        });
    }
    List<PipelineCacheRecovery> publishInTransaction(ClientSession session, StorageRecoveryCommands.Plan running) {
        requireTarget(session, running.originalScope());
        var changes = inspect(session, running.originalScope());
        if (!running.manifest().dependencyHash().equals(CacheRecoveryStore.clusterDependencyHash(running.originalScope(), changes)))
            throw MongoCacheRecoveryFacts.refused();
        if (!changes.stream().map(change -> change.targetScope().pipelineId()).collect(java.util.stream.Collectors.toSet())
                .equals(running.manifest().affectedPipelineIds())) throw MongoCacheRecoveryFacts.refused();
        var result = new ArrayList<PipelineCacheRecovery>();
        for (var change : changes) {
            String id = change.targetScope().pipelineId();
            var table = SystemCollections.OPERATOR_GENERATIONS.on(database);
            var row = table.find(session, new Document("_id", id)).first();
            var retained = MongoCacheRecoveryFacts.read(row);
            var summary = new PipelineCacheRecovery(running.operationId(), change.previousScope(), change.targetScope());
            if (retained.filter(summary::equals).isPresent()) {
                var stopped = intents().loadInTransaction(session, id).orElseThrow(MongoCacheRecoveryFacts::refused);
                if (!stopped.scope().equals(summary.targetScope()) || stopped.deleted() || stopped.desired().targetState() != PipelineState.STOPPED
                        || stopped.desired().purgeState() || stopped.desired().reassemble()) throw MongoCacheRecoveryFacts.refused();
                result.add(summary); continue;
            }
            if (retained.isPresent() && matches(running.originalScope(), retained.orElseThrow().targetScope())) throw MongoCacheRecoveryFacts.refused();
            var old = MongoPipelineStateMappings.decode(id, row);
            if (old.phase() != PipelineStateMapping.Phase.ACTIVE || summary.targetScope().stateGeneration() != old.stateGeneration() + 1
                    || row.getLong("noteWriteFence") >= Long.MAX_VALUE - 1) throw MongoCacheRecoveryFacts.refused();
            var replacement = new Document(row).append("stateGeneration", summary.targetScope().stateGeneration())
                    .append("operationId", running.operationId()).append("noteWriteFence", row.getLong("noteWriteFence") + 1)
                    .append("cachePreparation", new Document("phase", "UNASSIGNED")).append("cacheRecovery", MongoCacheRecoveryFacts.encode(summary));
            replacement.remove("stateRebuild");
            MongoPipelineSharedRecoveryFacts.removeOnVerifiedReplacement(row,replacement,summary.previousScope());
            MongoValidationPreparations.beginCacheReplacement(session, database, clusterId, row, replacement, running);
            intents().rebindStopped(session, summary, running.manifest().definitionHashes().get(id));
            if (table.replaceOne(session, row, replacement).getMatchedCount() != 1) throw MongoCacheRecoveryFacts.refused();
            result.add(summary);
        }
        return List.copyOf(result);
    }

    static final class ValidationClosure {
        private final MongoCacheRecoveryStore owner;
        private final StorageRecoveryCommands.Plan running;
        private final String pipeline;
        private ValidationClosure(MongoCacheRecoveryStore owner, StorageRecoveryCommands.Plan running, String pipeline) {
            this.owner = owner; this.running = running; this.pipeline = pipeline;
        }
        String pipeline() { return pipeline; }
        void require(MongoClient client, MongoDatabase database, ClientSession session) {
            owner.active.run();
            if (owner.client != client || !owner.database.getName().equals(database.getName())) throw MongoCacheRecoveryFacts.refused();
            owner.requirePlan(session, running);
            MongoValidationPreparations.requireCacheReplacement(session, database, owner.clusterId, pipeline, running);
        }
    }

    boolean closeValidationReports(StorageRecoveryCommands.Plan running, int maximum) {
        boolean complete = true;
        for (String pipeline : running.manifest().affectedPipelineIds()) {
            var closure = new ValidationClosure(this, running, pipeline);
            var store = MongoValidationAdmissionStore.forRecovery(client, database, clusterId, active, closure);
            if (!store.transaction(session -> store.closePipelineInTransaction(session, pipeline, maximum))) complete = false;
        }
        return complete;
    }
    @Override public Optional<PipelineCacheRecovery> read(String pipelineId) {
        return transaction(session -> MongoCacheRecoveryFacts.read(SystemCollections.OPERATOR_GENERATIONS.on(database)
                .find(session, new Document("_id", pipelineId)).first()));
    }
    @Override public List<RequiredNoteContext> inspectPrivateContexts(String pipelineId) {
        return transaction(session -> inspectNotes(session,pipelineId));
    }
    @Override public void requirePrivateRetirable(String pipelineId,RequiredNoteScope original) {
        transaction(session -> {
            var found=inspectNotes(session,pipelineId).stream().filter(context -> context.scope().equals(original)).findFirst().orElseThrow(MongoCacheRecoveryFacts::refused);
            if(!found.completed())artifacts.configurationNotes().requireRecoveryRetirable(session,original);
            return null;
        });
    }
    private List<RequiredNoteContext> inspectNotes(ClientSession session,String pipelineId) {
        var auth=bindings.cachePreparationForTransaction(session).orElseThrow(MongoCacheRecoveryFacts::refused).authScope();requireTarget(session,auth);
        var row=SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("_id",pipelineId)).first();
        var summary=MongoCacheRecoveryFacts.read(row).orElseThrow(MongoCacheRecoveryFacts::refused);
        var mapping=MongoPipelineStateMappings.decode(pipelineId,row);
        if(mapping.phase()!=PipelineStateMapping.Phase.ACTIVE || mapping.operationId()==null || !matches(auth,summary.targetScope()))throw MongoCacheRecoveryFacts.refused();
        var pipeline=artifacts.getInTransaction(session,pipelineId).filter(io.tapstate.core.model.PipelineResource.class::isInstance)
                .map(io.tapstate.core.model.PipelineResource.class::cast).orElseThrow(MongoCacheRecoveryFacts::refused);
        var notes=artifacts.configurationNotes();
        for(var node:io.tapstate.core.model.PipelineConnectorNodes.of(pipeline,id -> artifacts.getInTransaction(session,id).orElseThrow(MongoCacheRecoveryFacts::refused)))
            notes.reserveAuthoring(session,RequiredNoteOwner.forNode(auth.bindingVersion(),node.node(),node.purpose(),mapping.stateGeneration()),true);
        return notes.inspectRecoveryContexts(session,summary,mapping.operationId());
    }
    @Override public FullPreparation authorizeFull(StorageRecoveryCommands.Plan running, String pipelineId) {
        if (running.phase() != StorageRecoveryCommands.Phase.RUNNING || running.manifest().selection().extent() != StorageRecoveryCommands.Extent.PIPELINE
                || !running.manifest().selection().pipelineIds().equals(Set.of(pipelineId)) || !running.manifest().affectedPipelineIds().equals(Set.of(pipelineId)))
            throw MongoCacheRecoveryFacts.refused();
        var summary = transaction(session -> {
            requirePlan(session, running);
            var row = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", pipelineId)).first();
            var found = MongoCacheRecoveryFacts.read(row)
                    .orElseThrow(MongoCacheRecoveryFacts::refused);
            if (!matches(running.originalScope(), found.targetScope())) throw MongoCacheRecoveryFacts.refused();
            if (!running.operationId().equals(row.get("operationId"))) {
                if (!found.recoveryId().equals(row.get("operationId"))
                        || MongoPipelinePreparationFacts.decode(pipelineId, row, clusterId).phase() != PipelinePreparation.Phase.UNASSIGNED
                        || row.getLong("noteWriteFence") >= Long.MAX_VALUE - 1) throw MongoCacheRecoveryFacts.refused();
                fencePlan(session, running);
                if (SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session, row,
                        new Document("$set", new Document("operationId", running.operationId())).append("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1)
                    throw MongoCacheRecoveryFacts.refused();
            }
            return found;
        });
        return new Permission(this, running, summary);
    }
    static final class Permission implements FullPreparation {
        private final MongoCacheRecoveryStore owner;
        private final StorageRecoveryCommands.Plan plan;
        private final PipelineCacheRecovery recovery;
        private Permission(MongoCacheRecoveryStore owner, StorageRecoveryCommands.Plan plan, PipelineCacheRecovery recovery) {
            this.owner = owner; this.plan = plan; this.recovery = recovery;
        }
        @Override public PipelineCacheRecovery recovery() { return recovery; }
        @Override public String operationId() { return plan.operationId(); }
        @Override public void requireActive() { owner.transaction(session -> { require(session, owner.client, owner.database); return null; }); }
        void require(ClientSession session, MongoClient client, MongoDatabase database) {
            owner.active.run();
            if (owner.client != client || !owner.database.getName().equals(database.getName())) throw MongoCacheRecoveryFacts.refused();
            owner.requirePlan(session, plan);
            var mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", recovery.targetScope().pipelineId())).first();
            if (!MongoCacheRecoveryFacts.read(mapping).filter(recovery::equals).isPresent()
                    || !plan.operationId().equals(mapping.get("operationId"))
                    || MongoPipelineStateMappings.decode(recovery.targetScope().pipelineId(), mapping).phase() != PipelineStateMapping.Phase.ACTIVE)
                throw MongoCacheRecoveryFacts.refused();
        }
        void requireCacheTarget(MongoCacheDomain cache) {
            owner.transaction(session -> {
                require(session,owner.client,owner.database);
                var mapping=SystemCollections.OPERATOR_GENERATIONS.on(owner.database).find(session,new Document("_id",recovery.targetScope().pipelineId())).first();
                if(MongoPipelinePreparationFacts.decode(recovery.targetScope().pipelineId(),mapping,owner.clusterId).phase()!=PipelinePreparation.Phase.COMPLETE)
                    throw MongoCacheRecoveryFacts.refused();
                cache.requireRecoveryTarget(owner.bindings.loadForTransaction(session).orElseThrow(MongoCacheRecoveryFacts::refused));return null;
            });
        }
        void fence(ClientSession session, MongoClient client, MongoDatabase database) {
            require(session, client, database); owner.fencePlan(session, plan);
        }
    }
    private void fencePlan(ClientSession session, StorageRecoveryCommands.Plan original) {
        requirePlan(session, original);
        var root = SystemCollections.STORAGE_RECOVERY.on(database).find(session, new Document("_id", "cluster")).first();
        if (root == null || !(root.get("revision") instanceof Long revision) || revision >= Long.MAX_VALUE - 1) throw MongoCacheRecoveryFacts.refused();
        if (SystemCollections.STORAGE_RECOVERY.on(database).updateOne(session,
                new Document("_id", "cluster").append("revision", revision).append("pendingRecovery", root.get("pendingRecovery")),
                new Document("$inc", new Document("revision", 1L))).getMatchedCount() != 1) throw MongoCacheRecoveryFacts.refused();
    }
    void requirePlan(ClientSession session, StorageRecoveryCommands.Plan original) {
        requireTarget(session, original.originalScope());
        var root = SystemCollections.STORAGE_RECOVERY.on(database).find(session, new Document("_id", "cluster")).first();
        if (root == null || !MongoStorageRecoveryCommands.pending(root).filter(original::equals).isPresent()) throw MongoCacheRecoveryFacts.refused();
        new MongoStorageRecoveryCommands(client, database, bindings, artifacts, active).requireManifest(session, original.manifest());
    }
    void requireStopped(StorageRecoveryCommands.Plan running) {
        transaction(session -> {
            requireStopped(session, running); return null;
        });
    }
    void requireStopped(ClientSession session, StorageRecoveryCommands.Plan running) {
            requirePlan(session,running);
            if (running.phase()!=StorageRecoveryCommands.Phase.RUNNING || running.manifest().selection().extent()!=StorageRecoveryCommands.Extent.CLUSTER)
                throw MongoCacheRecoveryFacts.refused();
            for (String id:running.manifest().affectedPipelineIds()) {
                var row=SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("_id",id)).first();
                var summary=MongoCacheRecoveryFacts.read(row).orElseThrow(MongoCacheRecoveryFacts::refused);
                var intent=intents().loadInTransaction(session,id).orElseThrow(MongoCacheRecoveryFacts::refused);
                if (!summary.recoveryId().equals(running.operationId()) || !matches(running.originalScope(),summary.targetScope())
                        || !intent.scope().equals(summary.targetScope()) || intent.deleted() || intent.desired().targetState()!=PipelineState.STOPPED
                        || intent.desired().purgeState() || intent.desired().reassemble()
                        || MongoPipelinePreparationFacts.decode(id,row,clusterId).phase()!=PipelinePreparation.Phase.UNASSIGNED)
                    throw MongoCacheRecoveryFacts.refused();
            }
    }
    private void requireTarget(ClientSession session, CacheAuthScope target) {
        active.run();
        var current = bindings.cachePreparationForTransaction(session).orElseThrow(MongoCacheRecoveryFacts::refused);
        var setup = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
        if (target.authGeneration() < 2 || !current.authScope().equals(target) || setup == null || !"COMPLETE".equals(setup.get("phase")))
            throw MongoCacheRecoveryFacts.refused();
        var binding = bindings.loadForTransaction(session).orElseThrow(MongoCacheRecoveryFacts::refused);
        if (MongoStorageRecoveryStore.authentication(session, database, binding).pending()) throw MongoCacheRecoveryFacts.refused();
    }
    private MongoPipelineIntentStore intents() { return new MongoPipelineIntentStore(client, database, clusterId, bindings); }
    private static boolean matches(CacheAuthScope cache, PipelineIntentScope scope) {
        return cache.clusterId().equals(scope.clusterId()) && cache.bindingVersion() == scope.bindingVersion() && cache.cacheIncarnation().equals(scope.cacheIncarnation());
    }
    private <T> T transaction(Function<ClientSession,T> body) {
        return StoreIo.call(() -> { try (var session = client.startSession()) {
            return session.withTransaction(() -> { active.run(); var result = body.apply(session); active.run(); return result; }, TX);
        } });
    }
}
