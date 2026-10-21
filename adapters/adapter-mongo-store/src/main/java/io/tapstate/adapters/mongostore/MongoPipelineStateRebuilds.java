package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/** One fixed CONFIG confirmation; no CACHE client, lease IO or initial-creation capability is used. */
final class MongoPipelineStateRebuilds implements PipelineStateRebuilds {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Set<String> ANNEX = Set.of("formatVersion", "operationId", "originalScope", "targetScope", "preparationId",
            "previousPreparationId", "intentRevision", "originalClaim", "originalStateEpoch", "confirmationHash");
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final MongoPipelineIntentStore intents;
    private final MongoPipelineStatePreparations preparations;
    private final Runnable requireActive;

    MongoPipelineStateRebuilds(MongoClient client, MongoDatabase database, String clusterId, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId"); this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (!bindings.belongsTo(client, database.getName()) || !artifacts.belongsTo(client, database.getName()))
            throw new IllegalArgumentException("rebuild stores require one original CONFIG client and database");
        intents = new MongoPipelineIntentStore(client, database, clusterId, bindings);
        preparations = new MongoPipelineStatePreparations(client, database, clusterId, bindings, artifacts, requireActive);
    }
    @Override public PipelineStateRebuilds guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoPipelineStateRebuilds(client, database, clusterId, bindings, artifacts, () -> { requireActive.run(); additional.run(); });
    }
    @Override public PipelineRebuildPreparation begin(PipelineRebuildRequest original) {
        Objects.requireNonNull(original, "original");
        String hash = confirmationHash(original), id = UUID.randomUUID().toString();
        return transaction(session -> {
            Document mapping = requireOriginal(session, original);
            var state = MongoPipelineStateMappings.decode(original.originalScope().pipelineId(), mapping);
            if (state.phase() == PipelineStateMapping.Phase.REBUILDING) return retained(original, hash, mapping);
            if (state.phase() != PipelineStateMapping.Phase.ACTIVE) throw fenced();
            var previous = MongoPipelinePreparationFacts.decode(state.pipelineId(), mapping, clusterId);
            if (previous.phase() != PipelinePreparation.Phase.COMPLETE || !original.originalScope().equals(previous.scope())) throw fenced();
            requireCompletedPrior(mapping, previous);
            var pending = new Pending(this, original, id, previous.preparationId(), hash);
            long fence = mapping.getLong("noteWriteFence");
            if (fence >= Long.MAX_VALUE - 2 || ConfigurationVersion.read(session, SystemCollections.CONFIGURATION_VERSION.on(database)) >= Long.MAX_VALUE - 2) throw fenced();
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            if (SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,
                    new Document("_id", state.pipelineId()).append("stateGeneration", state.stateGeneration()).append("phase", "ACTIVE")
                            .append("noteWriteFence", fence).append("cachePreparation", mapping.get("cachePreparation")),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", original.operationId())
                            .append("stateRebuild", encode(pending))).append("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1)
                throw new IllegalStateException("Pipeline mapping changed inside one CONFIG snapshot");
            return pending;
        });
    }
    @Override public java.util.Optional<PipelineRebuildPreparation> pending(String pipelineId,PipelinePreparationInputs inputs,Map<String,String> dependencyHashes) {
        Objects.requireNonNull(pipelineId,"pipelineId");Objects.requireNonNull(inputs,"inputs");var captured=Map.copyOf(dependencyHashes);
        return transaction(session -> {
            var mapping=SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("_id",pipelineId)).first();
            if(mapping==null)return java.util.Optional.empty();var state=MongoPipelineStateMappings.decode(pipelineId,mapping);
            if(state.phase()!=PipelineStateMapping.Phase.REBUILDING)return java.util.Optional.empty();
            try {
                var annex=mapping.get("stateRebuild",Document.class);if(annex==null || !annex.keySet().equals(ANNEX))throw unreadable();
                var original=intents.loadInTransaction(session,pipelineId).orElseThrow(MongoPipelineStateRebuilds::fenced);
                var claim=annex.get("originalClaim",Document.class);
                if(claim==null || !claim.keySet().equals(Set.of("clusterId","pipelineId","nodeId","bootId","claimGeneration","executionGeneration","topologyRevision"))
                        || !(claim.get("claimGeneration") instanceof Long generation) || !(claim.get("executionGeneration") instanceof Long execution)
                        || !(claim.get("topologyRevision") instanceof Long topology) || !(annex.get("originalStateEpoch") instanceof Long epoch))throw unreadable();
                var fence=new WorkloadClaimFence(new WorkloadClaimKey(claim.getString("clusterId"),WorkloadClaimType.PIPELINE_ACTUATION,claim.getString("pipelineId")),
                        new WorkloadOwner(claim.getString("nodeId"),claim.getString("bootId")),generation,execution,topology);
                var request=new PipelineRebuildRequest(annex.getString("operationId"),original,fence,epoch,inputs,captured);
                var retained=retained(request,confirmationHash(request),requireOriginal(session,request));
                return java.util.Optional.of(retained);
            }catch(IllegalArgumentException|NullPointerException|ClassCastException invalid){throw unreadable();}
        });
    }

    @Override public void requirePending(PipelineRebuildPreparation original) {
        if (!(original instanceof Pending pending) || pending.owner.client != client
                || !pending.owner.database.getName().equals(database.getName()) || !pending.owner.clusterId.equals(clusterId)) throw fenced();
        pending.owner.requireActive.run();
        transaction(session -> {
            var retained = retained(pending.request, pending.hash, requireOriginal(session, pending.request));
            if (!encode(retained).equals(encode(pending))) throw fenced();
            return null;
        });
    }

    @Override public PipelinePreparation complete(PipelineRebuildReceipt receipt) {
        MongoCachePipelineRebuilds.requireVerifiedReceipt(receipt);
        if (!(receipt.preparation() instanceof Pending pending)) throw fenced();
        requireHandle(pending);
        var already = completed(pending.request);
        if (already.isPresent()) return already.orElseThrow();
        // No CACHE IO belongs in the CONFIG transaction or its retry callback.
        MongoCachePipelineRebuilds.requireCurrentReceipt(receipt);
        return transaction(session -> {
            var replay = completedInTransaction(session, pending.request);
            if (replay.isPresent()) return replay.orElseThrow();
            var mapping = requireOriginal(session, pending.request);
            var held = retained(pending.request, pending.hash, mapping);
            if (!encode(pending).equals(encode(held))) throw fenced();
            var target = pending.request.targetScope();
            var result = new PipelinePreparation(target.pipelineId(), target.stateGeneration(), PipelinePreparation.Phase.COMPLETE,
                    target, pending.id, receipt.issuerNode());
            long writeFence = mapping.getLong("noteWriteFence"); if (writeFence >= Long.MAX_VALUE - 1) throw fenced();
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            long intentRevision = intents.completeRebuild(session, pending.request.intent(), target);
            var annex = encode(pending).append("result", new Document("phase", "COMPLETE").append("checkpointEpoch", receipt.checkpointEpoch()).append("intentRevision", intentRevision));
            if (SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,
                    new Document("_id", target.pipelineId()).append("stateGeneration", pending.request.originalScope().stateGeneration())
                            .append("phase", "REBUILDING").append("operationId", pending.request.operationId()).append("stateRebuild", encode(pending))
                            .append("noteWriteFence", writeFence),
                    new Document("$set", new Document("stateGeneration", target.stateGeneration()).append("phase", "ACTIVE")
                            .append("cachePreparation", MongoPipelinePreparationFacts.encode(result)).append("stateRebuild", annex))
                            .append("$unset", new Document("cacheRecovery", ""))
                            .append("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1)
                throw new IllegalStateException("original rebuild changed within one CONFIG snapshot");
            return result;
        });
    }

    @Override public java.util.Optional<PipelinePreparation> completed(PipelineRebuildRequest original) {
        Objects.requireNonNull(original, "original");
        return transaction(session -> completedInTransaction(session, original));
    }

    private java.util.Optional<PipelinePreparation> completedInTransaction(ClientSession session, PipelineRebuildRequest original) {
        var business = bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineStateRebuilds::fenced).businessScope();
        var scope = original.originalScope();
        if (!business.equals(new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation())) || !clusterId.equals(scope.clusterId())) throw fenced();
        var mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", scope.pipelineId())).first();
        var state = MongoPipelineStateMappings.decode(scope.pipelineId(), mapping);
        if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() != original.targetScope().stateGeneration()
                || !original.operationId().equals(state.operationId())) return java.util.Optional.empty();
        var prepared = MongoPipelinePreparationFacts.decode(scope.pipelineId(), mapping, clusterId);
        if (!mapping.containsKey("stateRebuild")) throw unreadable();
        requireCompletedPrior(mapping, prepared);
        var annex = mapping.get("stateRebuild", Document.class);
        var pending = new Pending(this, original, checkedId(annex.getString("preparationId")), checkedId(annex.getString("previousPreparationId")), confirmationHash(original));
        var result = new Document("phase", "COMPLETE").append("checkpointEpoch", original.originalStateEpoch() + 1).append("intentRevision", original.intent().intentRevision() + 1);
        if (!encode(pending).append("result", result).equals(annex)) throw fenced();
        return java.util.Optional.of(prepared);
    }

    private void requireHandle(Pending pending) {
        if (pending.owner.client != client || !pending.owner.database.getName().equals(database.getName()) || !pending.owner.clusterId.equals(clusterId)) throw fenced();
        pending.owner.requireActive.run();
    }

    static void requireCompletedPrior(Document mapping, PipelinePreparation prepared) {
        if (!mapping.containsKey("stateRebuild")) return;
        try {
            var annex = mapping.get("stateRebuild", Document.class); var fields = new java.util.HashSet<>(ANNEX); fields.add("result");
            if (annex == null || !fields.equals(annex.keySet()) || !Integer.valueOf(1).equals(annex.get("formatVersion"))
                    || !prepared.preparationId().equals(checkedId(annex.getString("preparationId")))
                    || !prepared.scope().equals(MongoPipelinePreparationFacts.scope(annex.get("targetScope", Document.class)))
                    || !mapping.getString("operationId").equals(annex.getString("operationId"))
                    || !(annex.get("confirmationHash") instanceof String hash) || !hash.matches("[0-9a-f]{64}")
                    || !(annex.get("intentRevision") instanceof Long revision) || revision < 1 || revision >= Long.MAX_VALUE - 1
                    || !(annex.get("originalStateEpoch") instanceof Long epoch) || epoch < 0 || epoch >= Long.MAX_VALUE - 1) throw unreadable();
            checkedId(annex.getString("previousPreparationId"));
            var old = MongoPipelinePreparationFacts.scope(annex.get("originalScope", Document.class)); var target = prepared.scope();
            if (!new CacheBusinessScope(old.clusterId(), old.bindingVersion(), old.cacheIncarnation()).equals(new CacheBusinessScope(target.clusterId(), target.bindingVersion(), target.cacheIncarnation()))
                    || !old.pipelineId().equals(target.pipelineId()) || old.stateGeneration() + 1 != target.stateGeneration()) throw unreadable();
            var prior = annex.get("originalClaim", Document.class);
            if (prior == null || !prior.keySet().equals(Set.of("clusterId", "pipelineId", "nodeId", "bootId", "claimGeneration", "executionGeneration", "topologyRevision"))
                    || !old.clusterId().equals(prior.get("clusterId")) || !old.pipelineId().equals(prior.get("pipelineId"))
                    || !(prior.get("claimGeneration") instanceof Long claim) || claim < 1
                    || !(prior.get("executionGeneration") instanceof Long execution) || execution < 0
                    || !(prior.get("topologyRevision") instanceof Long topology) || topology < 0) throw unreadable();
            new WorkloadOwner(prior.getString("nodeId"), prior.getString("bootId"));
            var result = new Document("phase", "COMPLETE").append("checkpointEpoch", epoch + 1).append("intentRevision", revision + 1);
            if (!result.equals(annex.get("result")) || prepared.phase() != PipelinePreparation.Phase.COMPLETE) throw unreadable();
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }

    /** Later CACHE transitions call back through this original adapter, never an arbitrary port implementation. */
    static void requireOriginalPending(PipelineRebuildPreparation original) {
        if (!(original instanceof Pending pending)) throw fenced();
        pending.owner.requirePending(pending);
    }

    static void requireOriginalActive(PipelineRebuildPreparation original) {
        if (!(original instanceof Pending pending)) throw fenced();
        pending.owner.requireActive.run();
    }

    private Document requireOriginal(ClientSession session, PipelineRebuildRequest original) {
        var scope = original.originalScope();
        var preparation = bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineStateRebuilds::fenced);
        var business = preparation.businessScope();
        if (!clusterId.equals(scope.clusterId()) || !business.clusterId().equals(scope.clusterId())
                || business.bindingVersion() != scope.bindingVersion() || !business.cacheIncarnation().equals(scope.cacheIncarnation())) throw fenced();
        var current = intents.loadInTransaction(session, scope.pipelineId()).orElseThrow(MongoPipelineStateRebuilds::fenced);
        if (!original.intent().equals(current) || !original.intent().desired().revision().equals(original.inputs().pipelineHash())) throw fenced();
        if (original.intent().intentRevision() >= Long.MAX_VALUE - 1 || original.originalClaim().claimGeneration() >= Long.MAX_VALUE - 1
                || original.originalClaim().executionGeneration() >= Long.MAX_VALUE - 1
                || original.originalClaim().topologyRevision() >= Long.MAX_VALUE - 1) throw fenced();
        preparations.verifyInputs(session, original.targetScope(), original.inputs());
        verifyDependencies(session, original);
        var mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", scope.pipelineId())).first();
        var state = MongoPipelineStateMappings.decode(scope.pipelineId(), mapping);
        if (state.stateGeneration() != scope.stateGeneration()) throw fenced();
        return mapping;
    }
    private void verifyDependencies(ClientSession session, PipelineRebuildRequest original) {
        var root = artifacts.getInTransaction(session, original.originalScope().pipelineId()).orElseThrow(MongoPipelineStateRebuilds::fenced);
        var known = new java.util.HashMap<String, io.tapstate.core.model.Resource>(); known.put(root.id(), root);
        var queue = new java.util.ArrayDeque<io.tapstate.core.model.Resource>(); queue.add(root);
        var visited = new java.util.HashSet<String>(); var confirmed = new java.util.HashSet<String>();
        while (!queue.isEmpty()) {
            var resource = queue.remove(); if (!visited.add(resource.id())) continue;
            for (var edge : io.tapstate.core.dsl.ReferenceGraph.declaredReferences(resource)) {
                var target = known.get(edge.id());
                if (target == null) {
                    target = artifacts.getInTransaction(session, edge.id()).orElseThrow(MongoPipelineStateRebuilds::fenced);
                    known.put(target.id(), target); queue.add(target);
                }
                if (!edge.kind().equals(target.kind())) throw fenced();
                String actual = CanonicalHash.of(target), capture = original.inputs().sourceHashes().get(edge.id());
                String dependency = original.dependencyHashes().get(edge.id());
                if (capture == null ? !actual.equals(dependency) : !actual.equals(capture)) throw fenced();
                if (dependency != null) {
                    if (!actual.equals(dependency)) throw fenced();
                    confirmed.add(edge.id());
                }
            }
        }
        if (!confirmed.equals(original.dependencyHashes().keySet())) throw fenced();
    }

    private Pending retained(PipelineRebuildRequest original, String hash, Document mapping) {
        var state = MongoPipelineStateMappings.decode(original.originalScope().pipelineId(), mapping);
        if (state.phase() != PipelineStateMapping.Phase.REBUILDING || !original.operationId().equals(state.operationId())) throw fenced();
        try {
            if (!(mapping.get("stateRebuild") instanceof Document annex) || !annex.keySet().equals(ANNEX)
                    || !Integer.valueOf(1).equals(annex.get("formatVersion"))
                    || !(annex.get("intentRevision") instanceof Long) || !(annex.get("originalStateEpoch") instanceof Long)) throw unreadable();
            var pending = new Pending(this, original, checkedId(annex.getString("preparationId")), checkedId(annex.getString("previousPreparationId")), hash);
            if (!encode(pending).equals(annex)) throw fenced();
            var old = MongoPipelinePreparationFacts.decode(state.pipelineId(), mapping, clusterId);
            if (old.phase() != PipelinePreparation.Phase.COMPLETE || !old.scope().equals(original.originalScope())
                    || !old.preparationId().equals(pending.previous)) throw unreadable();
            return pending;
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) { throw unreadable(); }
    }
    private static String checkedId(String id) {
        if (!UUID.fromString(id).toString().equals(id)) throw new IllegalArgumentException("invalid rebuild preparation id");
        return id;
    }
    private static Document encode(Pending value) {
        var request = value.request;
        return new Document("formatVersion", 1).append("operationId", request.operationId())
                .append("originalScope", MongoPipelinePreparationFacts.scope(request.originalScope()))
                .append("targetScope", MongoPipelinePreparationFacts.scope(request.targetScope())).append("preparationId", value.id)
                .append("previousPreparationId", value.previous).append("intentRevision", request.intent().intentRevision())
                .append("originalClaim", claim(request.originalClaim())).append("originalStateEpoch", request.originalStateEpoch())
                .append("confirmationHash", value.hash);
    }
    private static Document claim(WorkloadClaimFence claim) {
        return new Document("clusterId", claim.key().clusterId()).append("pipelineId", claim.key().resourceId())
                .append("nodeId", claim.owner().nodeId()).append("bootId", claim.owner().bootId())
                .append("claimGeneration", claim.claimGeneration()).append("executionGeneration", claim.executionGeneration())
                .append("topologyRevision", claim.topologyRevision());
    }
    private static String confirmationHash(PipelineRebuildRequest request) {
        var desired = request.intent().desired();
        return CanonicalHash.ofText(JsonWriter.write(new Document("operationId", request.operationId())
                .append("scope", MongoPipelinePreparationFacts.scope(request.originalScope())).append("intentRevision", request.intent().intentRevision())
                .append("desired", new Document("targetState", desired.targetState().name()).append("revision", desired.revision())
                        .append("purgeState", desired.purgeState()).append("assemblyRevision", desired.assemblyRevision())
                        .append("reassemble", desired.reassemble()).append("rebuiltAtStateEpoch", desired.rebuiltAtStateEpoch()))
                .append("originalClaim", claim(request.originalClaim())).append("originalStateEpoch", request.originalStateEpoch())
                .append("dependencyHashes", new java.util.TreeMap<>(request.dependencyHashes()))
                .append("targetInputs", MongoPipelineSourcePreparations.encode(request.inputs()))));
    }
    private record Pending(MongoPipelineStateRebuilds owner, PipelineRebuildRequest request, String id, String previous, String hash)
            implements PipelineRebuildPreparation {
        @Override public String preparationId() { return id; }
        @Override public String previousPreparationId() { return previous; }
        @Override public String confirmationHash() { return hash; }
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            requireActive.run();
            try (var session = client.startSession()) {
                return session.withTransaction(() -> { requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
            }
        });
    }
    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,
            Map.of("id", "pipeline-rebuild", "field", "stateRebuild"), null); }
}
