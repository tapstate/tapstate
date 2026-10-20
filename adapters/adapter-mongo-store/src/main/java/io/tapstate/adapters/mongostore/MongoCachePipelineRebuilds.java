package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.*;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Existing-history replacement on the original CACHE client, separate from CONFIG publication. */
final class MongoCachePipelineRebuilds implements PipelineCacheRebuilds {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final List<SystemCollections> COLD = List.of(SystemCollections.CACHE_NEST_STATE, SystemCollections.CACHE_JOIN_FACTS,
            SystemCollections.CACHE_JOIN_DIMENSIONS, SystemCollections.CACHE_JOIN_INDEXES, SystemCollections.CACHE_JOIN_WRITERS);
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope business;
    private final Runnable requireActive;
    private final Duration retention;
    private final OperatorDatabaseBinding operatorDatabases;

    MongoCachePipelineRebuilds(MongoClient client, MongoDatabase database, CacheBusinessScope business, Runnable active, Duration retention) {
        this(client,database,business,active,retention,null);
    }
    MongoCachePipelineRebuilds(MongoClient client, MongoDatabase database, CacheBusinessScope business, Runnable active, Duration retention,
                              OperatorDatabaseBinding operatorDatabases) {
        this.operatorDatabases=operatorDatabases;
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.business = Objects.requireNonNull(business, "business"); this.requireActive = Objects.requireNonNull(active, "requireActive");
        this.retention = MongoCacheMetadataStructure.requireRetention(retention);
    }

    @Override public long reclaim(PipelineRebuildReceipt candidate) {
        requireVerifiedReceipt(candidate);return reclaim(candidate,candidate.issuerNode());
    }
    @Override public long reclaim(PipelineRebuildReceipt candidate,WorkloadClaimFence currentNode) {
        requireVerifiedReceipt(candidate);Objects.requireNonNull(currentNode,"currentNode"); var receipt=(Receipt)candidate;
        if (receipt.owner.client!=client || !receipt.owner.database.getName().equals(database.getName())
                || !receipt.owner.business.equals(business)) throw fenced();
        var original=receipt.preparation();
        var handle=StoreIo.call(() -> {
            active(original);try(var session=client.startSession()) {return session.withTransaction(() -> {
                active(original);var inventory=MongoOperatorInventory.requireRetired(session,database,original);
                return MongoOperatorInventory.frozen(session,database,original.request().originalScope(),inventory);
            },TRANSACTION);}
        });
        return new MongoRetiredPipelinePayloads(client,database,business,() -> active(original),operatorDatabases).reclaim(handle,currentNode);
    }

    @Override public PipelineRebuildReceipt prepare(PipelineRebuildPreparation original, WorkloadClaimFence currentNode, Duration ttl, Instant touchTime) {
        // Verify the adapter-issued original before calling any accessor supplied across the boundary.
        MongoPipelineStateRebuilds.requireOriginalPending(original);
        var request = original.request(); var old = request.originalScope(); var target = request.targetScope();
        if (!business.equals(new CacheBusinessScope(old.clusterId(), old.bindingVersion(), old.cacheIncarnation()))) throw MongoCacheBusinessIdentity.businessFenced();
        Objects.requireNonNull(currentNode, "currentNode"); Objects.requireNonNull(ttl, "ttl"); Objects.requireNonNull(touchTime, "touchTime");
        if (ttl.isZero() || ttl.isNegative() || ttl.toMillis() < 1) throw new IllegalArgumentException("rebuild ttl must be at least one millisecond");
        return StoreIo.call(() -> {
            try {
                active(original);
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> {
                        active(original);
                        var identity = new MongoCacheBusinessIdentity(database, business); identity.requireAdmissionState(session);
                        var access = new MongoCachePipelineAccess(client, database, old, null, () -> active(original), retention);
                        access.verifyStructures();
                        var structure = new MongoCacheMetadataStructure(database, retention);
                        structure.verifyDirect(); structure.verifySrs();
                        for (var table : COLD) structure.verifyFacet(table);
                        for (var table : List.of(SystemCollections.CACHE_NEST_SHAPES, SystemCollections.CACHE_OPERATOR_CLEANUP,
                                SystemCollections.CACHE_DERIVED_SCHEMAS, SystemCollections.CACHE_PIPELINE_RATE_HISTORY, SystemCollections.CACHE_SINK_PREPARATIONS,
                SystemCollections.CACHE_SNAPSHOT_ORDERS,SystemCollections.CACHE_SNAPSHOT_COUNTS)) structure.verifyFacet(table);
                        var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
                        var filter = new Document("_id", MongoWorkloadClaimStore.id(request.originalClaim().key()));
                        Document row = claims.find(session, filter).first();
                        if (row == null) throw rebuild(old);
                        WorkloadClaim claim = new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(row);
                        var scope = storedScope(row, old);
                        if (scope.equals(target)) {
                            var receipt = retained(session, original, row, claim);
                            MongoCachePipelinePreparations.proveNode(session, database, business, currentNode);
                            active(original); return receipt;
                        }
                        if (!scope.equals(old) || !request.originalClaim().equals(WorkloadClaimFence.from(claim))) throw fenced();
                        if (MongoCachePipelinePreparations.phase(row, old) != PipelineCachePreparations.Phase.PREPARED
                                || !original.previousPreparationId().equals(row.get("pipelinePreparation", Document.class).getString("preparationId"))) throw rebuild(old);
                        var state = access.state(session);
                        requirePriorMarker(row, old, original.previousPreparationId(), state);
                        if (state.getLong("epoch") != request.originalStateEpoch() || state.getLong("epoch") >= Long.MAX_VALUE - 1
                                || state.getLong("runtimeWriteFence") >= Long.MAX_VALUE - 1 || claim.claimGeneration() >= Long.MAX_VALUE - 1
                                || claim.executionGeneration() >= Long.MAX_VALUE - 1 || claim.topologyRevision() >= Long.MAX_VALUE - 1) throw rebuild(old);
                        MongoPipelineSourcePreparations.retained(session, database, old);
                        var consumers = new MongoSrsConsumerStore(client, database, business, () -> active(original), structure::verifySrs);
                        consumers.requireDetachedPipeline(session, old);
                        requireTargetEmpty(session, target);
                        identity.admitClaim(session);
                        var now = MongoCachePipelinePreparations.proveNode(session, database, business, currentNode);
                        var nextClaim = new WorkloadClaimFence(claim.key(), currentNode.owner(), claim.claimGeneration() + 1,
                                claim.executionGeneration(), claim.topologyRevision());
                        long epoch = state.getLong("epoch") + 1, writeFence = state.getLong("runtimeWriteFence") + 1;
                        var receipt = new Receipt(this, original, currentNode, nextClaim, epoch, writeFence);
                        MongoOperatorInventory.retire(session, database, original);
                        Document expected = new Document(filter).append("pipelineScope", MongoPipelinePreparationFacts.scope(old))
                                .append("pipelinePreparation", row.get("pipelinePreparation"));
                        for (String field : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "executionGeneration", "topologyRevision")) expected.append(field, row.get(field));
                        var values = new Document("ownerNodeId", nextClaim.owner().nodeId()).append("ownerBootId", nextClaim.owner().bootId())
                                .append("claimGeneration", nextClaim.claimGeneration()).append("leaseUntil", java.util.Date.from(now.toInstant().plusMillis(ttl.toMillis())))
                                .append("pipelineScope", MongoPipelinePreparationFacts.scope(target))
                                .append("pipelinePreparation", MongoCachePipelinePreparations.marker(original.preparationId(), PipelineCachePreparations.Phase.PREPARED))
                                .append("pipelineSources", MongoPipelineSourcePreparations.encode(request.inputs()))
                                .append("sinkPreparations",MongoSinkPreparationStore.initial(original.preparationId())).append("pipelineRebuild", marker(receipt));
                        if (claims.updateOne(session, expected, new Document("$set", values)).getMatchedCount() != 1) throw fenced();
                        if (SystemCollections.CACHE_PIPELINE_STATE.on(database).updateOne(session,
                                new Document("_id", old.pipelineId()).append("scope", state.get("scope")).append("epoch", state.getLong("epoch"))
                                        .append("runtimeWriteFence", state.getLong("runtimeWriteFence")),
                                new Document("$set", new Document("scope", MongoPipelinePreparationFacts.scope(target)).append("stateJson", StateJson.of(PipelineState.STOPPED))
                                        .append("epoch", epoch).append("runtimeWriteFence", writeFence).append("touchMillis", touchTime.toEpochMilli()))).getMatchedCount() != 1) throw fenced();
                        discardObservation(session, old);
                        MongoOperatorInventory.prepareInitial(session, database, target, original.preparationId());
                        MongoPipelineSourcePreparations.seedInitial(session, database, target, request.inputs());
                        MongoSnapshotStateStore.seedInitial(session,database,target);
                        active(original); return receipt;
                    }, TRANSACTION);
                }
            } catch (RuntimeException failure) { active(original); throw failure; }
        });
    }

    private Receipt retained(ClientSession session, PipelineRebuildPreparation original, Document row, WorkloadClaim claim) {
        var target = original.request().targetScope();
        try {
            var marker = row.get("pipelineRebuild", Document.class);
            if (marker == null || !(marker.get("checkpointEpoch") instanceof Long epoch) || !(marker.get("runtimeWriteFence") instanceof Long writeFence)) throw rebuild(target);
            var issuer = WorkloadClaimDocuments.readNode(marker.get("issuerNode", Document.class));
            var next = WorkloadClaimDocuments.readPipeline(marker.get("claim", Document.class));
            var receipt = new Receipt(this, original, issuer, next, epoch, writeFence);
            if (!marker(receipt).equals(marker) || !next.equals(WorkloadClaimFence.from(claim))
                    || next.claimGeneration() != original.request().originalClaim().claimGeneration() + 1
                    || next.executionGeneration() != original.request().originalClaim().executionGeneration()
                    || next.topologyRevision() != original.request().originalClaim().topologyRevision()
                    || epoch != original.request().originalStateEpoch() + 1 || !next.owner().equals(issuer.owner())
                    || !business.clusterId().equals(issuer.key().clusterId())) throw rebuild(target);
            if (MongoCachePipelinePreparations.phase(row, target) != PipelineCachePreparations.Phase.PREPARED
                    || !original.preparationId().equals(row.get("pipelinePreparation", Document.class).getString("preparationId"))
                    || !original.request().inputs().equals(MongoPipelineSourcePreparations.retained(session, database, target))) throw rebuild(target);
            MongoSinkPreparationStore.requireInitial(row,original.preparationId(),target);
            var state = new MongoCachePipelineAccess(client, database, target, null, () -> active(original), retention).state(session);
            if (state.getLong("epoch").longValue() != epoch.longValue() || state.getLong("runtimeWriteFence").longValue() != writeFence.longValue()
                    || !StateJson.of(PipelineState.STOPPED).equals(state.get("stateJson"))) throw rebuild(target);
            requireEmptyPayloads(session, target);
            new MongoSrsConsumerStore(client, database, business, () -> active(original), new MongoCacheMetadataStructure(database, retention)::verifySrs)
                    .requireDetachedPipeline(session, original.request().originalScope());
            MongoOperatorInventory.requireInitial(session, database, target, original.preparationId());
            MongoOperatorInventory.requireRetired(session, database, original);
            MongoPipelineSourcePreparations.verifyInitial(session, database, target, original.request().inputs());
            MongoSnapshotStateStore.verifyInitial(session,database,target);
            if (SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database).find(session, new Document("_id", target.pipelineId())).first() != null) throw rebuild(target);
            return receipt;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw rebuild(target); }
    }

    private void requireTargetEmpty(ClientSession session, PipelineIntentScope target) {
        var encoded = MongoPipelinePreparationFacts.scope(target);
        requireEmptyPayloads(session, target);
        absent(session, SystemCollections.CACHE_OPERATOR_CLEANUP, new Document("_id", encoded), target);
        MongoPipelineSourcePreparations.requireEmpty(session, database, target);
        absent(session,SystemCollections.CACHE_SNAPSHOT_ORDERS,new Document("_id",encoded),target);
        absent(session,SystemCollections.CACHE_SNAPSHOT_COUNTS,new Document("_id.scope",encoded),target);
    }
    private void requireEmptyPayloads(ClientSession session, PipelineIntentScope target) {
        var encoded = MongoPipelinePreparationFacts.scope(target); var structure = new MongoCacheMetadataStructure(database, retention);
        for (var table : COLD) { structure.verifyFacet(table); absent(session, table, new Document("_id.placement.scope", encoded), target); }
        for (var table : List.of(SystemCollections.CACHE_NEST_SHAPES, SystemCollections.CACHE_DERIVED_SCHEMAS, SystemCollections.CACHE_PIPELINE_RATE_HISTORY, SystemCollections.CACHE_SINK_PREPARATIONS,
                SystemCollections.CACHE_SNAPSHOT_ORDERS,SystemCollections.CACHE_SNAPSHOT_COUNTS)) structure.verifyFacet(table);
        MongoSinkPreparationStore.requireEmpty(session,database,target);
        absent(session, SystemCollections.CACHE_NEST_SHAPES, new Document("_id.scope", encoded), target);
        absent(session, SystemCollections.CACHE_DERIVED_SCHEMAS, new Document("scope", encoded), target);
        absent(session, SystemCollections.CACHE_PIPELINE_RATE_HISTORY, new Document("scope", encoded), target);
    }
    private void absent(ClientSession session, SystemCollections table, Document filter, PipelineIntentScope target) {
        if (table.on(database).find(session, filter).projection(new Document("_id", 1)).first() != null) throw rebuild(target);
    }
    private void discardObservation(ClientSession session, PipelineIntentScope old) {
        var table = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database);
        var row = table.find(session, new Document("_id", old.pipelineId())).first();
        if (row == null) return;
        // This diagnostic projection is replaceable; its complete physical scope is still mandatory.
        if (!(row.get("scope") instanceof Document scope) || !MongoPipelinePreparationFacts.scope(old).equals(scope)) throw rebuild(old);
        table.deleteOne(session, new Document("_id", old.pipelineId()).append("scope", scope));
    }
    private static PipelineIntentScope storedScope(Document row, PipelineIntentScope expected) {
        try { return MongoPipelinePreparationFacts.scope(row.get("pipelineScope", Document.class)); }
        catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw rebuild(expected); }
    }
    private static void requirePriorMarker(Document row, PipelineIntentScope scope, String preparationId, Document state) {
        if (!row.containsKey("pipelineRebuild")) return;
        try {
            var marker = row.get("pipelineRebuild", Document.class);
            if (marker == null || !marker.keySet().equals(Set.of("formatVersion", "operationId", "confirmationHash", "originalScope", "targetScope",
                    "preparationId", "previousPreparationId", "issuerNode", "claim", "checkpointEpoch", "runtimeWriteFence"))
                    || !Integer.valueOf(1).equals(marker.get("formatVersion"))
                    || !(marker.get("operationId") instanceof String operation) || operation.isBlank()
                    || !(marker.get("confirmationHash") instanceof String hash) || !hash.matches("[0-9a-f]{64}")
                    || !(marker.get("checkpointEpoch") instanceof Long epoch) || epoch < 1 || epoch > state.getLong("epoch")
                    || !(marker.get("runtimeWriteFence") instanceof Long writeFence) || writeFence < 2 || writeFence > state.getLong("runtimeWriteFence")
                    || !preparationId.equals(marker.get("preparationId"))) throw unreadable(scope);
            for (String field : List.of("preparationId", "previousPreparationId")) {
                String id = marker.getString(field);
                if (!java.util.UUID.fromString(id).toString().equals(id)) throw unreadable(scope);
            }
            var prior = MongoPipelinePreparationFacts.scope(marker.get("originalScope", Document.class));
            var target = MongoPipelinePreparationFacts.scope(marker.get("targetScope", Document.class));
            if (!target.equals(scope) || !prior.pipelineId().equals(target.pipelineId()) || prior.stateGeneration() + 1 != target.stateGeneration()
                    || !new CacheBusinessScope(prior.clusterId(), prior.bindingVersion(), prior.cacheIncarnation())
                            .equals(new CacheBusinessScope(target.clusterId(), target.bindingVersion(), target.cacheIncarnation()))) throw unreadable(scope);
            var issuer = WorkloadClaimDocuments.readNode(marker.get("issuerNode", Document.class));
            var claim = WorkloadClaimDocuments.readPipeline(marker.get("claim", Document.class));
            if (!issuer.key().clusterId().equals(scope.clusterId()) || !claim.key().clusterId().equals(scope.clusterId())
                    || !claim.key().resourceId().equals(scope.pipelineId()) || !claim.owner().equals(issuer.owner()) || claim.claimGeneration() < 2
                    || claim.claimGeneration() > row.getLong("claimGeneration") || claim.executionGeneration() > row.getLong("executionGeneration")) throw unreadable(scope);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(scope); }
    }
    private static TapstateException unreadable(PipelineIntentScope scope) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, java.util.Map.of("id", scope.pipelineId(), "field", "pipeline-rebuild"), null);
    }
    private static Document marker(Receipt receipt) {
        var original = receipt.preparation();
        return new Document("formatVersion", 1).append("operationId", original.request().operationId()).append("confirmationHash", original.confirmationHash())
                .append("originalScope", MongoPipelinePreparationFacts.scope(original.request().originalScope()))
                .append("targetScope", MongoPipelinePreparationFacts.scope(original.request().targetScope())).append("preparationId", original.preparationId())
                .append("previousPreparationId", original.previousPreparationId()).append("issuerNode", WorkloadClaimDocuments.stored(receipt.issuerNode()))
                .append("claim", WorkloadClaimDocuments.stored(receipt.claim())).append("checkpointEpoch", receipt.checkpointEpoch()).append("runtimeWriteFence", receipt.runtimeWriteFence());
    }
    private record Receipt(MongoCachePipelineRebuilds owner, PipelineRebuildPreparation preparation, WorkloadClaimFence issuerNode, WorkloadClaimFence claim,
            long checkpointEpoch, long runtimeWriteFence) implements PipelineRebuildReceipt {}
    static void requireVerifiedReceipt(PipelineRebuildReceipt receipt) {
        if (!(receipt instanceof Receipt)) throw fenced();
    }
    /** Cross-domain callers perform this CACHE read before entering their CONFIG transaction. */
    static void requireCurrentReceipt(PipelineRebuildReceipt candidate) {
        requireVerifiedReceipt(candidate);
        var receipt = (Receipt) candidate; var owner = receipt.owner();
        StoreIo.run(() -> {
            owner.active(receipt.preparation());
            try (var session = owner.client.startSession()) {
                session.withTransaction(() -> {
                    owner.active(receipt.preparation());
                    new MongoCacheBusinessIdentity(owner.database, owner.business).requireAdmissionState(session);
                    var access = new MongoCachePipelineAccess(owner.client, owner.database, receipt.preparation().request().targetScope(), null,
                            () -> owner.active(receipt.preparation()), owner.retention);
                    access.verifyStructures();
                    new MongoCacheMetadataStructure(owner.database, owner.retention).verifyDirect();
                    var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(owner.database);
                    var row = claims.find(session, new Document("_id", MongoWorkloadClaimStore.id(receipt.claim().key()))).first();
                    if (row == null) throw rebuild(receipt.preparation().request().targetScope());
                    var current = new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(row);
                    var verified = owner.retained(session, receipt.preparation(), row, current);
                    if (!marker(receipt).equals(marker(verified))) throw fenced();
                    owner.active(receipt.preparation()); return null;
                }, TRANSACTION);
            }
        });
    }

    private void active(PipelineRebuildPreparation original) { requireActive.run(); MongoPipelineStateRebuilds.requireOriginalActive(original); }
    private static TapstateException fenced() { return MongoCachePipelineAccess.claimFenced(); }
    private static TapstateException rebuild(PipelineIntentScope scope) { return MongoCachePipelinePreparations.rebuild(scope); }
}
