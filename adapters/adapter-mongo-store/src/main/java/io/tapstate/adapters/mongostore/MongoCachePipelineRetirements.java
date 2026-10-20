package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;
import java.util.function.Function;

/** Existing-only retirement; a known claim and its counters are never deleted or recreated. */
final class MongoCachePipelineRetirements implements PipelineCacheRetirements {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final CacheBusinessScope business;private final Runnable active;
    MongoCachePipelineRetirements(MongoClient client,MongoDatabase database,CacheBusinessScope business,Runnable active) {
        this.client=Objects.requireNonNull(client);this.database=Objects.requireNonNull(database);this.business=Objects.requireNonNull(business);this.active=Objects.requireNonNull(active);
    }
    @Override public PipelineRetirementReceipt retire(PipelineRetirementPreparation original,WorkloadClaimFence currentNode) {
        MongoPipelineRetirements.requireOriginalPending(original);return retire(original,currentNode,original.request().cache());
    }
    @Override public PipelineRetirementReceipt retire(PipelineRetirementPreparation original,WorkloadClaimFence currentNode,Optional<PipelineRetirementRequest.CacheOriginal> isolated) {
        MongoPipelineRetirements.requireOriginalPending(original);Objects.requireNonNull(isolated);requireScope(original.request().scope());Objects.requireNonNull(currentNode);
        return transaction(original,session -> {
            MongoCachePipelinePreparations.proveNode(session,database,business,currentNode);
            if(original.request().cache().isEmpty()) {
                if(original.previousPreparation().phase()!=PipelinePreparation.Phase.UNASSIGNED)throw fenced();
                if(original.predecessor().isPresent())verifyRetired(session,original.predecessor().orElseThrow());
                else requireAbsent(session,original.request().scope());
                return new Receipt(this,original,original.predecessor());
            }
            var scope=original.request().scope();var declared=original.request().cache().orElseThrow();
            var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);var key=declared.claim().key();
            var row=claims.find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();if(row==null)throw rebuild(scope);
            if(row.get("pipelineRetirement") instanceof Document saved) {
                var retired=MongoPipelineRetirementFacts.cache(saved);
                if(retired.scope().equals(scope) && retired.operationId().equals(original.request().operationId())) {
                    verifyRetired(session,retired);return new Receipt(this,original,Optional.of(retired));
                }
            }
            var request=isolated.orElseThrow(MongoCachePipelineRetirements::fenced);
            if(!request.claim().key().equals(declared.claim().key()) || request.claim().claimGeneration()<declared.claim().claimGeneration()
                    || request.claim().executionGeneration()<declared.claim().executionGeneration() || request.epoch()<declared.epoch())throw fenced();
            var claim=new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(row);
            if(!request.claim().equals(WorkloadClaimFence.from(claim)) || !MongoPipelinePreparationFacts.scope(scope).equals(row.get("pipelineScope"))
                    || MongoCachePipelinePreparations.phase(row,scope)!=PipelineCachePreparations.Phase.PREPARED
                    || !original.previousPreparation().preparationId().equals(row.get("pipelinePreparation",Document.class).get("preparationId")))throw fenced();
            var access=new MongoCachePipelineAccess(client,database,scope,null,() -> check(original));access.verifyStateStructure();var state=access.state(session);
            if(state.getLong("epoch")!=request.epoch() || request.epoch()>=Long.MAX_VALUE-2 || state.getLong("runtimeWriteFence")>=Long.MAX_VALUE-2
                    || claim.claimGeneration()>=Long.MAX_VALUE-2)throw rebuild(scope);
            new MongoSrsConsumerStore(client,database,business,() -> check(original),null).requireDetachedPipeline(session,scope);
            MongoPipelineSourcePreparations.retained(session,database,scope);
            var retiredClaim=new WorkloadClaimFence(claim.key(),currentNode.owner(),claim.claimGeneration()+1,claim.executionGeneration(),claim.topologyRevision());
            long epoch=state.getLong("epoch")+1,write=state.getLong("runtimeWriteFence")+1;
            String hash=MongoOperatorInventory.retireDeletion(session,database,original,retiredClaim,epoch,write);
            var retired=new PipelineRetiredState(scope,original.request().operationId(),retiredClaim,epoch,write,original.previousPreparation().preparationId(),hash);
            var expected=WorkloadClaimDocuments.live(request.claim());expected.remove("$expr");
            expected.append("pipelineScope",MongoPipelinePreparationFacts.scope(scope)).append("pipelinePreparation",row.get("pipelinePreparation"));
            var values=new Document("ownerNodeId",retiredClaim.owner().nodeId()).append("ownerBootId",retiredClaim.owner().bootId())
                    .append("claimGeneration",retiredClaim.claimGeneration()).append("leaseUntil",new Date(0))
                    .append("pipelinePreparation",MongoCachePipelinePreparations.marker(retired.preparationId(),PipelineCachePreparations.Phase.RETIRED))
                    .append("pipelineRetirement",MongoPipelineRetirementFacts.cache(retired));
            if(claims.updateOne(session,expected,new Document("$set",values)).getMatchedCount()!=1)throw fenced();
            if(SystemCollections.CACHE_PIPELINE_STATE.on(database).updateOne(session,new Document("_id",scope.pipelineId()).append("scope",state.get("scope"))
                            .append("epoch",state.get("epoch")).append("runtimeWriteFence",state.get("runtimeWriteFence")),
                    new Document("$set",new Document("stateJson",StateJson.of(PipelineState.STOPPED)).append("epoch",epoch).append("runtimeWriteFence",write))).getMatchedCount()!=1)throw fenced();
            var observation=SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database);var previous=observation.find(session,new Document("_id",scope.pipelineId())).first();
            if(previous!=null) {
                if(!MongoPipelinePreparationFacts.scope(scope).equals(previous.get("scope")))throw rebuild(scope);
                observation.deleteOne(session,new Document("_id",scope.pipelineId()).append("scope",previous.get("scope")));
            }
            return new Receipt(this,original,Optional.of(retired));
        });
    }
    void verifyRetired(ClientSession session,PipelineRetiredState retired) {
        requireScope(retired.scope());var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var row=claims.find(session,new Document("_id",MongoWorkloadClaimStore.id(retired.claim().key()))).first();if(row==null)throw rebuild(retired.scope());
        var claim=new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(row);
        if(!retired.claim().equals(WorkloadClaimFence.from(claim)) || !MongoPipelinePreparationFacts.scope(retired.scope()).equals(row.get("pipelineScope"))
                || !MongoPipelineRetirementFacts.cache(retired).equals(row.get("pipelineRetirement"))
                || !MongoCachePipelinePreparations.marker(retired.preparationId(),PipelineCachePreparations.Phase.RETIRED).equals(row.get("pipelinePreparation")))throw fenced();
        var state=SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session,new Document("_id",retired.scope().pipelineId())).first();
        if(state==null || !MongoPipelinePreparationFacts.scope(retired.scope()).equals(state.get("scope")) || !Long.valueOf(retired.epoch()).equals(state.get("epoch"))
                || !Long.valueOf(retired.writeFence()).equals(state.get("runtimeWriteFence")) || !StateJson.of(PipelineState.STOPPED).equals(state.get("stateJson")))throw rebuild(retired.scope());
        MongoOperatorInventory.requireDeleted(session,database,retired);
    }
    private void requireAbsent(ClientSession session,PipelineIntentScope scope) {
        var key=new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId());
        if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first()!=null
                || SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session,new Document("_id",scope.pipelineId())).first()!=null
                || SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session,new Document("_id.pipelineId",scope.pipelineId())).first()!=null
                || SystemCollections.SRS_CONSUMERS.on(database).find(session,new Document("pipelineId",scope.pipelineId())).first()!=null)throw rebuild(scope);
        var tables=List.of(SystemCollections.CACHE_NEST_STATE,SystemCollections.CACHE_JOIN_FACTS,SystemCollections.CACHE_JOIN_DIMENSIONS,SystemCollections.CACHE_JOIN_INDEXES,SystemCollections.CACHE_JOIN_WRITERS);
        for(var table:tables)if(table.on(database).find(session,new Document("_id.placement.scope.pipelineId",scope.pipelineId())).first()!=null)throw rebuild(scope);
        for(var entry:List.of(Map.entry(SystemCollections.DIRECT_CHANNEL_OFFSETS,"_id.scope.pipelineId"),Map.entry(SystemCollections.DIRECT_BATCHES,"_id.channel.scope.pipelineId"),
                Map.entry(SystemCollections.CACHE_NEST_SHAPES,"_id.scope.pipelineId"),Map.entry(SystemCollections.CACHE_SINK_PREPARATIONS,"_id.scope.pipelineId"),
                Map.entry(SystemCollections.CACHE_SNAPSHOT_ORDERS,"_id.pipelineId"),Map.entry(SystemCollections.CACHE_SNAPSHOT_COUNTS,"_id.scope.pipelineId"),
                Map.entry(SystemCollections.CACHE_DERIVED_SCHEMAS,"scope.pipelineId"),Map.entry(SystemCollections.CACHE_PIPELINE_RATE_HISTORY,"scope.pipelineId")))
            if(entry.getKey().on(database).find(session,new Document(entry.getValue(),scope.pipelineId())).first()!=null)throw rebuild(scope);
    }
    private record Receipt(MongoCachePipelineRetirements owner,PipelineRetirementPreparation preparation,Optional<PipelineRetiredState> retiredState) implements PipelineRetirementReceipt {}
    static void requireNativeReceipt(PipelineRetirementReceipt receipt) {
        if(!(receipt instanceof Receipt))throw fenced();MongoPipelineRetirements.requireOriginalActive(receipt.preparation());
    }
    static void requireCurrentReceipt(PipelineRetirementReceipt candidate) {
        requireNativeReceipt(candidate);var receipt=(Receipt)candidate;
        receipt.owner.transaction(receipt.preparation(),session -> {
            if(receipt.retiredState().isPresent())receipt.owner.verifyRetired(session,receipt.retiredState().orElseThrow());
            else receipt.owner.requireAbsent(session,receipt.preparation().request().scope());return null;
        });
    }
    private <T>T transaction(PipelineRetirementPreparation original,Function<ClientSession,T> action) {
        return StoreIo.call(() -> {check(original);try(var session=client.startSession()) {
            return session.withTransaction(() -> {
                check(original);new MongoCacheBusinessIdentity(database,business).requireAdmissionState(session);
                var structure=new MongoCacheMetadataStructure(database,MongoRateHistoryStore.DEFAULT_RETENTION);structure.verifyDirect();structure.verifySrs();
                for(var table:List.of(SystemCollections.CACHE_OPERATOR_CLEANUP,SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.CACHE_PIPELINE_OBSERVATION))structure.verifyFacet(table);
                T result=action.apply(session);check(original);return result;
            },TRANSACTION);
        }});
    }
    private void check(PipelineRetirementPreparation original){active.run();MongoPipelineRetirements.requireOriginalActive(original);}
    private void requireScope(PipelineIntentScope scope){if(!business.equals(new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation())))throw MongoCacheBusinessIdentity.businessFenced();}
    private static TapstateException fenced(){return MongoPipelineRetirementFacts.fenced();}
    private static TapstateException rebuild(PipelineIntentScope scope){return MongoCachePipelinePreparations.rebuild(scope);}
}
