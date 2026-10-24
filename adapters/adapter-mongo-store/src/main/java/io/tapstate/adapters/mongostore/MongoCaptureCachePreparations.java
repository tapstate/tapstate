package io.tapstate.adapters.mongostore;

import com.mongodb.*;
import com.mongodb.client.*;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import org.bson.Document;

/** First canonical CAPTURE history and its optional SRS birth commit together under the original CACHE authority. */
final class MongoCaptureCachePreparations implements CaptureCachePreparations {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final CacheBusinessScope scope;private final String targetFingerprint;private final Runnable active;private final Duration retention;
    MongoCaptureCachePreparations(MongoClient client,MongoDatabase database,CacheBusinessScope scope,String targetFingerprint,Runnable active,Duration retention){this.client=Objects.requireNonNull(client,"client");this.database=Objects.requireNonNull(database,"database");this.scope=Objects.requireNonNull(scope,"scope");this.targetFingerprint=Objects.requireNonNull(targetFingerprint,"targetFingerprint");this.active=Objects.requireNonNull(active,"active");this.retention=retention;}
    @Override public CaptureCacheState createFirst(FirstCaptureCreation invocation) {
        var original=MongoCaptureStatePreparations.consumeFirst(invocation,targetFingerprint);
        var preparation=original.preparation();var request=original.request();String ringRetention=original.retention();requireScope(preparation);
        if(preparation.replacement())throw history(preparation);
        return transaction(original::requireActive,session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);
            if(preparation.mode()==CapturePreparation.Mode.SHARED_SRS)structure.verifySrs();
            if(preparation.mode()==CapturePreparation.Mode.DIRECT)structure.verifyDirect();
            var pipeline=new MongoCachePipelineAccess(client,database,request.pipeline(),request.originalPipeline(),() -> {active.run();original.requireActive();});
            pipeline.verifyStateStructure();var state=pipeline.state(session);pipeline.proveClaim(session);pipeline.updateState(session,state,new Document(),false);
            var retained=MongoPipelineSourcePreparations.retained(session,database,request.pipeline());
            if(!retained.equals(request.inputs()))throw MongoPipelineSourcePreparations.identityUnproven(request.pipeline());
            var identity=new MongoCacheBusinessIdentity(database,scope);identity.admitClaim(session);
            Date now=MongoCachePipelinePreparations.proveNode(session,database,scope,request.originalNode());
            var key=new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,preparation.captureId());
            if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first()!=null)throw history(preparation);
            requireNoPriorFacts(session,preparation);
            var initial=new WorkloadClaimFence(key,request.originalNode().owner(),1,0,request.originalNode().topologyRevision());
            var row=new Document("_id",MongoWorkloadClaimStore.id(key)).append("clusterId",key.clusterId()).append("resourceType",key.type().name()).append("resourceId",key.resourceId())
                    .append("ownerNodeId",initial.owner().nodeId()).append("ownerBootId",initial.owner().bootId()).append("claimGeneration",1L).append("executionGeneration",0L)
                    .append("topologyRevision",initial.topologyRevision()).append("leaseUntil",now).append("capturePreparation",MongoCapturePreparationFacts.row(preparation));
            if(preparation.mode()==CapturePreparation.Mode.SHARED_SRS)row.append("fencedChainWrites",1L);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).insertOne(session,row);
            if(preparation.mode()==CapturePreparation.Mode.SHARED_SRS)SystemCollections.SRS_CHAINS.on(database).insertOne(session,MongoSrsChainStore.initialRow(scope,preparation.chainId(),ringRetention,initial));
            return loadInSession(session,preparation);
        });
    }
    @Override public CaptureCacheState load(CapturePreparation original){Objects.requireNonNull(original,"original");requireScope(original);return transaction(() -> {},session -> loadInSession(session,original));}
    private CaptureCacheState loadInSession(ClientSession session,CapturePreparation original) {
        var key=new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,original.captureId());
        var row=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
        if(row==null)throw history(original);
        if(!original.equals(requirePreparation(row,key,scope)))throw history(original);
        var claim=new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).inStrictSession(session).decodeStored(row);
        Optional<SrsChainState> chain=Optional.empty();
        if(original.mode()==CapturePreparation.Mode.SHARED_SRS) {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",original.chainId())).first();if(root==null || !original.captureId().equals(root.get("captureId")))throw history(original);
            chain=Optional.of(new MongoSrsChainStore(client,database,scope).state(session,root));
        }
        return new CaptureCacheState(original,WorkloadClaimFence.from(claim),chain);
    }
    static CapturePreparation requirePreparation(Document row,WorkloadClaimKey key,CacheBusinessScope scope) {
        if(!(row.get("capturePreparation")instanceof Document marker))throw MongoCapturePreparationFacts.unreadable();
        var preparation=MongoCapturePreparationFacts.annex(marker);
        if(!preparation.scope().equals(scope) || !preparation.captureId().equals(key.resourceId()) || !key.clusterId().equals(scope.clusterId()) || key.type()!=WorkloadClaimType.CAPTURE)throw MongoCapturePreparationFacts.fenced();return preparation;
    }
    private void requireNoPriorFacts(ClientSession session,CapturePreparation original) {
        var byCapture=new Document("captureId",original.captureId());
        if(SystemCollections.SRS_CHAINS.on(database).find(session,byCapture).first()!=null)throw history(original);
        if(original.mode()==CapturePreparation.Mode.SHARED_SRS) {
            if(SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",original.chainId())).first()!=null)throw history(original);
            for(var table:List.of(SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS))
                if(table.on(database).find(session,new Document("_id.chain",original.chainId())).first()!=null)throw history(original);
            if(SystemCollections.SRS_EVENTS.on(database).find(session,new Document("captureFence.resourceId",original.captureId())).first()!=null)throw history(original);
        }
        if(original.mode()==CapturePreparation.Mode.DIRECT) {
            try(var rows=SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(session,new Document("_id.captureId",original.captureId())).iterator()) {
                while(rows.hasNext()){var row=rows.next();if(row.get("current")!=null || row.get("retained")!=null)throw history(original);}
            }
        }
    }
    private void requireScope(CapturePreparation original){if(!scope.equals(original.scope()))throw MongoCapturePreparationFacts.fenced();}
    private <T>T transaction(Runnable originalActive,Function<ClientSession,T> body) {
        return StoreIo.call(() -> {
            try {
                active.run();originalActive.run();MongoCacheWorkloadClaimStore.requireCollection(database);
                try(var session=client.startSession()) {
                    var result=session.withTransaction(() -> {active.run();originalActive.run();new MongoCacheBusinessIdentity(database,scope).requireAdmissionState(session);var value=body.apply(session);active.run();originalActive.run();return value;},TRANSACTION);
                    active.run();originalActive.run();return result;
                }
            }catch(RuntimeException failure){active.run();originalActive.run();throw failure;}
        });
    }
    private static io.tapstate.core.common.TapstateException history(CapturePreparation preparation){return MongoCapturePreparationFacts.history(preparation.captureId());}
}
