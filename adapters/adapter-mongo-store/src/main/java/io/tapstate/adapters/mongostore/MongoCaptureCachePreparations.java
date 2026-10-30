package io.tapstate.adapters.mongostore;

import com.mongodb.*;
import com.mongodb.client.*;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import org.bson.Document;

/** First canonical CAPTURE history and its optional SRS birth commit together under the original CACHE authority. */
final class MongoCaptureCachePreparations implements CaptureCachePreparations,SharedSrsCacheRecoveries {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final CacheBusinessScope scope;private final String targetFingerprint;private final Runnable active;private final Duration retention;
    MongoCaptureCachePreparations(MongoClient client,MongoDatabase database,CacheBusinessScope scope,String targetFingerprint,Runnable active,Duration retention){this.client=Objects.requireNonNull(client,"client");this.database=Objects.requireNonNull(database,"database");this.scope=Objects.requireNonNull(scope,"scope");this.targetFingerprint=Objects.requireNonNull(targetFingerprint,"targetFingerprint");this.active=Objects.requireNonNull(active,"active");this.retention=retention;}
    @Override public SharedSrsRecoveryReceipt replace(SharedSrsRecoveryPreparation original,WorkloadClaimFence currentNode) {
        var admitted=MongoStorageRecoveryCommands.requireSharedOriginal(original,targetFingerprint);requireScope(admitted.replacement());
        requireSharedNode(currentNode);
        var result=transaction(() -> {},session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            new MongoCacheBusinessIdentity(database,scope).requireReady(session);
            var consumers=new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs);
            var roots=SystemCollections.SRS_CHAINS.on(database);
            var previous=admitted.plan().manifest().sharedSrs().previous();
            var root=roots.find(session,new Document("_id",previous.chainId())).first();
            if(root==null)throw history(previous);
            var isolation=MongoSrsRecoveryControls.read(root);
            if(isolation.filter(control -> control.phase()==MongoSrsRecoveryControls.Phase.RETIRED).isPresent()) {
                verifyApplied(session,admitted,consumers);return new SharedReceipt(this,admitted,SharedSrsRecoveryReceipt.Outcome.RETIRED);
            }
            var observed=isolation.isEmpty()?consumers.sharedRecoveryView(session,previous)
                    :consumers.cancelledSharedRecoveryView(session,previous,admitted.plan().operationId(),isolation.orElseThrow().planHash(),admitted.replacement().chainId());
            if(!observed.snapshot().equals(admitted.plan().manifest().sharedSrs()))throw history(previous);
            requireEmptySharedNamespace(session,admitted.replacement());
            new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
            Date now=MongoCachePipelinePreparations.proveNode(session,database,scope,currentNode);
            for(var row:observed.claims())requireEnded(row,now);
            var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
            var originalCapture=observed.claims().getFirst();var before=observed.snapshot().captureFence();
            if(before.claimGeneration()>=Long.MAX_VALUE-2)throw history(previous);
            var next=new WorkloadClaimFence(before.key(),currentNode.owner(),before.claimGeneration()+1,before.executionGeneration(),
                    Math.max(before.topologyRevision(),currentNode.topologyRevision()));
            var replacement=new Document(originalCapture).append("ownerNodeId",next.owner().nodeId()).append("ownerBootId",next.owner().bootId())
                    .append("claimGeneration",next.claimGeneration()).append("topologyRevision",next.topologyRevision()).append("leaseUntil",new Date(0))
                    .append("capturePreparation",MongoCapturePreparationFacts.row(admitted.replacement()))
                    .append("fencedChainWrites",originalCapture.getLong("fencedChainWrites")+1);
            if(claims.replaceOne(session,originalCapture,replacement).getMatchedCount()!=1)throw history(previous);
            for(int i=1;i<observed.claims().size();i++) {
                var row=observed.claims().get(i);
                if(!(row.get("fencedConsumerWrites") instanceof Long writes) || writes<1 || writes>=Long.MAX_VALUE-1)throw history(previous);
                if(claims.updateOne(session,row,new Document("$set",new Document("leaseUntil",new Date(0)))
                        .append("$inc",new Document("fencedConsumerWrites",1L))).getMatchedCount()!=1)throw history(previous);
            }
            consumers.retireSharedRecovery(session,observed,admitted.plan(),admitted.replacement().chainId());
            roots.insertOne(session,MongoSrsChainStore.initialRow(scope,admitted.replacement().chainId(),observed.root().getString("retention"),next));
            verifyApplied(session,admitted,consumers);
            return new SharedReceipt(this,admitted,SharedSrsRecoveryReceipt.Outcome.RETIRED);
        });
        admitted.requireCurrent().run();return result;
    }
    @Override public Optional<SharedSrsRecoveryReceipt> applied(SharedSrsRecoveryPreparation original) {
        var admitted=MongoStorageRecoveryCommands.requireSharedOriginal(original,targetFingerprint);requireScope(admitted.replacement());
        var result=transaction(() -> {},session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            new MongoCacheBusinessIdentity(database,scope).requireReady(session);
            var consumers=new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs);
            var previous=admitted.plan().manifest().sharedSrs().previous();
            var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",previous.chainId())).first();
            if(root==null)throw history(previous);
            var control=MongoSrsRecoveryControls.read(root);
            if(control.filter(value -> value.phase()==MongoSrsRecoveryControls.Phase.RETIRED).isPresent()) {
                verifyApplied(session,admitted,consumers);
                return Optional.<SharedSrsRecoveryReceipt>of(new SharedReceipt(this,admitted,SharedSrsRecoveryReceipt.Outcome.RETIRED));
            }
            if(control.isEmpty())consumers.sharedRecoveryView(session,previous);
            else consumers.cancelledSharedRecoveryView(session,previous,admitted.plan().operationId(),control.orElseThrow().planHash(),admitted.replacement().chainId());
            requireEmptySharedNamespace(session,admitted.replacement());return Optional.<SharedSrsRecoveryReceipt>empty();
        });
        admitted.requireCurrent().run();return result;
    }
    @Override public SharedSrsRecoveryReceipt cancel(SharedSrsRecoveryPreparation original,WorkloadClaimFence currentNode) {
        var admitted=MongoStorageRecoveryCommands.requireSharedCancellationAuthority(original,targetFingerprint);requireScope(admitted.replacement());requireSharedNode(currentNode);
        // Fix the CACHE attempt before CONFIG admission; a delayed old admission cannot undo a later cancellation.
        var boundary=transaction(() -> {},session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            new MongoCacheBusinessIdentity(database,scope).requireReady(session);
            var previous=admitted.plan().manifest().sharedSrs().previous();
            var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",previous.chainId())).first();
            if(root==null)throw history(previous);
            MongoSrsChainStore.controlState(root,scope,new MongoSrsModelContexts(database,scope));
            return new Document(root);
        });
        admitted.requireCurrent().run();
        var result=transaction(() -> {},session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            new MongoCacheBusinessIdentity(database,scope).requireReady(session);
            var consumers=new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs);
            var previous=admitted.plan().manifest().sharedSrs().previous();
            var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",previous.chainId())).first();
            if(root==null)throw history(previous);
            var control=MongoSrsRecoveryControls.read(root);
            if(control.isPresent()) {
                if(control.orElseThrow().phase()==MongoSrsRecoveryControls.Phase.RETIRED) {
                    if(!(admitted instanceof MongoStorageRecoveryCommands.SharedOriginal running))throw history(previous);
                    verifyApplied(session,running,consumers);return new SharedReceipt(this,running,SharedSrsRecoveryReceipt.Outcome.RETIRED);
                }
                var observed=consumers.cancelledSharedRecoveryView(session,previous,admitted.plan().operationId(),control.orElseThrow().planHash(),admitted.replacement().chainId());
                requireEmptySharedNamespace(session,admitted.replacement());
                if(control.orElseThrow().planHash().equals(admitted.cancellationPlanHash()))
                    return new SharedReceipt(this,admitted,SharedSrsRecoveryReceipt.Outcome.CANCELLED);
                if(!(admitted instanceof MongoStorageRecoveryCommands.SharedOriginal) || !root.equals(boundary))throw history(previous);
                new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
                MongoCachePipelinePreparations.proveNode(session,database,scope,currentNode);
                consumers.cancelSharedRecovery(session,observed,admitted.plan(),admitted.replacement().chainId());
                verifyCancelled(session,admitted,consumers);return new SharedReceipt(this,admitted,SharedSrsRecoveryReceipt.Outcome.CANCELLED);
            }
            if(!(admitted instanceof MongoStorageRecoveryCommands.SharedOriginal) || !root.equals(boundary))throw history(previous);
            // Cancel the current typed controls, even when they no longer match the old preview.
            var current=consumers.sharedRecoveryView(session,previous);
            requireEmptySharedNamespace(session,admitted.replacement());
            new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
            MongoCachePipelinePreparations.proveNode(session,database,scope,currentNode);
            consumers.cancelSharedRecovery(session,current,admitted.plan(),admitted.replacement().chainId());
            verifyCancelled(session,admitted,consumers);
            return new SharedReceipt(this,admitted,SharedSrsRecoveryReceipt.Outcome.CANCELLED);
        });
        admitted.requireCurrent().run();return result;
    }

    private void requireSharedNode(WorkloadClaimFence node) {
        if(node==null || node.key().type()!=WorkloadClaimType.NODE_SESSION || !scope.clusterId().equals(node.key().clusterId())
                || !node.key().resourceId().equals(node.owner().nodeId()))throw MongoCapturePreparationFacts.fenced();
    }
    private void requireEmptySharedNamespace(ClientSession session,CapturePreparation replacement) {
        if(SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",replacement.chainId())).first()!=null)throw history(replacement);
        requireEmptySharedPayloads(session,replacement);
    }
    private void requireEmptySharedPayloads(ClientSession session,CapturePreparation replacement) {
        for(var table:List.of(SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS))
            if(table.on(database).find(session,new Document("_id.chain",replacement.chainId())).projection(new Document("_id",1)).first()!=null)throw history(replacement);
        MongoSrsLogStore.requireEmptyPhysicalNamespace(session,database,replacement.chainId());
    }
    private static void requireEnded(Document row,Date now) {
        if(!(row.get("leaseUntil") instanceof Date lease) || lease.after(now))throw MongoCapturePreparationFacts.fenced();
    }
    private void verifyApplied(ClientSession session,MongoStorageRecoveryCommands.SharedOriginal original,MongoSrsConsumerStore consumers) {
        var plan=original.plan();var before=plan.manifest().sharedSrs();var replacement=original.replacement();
        var retired=consumers.requireRetiredSharedRecovery(session,plan,replacement.chainId());
        var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var reader=new MongoWorkloadClaimStore(claims).inStrictSession(session);
        var row=claims.find(session,new Document("_id",MongoWorkloadClaimStore.id(before.captureFence().key()))).first();
        if(row==null || !replacement.equals(requirePreparation(row,before.captureFence().key(),scope))
                || !(row.get("fencedChainWrites") instanceof Long writes) || writes<2 || writes==Long.MAX_VALUE)throw history(replacement);
        var current=WorkloadClaimFence.from(reader.decodeStored(row));
        var fresh=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",replacement.chainId())).first();
        if(fresh==null)throw history(replacement);
        MongoSrsChainStore.decode(fresh,scope,new MongoSrsModelContexts(database,scope));
        var birth=WorkloadClaimDocuments.readCapture(fresh.get("captureFence",Document.class));
        if(!birth.key().equals(before.captureFence().key()) || birth.claimGeneration()!=before.captureFence().claimGeneration()+1
                || birth.executionGeneration()!=before.captureFence().executionGeneration() || birth.topologyRevision()<before.captureFence().topologyRevision()
                || !MongoPositionFences.retained(birth,current)
                || !MongoSrsChainStore.initialRow(scope,replacement.chainId(),retired.getString("retention"),birth).equals(fresh))throw history(replacement);
        requireEmptySharedPayloads(session,replacement);
        if(current.claimGeneration()==birth.claimGeneration() && (!birth.equals(current) || row.getDate("leaseUntil").getTime()!=0))throw history(replacement);
        for(var pipeline:before.pipelines().values()) {
            var held=claims.find(session,new Document("_id",MongoWorkloadClaimStore.id(pipeline.claim().key()))).first();
            if(held==null || !pipeline.scope().equals(MongoCacheWorkloadClaimStore.preparedScope(held,pipeline.claim().key(),scope)))throw history(replacement);
            var fence=WorkloadClaimFence.from(reader.decodeStored(held));
            if(!MongoPositionFences.retained(pipeline.claim(),fence) || !(held.get("fencedConsumerWrites") instanceof Long consumerWrites)
                    || consumerWrites<2 || consumerWrites==Long.MAX_VALUE)throw history(replacement);
            if(fence.claimGeneration()==pipeline.claim().claimGeneration() && (!fence.equals(pipeline.claim()) || held.getDate("leaseUntil").getTime()!=0))throw history(replacement);
        }
    }
    private void requireSharedReceipt(SharedReceipt receipt) {
        receipt.original.requireCurrent().run();
        transaction(() -> {},session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            new MongoCacheBusinessIdentity(database,scope).requireReady(session);
            var consumers=new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs);
            if(receipt.outcome==SharedSrsRecoveryReceipt.Outcome.RETIRED) {
                if(!(receipt.original instanceof MongoStorageRecoveryCommands.SharedOriginal running))throw MongoCapturePreparationFacts.fenced();
                verifyApplied(session,running,consumers);
            }
            else verifyCancelled(session,receipt.original,consumers);
            return null;
        });
        receipt.original.requireCurrent().run();
    }
    private MongoSrsConsumerStore.SharedView verifyCancelled(ClientSession session,MongoStorageRecoveryCommands.SharedAuthority original,MongoSrsConsumerStore consumers) {
        requireEmptySharedNamespace(session,original.replacement());
        var plan=original.plan();
        return consumers.cancelledSharedRecoveryView(session,plan.manifest().sharedSrs().previous(),plan.operationId(),original.cancellationPlanHash(),original.replacement().chainId());
    }
    private SharedSrsRecoverySnapshot cancelledSnapshot(SharedReceipt receipt) {
        if(receipt.outcome!=SharedSrsRecoveryReceipt.Outcome.CANCELLED)throw MongoCapturePreparationFacts.fenced();
        receipt.original.requireCurrent().run();
        var result=transaction(() -> {},session -> {
            var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
            new MongoCacheBusinessIdentity(database,scope).requireReady(session);
            return verifyCancelled(session,receipt.original,new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs)).snapshot();
        });
        receipt.original.requireCurrent().run();return result;
    }
    private static final class SharedReceipt implements SharedSrsRecoveryReceipt {
        private final MongoCaptureCachePreparations owner;private final MongoStorageRecoveryCommands.SharedAuthority original;private final Outcome outcome;
        private SharedReceipt(MongoCaptureCachePreparations owner,MongoStorageRecoveryCommands.SharedAuthority original,Outcome outcome) {
            this.owner=owner;this.original=original;this.outcome=outcome;
        }
        @Override public SharedSrsRecoveryPreparation preparation(){return original.preparation();}
        @Override public Outcome outcome(){return outcome;}
        @Override public void requireCurrent(){owner.requireSharedReceipt(this);}
        @Override public SharedSrsRecoverySnapshot cancelledSnapshot(){return owner.cancelledSnapshot(this);}
    }
    static MongoStorageRecoveryCommands.SharedOriginal requireAppliedSharedOriginal(SharedSrsRecoveryReceipt receipt) {
        if(!(receipt instanceof SharedReceipt retained) || retained.outcome!=SharedSrsRecoveryReceipt.Outcome.RETIRED
                || !(retained.original instanceof MongoStorageRecoveryCommands.SharedOriginal original))
            throw MongoCapturePreparationFacts.fenced();
        return original;
    }
    static MongoStorageRecoveryCommands.SharedAuthority requireCancelledSharedAuthority(SharedSrsRecoveryReceipt receipt) {
        if(!(receipt instanceof SharedReceipt retained) || retained.outcome!=SharedSrsRecoveryReceipt.Outcome.CANCELLED)throw MongoCapturePreparationFacts.fenced();
        return retained.original;
    }
    static SharedSrsRecoverySnapshot currentCancelledSnapshot(SharedSrsRecoveryReceipt receipt) {
        requireCancelledSharedAuthority(receipt);
        var retained=(SharedReceipt)receipt;return retained.owner.cancelledSnapshot(retained);
    }
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
