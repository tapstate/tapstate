package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.bson.Document;

/** Explicit-clear roots and receipts use one real CACHE client, independently from CONFIG. */
final class MongoCaptureClearCache implements CaptureClearCache {
    private static final TransactionOptions TX = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final String fingerprint;
    private final Runnable active;
    private final Duration retention;
    MongoCaptureClearCache(MongoClient client, MongoDatabase database, CacheBusinessScope scope,
            String fingerprint, Runnable active, Duration retention) {
        this.client=client;this.database=database;this.scope=scope;this.fingerprint=fingerprint;this.active=active;this.retention=retention;
    }
    @Override public CaptureStorageUpgrade.CacheObservation observeLegacyUpgrade(List<CapturePreparation> captures) {
        var originals=List.copyOf(captures);
        if(originals.stream().anyMatch(p -> !p.scope().equals(scope)) || originals.stream().map(CapturePreparation::captureId).distinct().count()!=originals.size())
            throw refused(null,"legacy-capture-coordinates");
        return legacyTransaction(session -> new LegacyObservation(this,originals,legacyFacts(session,originals)));
    }
    static LegacyObservation requireLegacy(CaptureStorageUpgrade.CacheObservation original) {
        if(!(original instanceof LegacyObservation held))throw refused(null,"legacy-cache-observation-not-issued");return held;
    }
    static final class LegacyObservation implements CaptureStorageUpgrade.CacheObservation {
        private final MongoCaptureClearCache owner;
        private final List<CapturePreparation> captures;
        private final Document facts;
        private LegacyObservation(MongoCaptureClearCache owner,List<CapturePreparation> captures,Document facts){this.owner=owner;this.captures=captures;this.facts=facts;}
        String fingerprint(){return owner.fingerprint;}
        Document openingCoverage(CapturePreparation original) {
            if(!captures.contains(original) || !(facts.get("captures")instanceof Map<?,?> all)
                    || !(all.get(original.captureId())instanceof Document observed)
                    || !(observed.get("claim")instanceof Document claim))throw refused(original,"legacy-opening-coverage-missing");
            var previous=new Document("ownerNodeId",claim.getString("ownerNodeId")).append("ownerBootId",claim.getString("ownerBootId"))
                    .append("claimGeneration",claim.getLong("claimGeneration")).append("executionGeneration",claim.getLong("executionGeneration"))
                    .append("topologyRevision",claim.getLong("topologyRevision"));
            // These are observed coordinates, not proof that every historical physical owner exited.
            return new Document("oldBusinessScope",new Document("clusterId",scope().clusterId())
                    .append("bindingVersion",scope().bindingVersion()).append("cacheIncarnation",scope().cacheIncarnation()))
                    .append("previousCaptureFence",previous).append("coverageDigest",MongoCaptureClearControls.hash(observed));
        }
        @Override public CacheBusinessScope scope(){return owner.scope;}
        @Override public List<CapturePreparation> captures(){return captures;}
        @Override public void requireCurrent(){owner.legacyTransaction(session -> {
            if(!facts.equals(owner.legacyFacts(session,captures)))throw refused(null,"legacy-cache-controls-changed");return null;
        });}
        @Override public void complete(CaptureStorageUpgrade.ConfigurationReceipt original) {
            MongoCaptureExternalRetirements.requireLegacyReceipt(original,this);
            owner.legacyTransaction(session -> {
                var schema=SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(owner.database).find(session,new Document("_id","business")).first();
                if(schema!=null && Integer.valueOf(2).equals(schema.get("installedVersion"))) {
                    new MongoCacheBusinessIdentity(owner.database,owner.scope).requireReady(session);return null;
                }
                if(!facts.equals(owner.legacyFacts(session,captures)))throw refused(null,"legacy-cache-controls-changed");
                if(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(owner.database).replaceOne(session,schema,new Document(schema).append("installedVersion",2)).getMatchedCount()!=1)
                    throw refused(null,"legacy-cache-version-raced");
                new MongoCacheBusinessIdentity(owner.database,owner.scope).requireReady(session);return null;
            });
            MongoCaptureExternalRetirements.requireLegacyReceipt(original,this);
        }
    }
    private Document legacyFacts(ClientSession session,List<CapturePreparation> captures) {
        var identity=SystemCollections.CACHE_IDENTITY.on(database).find(session,new Document("_id","cluster")).first();
        var schema=SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find(session,new Document("_id","business")).first();
        if(identity==null || schema==null || !Integer.valueOf(1).equals(schema.get("installedVersion")) || !"READY".equals(schema.getString("phase"))
                || !"READY".equals(identity.getString("businessPhase")) || !identity.get("businessPreparationId").equals(schema.get("operationId")))
            throw refused(null,"legacy-cache-version-unproven");
        var auth=new CacheAuthScope(scope.clusterId(),scope.bindingVersion(),StorageBinding.Mode.valueOf(identity.getString("mode")),scope.cacheIncarnation(),identity.getLong("authGeneration"));
        new MongoCacheIdentity(client,database,auth).requireReady(session);
        var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
        var known=captures.stream().map(CapturePreparation::captureId).collect(java.util.stream.Collectors.toSet());
        for(var row:SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id.resourceType","CAPTURE")))
            if(!known.contains(row.get("_id",Document.class).getString("resourceId")))throw refused(null,"legacy-capture-set-incomplete");
        for(var row:SystemCollections.SRS_CHAINS.on(database).find(session))
            if(!known.contains(row.getString("captureId")))throw refused(null,"legacy-root-owner-unproven");
        var prepared=new java.util.TreeMap<String,Document>();
        for(var capture:captures) {
            var key=new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,capture.captureId());
            var row=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
            if(row==null || !capture.equals(MongoCaptureCachePreparations.requirePreparation(row,key,scope)))throw refused(capture,"legacy-capture-annex-missing");
            new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).inStrictSession(session).decodeStored(row);
            var value=new Document("claim",row);
            if(capture.mode()==CapturePreparation.Mode.SHARED_SRS) {
                var view=new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs).sharedRecoveryView(session,capture);
                value.append("root",view.root()).append("memberships",view.memberships()).append("cursors",view.cursors())
                        .append("claims",view.claims()).append("controlHash",view.snapshot().controlHash());
            }
            prepared.put(capture.captureId(),value);
        }
        return new Document("identity",identity).append("schema",schema).append("captures",prepared);
    }
    private <T>T legacyTransaction(Function<ClientSession,T> action) {
        return StoreIo.call(() -> {active.run();try(var session=client.startSession()) {
            return session.withTransaction(() -> {active.run();return action.apply(session);},TX);
        }});
    }
    record Observation(SharedSrsRecoverySnapshot snapshot,List<Document> memberships,List<Document> pipelineClaims,
            String targetFingerprint) implements CaptureClearObservation {}
    @Override public CaptureClearObservation inspect(CapturePreparation original) {
        return transaction(session -> {
            var observed=view(session,original);var claims=new java.util.TreeMap<String,Document>();
            var records=new MongoSrsConsumerRecords(scope);
            for(var row:observed.memberships()) {
                var member=records.membership(row);var pipeline=member.context().key().pipelineId();
                var key=new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,pipeline);
                var claim=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)
                        .find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
                if(claim==null)throw refused(original,"detached-pipeline-control-missing");
                claims.put(pipeline,claim);
            }
            return new Observation(observed.snapshot(),List.copyOf(observed.memberships()),List.copyOf(claims.values()),fingerprint);
        });
    }
    @Override public Receipt freeze(CaptureClearPreparation original,WorkloadClaimFence node) {
        var held=authority(original);held.requirePhase("RESERVED","FROZEN");
        var result=transaction(session -> {
            var previous=original.observation().previous();var root=root(session,previous);
            var control=MongoCaptureClearControls.read(root);
            if(control.isPresent() && MongoCaptureClearControls.same(control.orElseThrow(),original))
                return receipt(original,control.orElseThrow());
            var observed=view(session,previous);
            if(!observed.snapshot().consumers().isEmpty() || !original.observation().consumers().isEmpty()
                    || !observed.snapshot().captureFence().equals(original.observation().captureFence())
                    || !observed.snapshot().controlHash().equals(original.observation().controlHash()))throw refused(previous,"attachment-raced");
            new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
            MongoCachePipelinePreparations.proveNode(session,database,scope,node);
            String hash=MongoCaptureClearControls.hash(new Document("operation",original.operationId()).append("sequence",original.sequence())
                    .append("requestHash",original.requestHash()).append("root",root)
                    .append("memberships",observed.memberships()).append("cursors",observed.cursors()));
            var next=MongoCaptureClearControls.candidate(root,original,MongoCaptureClearControls.Phase.FROZEN,hash);
            if(SystemCollections.SRS_CHAINS.on(database).replaceOne(session,root,next).getMatchedCount()!=1)throw refused(previous,"root-raced");
            return receipt(original,MongoCaptureClearControls.read(next).orElseThrow());
        });
        held.requireCurrent();return result;
    }
    @Override public Optional<Receipt> frozen(CaptureClearPreparation original) {
        var held=authority(original);held.requireCurrent();
        return transaction(session -> {
            var root=root(session,original.observation().previous());var control=MongoCaptureClearControls.read(root);
            if(control.isEmpty())return Optional.empty();
            if(!MongoCaptureClearControls.same(control.orElseThrow(),original))throw refused(original.replacement(),"different-operation");
            return Optional.of(receipt(original,control.orElseThrow()));
        });
    }
    @Override public Receipt apply(CaptureClearPreparation original,WorkloadClaimFence node) {
        var held=authority(original);held.requirePhase("VERIFIED");
        var result=transaction(session -> {
            var previous=original.observation().previous();var old=root(session,previous);
            var control=MongoCaptureClearControls.read(old).orElseThrow(() -> refused(previous,"root-not-frozen"));
            if(!MongoCaptureClearControls.same(control,original))throw refused(previous,"different-operation");
            if(control.phase()==MongoCaptureClearControls.Phase.RETIRED) {requireApplied(session,original,old);return receipt(original,control);}
            if(control.phase()!=MongoCaptureClearControls.Phase.FROZEN)throw refused(previous,"root-cancelled");
            var observed=view(session,previous);
            if(!observed.snapshot().consumers().isEmpty())throw refused(previous,"consumer-retained");
            requireEmpty(session,original.replacement());
            new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
            MongoCachePipelinePreparations.proveNode(session,database,scope,node);
            var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
            var key=original.observation().captureFence().key();var row=claims.find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
            if(row==null || !previous.equals(MongoCaptureCachePreparations.requirePreparation(row,key,scope)))throw refused(previous,"claim-annex");
            var before=WorkloadClaimFence.from(new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(row));
            if(!before.equals(original.observation().captureFence()) || before.claimGeneration()>=Long.MAX_VALUE-1)throw refused(previous,"capture-fence");
            var next=new WorkloadClaimFence(key,node.owner(),before.claimGeneration()+1,before.executionGeneration(),Math.max(before.topologyRevision(),node.topologyRevision()));
            var fresh=MongoSrsChainStore.initialRow(scope,original.replacement().chainId(),old.getString("retention"),next);
            String hash=MongoCaptureClearControls.hash(new Document("operation",original.operationId()).append("sequence",original.sequence())
                    .append("scope",MongoCapturePreparationFacts.scope(scope)).append("targetFingerprint",fingerprint)
                    .append("previous",MongoCapturePreparationFacts.row(previous)).append("next",MongoCapturePreparationFacts.row(original.replacement()))
                    .append("frozenRoot",old).append("birthRoot",fresh).append("oldFence",WorkloadClaimDocuments.stored(before))
                    .append("newFence",WorkloadClaimDocuments.stored(next)).append("emptyPrefix",true));
            var replacement=new Document(row).append("ownerNodeId",next.owner().nodeId()).append("ownerBootId",next.owner().bootId())
                    .append("claimGeneration",next.claimGeneration()).append("topologyRevision",next.topologyRevision()).append("leaseUntil",new java.util.Date(0))
                    .append("capturePreparation",MongoCapturePreparationFacts.row(original.replacement()))
                    .append("clearPreparation",new Document("operationId",original.operationId()).append("sequence",original.sequence()).append("receiptHash",hash));
            if(claims.replaceOne(session,row,replacement).getMatchedCount()!=1)throw refused(previous,"claim-raced");
            var retired=MongoCaptureClearControls.candidate(old,original,MongoCaptureClearControls.Phase.RETIRED,hash);
            if(SystemCollections.SRS_CHAINS.on(database).replaceOne(session,old,retired).getMatchedCount()!=1)throw refused(previous,"root-raced");
            SystemCollections.SRS_CHAINS.on(database).insertOne(session,fresh);
            requireApplied(session,original,retired);
            return receipt(original,MongoCaptureClearControls.read(retired).orElseThrow());
        });
        held.requireCurrent();return result;
    }
    @Override public Optional<Receipt> applied(CaptureClearPreparation original) {
        var held=authority(original);held.requireCurrent();
        return transaction(session -> {
            var previous=original.observation().previous();var old=root(session,previous);var control=MongoCaptureClearControls.read(old);
            if(control.isEmpty() || control.orElseThrow().phase()!=MongoCaptureClearControls.Phase.RETIRED) {
                requireEmpty(session,original.replacement());return Optional.empty();
            }
            requireApplied(session,original,old);return Optional.of(receipt(original,control.orElseThrow()));
        });
    }
    @Override public Receipt cancel(CaptureClearPreparation original,WorkloadClaimFence node) {
        var held=authority(original);held.requirePhase("CANCELLING");
        return transaction(session -> {
            var previous=original.observation().previous();var old=root(session,previous);var control=MongoCaptureClearControls.read(old);
            if(control.isPresent() && (!MongoCaptureClearControls.same(control.orElseThrow(),original)
                    || control.orElseThrow().phase()==MongoCaptureClearControls.Phase.RETIRED))throw refused(previous,"cannot-cancel");
            requireEmpty(session,original.replacement());
            new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
            MongoCachePipelinePreparations.proveNode(session,database,scope,node);
            var next=MongoCaptureClearControls.candidate(old,original,MongoCaptureClearControls.Phase.CANCELLED,
                    MongoCaptureClearControls.hash(new Document("cancelled",original.operationId()).append("root",old)));
            if(SystemCollections.SRS_CHAINS.on(database).replaceOne(session,old,next).getMatchedCount()!=1)throw refused(previous,"cancel-raced");
            return receipt(original,MongoCaptureClearControls.read(next).orElseThrow());
        });
    }
    @Override public boolean reclaim(CaptureClearPreparation original,WorkloadClaimFence node) {
        var held=authority(original);held.requirePhase("READY");
        return transaction(session -> {
            new MongoCacheBusinessIdentity(database,scope).admitMetadataWrite(session);
            MongoCachePipelinePreparations.proveNode(session,database,scope,node);
            String chain=original.observation().previous().chainId();
            // Old explicit chain keys were not length-delimited in ring names. Never reclaim a
            // prefix that another retained control namespace could also name.
            for(var table:List.of(SystemCollections.SRS_CHAINS,SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,
                    SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
                var projection=table==SystemCollections.SRS_CHAINS?new Document("_id",1):new Document("_id.chain",1);
                for(var metadata:table.on(database).find(session).projection(projection)) {
                    Object key=metadata.get("_id");
                    String other=key instanceof String name?name:key instanceof Document id?id.getString("chain"):null;
                    if(other!=null && !other.equals(chain) && (other.startsWith(chain+".") || chain.startsWith(other+".")))
                        throw refused(original.replacement(),"gc-namespace-still-shared");
                }
            }
            int removed=0;
            for(var table:List.of(SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
                if(removed>=256)return false;
                var collection=table.on(database);
                var keys=collection.find(session,new Document("_id.chain",chain)).projection(new Document("_id",1)).limit(256-removed)
                        .into(new java.util.ArrayList<>()).stream().map(d -> d.get("_id")).toList();
                if(!keys.isEmpty())removed+=collection.deleteMany(session,new Document("_id",new Document("$in",keys))).getDeletedCount();
            }
            var rings=java.util.regex.Pattern.compile("^"+java.util.regex.Pattern.quote("srs."+chain+"."));
            var bounds=java.util.regex.Pattern.compile("^"+java.util.regex.Pattern.quote(MongoSrsLogStore.boundsId("srs."+chain+".")));
            for(var spec:List.of(new Object[]{SystemCollections.SRS_EVENTS,new Document("_id.ring",rings)},
                    new Object[]{SystemCollections.SRS_LOG_BOUNDS,new Document("_id",bounds)})) {
                if(removed>=256)return false;
                var collection=((SystemCollections)spec[0]).on(database);
                var keys=collection.find(session,(Document)spec[1]).projection(new Document("_id",1)).limit(256-removed)
                        .into(new java.util.ArrayList<>()).stream().map(d -> d.get("_id")).toList();
                if(!keys.isEmpty())removed+=collection.deleteMany(session,new Document("_id",new Document("$in",keys))).getDeletedCount();
            }
            if(removed>0)return false;
            var row=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chain)).first();
            if(row!=null) {
                var marker=MongoCaptureClearControls.read(row).orElseThrow(() -> refused(original.replacement(),"gc-control"));
                if(!MongoCaptureClearControls.same(marker,original) || marker.phase()!=MongoCaptureClearControls.Phase.RETIRED)
                    throw refused(original.replacement(),"gc-scope");
                SystemCollections.SRS_CHAINS.on(database).deleteOne(session,row);
            }
            return true;
        });
    }
    private MongoCaptureExternalRetirements.HeldClear authority(CaptureClearPreparation original) {
        var held=MongoCaptureExternalRetirements.requireClear(original,fingerprint);
        if(!held.observation().previous().scope().equals(scope))throw refused(held.replacement(),"cache-scope");return held;
    }
    private MongoSrsConsumerStore.SharedView view(ClientSession session,CapturePreparation previous) {
        var structure=new MongoCacheMetadataStructure(database,retention);structure.verifySrs();
        return new MongoSrsConsumerStore(client,database,scope,active,structure::verifySrs).explicitClearView(session,previous);
    }
    private Document root(ClientSession session,CapturePreparation previous) {
        var row=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",previous.chainId())).first();
        if(row==null || !previous.captureId().equals(row.getString("captureId")) || !scope.cacheIncarnation().equals(row.getString("cacheIncarnation")))
            throw refused(previous,"root-unproven");return row;
    }
    private void requireEmpty(ClientSession session,CapturePreparation replacement) {
        if(SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",replacement.chainId())).first()!=null)throw refused(replacement,"next-root-exists");
        requireEmptyPayloads(session,replacement);
    }
    private void requireEmptyPayloads(ClientSession session,CapturePreparation replacement) {
        for(var table:List.of(SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS))
            if(table.on(database).find(session,new Document("_id.chain",replacement.chainId())).projection(new Document("_id",1)).first()!=null)
                throw refused(replacement,"next-prefix-exists");
        MongoSrsLogStore.requireEmptyPhysicalNamespace(session,database,replacement.chainId());
    }
    private void requireApplied(ClientSession session,CaptureClearPreparation original,Document retired) {
        var control=MongoCaptureClearControls.read(retired).orElseThrow(() -> refused(original.replacement(),"missing-receipt"));
        if(!MongoCaptureClearControls.same(control,original) || control.phase()!=MongoCaptureClearControls.Phase.RETIRED)throw refused(original.replacement(),"receipt-mismatch");
        var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);var key=original.observation().captureFence().key();
        var claim=claims.find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
        var marker=claim==null?null:claim.get("clearPreparation",Document.class);
        if(claim==null || !original.replacement().equals(MongoCaptureCachePreparations.requirePreparation(claim,key,scope))
                || marker==null || !original.operationId().equals(marker.getString("operationId"))
                || !Long.valueOf(original.sequence()).equals(marker.get("sequence")) || !control.receiptHash().equals(marker.getString("receiptHash")))
            throw refused(original.replacement(),"next-annex");
        var next=root(session,original.replacement());
        var birth=WorkloadClaimDocuments.readCapture(next.get("captureFence",Document.class));
        if(birth.claimGeneration()!=original.observation().captureFence().claimGeneration()+1
                || !MongoSrsChainStore.initialRow(scope,original.replacement().chainId(),retired.getString("retention"),birth).equals(next))
            throw refused(original.replacement(),"next-root-not-initial");
        requireEmptyPayloads(session,original.replacement());
    }
    private Receipt receipt(CaptureClearPreparation original,MongoCaptureClearControls.Control control) {
        var outcome=switch(control.phase()){case FROZEN->Outcome.FROZEN;case RETIRED->Outcome.APPLIED;case CANCELLED->Outcome.CANCELLED;};
        return new CacheReceipt(this,original,control.receiptHash(),outcome);
    }
    private record CacheReceipt(MongoCaptureClearCache owner,CaptureClearPreparation preparation,String hash,Outcome outcome) implements Receipt {
        @Override public void requireCurrent() {
            var found=owner.frozen(preparation).orElseThrow(() -> refused(preparation.replacement(),"receipt-missing"));
            if(!hash.equals(found.hash()) || outcome!=found.outcome())throw refused(preparation.replacement(),"receipt-changed");
        }
    }
    static void requireReceipt(Receipt value,CaptureClearPreparation original) {
        if(!(value instanceof CacheReceipt receipt) || receipt.preparation()!=original || receipt.outcome()!=Outcome.APPLIED)
            throw refused(original.replacement(),"receipt-not-issued");
        receipt.requireCurrent();
    }
    private <T>T transaction(Function<ClientSession,T> action) {
        return StoreIo.call(() -> {active.run();try(var session=client.startSession()){
            return session.withTransaction(() -> {active.run();new MongoCacheBusinessIdentity(database,scope).requireReady(session);return action.apply(session);},TX);
        }});
    }
    private static io.tapstate.core.common.TapstateException refused(CapturePreparation p,String reason) {
        return MongoCaptureExternalRetirements.pending(p,reason);
    }
}
