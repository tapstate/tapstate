package io.tapstate.adapters.mongostore;

import com.mongodb.*;
import com.mongodb.client.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.bson.Document;

/** One-time CONFIG insertion authority, constructed only after the winning transaction commits. */
final class MongoCaptureStatePreparations implements CaptureStatePreparations {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final String cluster;
    private final MongoStorageBindingStore bindings;private final MongoConfigurationArtifactStore artifacts;private final Runnable active;
    MongoCaptureStatePreparations(MongoClient client,MongoDatabase database,String cluster,MongoStorageBindingStore bindings,MongoConfigurationArtifactStore artifacts,Runnable active) {
        this.client=Objects.requireNonNull(client,"client");this.database=Objects.requireNonNull(database,"database");this.cluster=Objects.requireNonNull(cluster,"cluster");this.bindings=Objects.requireNonNull(bindings,"bindings");this.artifacts=Objects.requireNonNull(artifacts,"artifacts");this.active=Objects.requireNonNull(active,"active");
        if(!bindings.belongsTo(client,database.getName()) || !artifacts.belongsTo(client,database.getName()))throw new IllegalArgumentException("capture preparation requires one original CONFIG client");
    }
    @Override public CaptureStatePreparations guardedBy(Runnable guard){Objects.requireNonNull(guard,"guard");return new MongoCaptureStatePreparations(client,database,cluster,bindings,artifacts,() -> {active.run();guard.run();});}
    @Override public Optional<CapturePreparation> read(CacheBusinessScope scope,String capture) {
        Objects.requireNonNull(scope,"scope");if(capture==null || capture.isBlank())throw new IllegalArgumentException("blank capture identity");
        return transaction(session -> {requireScope(session,scope);return Optional.ofNullable(SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first()).map(MongoCapturePreparationFacts::available);});
    }
    @Override public Optional<RequiredNoteOrigin> inspectSharedOrigin(CacheBusinessScope scope,String sourceId,String sourceHash) {
        return transaction(session -> {
            requireScope(session,scope);
            var source=artifacts.getInTransaction(session,sourceId).filter(SourceResource.class::isInstance).map(SourceResource.class::cast).orElseThrow(MongoCaptureStatePreparations::fenced);
            if(!CanonicalHash.of(source).equals(sourceHash))throw fenced();
            String chain=SourceCaptureIdentity.resolve(source.connector(),source.config(),source.srs()==null?null:source.srs().key());
            String capture=SourceCaptureIdentity.sharedCapture(chain);
            var preparations=SystemCollections.CAPTURE_PREPARATIONS.on(database);
            var current=preparations.find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first();
            if(current!=null) {
                var original=MongoCapturePreparationFacts.available(current);
                if(original.mode()!=CapturePreparation.Mode.SHARED_SRS || !original.canonicalChainId().equals(chain)
                        || !original.sourceIdentityHash().equals(SourceCaptureIdentity.physical(source.connector(),source.config())))throw fenced();
            }
            var prior=preparations.find(session,new Document("_id.clusterId",scope.clusterId())
                    .append("_id.bindingVersion",scope.bindingVersion()).append("_id.capture",OperatorStateText.encode(capture))).first();
            if(prior!=null){var original=MongoCapturePreparationFacts.decode(prior);if(original.mode()!=CapturePreparation.Mode.SHARED_SRS || !original.canonicalChainId().equals(chain)
                    || !original.sourceIdentityHash().equals(SourceCaptureIdentity.physical(source.connector(),source.config())))throw fenced();}
            return artifacts.configurationNotes().inspectSharedOrigin(session,new RequiredNoteOwner(scope.bindingVersion(),"pdk.chain."+chain,RequiredNoteOwner.Kind.CHAIN,chain,0),prior!=null);
        });
    }
    @Override public CaptureSharedHistory inspectSharedHistory(CapturePreparation original, String sourceId, String sourceHash) {
        Objects.requireNonNull(original, "original");
        if (original.mode() != CapturePreparation.Mode.SHARED_SRS) throw fenced();
        return transaction(session -> {
            requireScope(session, original.scope());
            var source = artifacts.getInTransaction(session, sourceId).filter(SourceResource.class::isInstance)
                    .map(SourceResource.class::cast).orElseThrow(MongoCaptureStatePreparations::fenced);
            String chain = SourceCaptureIdentity.resolve(source.connector(), source.config(), source.srs() == null ? null : source.srs().key());
            if (!CanonicalHash.of(source).equals(sourceHash) || !chain.equals(original.canonicalChainId())
                    || !SourceCaptureIdentity.sharedCapture(chain).equals(original.captureId())
                    || !SourceCaptureIdentity.physical(source.connector(), source.config()).equals(original.sourceIdentityHash())) throw fenced();
            var table = SystemCollections.CAPTURE_PREPARATIONS.on(database);
            var current = MongoCapturePreparationFacts.available(table.find(session,
                    new Document("_id", MongoCapturePreparationFacts.id(original.scope(), original.captureId()))).first());
            if (!current.equals(original)) throw fenced();
            boolean historical = current.replacement();
            var filter = new Document("_id.clusterId", original.scope().clusterId())
                    .append("_id.bindingVersion", original.scope().bindingVersion())
                    .append("_id.capture", OperatorStateText.encode(original.captureId()))
                    .append("_id.cacheIncarnation", new Document("$ne", original.scope().cacheIncarnation()));
            try (var rows = table.find(session, filter).batchSize(64).iterator()) {
                while (rows.hasNext()) {
                    var prior = MongoCapturePreparationFacts.decode(rows.next());
                    if (prior.mode() != original.mode() || !prior.canonicalChainId().equals(chain)
                            || !prior.captureId().equals(original.captureId())
                            || !prior.sourceIdentityHash().equals(original.sourceIdentityHash())) throw fenced();
                    historical = true;
                }
            }
            // A retained current record owns this already-reserved namespace. No inspection repairs it.
            artifacts.configurationNotes().inspectSharedOrigin(session, new RequiredNoteOwner(original.scope().bindingVersion(),
                    "pdk.chain." + chain, RequiredNoteOwner.Kind.CHAIN, chain, 0), true);
            return new CaptureSharedHistory(current, historical);
        });
    }
    @Override public CaptureCreationResult issueFirstCreate(CapturePreparationRequest request) {
        Objects.requireNonNull(request,"request");String operation=UUID.randomUUID().toString();
        var issued=transaction(session -> {
            var scope=scopeOf(request.pipeline());requireScope(session,scope);
            var mapping=SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("_id",request.pipeline().pipelineId())).first();
            var state=MongoPipelineStateMappings.decode(request.pipeline().pipelineId(),mapping);
            if(state.phase()!=PipelineStateMapping.Phase.ACTIVE || state.stateGeneration()!=request.pipeline().stateGeneration())throw fenced();
            new MongoPipelineStatePreparations(client,database,cluster,bindings,artifacts,active).verifyInputs(session,request.pipeline(),request.inputs());
            var pipeline=(PipelineResource)artifacts.getInTransaction(session,request.pipeline().pipelineId()).orElseThrow(MongoCaptureStatePreparations::fenced);
            var source=(SourceResource)artifacts.getInTransaction(session,request.sourceId()).orElseThrow(MongoCaptureStatePreparations::fenced);
            var reference=pipeline.sources().stream().filter(ref -> ref.id().equals(source.id())).findFirst().orElseThrow(MongoCaptureStatePreparations::fenced);
            if(!(reference instanceof SourceRef.Spec spec))throw new IllegalStateException("a capture requires a materialized source switch");
            var readMode=pipeline.settings()==null || pipeline.settings().readMode()==null?ReadMode.SNAPSHOT_AND_CDC:pipeline.settings().readMode();
            var mode=readMode==ReadMode.SNAPSHOT_ONLY?CapturePreparation.Mode.SNAPSHOT_ONLY:spec.srs()?CapturePreparation.Mode.SHARED_SRS:CapturePreparation.Mode.DIRECT;
            String key=source.srs()==null?null:source.srs().key();
            String chain=mode==CapturePreparation.Mode.DIRECT?SourceCaptureIdentity.directChannel(source.connector(),source.config(),key,pipeline.id(),source.id()):SourceCaptureIdentity.resolve(source.connector(),source.config(),key);
            var tables=request.inputs().selectedTables().get(source.id());
            String capture=switch(mode){
                case SHARED_SRS -> SourceCaptureIdentity.sharedCapture(chain);
                case SNAPSHOT_ONLY -> SourceCaptureIdentity.snapshotCapture(chain,tables);
                case DIRECT -> SourceCaptureIdentity.directCapture(source.connector(),source.config(),key,pipeline.id(),source.id(),pipeline.settings()==null?null:pipeline.settings().startFrom(),tables);
            };
            var preparation=new CapturePreparation(scope,mode,chain,capture,SourceCaptureIdentity.physical(source.connector(),source.config()),operation);
            var table=SystemCollections.CAPTURE_PREPARATIONS.on(database);var existing=table.find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first();
            if(existing!=null) {
                var retained=MongoCapturePreparationFacts.available(existing);
                if(retained.mode()!=preparation.mode() || !retained.canonicalChainId().equals(preparation.chainId())
                        || !retained.sourceIdentityHash().equals(preparation.sourceIdentityHash()))
                    throw MongoPipelineSourcePreparations.identityUnproven(request.pipeline());
                return new Issuance(retained,false,null,null);
            }
            var binding=bindings.loadForTransaction(session).orElseThrow(MongoCaptureStatePreparations::fenced);
            // This is the existing CONFIG contention point used by authoring, binding and rebuild.
            // A dependency changed after our snapshot must abort and revalidate before any grant.
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), client);
            if(mode==CapturePreparation.Mode.SHARED_SRS) {
                var prior=table.find(session,new Document("_id.clusterId",scope.clusterId()).append("_id.bindingVersion",scope.bindingVersion())
                        .append("_id.capture",OperatorStateText.encode(capture))).first();
                if(prior!=null) {
                    var previous=MongoCapturePreparationFacts.decode(prior);
                    if(previous.mode()!=mode || !previous.canonicalChainId().equals(chain)
                            || !previous.sourceIdentityHash().equals(preparation.sourceIdentityHash()))throw MongoCapturePreparationFacts.unreadable();
                }
                artifacts.configurationNotes().inspectSharedOrigin(session,new RequiredNoteOwner(scope.bindingVersion(),
                        "pdk.chain."+chain,RequiredNoteOwner.Kind.CHAIN,chain,0),prior!=null);
                // Necessary notes survive CACHE replacement; a prior incarnation forbids scope repair.
                artifacts.prepareSharedCaptureNotes(session,scope.bindingVersion(),chain,prior!=null);
            }
            table.insertOne(session,MongoCapturePreparationFacts.row(preparation));
            return new Issuance(preparation,true,MongoStorageBindingStore.targetFingerprint(binding.cache()),source.srs()==null?null:source.srs().retention());
        });
        active.run();return issued.won()?new CaptureCreationResult.Issued(new Creation(issued.preparation(),request,issued.targetFingerprint(),issued.retention(),active)):new CaptureCreationResult.AlreadyIssued(issued.preparation());
    }
    private void requireScope(ClientSession session,CacheBusinessScope expected){var current=bindings.cachePreparationForTransaction(session).orElseThrow(MongoCaptureStatePreparations::fenced).businessScope();if(!cluster.equals(expected.clusterId()) || !expected.equals(current))throw fenced();}
    MongoCapturePreparationFacts.Recovery reserveSharedRecovery(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var previous=plan.manifest().sharedSrs().previous();requireScope(session,previous.scope());
        var table=SystemCollections.CAPTURE_PREPARATIONS.on(database);
        var row=table.find(session,new Document("_id",MongoCapturePreparationFacts.id(previous.scope(),previous.captureId()))).first();
        if(row==null || !previous.equals(MongoCapturePreparationFacts.available(row)))throw fenced();
        requireSharedRecoveryOrigin(session,previous);
        if(previous.replacement())MongoPipelineStateMappings.requireNoSharedReferences(session,database,previous.preparationId());
        var recovery=sharedReservation(plan);
        if(table.replaceOne(session,row,MongoCapturePreparationFacts.configurationRow(recovery)).getMatchedCount()!=1)throw fenced();
        return recovery;
    }
    MongoCapturePreparationFacts.Recovery requireSharedReservation(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var expected=sharedReservation(plan);requireScope(session,expected.previous().scope());
        var actual=currentRecoveryForOperation(session,database,expected.previous().scope(),plan.operationId());
        if(!expected.equals(actual))throw fenced();requireSharedRecoveryOrigin(session,expected.previous());return actual;
    }
    void publishSharedRecovery(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var original=requireSharedReservation(session,plan);
        var ready=new MongoCapturePreparationFacts.Recovery(original.previous(),original.replacement(),original.planHash(),MongoCapturePreparationFacts.RecoveryPhase.READY);
        if(SystemCollections.CAPTURE_PREPARATIONS.on(database).replaceOne(session,MongoCapturePreparationFacts.configurationRow(original),
                MongoCapturePreparationFacts.configurationRow(ready)).getMatchedCount()!=1)throw fenced();
    }
    boolean hasSharedRecoveryReservation(ClientSession session,String operation) {
        return SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("currentRecovery.operationId",operation)).first()!=null;
    }
    MongoCapturePreparationFacts.Recovery requireSharedReconfirmation(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var expected=sharedReservation(plan);requireScope(session,expected.previous().scope());
        var actual=currentRecoveryForOperation(session,database,expected.previous().scope(),plan.operationId());
        if(actual.phase()!=MongoCapturePreparationFacts.RecoveryPhase.RESERVED || !actual.previous().equals(expected.previous())
                || !actual.replacement().equals(expected.replacement()))throw fenced();
        requireSharedRecoveryOrigin(session,actual.previous());return actual;
    }
    void rebindSharedRecovery(ClientSession session,StorageRecoveryCommands.Plan plan,MongoCapturePreparationFacts.Recovery original) {
        var actual=requireSharedReconfirmation(session,plan);
        if(!actual.equals(original))throw fenced();
        if(SystemCollections.CAPTURE_PREPARATIONS.on(database).replaceOne(session,MongoCapturePreparationFacts.configurationRow(original),
                MongoCapturePreparationFacts.configurationRow(sharedReservation(plan))).getMatchedCount()!=1)throw fenced();
    }
    private void requireSharedRecoveryOrigin(ClientSession session,CapturePreparation previous) {
        var owner=new RequiredNoteOwner(previous.scope().bindingVersion(),"pdk.chain."+previous.canonicalChainId(),RequiredNoteOwner.Kind.CHAIN,previous.canonicalChainId(),0);
        if(artifacts.configurationNotes().inspectSharedOrigin(session,owner,true).isEmpty())
            throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
    }
    private static MongoCapturePreparationFacts.Recovery sharedReservation(StorageRecoveryCommands.Plan plan) {
        if(plan.manifest().selection().extent()!=StorageRecoveryCommands.Extent.SHARED_SRS || plan.manifest().sharedSrs()==null)throw fenced();
        var previous=plan.manifest().sharedSrs().previous();
        try {
            var replacement=new CapturePreparation(previous.scope(),CapturePreparation.Mode.SHARED_SRS,
                    MongoCapturePreparationFacts.replacementChain(previous,plan.operationId()),previous.captureId(),previous.sourceIdentityHash(),plan.operationId(),
                    previous.canonicalChainId(),previous.birthPreparationId());
            return new MongoCapturePreparationFacts.Recovery(previous,replacement,plan.planHash(),MongoCapturePreparationFacts.RecoveryPhase.RESERVED);
        }catch(IllegalArgumentException invalid){throw fenced();}
    }
    /** CONFIG-only lookup of an existing reservation; it does not issue or consume any creation permission. */
    static MongoCapturePreparationFacts.Recovery currentRecoveryForOperation(ClientSession session,MongoDatabase database,
            CacheBusinessScope expected,String operation) {
        if(!session.hasActiveTransaction())throw new IllegalStateException("capture recovery lookup requires its CONFIG transaction");
        var rows=SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("currentRecovery.operationId",operation))
                .limit(2).into(new ArrayList<>());
        if(rows.size()!=1)throw MongoCapturePreparationFacts.unreadable();
        var retained=MongoCapturePreparationFacts.recovery(rows.getFirst()).orElseThrow(MongoCapturePreparationFacts::unreadable);
        if(!expected.equals(retained.replacement().scope()) || !operation.equals(retained.replacement().preparationId()))throw MongoCapturePreparationFacts.unreadable();
        return retained;
    }
    private <T>T transaction(Function<ClientSession,T> action) {
        return StoreIo.call(() -> {
            try {
                active.run();MongoCapturePreparationFacts.verify(database);
                for(int attempt=0;;attempt++)try(var session=client.startSession()) {
                    var result=session.withTransaction(() -> {active.run();var value=action.apply(session);active.run();return value;},TRANSACTION);active.run();return result;
                }catch(MongoException raced){if(attempt>=2 || ErrorCategory.fromErrorCode(raced.getCode())!=ErrorCategory.DUPLICATE_KEY)throw raced;}
            }catch(RuntimeException failure){active.run();throw failure;}
        });
    }
    static FirstCaptureCreation consumeFirst(FirstCaptureCreation invocation,String targetFingerprint) {
        if(!(invocation instanceof Creation creation))throw fenced();creation.consume();if(!creation.targetFingerprint.equals(targetFingerprint))throw fenced();return creation;
    }
    private record Issuance(CapturePreparation preparation,boolean won,String targetFingerprint,String retention){}
    private static final class Creation implements FirstCaptureCreation {
        private final CapturePreparation preparation;private final CapturePreparationRequest request;private final String targetFingerprint;private final String retention;private final Runnable active;private final AtomicBoolean consumed=new AtomicBoolean();
        Creation(CapturePreparation preparation,CapturePreparationRequest request,String targetFingerprint,String retention,Runnable active){this.preparation=preparation;this.request=request;this.targetFingerprint=targetFingerprint;this.retention=retention;this.active=active;}
        public CapturePreparation preparation(){return preparation;}public CapturePreparationRequest request(){return request;}public String retention(){return retention;}
        public void consume(){if(!consumed.compareAndSet(false,true))throw fenced();active.run();}public void requireActive(){active.run();}
        @Override public String toString(){return "FirstCaptureCreation[redacted]";}
    }
    private static CacheBusinessScope scopeOf(PipelineIntentScope scope){return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation());}
    private static TapstateException fenced(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
