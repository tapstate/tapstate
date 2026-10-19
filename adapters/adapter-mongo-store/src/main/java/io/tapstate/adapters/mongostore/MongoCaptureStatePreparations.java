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
        return transaction(session -> {requireScope(session,scope);return Optional.ofNullable(SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first()).map(MongoCapturePreparationFacts::decode);});
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
                var retained=MongoCapturePreparationFacts.decode(existing);
                if(retained.mode()!=preparation.mode() || !retained.chainId().equals(preparation.chainId())
                        || !retained.sourceIdentityHash().equals(preparation.sourceIdentityHash()))
                    throw MongoPipelineSourcePreparations.identityUnproven(request.pipeline());
                return new Issuance(retained,false,null,null);
            }
            var binding=bindings.loadForTransaction(session).orElseThrow(MongoCaptureStatePreparations::fenced);
            // This is the existing CONFIG contention point used by authoring, binding and rebuild.
            // A dependency changed after our snapshot must abort and revalidate before any grant.
            ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
            if(mode==CapturePreparation.Mode.SHARED_SRS) {
                var prior=table.find(session,new Document("_id.clusterId",scope.clusterId()).append("_id.bindingVersion",scope.bindingVersion())
                        .append("_id.capture",OperatorStateText.encode(capture))).first();
                if(prior!=null) {
                    var previous=MongoCapturePreparationFacts.decode(prior);
                    if(previous.mode()!=mode || !previous.chainId().equals(chain))throw MongoCapturePreparationFacts.unreadable();
                }
                // Necessary notes survive CACHE replacement; a prior incarnation forbids scope repair.
                artifacts.prepareSharedCaptureNotes(session,scope.bindingVersion(),chain,prior!=null);
            }
            table.insertOne(session,MongoCapturePreparationFacts.row(preparation));
            return new Issuance(preparation,true,MongoStorageBindingStore.targetFingerprint(binding.cache()),source.srs()==null?null:source.srs().retention());
        });
        active.run();return issued.won()?new CaptureCreationResult.Issued(new Creation(issued.preparation(),request,issued.targetFingerprint(),issued.retention(),active)):new CaptureCreationResult.AlreadyIssued(issued.preparation());
    }
    private void requireScope(ClientSession session,CacheBusinessScope expected){var current=bindings.cachePreparationForTransaction(session).orElseThrow(MongoCaptureStatePreparations::fenced).businessScope();if(!cluster.equals(expected.clusterId()) || !expected.equals(current))throw fenced();}
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
