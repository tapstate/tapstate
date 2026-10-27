package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;
import java.util.function.Function;

/** One original CONFIG deletion and its compact completed result, separate from CACHE and external IO. */
final class MongoPipelineRetirements implements PipelineRetirements {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final String cluster;
    private final MongoStorageBindingStore bindings;private final MongoConfigurationArtifactStore artifacts;
    private final MongoRequiredNoteStore notes;private final Runnable active;
    MongoPipelineRetirements(MongoClient client,MongoDatabase database,String cluster,MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts,MongoRequiredNoteStore notes,Runnable active) {
        this.client=Objects.requireNonNull(client);this.database=Objects.requireNonNull(database);this.cluster=Objects.requireNonNull(cluster);
        this.bindings=Objects.requireNonNull(bindings);this.artifacts=Objects.requireNonNull(artifacts);this.notes=Objects.requireNonNull(notes);this.active=Objects.requireNonNull(active);
        if(!bindings.belongsTo(client,database.getName()) || !artifacts.belongsTo(client,database.getName()) || !notes.belongsTo(client,database.getName()))throw new IllegalArgumentException("retirement requires one CONFIG transaction domain");
    }
    @Override public PipelineRetirements guardedBy(Runnable guard) {
        Objects.requireNonNull(guard);return new MongoPipelineRetirements(client,database,cluster,bindings,artifacts,notes,() -> {active.run();guard.run();});
    }
    @Override public PipelineRetirementPreparation begin(PipelineRetirementRequest original) {
        Objects.requireNonNull(original);
        var validationClosing = UUID.randomUUID();
        return transaction(session -> {
            requireBusiness(session,original.scope());var row=mapping(session,original.scope().pipelineId());
            var state=MongoPipelineStateMappings.decode(original.scope().pipelineId(),row);
            if(state.phase()==PipelineStateMapping.Phase.RETIRING)return retained(session,original,row);
            if(state.phase()!=PipelineStateMapping.Phase.ACTIVE || state.stateGeneration()!=original.scope().stateGeneration())throw fenced();
            MongoPipelineSharedRecoveryFacts.requireReady(session,database,row,original.scope());
            if(row.containsKey("retirement") && MongoPipelineRetirementFacts.read(row).request().operationId().equals(original.operationId()))throw fenced();
            requireDefinition(session,original);var preparation=MongoPipelinePreparationFacts.decode(state.pipelineId(),row,cluster);
            Optional<PipelineRetiredState> predecessor=Optional.empty();
            if(preparation.phase()==PipelinePreparation.Phase.COMPLETE) {
                if(!original.scope().equals(preparation.scope()) || original.cache().isEmpty())throw fenced();
            } else if(preparation.phase()==PipelinePreparation.Phase.UNASSIGNED) {
                if(original.cache().isPresent())throw fenced();
                if(row.containsKey("retirement")) {
                    var prior=MongoPipelineRetirementFacts.read(row).completed();
                    if(prior.operationId().equals(original.operationId()) || prior.scope().stateGeneration()>=original.scope().stateGeneration())throw fenced();
                    if(MongoPipelineRetirementFacts.sameBusiness(original.scope(),prior.scope()))predecessor=prior.cache();
                }
            } else throw fenced();
            if(original.scope().stateGeneration()>=Long.MAX_VALUE-2 || row.getLong("noteWriteFence")>=Long.MAX_VALUE-2
                    || ConfigurationVersion.read(session,SystemCollections.CONFIGURATION_VERSION.on(database))>=Long.MAX_VALUE-2)throw fenced();
            var tokens=notes.retirementScopes(session,original.scope(),original.operationId());
            var fact=new MongoPipelineRetirementFacts.Fact(original,MongoPipelineRetirementFacts.notesHash(tokens),tokens.size(),"RETIRING",predecessor,Optional.empty());
            ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
            MongoValidationPreparations.beginRetirement(session,database,cluster,original,validationClosing);
            var filter=new Document("_id",state.pipelineId()).append("phase","ACTIVE").append("stateGeneration",state.stateGeneration())
                    .append("noteWriteFence",row.get("noteWriteFence")).append("cachePreparation",row.get("cachePreparation"));
            if(SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,filter,new Document("$set",new Document("phase","RETIRING")
                    .append("operationId",original.operationId()).append("retirement",MongoPipelineRetirementFacts.encode(fact)))
                    .append("$inc",new Document("noteWriteFence",1L))).getMatchedCount()!=1)throw fenced();
            return new Pending(this,fact,preparation);
        });
    }
    @Override public Optional<PipelineRetirementPreparation> pending(PipelineIntentScope scope,String operationId) {
        Objects.requireNonNull(scope);Objects.requireNonNull(operationId);
        return transaction(session -> {
            requireBusiness(session,scope);var row=mapping(session,scope.pipelineId());if(row==null)return Optional.empty();
            var state=MongoPipelineStateMappings.decode(scope.pipelineId(),row);
            if(state.phase()!=PipelineStateMapping.Phase.RETIRING)return Optional.empty();
            var fact=MongoPipelineRetirementFacts.read(row);
            if(!fact.request().scope().equals(scope) || !fact.request().operationId().equals(operationId))throw fenced();
            return Optional.of(retained(session,fact.request(),row));
        });
    }
    @Override public void requirePending(PipelineRetirementPreparation original) {
        var pending=requireHandle(original);pending.owner.active.run();
        transaction(session -> {
            requireBusiness(session,pending.request().scope());
            var current=retained(session,pending.request(),mapping(session,pending.request().scope().pipelineId()));
            if(!pending.fact.equals(current.fact) || !pending.previous.equals(current.previous))throw fenced();return null;
        });
    }
    @Override public PipelineRetirementResult complete(PipelineRetirementReceipt receipt) {
        MongoCachePipelineRetirements.requireNativeReceipt(receipt);var pending=requireHandle(receipt.preparation());
        var done=completed(pending.request().scope().pipelineId(),pending.request().operationId());if(done.isPresent())return sameResult(pending,done.orElseThrow());
        MongoCachePipelineRetirements.requireCurrentReceipt(receipt);
        return transaction(session -> {
            var replay=completed(session,pending.request().scope().pipelineId(),pending.request().operationId());if(replay.isPresent())return sameResult(pending,replay.orElseThrow());
            requireBusiness(session,pending.request().scope());var row=mapping(session,pending.request().scope().pipelineId());
            var held=retained(session,pending.request(),row);if(!held.fact.equals(pending.fact))throw fenced();
            notes.requireRetiredScopes(session,pending);
            var complete=new MongoPipelineRetirementFacts.Fact(pending.request(),pending.privateScopesHash(),pending.privateScopesCount(),"COMPLETE",pending.predecessor(),receipt.retiredState());
            long fence=row.getLong("noteWriteFence");if(fence>=Long.MAX_VALUE-1)throw fenced();
            ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
            MongoValidationPreparations.requireRetired(session,database,cluster,pending.request().scope().pipelineId());
            artifacts.deleteRetiredPipeline(session,pending.request().scope().pipelineId(),pending.request().definitionHash());
            new MongoPipelineIntentStore(client,database,cluster,bindings).retire(session,pending.request().scope());
            if(SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,new Document("_id",pending.request().scope().pipelineId())
                            .append("stateGeneration",pending.request().scope().stateGeneration()).append("phase","RETIRING")
                            .append("operationId",pending.request().operationId()).append("retirement",MongoPipelineRetirementFacts.encode(pending.fact)).append("noteWriteFence",fence),
                    new Document("$set",new Document("phase","RETIRED").append("retirement",MongoPipelineRetirementFacts.encode(complete)))
                            .append("$inc",new Document("noteWriteFence",1L))).getMatchedCount()!=1)throw fenced();
            return complete.completed();
        });
    }
    @Override public Optional<PipelineRetirementResult> completed(String pipelineId,String operationId) {
        Objects.requireNonNull(pipelineId);Objects.requireNonNull(operationId);return transaction(session -> completed(session,pipelineId,operationId));
    }
    private Optional<PipelineRetirementResult> completed(ClientSession session,String pipelineId,String operationId) {
        var row=mapping(session,pipelineId);if(row==null || !row.containsKey("retirement"))return Optional.empty();
        var fact=MongoPipelineRetirementFacts.read(row);
        if(!cluster.equals(fact.request().scope().clusterId()))throw fenced();
        if(!fact.phase().equals("COMPLETE") || !fact.request().operationId().equals(operationId))return Optional.empty();
        return Optional.of(fact.completed());
    }
    private PipelineRetirementResult sameResult(Pending original,PipelineRetirementResult result) {
        if(!original.request().scope().equals(result.scope()) || !original.request().definitionHash().equals(result.definitionHash())
                || !original.privateScopesHash().equals(result.privateScopesHash()) || original.privateScopesCount()!=result.privateScopesCount())throw fenced();
        return result;
    }
    private Pending retained(ClientSession session,PipelineRetirementRequest original,Document row) {
        var state=MongoPipelineStateMappings.decode(original.scope().pipelineId(),row);var fact=MongoPipelineRetirementFacts.read(row);
        MongoPipelineSharedRecoveryFacts.requireReady(session,database,row,original.scope());
        if(state.phase()!=PipelineStateMapping.Phase.RETIRING || state.stateGeneration()!=original.scope().stateGeneration()
                || !original.operationId().equals(state.operationId()) || !original.equals(fact.request()) || !fact.phase().equals("RETIRING"))throw fenced();
        requireDefinition(session,original);var tokens=notes.retirementScopes(session,original.scope(),original.operationId());
        if(tokens.size()!=fact.notesCount() || !MongoPipelineRetirementFacts.notesHash(tokens).equals(fact.notesHash()))throw MongoPipelineRetirementFacts.unreadable();
        return new Pending(this,fact,MongoPipelinePreparationFacts.decode(original.scope().pipelineId(),row,cluster));
    }
    private void requireDefinition(ClientSession session,PipelineRetirementRequest original) {
        var resource=artifacts.getInTransaction(session,original.scope().pipelineId()).orElseThrow(MongoPipelineRetirements::fenced);
        if(!(resource instanceof PipelineResource) || !CanonicalHash.of(resource).equals(original.definitionHash()))throw fenced();
    }
    private void requireBusiness(ClientSession session,PipelineIntentScope scope) {
        var business=bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineRetirements::fenced).businessScope();
        if(!cluster.equals(scope.clusterId()) || !business.equals(new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation())))throw fenced();
    }
    private Document mapping(ClientSession session,String id){return SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("_id",id)).first();}
    private Pending requireHandle(PipelineRetirementPreparation original) {
        if(!(original instanceof Pending pending) || pending.owner.client!=client || !pending.owner.database.getName().equals(database.getName()) || !pending.owner.cluster.equals(cluster))throw fenced();
        return pending;
    }
    static void requireOriginalPending(PipelineRetirementPreparation original) {
        if(!(original instanceof Pending pending))throw fenced();pending.owner.requirePending(pending);
    }
    static void requireOriginalActive(PipelineRetirementPreparation original) {
        if(!(original instanceof Pending pending))throw fenced();pending.owner.active.run();
    }
    private record Pending(MongoPipelineRetirements owner,MongoPipelineRetirementFacts.Fact fact,PipelinePreparation previous) implements PipelineRetirementPreparation {
        public PipelineRetirementRequest request(){return fact.request();}public String privateScopesHash(){return fact.notesHash();}
        public int privateScopesCount(){return fact.notesCount();}public PipelinePreparation previousPreparation(){return previous;}
        public Optional<PipelineRetiredState> predecessor(){return fact.predecessor();}
    }
    private <T>T transaction(Function<ClientSession,T> action) {
        return StoreIo.call(() -> {active.run();try(var session=client.startSession()) {
            return session.withTransaction(() -> {active.run();T result=action.apply(session);active.run();return result;},TRANSACTION);
        }});
    }
    private static TapstateException fenced(){return MongoPipelineRetirementFacts.fenced();}
}
