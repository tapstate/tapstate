package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;

/** The original CACHE client owns both target preparation facts and their Pipeline claim competition. */
final class MongoSinkPreparationStore implements SinkPreparationStore {
    private final MongoCachePipelineAccess access;
    MongoSinkPreparationStore(MongoCachePipelineAccess access) { this.access=Objects.requireNonNull(access,"access"); }
    static Document initial(String preparationId) {
        if (!UUID.fromString(preparationId).toString().equals(preparationId)) throw new IllegalArgumentException("preparation id must be canonical");
        return new Document("formatVersion",1).append("preparationId",preparationId).append("targets",null);
    }
    static void requireInitial(Document claim,String preparationId,PipelineIntentScope scope) {
        if (!initial(preparationId).equals(claim.get("sinkPreparations"))) throw rebuild(scope);
    }
    @Override public void prepareTargets(List<Input> originalTargets) {
        var captured=List.copyOf(originalTargets); var targets=new TreeMap<String,Document>();
        for (var input:captured) {
            requireScope(input); var encoded=input(input);
            if (targets.put(id(input.key()).toJson(),encoded)!=null) throw new IllegalArgumentException("target preparation cannot repeat a physical target");
        }
        var manifest=List.copyOf(targets.values()); requireWriter();
        access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,true,session -> {
            var claim=claim(session); var before=marker(claim); String operation=before.getString("preparationId");
            if (before.get("targets")!=null) {
                if (!manifest.equals(before.get("targets"))) throw rebuild(access.scope);
                verifyClosure(session,manifest,operation); return null;
            }
            if (SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,scopeFilter(access.scope)).first()!=null) throw rebuild(access.scope);
            for (var input:captured) SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).insertOne(session,
                    row(new Snapshot(input,operation,0,Phase.PREPARING,null,null)));
            var next=new Document(before).append("targets",manifest);
            if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(access.database).updateOne(session,
                    new Document("_id",MongoWorkloadClaimStore.id(access.originalClaim().key())).append("sinkPreparations",before),
                    new Document("$set",new Document("sinkPreparations",next))).getMatchedCount()!=1) throw fenced();
            return null;
        });
    }
    @Override public Snapshot load(Input original) {
        requireScope(original);
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,false,session -> require(session,original));
    }
    @Override public Attempt begin(Input original) {
        requireScope(original); requireWriter();
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,true,session -> {
            Snapshot held=require(session,original);
            if (held.phase()!=Phase.PREPARING || held.revision()!=0) throw rebuild(access.scope);
            var started=new Snapshot(original,held.operationId(),1,Phase.PREPARING,access.originalClaim(),null);
            replace(session,held,started); return new Issued(this,started);
        });
    }
    @Override public Snapshot complete(Attempt originalAttempt,Result originalResult) {
        Objects.requireNonNull(originalResult,"originalResult");
        if (!(originalAttempt instanceof Issued issued) || issued.owner()!=this) throw fenced();
        Snapshot original=issued.original(); requireWriter();
        if (!access.originalClaim().equals(original.originalClaim())) throw fenced();
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,true,session -> {
            var held=require(session,original.input()); var ready=new Snapshot(original.input(),original.operationId(),2,Phase.READY,access.originalClaim(),originalResult);
            if (held.equals(ready)) return held;
            if (!held.equals(original)) throw rebuild(access.scope);
            replace(session,original,ready); return ready;
        });
    }
    private record Issued(MongoSinkPreparationStore owner,Snapshot original) implements Attempt {}
    private void replace(ClientSession session,Snapshot before,Snapshot next) {
        if (SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).replaceOne(session,row(before),row(next)).getMatchedCount()!=1) throw fenced();
    }
    private Snapshot require(ClientSession session,Input expected) {
        var control=marker(claim(session));
        if (!(control.get("targets") instanceof List<?> targets) || !targets.contains(input(expected))) throw rebuild(access.scope);
        var raw=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,new Document("_id",id(expected.key()))).first();
        Snapshot held=decode(raw); if (!held.input().equals(expected) || !held.operationId().equals(control.getString("preparationId"))) throw rebuild(access.scope);
        return held;
    }
    private void verifyClosure(ClientSession session,List<Document> targets,String operation) {
        var expected=new HashSet<>(targets);
        try (var rows=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,scopeFilter(access.scope)).iterator()) {
            while(rows.hasNext()) { var held=decode(rows.next()); if (!held.operationId().equals(operation) || !expected.remove(input(held.input()))) throw rebuild(access.scope); }
        }
        if (!expected.isEmpty()) throw rebuild(access.scope);
    }
    private Document claim(ClientSession session) {
        var key=new WorkloadClaimKey(access.scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,access.scope.pipelineId());
        var claim=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(access.database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
        if (claim==null || !MongoPipelinePreparationFacts.scope(access.scope).equals(claim.get("pipelineScope"))) throw rebuild(access.scope);
        return claim;
    }
    private Document marker(Document claim) {
        try {
            if (!(claim.get("pipelinePreparation") instanceof Document prepared) || !(claim.get("sinkPreparations") instanceof Document marker)
                    || !marker.keySet().equals(Set.of("formatVersion","preparationId","targets")) || !Integer.valueOf(1).equals(marker.get("formatVersion"))
                    || !prepared.getString("preparationId").equals(marker.getString("preparationId"))
                    || !UUID.fromString(marker.getString("preparationId")).toString().equals(marker.getString("preparationId"))) throw rebuild(access.scope);
            Object entries=marker.get("targets");
            if (entries!=null) {
                if (!(entries instanceof List<?> values)) throw rebuild(access.scope);
                String previous=null;
                for (Object value:values) {
                    if (!(value instanceof Document encoded)) throw rebuild(access.scope);
                    var input=decodeInput(encoded); requireScope(input); String key=id(input.key()).toJson();
                    if (previous!=null && previous.compareTo(key)>=0) throw rebuild(access.scope); previous=key;
                }
            }
            return marker;
        } catch (IllegalArgumentException|ClassCastException|NullPointerException bad) { throw unreadable(); }
    }
    private void requireScope(Input original) { if (!access.scope.equals(Objects.requireNonNull(original,"originalTarget").key().scope())) throw fenced(); }
    private void requireWriter() { if (access.originalClaim()==null || access.originalClaim().executionGeneration()<1) throw fenced(); }
    static Document scopeFilter(PipelineIntentScope scope) {
        return new Document("_id.scope.clusterId",scope.clusterId()).append("_id.scope.bindingVersion",scope.bindingVersion())
                .append("_id.scope.cacheIncarnation",scope.cacheIncarnation()).append("_id.scope.pipelineId",scope.pipelineId())
                .append("_id.scope.stateGeneration",scope.stateGeneration());
    }
    static void requireEmpty(ClientSession session,MongoDatabase database,PipelineIntentScope scope) {
        if (SystemCollections.CACHE_SINK_PREPARATIONS.on(database).find(session,scopeFilter(scope)).first()!=null) throw rebuild(scope);
    }
    static Document id(Key key) {
        return new Document("scope",MongoPipelinePreparationFacts.scope(key.scope())).append("purpose",key.purpose().segment())
                .append("node",OperatorStateText.encode(key.nodeId())).append("target",OperatorStateText.encode(key.target()));
    }
    private static Document input(Input value) {
        return new Document("key",id(value.key())).append("modelHash",value.modelHash()).append("policy",value.policy().yaml()).append("fullLoad",value.fullLoad());
    }
    private static Input decodeInput(Document value) {
        var key=value.get("key",Document.class); var purpose=switch(key.getString("purpose")) { case "view" -> PipelineNode.Purpose.VIEW; case "sync" -> PipelineNode.Purpose.SYNC; default -> throw unreadable(); };
        var policy=switch(value.getString("policy")) { case "clear" -> OnFullLoad.CLEAR; case "append" -> OnFullLoad.APPEND; case "fail" -> OnFullLoad.FAIL; default -> throw unreadable(); };
        if (!(value.get("fullLoad") instanceof Boolean full)) throw unreadable();
        var result=new Input(new Key(MongoPipelinePreparationFacts.scope(key.get("scope",Document.class)),purpose,OperatorStateText.decode(key.getString("node")),OperatorStateText.decode(key.getString("target"))),value.getString("modelHash"),policy,full);
        if (!input(result).equals(value)) throw unreadable(); return result;
    }
    private static Document row(Snapshot value) {
        return new Document("_id",id(value.input().key())).append("formatVersion",1).append("input",input(value.input())).append("operationId",value.operationId())
                .append("revision",value.revision()).append("phase",value.phase().name())
                .append("originalClaim",value.originalClaim()==null?null:WorkloadClaimDocuments.stored(value.originalClaim()))
                .append("result",value.result()==null?null:new Document("created",value.result().created()));
    }
    private static Snapshot decode(Document value) {
        if(value==null) throw unreadable();
        try {
            if (!Integer.valueOf(1).equals(value.get("formatVersion")) || !(value.get("revision") instanceof Long revision)) throw unreadable();
            Phase phase=switch(value.getString("phase")) { case "PREPARING" -> Phase.PREPARING; case "READY" -> Phase.READY; default -> throw unreadable(); };
            var original=value.get("originalClaim",Document.class); var result=value.get("result",Document.class);
            if (result!=null && !(result.get("created") instanceof Boolean)) throw unreadable();
            var decoded=new Snapshot(decodeInput(value.get("input",Document.class)),value.getString("operationId"),revision,phase,
                    original==null?null:WorkloadClaimDocuments.readPipeline(original),result==null?null:new Result(result.getBoolean("created")));
            if (!row(decoded).equals(value)) throw unreadable(); return decoded;
        } catch (IllegalArgumentException|ClassCastException|NullPointerException bad) { throw unreadable(); }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    private static TapstateException rebuild(PipelineIntentScope scope) { return new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","sink-preparation","field","preparation"),null); }
}
