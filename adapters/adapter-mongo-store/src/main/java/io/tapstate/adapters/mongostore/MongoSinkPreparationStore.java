package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.bson.types.Binary;
import io.tapstate.core.common.PreparedTargetModel;

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
    @Override public List<Prepared> resolveModels(List<Prepared> candidates,PipelinePreparationInputs authoring) {
        var captured=List.copyOf(candidates);captured.forEach(this::validateModel);requireWriter();
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,false,session -> {
            var control=marker(claim(session));
            if(control.get("targets")==null)return captured;
            var manifest=documents(control.get("targets"));verifyClosure(session,manifest,control.getString("preparationId"));
            var answer=new ArrayList<Prepared>();
            for(var candidate:captured) {
                var raw=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,new Document("_id",id(candidate.input().key()))).first();
                var held=decode(raw);var original=held.input();
                if(!held.operationId().equals(control.getString("preparationId")) || !manifest.contains(input(original))
                        || !original.key().equals(candidate.input().key()) || original.policy()!=candidate.input().policy()
                        || original.fullLoad()!=candidate.input().fullLoad())throw rebuild(access.scope);
                var bytes=storedModel(raw);
                if(bytes==null) {
                    if(!original.equals(candidate.input()))throw rebuild(access.scope);
                    answer.add(candidate);
                } else {
                    if(!original.modelHash().equals(candidate.input().modelHash())) {
                        var retained=MongoPipelineSourcePreparations.retained(session,access.database,access.scope);
                        if(authoring==null || !retained.equals(authoring))throw rebuild(access.scope);
                    }
                    answer.add(new Prepared(original,bytes));
                }
            }
            return List.copyOf(answer);
        });
    }
    @Override public boolean hasPreparedModels() {
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,false,session -> {
            var control=marker(claim(session));if(control.get("targets")==null)return false;
            var manifest=documents(control.get("targets"));verifyClosure(session,manifest,control.getString("preparationId"));
            if(manifest.isEmpty())return false;
            for(var input:manifest) {
                var raw=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,new Document("_id",id(decodeInput(input).key()))).first();
                if(storedModel(raw)!=null)return true;
            }
            return false;
        });
    }
    @Override public Optional<Prepared> retainedModel(Key key,OnFullLoad policy,boolean fullLoad,PipelinePreparationInputs authoring) {
        return Optional.ofNullable(retainedModels(List.of(key),policy,fullLoad,authoring).get(key));
    }
    @Override public Map<Key,Prepared> retainedModels(Collection<Key> keys,OnFullLoad policy,boolean fullLoad,PipelinePreparationInputs authoring) {
        var captured=List.copyOf(keys);for(var key:captured)if(!access.scope.equals(key.scope()))throw fenced();requireWriter();
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,false,session -> {
            var control=marker(claim(session));if(control.get("targets")==null)return Map.of();
            var manifest=documents(control.get("targets"));var rows=verifyClosure(session,manifest,control.getString("preparationId"));
            var result=new LinkedHashMap<Key,Prepared>();boolean sameAuthoring=false;
            boolean hasBody=captured.stream().map(rows::get).filter(Objects::nonNull).anyMatch(row -> storedModel(row)!=null);
            if(hasBody && authoring!=null)sameAuthoring=MongoPipelineSourcePreparations.retained(session,access.database,access.scope).equals(authoring);
            for(var key:captured) {
                var raw=rows.get(key);if(raw==null)continue;var held=decode(raw);var original=held.input();
                if(original.policy()!=policy || original.fullLoad()!=fullLoad)throw rebuild(access.scope);
                var bytes=storedModel(raw);if(bytes!=null && sameAuthoring)result.put(key,new Prepared(original,bytes));
            }
            return Map.copyOf(result);
        });
    }
    @Override public void prepareModels(List<Prepared> targets) {prepareTargets(targets.stream().map(Prepared::input).toList(),targets);}
    @Override public void prepareTargets(List<Input> originalTargets,List<Prepared> originalModels) {
        var captured=List.copyOf(originalTargets);var sorted=new TreeMap<String,Input>();var bodies=new HashMap<String,byte[]>();long total=0;
        for(var input:captured){requireScope(input);if(sorted.put(id(input.key()).toJson(),input)!=null)throw new IllegalArgumentException("target preparation cannot repeat a physical target");}
        for(var model:List.copyOf(originalModels)) {
            validateModel(model);String key=id(model.input().key()).toJson();
            if(!model.input().equals(sorted.get(key)) || bodies.put(key,model.canonical())!=null)throw new IllegalArgumentException("prepared body does not match the complete roster");
            total+=model.canonical().length;
        }
        requireScopeBudget(total);requireWriter();var manifest=sorted.values().stream().map(MongoSinkPreparationStore::input).toList();
        access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS,true,session -> {
            var table=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database);var claim=claim(session);var before=marker(claim);String operation=before.getString("preparationId");
            var originals=new LinkedHashMap<Document,Document>();var replacements=new LinkedHashMap<Document,Document>();
            if(before.get("targets")!=null) {
                if(!manifest.equals(before.get("targets")))throw rebuild(access.scope);verifyClosure(session,manifest,operation);
                long retainedTotal=0;
                for(var entry:sorted.entrySet()) {
                    var input=entry.getValue();var raw=table.find(session,new Document("_id",id(input.key()))).first();var held=decode(raw);
                    if(!held.input().equals(input) || !held.operationId().equals(operation))throw rebuild(access.scope);
                    var existing=storedModel(raw);var proposed=bodies.get(entry.getKey());
                    if(existing!=null && proposed!=null && !Arrays.equals(existing,proposed))throw rebuild(access.scope);
                    var bytes=existing!=null?existing:proposed;if(bytes!=null)retainedTotal+=bytes.length;
                    var upgraded=row(held,bytes);if(bytes!=null)requirePublicationBudget(held,bytes);
                    originals.put(id(input.key()),raw);replacements.put(id(input.key()),upgraded);
                }
                requireScopeBudget(retainedTotal);
                for(var entry:replacements.entrySet())if(!entry.getValue().equals(originals.get(entry.getKey())))
                    if(table.replaceOne(session,originals.get(entry.getKey()),entry.getValue()).getMatchedCount()!=1)throw fenced();
                return null;
            }
            if(table.find(session,scopeFilter(access.scope)).first()!=null)throw rebuild(access.scope);
            for(var entry:sorted.entrySet()) {
                var input=entry.getValue();var held=new Snapshot(input,operation,0,Phase.PREPARING,null,null);var bytes=bodies.get(entry.getKey());
                if(bytes!=null)requirePublicationBudget(held,bytes);replacements.put(id(input.key()),row(held,bytes));
            }
            for(var encoded:replacements.values())table.insertOne(session,encoded);
            var next=new Document(before).append("targets",manifest);
            if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(access.database).updateOne(session,
                    new Document("_id",MongoWorkloadClaimStore.id(access.originalClaim().key())).append("sinkPreparations",before),
                    new Document("$set",new Document("sinkPreparations",next))).getMatchedCount()!=1)throw fenced();return null;
        });
    }
    private void requirePublicationBudget(Snapshot held,byte[] bytes) {
        requireRowBudget(row(held,bytes),held.input().key().target());
        if(held.phase()==Phase.PREPARING) {
            var originalClaim=held.revision()==0?access.originalClaim():held.originalClaim();
            if(held.revision()==0)requireRowBudget(row(new Snapshot(held.input(),held.operationId(),1,Phase.PREPARING,originalClaim,null),bytes),held.input().key().target());
            requireRowBudget(row(new Snapshot(held.input(),held.operationId(),2,Phase.READY,originalClaim,new Result(true)),bytes),held.input().key().target());
        }
    }
    private void validateModel(Prepared model) {
        requireScope(model.input());var bytes=model.canonical();
        if(bytes.length>MAX_MODEL_ROW_BYTES)throw capacity(model.input().key().target(),bytes.length,MAX_MODEL_ROW_BYTES);
        try {
            var decoded=PreparedTargetModel.decode(bytes);
            if(!decoded.name().equals(model.input().key().target()) || !PreparedTargetModel.hash(bytes).equals(model.input().modelHash()))throw unreadable();
        }catch(IllegalArgumentException invalid){throw unreadable();}
    }
    private void requireRowBudget(Document row,String target) {
        int bytes=new RawBsonDocument(row,new DocumentCodec()).getByteBuffer().remaining();
        if(bytes>MAX_MODEL_ROW_BYTES)throw capacity(target,bytes,MAX_MODEL_ROW_BYTES);
    }
    private void requireScopeBudget(long bytes){if(bytes>MAX_SCOPE_MODEL_BYTES)throw capacity("all-targets",bytes,MAX_SCOPE_MODEL_BYTES);}
    private TapstateException capacity(String target,long bytes,long limit){return new TapstateException(IoError.SINK_MODEL_CAPACITY,Map.of("pipeline",access.scope.pipelineId(),"target",target,"bytes",bytes,"limit",limit),null);}
    private static List<Document> documents(Object value) {
        if(!(value instanceof List<?> list))throw unreadable();var result=new ArrayList<Document>();
        for(Object entry:list){if(!(entry instanceof Document document))throw unreadable();result.add(document);}return result;
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
        var table=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database);
        var raw=table.find(session,new Document("_id",id(before.input().key()))).first();
        if(!decode(raw).equals(before))throw fenced();var bytes=storedModel(raw);
        var replacement=row(next,bytes);if(bytes!=null)requireRowBudget(replacement,next.input().key().target());
        if(table.replaceOne(session,raw,replacement).getMatchedCount()!=1)throw fenced();
    }
    private Snapshot require(ClientSession session,Input expected) {
        var control=marker(claim(session));
        if (!(control.get("targets") instanceof List<?> targets) || !targets.contains(input(expected))) throw rebuild(access.scope);
        var raw=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,new Document("_id",id(expected.key()))).first();
        Snapshot held=decode(raw); if (!held.input().equals(expected) || !held.operationId().equals(control.getString("preparationId"))) throw rebuild(access.scope);
        if(storedModel(raw)!=null)requireRowBudget(raw,held.input().key().target());
        return held;
    }
    private Map<Key,Document> verifyClosure(ClientSession session,List<Document> targets,String operation) {
        var expected=new HashSet<>(targets);long total=0;var result=new LinkedHashMap<Key,Document>();
        try (var rows=SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session,scopeFilter(access.scope)).iterator()) {
            while(rows.hasNext()) {var raw=rows.next();var held=decode(raw);var bytes=storedModel(raw);if(bytes!=null){requireRowBudget(raw,held.input().key().target());total+=bytes.length;} if (!held.operationId().equals(operation) || !expected.remove(input(held.input()))) throw rebuild(access.scope);result.put(held.input().key(),raw); }
        }
        if (!expected.isEmpty()) throw rebuild(access.scope);
        requireScopeBudget(total);return result;
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
    private static Document row(Snapshot value,byte[] canonical) {
        var row=row(value);if(canonical!=null)row.put("targetModel",new Document("formatVersion",1).append("canonical",new Binary(canonical)));
        if(canonical!=null)row.put("formatVersion",2);return row;
    }
    private static byte[] storedModel(Document row) {
        if(Integer.valueOf(1).equals(row.get("formatVersion")))return null;
        try {
            var body=row.get("targetModel",Document.class);
            if(body==null || !body.keySet().equals(Set.of("formatVersion","canonical")) || !Integer.valueOf(1).equals(body.get("formatVersion"))
                    || !(body.get("canonical") instanceof Binary binary) || binary.getType()!=0)throw unreadable();
            var bytes=binary.getData();var model=PreparedTargetModel.decode(bytes);var input=decodeInput(row.get("input",Document.class));
            if(!model.name().equals(input.key().target()) || !PreparedTargetModel.hash(bytes).equals(input.modelHash()))throw unreadable();return bytes.clone();
        }catch(IllegalArgumentException|ClassCastException invalid){throw unreadable();}
    }
    private static Snapshot decode(Document value) {
        if(value==null) throw unreadable();
        try {
            if ((!Integer.valueOf(1).equals(value.get("formatVersion")) && !Integer.valueOf(2).equals(value.get("formatVersion"))) || !(value.get("revision") instanceof Long revision)) throw unreadable();
            Phase phase=switch(value.getString("phase")) { case "PREPARING" -> Phase.PREPARING; case "READY" -> Phase.READY; default -> throw unreadable(); };
            var original=value.get("originalClaim",Document.class); var result=value.get("result",Document.class);
            if (result!=null && !(result.get("created") instanceof Boolean)) throw unreadable();
            var decoded=new Snapshot(decodeInput(value.get("input",Document.class)),value.getString("operationId"),revision,phase,
                    original==null?null:WorkloadClaimDocuments.readPipeline(original),result==null?null:new Result(result.getBoolean("created")));
            if (!row(decoded,storedModel(value)).equals(value)) throw unreadable(); return decoded;
        } catch (IllegalArgumentException|ClassCastException|NullPointerException bad) { throw unreadable(); }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    private static TapstateException rebuild(PipelineIntentScope scope) { return new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","sink-preparation","field","preparation"),null); }
}
