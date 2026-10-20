package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;

/** Strict lifecycle facts contain no credentials, note payloads or lease deadlines. */
final class MongoPipelineRetirementFacts {
    private static final Set<String> FIELDS=Set.of("formatVersion","operationId","scope","definitionHash","originalCache","privateScopesHash","privateScopesCount","phase","predecessor","cacheResult");
    record Fact(PipelineRetirementRequest request,String notesHash,int notesCount,String phase,Optional<PipelineRetiredState> predecessor,Optional<PipelineRetiredState> result) {
        Fact {
            Objects.requireNonNull(request);predecessor=Objects.requireNonNull(predecessor);result=Objects.requireNonNull(result);
            if(notesHash==null || !notesHash.matches("[a-f0-9]{64}") || notesCount<0 || !Set.of("RETIRING","COMPLETE").contains(phase)
                    || phase.equals("RETIRING") && result.isPresent() || request.cache().isPresent() && predecessor.isPresent())throw new IllegalArgumentException("invalid retirement fact");
            predecessor.ifPresent(value -> requireDomain(request.scope(),value.scope()));result.ifPresent(value -> requireDomain(request.scope(),value.scope()));
            predecessor.ifPresent(value -> {
                if(value.scope().stateGeneration()>=request.scope().stateGeneration())throw new IllegalArgumentException("predecessor is not an earlier resource");
            });
            if(phase.equals("COMPLETE")) {
                if(request.cache().isEmpty()) {
                    if(!result.equals(predecessor))throw new IllegalArgumentException("unassigned retirement changed known history");
                } else {
                    var before=request.cache().orElseThrow();var after=result.orElseThrow(() -> new IllegalArgumentException("known retirement has no result"));
                    if(!after.scope().equals(request.scope()) || !after.operationId().equals(request.operationId())
                            || after.claim().claimGeneration()<=before.claim().claimGeneration()
                            || after.claim().executionGeneration()<before.claim().executionGeneration()
                            || after.epoch()<=before.epoch())
                        throw new IllegalArgumentException("retirement result does not continue original history");
                }
            }
        }
        PipelineRetirementResult completed() {
            if(!phase.equals("COMPLETE"))throw fenced();
            return new PipelineRetirementResult(request.operationId(),request.scope(),request.definitionHash(),notesHash,notesCount,result);
        }
    }
    private MongoPipelineRetirementFacts(){}
    static Fact read(Document mapping) {
        try {
            if(!(mapping.get("retirement") instanceof Document row) || !row.keySet().equals(FIELDS) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !(row.get("privateScopesCount") instanceof Integer count))throw unreadable();
            var scope=MongoPipelinePreparationFacts.scope(row.get("scope",Document.class));
            if(!scope.pipelineId().equals(mapping.get("_id")))throw unreadable();
            Optional<PipelineRetirementRequest.CacheOriginal> original=Optional.empty();
            if(row.get("originalCache")!=null) {
                var cached=row.get("originalCache",Document.class);
                if(!cached.keySet().equals(Set.of("claim","epoch")) || !(cached.get("epoch") instanceof Long epoch))throw unreadable();
                original=Optional.of(new PipelineRetirementRequest.CacheOriginal(WorkloadClaimDocuments.readPipeline(cached.get("claim",Document.class)),epoch));
            }
            var request=new PipelineRetirementRequest(row.getString("operationId"),scope,row.getString("definitionHash"),original);
            return new Fact(request,row.getString("privateScopesHash"),count,row.getString("phase"),optional(row,"predecessor"),optional(row,"cacheResult"));
        }catch(IllegalArgumentException|ClassCastException|NullPointerException invalid){throw unreadable();}
    }
    static Document encode(Fact fact) {
        var request=fact.request();Document original=null;
        if(request.cache().isPresent()) {
            var value=request.cache().orElseThrow();original=new Document("claim",WorkloadClaimDocuments.stored(value.claim())).append("epoch",value.epoch());
        }
        return new Document("formatVersion",1).append("operationId",request.operationId()).append("scope",MongoPipelinePreparationFacts.scope(request.scope()))
                .append("definitionHash",request.definitionHash()).append("originalCache",original).append("privateScopesHash",fact.notesHash())
                .append("privateScopesCount",fact.notesCount()).append("phase",fact.phase())
                .append("predecessor",fact.predecessor().map(MongoPipelineRetirementFacts::cache).orElse(null))
                .append("cacheResult",fact.result().map(MongoPipelineRetirementFacts::cache).orElse(null));
    }
    static Document cache(PipelineRetiredState value) {
        return new Document("scope",MongoPipelinePreparationFacts.scope(value.scope())).append("operationId",value.operationId())
                .append("claim",WorkloadClaimDocuments.stored(value.claim())).append("epoch",value.epoch()).append("writeFence",value.writeFence())
                .append("preparationId",value.preparationId()).append("inventoryHash",value.inventoryHash());
    }
    static PipelineRetiredState cache(Document row) {
        try {
            if(row==null || !row.keySet().equals(Set.of("scope","operationId","claim","epoch","writeFence","preparationId","inventoryHash"))
                    || !(row.get("epoch") instanceof Long epoch) || !(row.get("writeFence") instanceof Long write))throw unreadable();
            return new PipelineRetiredState(MongoPipelinePreparationFacts.scope(row.get("scope",Document.class)),row.getString("operationId"),
                    WorkloadClaimDocuments.readPipeline(row.get("claim",Document.class)),epoch,write,row.getString("preparationId"),row.getString("inventoryHash"));
        }catch(IllegalArgumentException|ClassCastException|NullPointerException invalid){throw unreadable();}
    }
    private static Optional<PipelineRetiredState> optional(Document row,String field) {
        return row.get(field)==null?Optional.empty():Optional.of(cache(row.get(field,Document.class)));
    }
    static String notesHash(List<RequiredNoteScope> scopes) {
        var encoded=scopes.stream().sorted(Comparator.comparing(value -> value.owner().namespace())).map(value -> List.of(
                value.owner().bindingVersion(),value.owner().namespace(),value.generation())).toList();
        return CanonicalHash.ofText(JsonWriter.write(encoded));
    }
    static boolean sameBusiness(PipelineIntentScope a,PipelineIntentScope b) {
        return a.clusterId().equals(b.clusterId()) && a.bindingVersion()==b.bindingVersion() && a.cacheIncarnation().equals(b.cacheIncarnation());
    }
    private static void requireDomain(PipelineIntentScope current,PipelineIntentScope old) {
        if(!sameBusiness(current,old) || !current.pipelineId().equals(old.pipelineId()) || old.stateGeneration()>current.stateGeneration())throw new IllegalArgumentException("retirement crosses lifecycle");
    }
    static TapstateException fenced(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
    static TapstateException unreadable(){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","pipeline","field","retirement"),null);}
}
