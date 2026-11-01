package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;

/** One strict immutable CONFIG birth record and its same-content CACHE claim annex. */
final class MongoCapturePreparationFacts {
    private static final Set<String> BIRTH_FIELDS=Set.of("_id","formatVersion","phase","mode","chain","captureId","sourceIdentityHash","preparationId");
    private MongoCapturePreparationFacts(){}
    static Document scope(CacheBusinessScope scope){return new Document("clusterId",scope.clusterId()).append("bindingVersion",scope.bindingVersion()).append("cacheIncarnation",scope.cacheIncarnation());}
    static Document id(CacheBusinessScope scope,String capture){return scope(scope).append("capture",OperatorStateText.encode(capture));}
    static Document row(CapturePreparation value){
        if(value.replacement()) {
            if(value.chainId().matches("srs-clear-[a-f0-9]{64}"))
                return new Document("formatVersion",3).append("original",row(value.birth()))
                        .append("physicalChain",value.chainId()).append("preparationId",value.preparationId());
            if(!replacementChain(value.birth(),value.preparationId()).equals(value.chainId()))throw unreadable();
            return new Document("formatVersion",2).append("original",row(value.birth()))
                    .append("physicalChain",value.chainId()).append("preparationId",value.preparationId());
        }
        return new Document("_id",id(value.scope(),value.captureId())).append("formatVersion",1).append("phase","ISSUED")
            .append("mode",value.mode().name()).append("chain",OperatorStateText.encode(value.chainId())).append("captureId",value.captureId()).append("sourceIdentityHash",value.sourceIdentityHash()).append("preparationId",value.preparationId());}
    static CapturePreparation decode(Document row) {
        if(row!=null && Integer.valueOf(4).equals(row.get("formatVersion")))return taggedRecovery(row).replacement();
        if(row!=null && Integer.valueOf(3).equals(row.get("formatVersion")))
            return row.containsKey("currentClear")?clearConfiguration(row,false):clearAnnex(row);
        if(row!=null && Integer.valueOf(2).equals(row.get("formatVersion"))) {
            if(row.containsKey("currentRecovery"))return recovery(row).orElseThrow(MongoCapturePreparationFacts::unreadable).replacement();
            return replacementAnnex(row);
        }
        return birth(row);
    }
    private static CapturePreparation birth(Document row) {
        try {
            if(row==null || !(row.get("_id")instanceof Document id) || !(id.get("bindingVersion")instanceof Long binding)
                    || !Integer.valueOf(1).equals(row.get("formatVersion")))throw unreadable();
            var scope=new CacheBusinessScope(id.getString("clusterId"),binding,id.getString("cacheIncarnation"));
            CapturePreparation.Mode mode=switch(row.getString("mode")){case "SHARED_SRS" -> CapturePreparation.Mode.SHARED_SRS;case "DIRECT" -> CapturePreparation.Mode.DIRECT;case "SNAPSHOT_ONLY" -> CapturePreparation.Mode.SNAPSHOT_ONLY;default -> throw unreadable();};
            var decoded=new CapturePreparation(scope,mode,OperatorStateText.decode(row.getString("chain")),row.getString("captureId"),row.getString("sourceIdentityHash"),row.getString("preparationId"));
            if(!row(decoded).equals(row) || !decoded.captureId().equals(OperatorStateText.decode(id.getString("capture"))))throw unreadable();return decoded;
        }catch(IllegalArgumentException|NullPointerException|ClassCastException bad){throw unreadable();}
    }
    static CapturePreparation available(Document row) {
        if(row!=null && Integer.valueOf(3).equals(row.get("formatVersion")))return clearConfiguration(row,true);
        var retained=recovery(row);
        if(retained.isEmpty())return birth(row);
        var original=retained.orElseThrow();
        if(original.phase()!=RecoveryPhase.READY)throw history(original.replacement().captureId());
        return original.replacement();
    }
    static CapturePreparation annex(Document row) {
        if(row!=null && Integer.valueOf(3).equals(row.get("formatVersion")))return clearAnnex(row);
        return row!=null && Integer.valueOf(2).equals(row.get("formatVersion"))?replacementAnnex(row):birth(row);
    }
    private static CapturePreparation replacementAnnex(Document row) {
        try {
            if(!row.keySet().equals(Set.of("formatVersion","original","physicalChain","preparationId")))throw unreadable();
            var original=birth(row.get("original",Document.class));
            var replacement=replacement(original,row.getString("physicalChain"),row.getString("preparationId"));
            if(!row(replacement).equals(row))throw unreadable();return replacement;
        }catch(IllegalArgumentException|NullPointerException|ClassCastException bad){throw unreadable();}
    }
    private static CapturePreparation clearAnnex(Document row) {
        try {
            if(row==null || !row.keySet().equals(Set.of("formatVersion","original","physicalChain","preparationId"))
                    || !Integer.valueOf(3).equals(row.get("formatVersion")))throw unreadable();
            var birth=birth(row.get("original",Document.class));
            var result=new CapturePreparation(birth.scope(),birth.mode(),row.getString("physicalChain"),birth.captureId(),
                    birth.sourceIdentityHash(),row.getString("preparationId"),birth.canonicalChainId(),birth.birthPreparationId());
            if(!result.chainId().matches("srs-clear-[a-f0-9]{64}") || !row(result).equals(row))throw unreadable();
            return result;
        }catch(IllegalArgumentException|NullPointerException|ClassCastException invalid){throw unreadable();}
    }
    private static CapturePreparation clearConfiguration(Document row,boolean requireReady) {
        try {
            if(!row.keySet().equals(Set.of("_id","formatVersion","original","currentClear")))throw unreadable();
            var birth=birth(row.get("original",Document.class));var marker=row.get("currentClear",Document.class);
            if(marker!=null)requireBytes(marker,4096);
            if(marker==null || !marker.keySet().equals(Set.of("operationId","sequence","previousChain","previousPreparationId",
                    "nextChain","nextPreparationId","phase","receiptHash")) || !(marker.get("sequence")instanceof Long sequence)
                    || sequence<1 || sequence==Long.MAX_VALUE || !id(birth.scope(),birth.captureId()).equals(row.get("_id")))throw unreadable();
            String phase=marker.getString("phase");
            if(!Set.of("RESERVED","READY").contains(phase))throw unreadable();
            if(requireReady && !"READY".equals(phase))throw new TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING,
                    Map.of("capture",birth.captureId(),"operation",marker.getString("operationId"),"reason","clear-successor-not-ready"),null);
            var previous=new CapturePreparation(birth.scope(),birth.mode(),marker.getString("previousChain"),birth.captureId(),
                    birth.sourceIdentityHash(),marker.getString("previousPreparationId"),birth.canonicalChainId(),birth.birthPreparationId());
            var result=new CapturePreparation(birth.scope(),birth.mode(),marker.getString("nextChain"),birth.captureId(),
                    birth.sourceIdentityHash(),marker.getString("nextPreparationId"),birth.canonicalChainId(),birth.birthPreparationId());
            if(!result.preparationId().equals(marker.getString("operationId"))
                    || !result.chainId().equals(MongoCaptureExternalRetirements.clearChain(previous,result.preparationId()))
                    || "READY".equals(phase) && !(marker.get("receiptHash")instanceof String hash && hash.matches("[a-f0-9]{64}")))throw unreadable();
            return result;
        }catch(IllegalArgumentException|NullPointerException|ClassCastException invalid){throw unreadable();}
    }
    enum RecoveryPhase { RESERVED, READY }
    record Recovery(CapturePreparation previous,CapturePreparation replacement,String planHash,RecoveryPhase phase) {
        Recovery {
            Objects.requireNonNull(previous);Objects.requireNonNull(replacement);Objects.requireNonNull(phase);
            if(!previous.birth().equals(replacement.birth()) || !replacement.replacement()
                    || previous.chainId().equals(replacement.chainId()) || previous.preparationId().equals(replacement.preparationId())
                    || planHash==null || !planHash.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("invalid capture replacement");
            row(previous);row(replacement);
        }
    }
    static Optional<Recovery> recovery(Document row) {
        try {
            if(row!=null && Integer.valueOf(4).equals(row.get("formatVersion")))return Optional.of(taggedRecovery(row));
            if(row!=null && Integer.valueOf(1).equals(row.get("formatVersion"))){birth(row);return Optional.empty();}
            var expected=new HashSet<>(BIRTH_FIELDS);expected.add("currentRecovery");
            if(row==null || !Integer.valueOf(2).equals(row.get("formatVersion")) || !row.keySet().equals(expected))throw unreadable();
            var originalRow=new Document(row);originalRow.remove("currentRecovery");originalRow.put("formatVersion",1);
            var original=birth(originalRow);var value=row.get("currentRecovery",Document.class);
            if(value==null || !value.keySet().equals(Set.of("formatVersion","operationId","planHash","previousPhysicalChain","previousPreparationId","physicalChain","phase"))
                    || !Integer.valueOf(1).equals(value.get("formatVersion")))throw unreadable();
            var next=replacement(original,value.getString("physicalChain"),value.getString("operationId"));
            String previousChain=value.getString("previousPhysicalChain"),previousId=value.getString("previousPreparationId");
            var previous=original.chainId().equals(previousChain)?original:replacement(original,previousChain,previousId);
            if(!previous.preparationId().equals(previousId))throw unreadable();
            var result=new Recovery(previous,next,value.getString("planHash"),RecoveryPhase.valueOf(value.getString("phase")));
            if(!configurationRow(result).equals(row))throw unreadable();return Optional.of(result);
        }catch(IllegalArgumentException|NullPointerException|ClassCastException bad){throw unreadable();}
    }
    static Document configurationRow(Recovery value) {
        if(value.previous().chainId().matches("srs-clear-[a-f0-9]{64}")) {
            var row=new Document("_id",id(value.previous().scope(),value.previous().captureId())).append("formatVersion",4)
                    .append("original",row(value.previous().birth()))
                    .append("currentRecovery",new Document("formatVersion",2).append("operationId",value.replacement().preparationId())
                            .append("planHash",value.planHash()).append("previous",row(value.previous())).append("replacement",row(value.replacement()))
                            .append("phase",value.phase().name()));
            requireBytes(row,8192);return row;
        }
        return legacyConfigurationRow(value);
    }
    /** Unchanged legacy wire form; a clear predecessor remains invalid to its original reader. */
    static Document legacyConfigurationRow(Recovery value) {
        var row=row(value.replacement().birth());row.put("formatVersion",2);
        row.put("currentRecovery",new Document("formatVersion",1).append("operationId",value.replacement().preparationId())
                .append("planHash",value.planHash()).append("previousPhysicalChain",value.previous().chainId())
                .append("previousPreparationId",value.previous().preparationId()).append("physicalChain",value.replacement().chainId()).append("phase",value.phase().name()));
        return row;
    }
    private static Recovery taggedRecovery(Document row) {
        try {
            requireBytes(row,8192);
            if(!Integer.valueOf(4).equals(row.get("formatVersion")) || !row.keySet().equals(Set.of("_id","formatVersion","original","currentRecovery")))throw unreadable();
            var original=birth(row.get("original",Document.class));var control=row.get("currentRecovery",Document.class);
            if(control==null || !control.keySet().equals(Set.of("formatVersion","operationId","planHash","previous","replacement","phase"))
                    || !Integer.valueOf(2).equals(control.get("formatVersion")))throw unreadable();
            var previous=clearAnnex(control.get("previous",Document.class));
            var replacement=replacementAnnex(control.get("replacement",Document.class));
            if(!original.equals(previous.birth()) || !original.equals(replacement.birth()) || !row.get("_id").equals(id(original.scope(),original.captureId()))
                    || !replacement.preparationId().equals(control.getString("operationId")))throw unreadable();
            var result=new Recovery(previous,replacement,control.getString("planHash"),RecoveryPhase.valueOf(control.getString("phase")));
            if(!configurationRow(result).equals(row))throw unreadable();return result;
        }catch(IllegalArgumentException|ClassCastException|NullPointerException invalid){throw unreadable();}
    }
    static void requireBytes(Document row,int limit) {
        if(MongoCaptureExternalRetirements.bytes(row)>limit)throw new TapstateException(IoError.CAPTURE_RETIREMENT_CAPACITY,
                Map.of("capture","capture-preparation","operation","encoding","reason","preparation-bson-bytes"),null);
    }
    static String replacementChain(CapturePreparation original,String operationId) {
        if(!UUID.fromString(operationId).toString().equals(operationId))throw new IllegalArgumentException("invalid replacement operation");
        var scope=original.scope();return "srs-recovery-"+CanonicalHash.ofText(JsonWriter.write(List.of(
                "shared-srs-replacement",scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),original.canonicalChainId(),operationId)));
    }
    private static CapturePreparation replacement(CapturePreparation original,String physical,String operationId) {
        if(original.mode()!=CapturePreparation.Mode.SHARED_SRS || !replacementChain(original,operationId).equals(physical))throw unreadable();
        return new CapturePreparation(original.scope(),original.mode(),physical,original.captureId(),original.sourceIdentityHash(),operationId,
                original.canonicalChainId(),original.birthPreparationId());
    }
    static void verify(MongoDatabase database) {
        var table=SystemCollections.CAPTURE_PREPARATIONS;
        var found=database.listCollections().filter(new Document("name",table.collectionName())).first();
        if(found==null || !"collection".equals(found.get("type")))throw unreadable();
        var options=found.get("options",Document.class);
        if(options!=null && (Boolean.TRUE.equals(options.get("capped")) || options.containsKey("timeseries") || options.containsKey("collation")))throw unreadable();
        var indexes=table.on(database).listIndexes().into(new ArrayList<>());
        if(indexes.stream().anyMatch(index -> index.containsKey("expireAfterSeconds")))throw unreadable();
        var id=indexes.stream().filter(index -> "_id_".equals(index.get("name"))).findFirst().orElse(null);
        if(id==null || !new Document("_id",1).equals(id.get("key")) || Boolean.TRUE.equals(id.get("hidden")) || Boolean.TRUE.equals(id.get("sparse")) || id.containsKey("partialFilterExpression"))throw unreadable();
        for(var expected:table.indexes()) {
            var keys=new Document();expected.keys().forEach(key -> keys.append(key,1));
            var foundIndex=indexes.stream().filter(index -> expected.indexName().equals(index.get("name"))).findFirst().orElse(null);
            if(foundIndex==null || !keys.equals(foundIndex.get("key")) || Boolean.TRUE.equals(foundIndex.get("unique"))
                    || Boolean.TRUE.equals(foundIndex.get("hidden")) || Boolean.TRUE.equals(foundIndex.get("sparse"))
                    || foundIndex.containsKey("partialFilterExpression"))throw unreadable();
        }
    }
    static TapstateException unreadable(){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","capture-preparation","field","original-facts"),null);}
    static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
    static TapstateException history(String capture){return new TapstateException(IoError.CAPTURE_STATE_REBUILD_REQUIRED,Map.of("capture",capture),null);}
}
