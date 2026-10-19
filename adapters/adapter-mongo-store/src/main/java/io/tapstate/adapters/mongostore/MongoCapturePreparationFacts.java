package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;

/** One strict immutable CONFIG birth record and its same-content CACHE claim annex. */
final class MongoCapturePreparationFacts {
    private MongoCapturePreparationFacts(){}
    static Document scope(CacheBusinessScope scope){return new Document("clusterId",scope.clusterId()).append("bindingVersion",scope.bindingVersion()).append("cacheIncarnation",scope.cacheIncarnation());}
    static Document id(CacheBusinessScope scope,String capture){return scope(scope).append("capture",OperatorStateText.encode(capture));}
    static Document row(CapturePreparation value){return new Document("_id",id(value.scope(),value.captureId())).append("formatVersion",1).append("phase","ISSUED")
            .append("mode",value.mode().name()).append("chain",OperatorStateText.encode(value.chainId())).append("captureId",value.captureId()).append("sourceIdentityHash",value.sourceIdentityHash()).append("preparationId",value.preparationId());}
    static CapturePreparation decode(Document row) {
        try {
            if(row==null || !(row.get("_id")instanceof Document id) || !(id.get("bindingVersion")instanceof Long binding)
                    || !Integer.valueOf(1).equals(row.get("formatVersion")))throw unreadable();
            var scope=new CacheBusinessScope(id.getString("clusterId"),binding,id.getString("cacheIncarnation"));
            CapturePreparation.Mode mode=switch(row.getString("mode")){case "SHARED_SRS" -> CapturePreparation.Mode.SHARED_SRS;case "DIRECT" -> CapturePreparation.Mode.DIRECT;case "SNAPSHOT_ONLY" -> CapturePreparation.Mode.SNAPSHOT_ONLY;default -> throw unreadable();};
            var decoded=new CapturePreparation(scope,mode,OperatorStateText.decode(row.getString("chain")),row.getString("captureId"),row.getString("sourceIdentityHash"),row.getString("preparationId"));
            if(!row(decoded).equals(row) || !decoded.captureId().equals(OperatorStateText.decode(id.getString("capture"))))throw unreadable();return decoded;
        }catch(IllegalArgumentException|NullPointerException|ClassCastException bad){throw unreadable();}
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
    }
    static TapstateException unreadable(){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","capture-preparation","field","original-facts"),null);}
    static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
    static TapstateException history(String capture){return new TapstateException(IoError.CAPTURE_STATE_REBUILD_REQUIRED,Map.of("capture",capture),null);}
}
