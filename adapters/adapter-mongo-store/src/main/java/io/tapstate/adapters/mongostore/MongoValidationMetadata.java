package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.util.*;
import org.bson.Document;

/** Server-side projection and byte gate precede driver materialization of validation control facts. */
final class MongoValidationMetadata {
    private MongoValidationMetadata(){}
    static Document claimProjection(String...annex){
        var fields=new Document();for(String field:List.of("clusterId","resourceType","resourceId","ownerNodeId","ownerBootId","claimGeneration","executionGeneration","topologyRevision","leaseUntil","contextExecutionGeneration","executionClaimGeneration","executionNodeIds","failureClaimGeneration","failureAfterMemberLoss"))fields.append(field,1);
        for(String field:annex)fields.append(field,1);return fields;
    }
    static Document read(ClientSession session,MongoCollection<Document> rows,Document filter,Document projection,int maximum){
        var pipeline=new ArrayList<Document>();pipeline.add(new Document("$match",filter));pipeline.add(new Document("$limit",1));
        if(projection!=null)pipeline.add(new Document("$project",projection));
        var bytes=new Document("$bsonSize","$$ROOT");
        pipeline.add(new Document("$project",new Document("_id",0).append("bytes",bytes)
                .append("value",new Document("$cond",List.of(new Document("$lte",List.of(bytes,maximum)),"$$ROOT",nullValue())))));
        var checked=rows.aggregate(session,pipeline).first();if(checked==null)return null;
        if(!(checked.get("bytes") instanceof Number size)||size.longValue()<1||size.longValue()>maximum||!(checked.get("value") instanceof Document value))
            throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.controlMetadata"),null);
        return value;
    }
    private static Document nullValue(){return new Document("$literal",null);}
}
