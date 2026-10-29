package io.tapstate.adapters.mongostore;

import java.util.*;
import org.bson.Document;

/** Recognizes typed optional envelopes before decoding, including unknown-reference protection. */
final class MongoAlertEnvelopeSupport {
    private static final String CHANNEL_ID="^(?=.{1,64}$)[a-z][a-z0-9]*([.-][a-z0-9]+)*$";
    private static final List<String> RESERVED=List.of("email","webhook","cloud-sdk");
    private MongoAlertEnvelopeSupport() { }
    static Document intentFilter(){
        return new Document("$and",List.of(new Document("formatVersion",new Document("$type","int")),new Document("$or",List.of(new Document("formatVersion",new Document("$in",List.of(1,2))),
                new Document("formatVersion",3).append("channelType",channelId())
                        .append("frozen.target.kind",new Document("$in",List.of("receiver","candidate")))
                        .append("frozen.target.transport.family","HTTPS_JSON").append("frozen.target.transport.transportVersion",new Document("$eq",1).append("$type","int"))
                        .append("$expr",new Document("$eq",List.of("$channelType","$frozen.target.transport.channelType")))))));
    }
    static Document receiverFilter(){
        return new Document("$and",List.of(new Document("formatVersion",new Document("$type","int")),new Document("$or",List.of(new Document("formatVersion",1),new Document("formatVersion",2)
                .append("body.transport.family","HTTPS_JSON").append("body.transport.transportVersion",new Document("$eq",1).append("$type","int")).append("body.transport.channelType",channelId())))));
    }
    private static Document channelId(){return new Document("$type","string").append("$nin",RESERVED).append("$regex",CHANNEL_ID);}
    static boolean intent(Document row){
        if(Integer.valueOf(1).equals(row.get("formatVersion"))||Integer.valueOf(2).equals(row.get("formatVersion")))return true;
        if(!Integer.valueOf(3).equals(row.get("formatVersion"))||!(row.get("channelType") instanceof String channel)
                ||RESERVED.contains(channel)||!channel.matches(CHANNEL_ID))return false;
        if(!(row.get("frozen") instanceof Document frozen)||!(frozen.get("target") instanceof Document target)
                ||!("receiver".equals(target.get("kind"))||"candidate".equals(target.get("kind")))||!(target.get("transport") instanceof Document transport))return false;
        return "HTTPS_JSON".equals(transport.get("family"))&&Integer.valueOf(1).equals(transport.get("transportVersion"))&&channel.equals(transport.get("channelType"));
    }
}
