package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import java.util.*;
import org.bson.Document;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertConfiguration;

/** Bounded positive facts belonging only to the current alert transaction callback. */
final class MongoAlertWriteFacts {
    static final List<SystemCollections> ARTIFACTS = List.of(SystemCollections.SOURCES, SystemCollections.PIPELINES,
            SystemCollections.TRANSFORM_DEFINITIONS, SystemCollections.VIEW_DEFINITIONS, SystemCollections.SERVE_DEFINITIONS);
    private final Document captured;
    private final MongoDatabase database;
    private final ClientSession session;

    private MongoAlertWriteFacts(Document captured, MongoDatabase database, ClientSession session) {
        this.captured=captured;this.database=database;this.session=session;
    }
    record Request(AlarmKey key,AlertConfiguration.Identity rule) { }
    static MongoAlertWriteFacts read(MongoDatabase database, ClientSession session,Request request) {
        var stages=new ArrayList<Document>();
        stages.add(new Document("$match",new Document("_id","cluster")));
        stages.add(new Document("$project",new Document("_id",0).append("identity",MongoStartConfigRead.boundedValue())));
        var tables=new ArrayList<SystemCollections>();
        tables.addAll(List.of(SystemCollections.STORAGE_SETUP,SystemCollections.STORAGE_BINDING,SystemCollections.STORAGE_RECOVERY,
                SystemCollections.SCHEMA_MIGRATIONS,SystemCollections.CONFIGURATION_VERSION,SystemCollections.PDK_NOTE_SCOPES,
                SystemCollections.ALERT_INSTANCES,SystemCollections.ALERT_EPISODES,SystemCollections.ALERT_TRANSITIONS,SystemCollections.CLUSTER_KEYS,SystemCollections.ALERT_RULES,SystemCollections.OPERATOR_GENERATIONS));
        tables.addAll(ARTIFACTS);
        for(var table:tables) stages.add(new Document("$lookup",new Document("from",table.collectionName())
                .append("as",table.collectionName()).append("pipeline",List.of(new Document("$limit",2),
                        new Document("$project",new Document("_id",0).append("value",MongoStartConfigRead.boundedValue()))))));
        if(request!=null) {
            var wanted=new Document("$setUnion",List.of(new Document("$literal",List.of(request.key().resource().id())),new Document("$ifNull",List.of("$storage_binding.value.defaultSourceId",List.of()))));
            for(var table:ARTIFACTS)stages.add(new Document("$lookup",new Document("from",table.collectionName())
                    .append("as",table.collectionName()+"Known").append("let",new Document("ids",wanted))
                    .append("pipeline",List.of(new Document("$match",new Document("$expr",new Document("$in",List.of("$_id","$$ids")))),
                            new Document("$limit",4),new Document("$project",new Document("_id",0).append("value",MongoStartConfigRead.boundedValue()))))));
            positive(stages,SystemCollections.ALERT_INSTANCES,MongoAlertStateCodec.key(request.key()));
            if(request.rule()!=null)positive(stages,SystemCollections.ALERT_RULES,MongoAlertConfigurationCodec.key(request.rule().scope(),request.rule().id()));
            if(request.key().resource().kind().equals("pipeline"))positive(stages,SystemCollections.OPERATOR_GENERATIONS,request.key().resource().id());
        }
        var row=SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).aggregate(session,stages).first();
        return row==null||!(row.get("identity") instanceof Document)?null:new MongoAlertWriteFacts(row,database,session);
    }
    private static void positive(List<Document> stages,SystemCollections table,Object id){
        stages.add(new Document("$lookup",new Document("from",table.collectionName()).append("as",table.collectionName()+"Known")
                .append("pipeline",List.of(new Document("$match",new Document("_id",id)),new Document("$limit",2),
                        new Document("$project",new Document("_id",0).append("value",MongoStartConfigRead.boundedValue()))))));
    }
    Document identity(){return captured.get("identity",Document.class);}
    boolean singletonRecovery(){
        var values=captured.getList(SystemCollections.STORAGE_RECOVERY.collectionName(),Document.class);
        return values.size()==1&&values.getFirst().get("value") instanceof Document row&&"cluster".equals(row.get("_id"));
    }
    boolean empty(SystemCollections table){return captured.getList(table.collectionName(),Document.class).isEmpty();}
    Document row(SystemCollections table,Object id) {
        if(captured.get(table.collectionName()+"Known") instanceof List<?> known) {
            Document found=null;
            for(Object item:known)if(item instanceof Document wrapped&&wrapped.get("value") instanceof Document row&&exact(id,row.get("_id"))) {
                if(found!=null){found=null;break;}found=row;
            }
            if(found!=null)return found;
        }
        var values=captured.getList(table.collectionName(),Document.class);
        if(values.isEmpty())return null;
        if(values.size()==1&&values.getFirst().get("value") instanceof Document row&&exact(id,row.get("_id")))return row;
        // A populated ambiguous namespace evaluates its predicate under its own collation.
        return table.on(database).find(session,new Document("_id",id)).first();
    }
    private static boolean exact(Object expected,Object actual){
        if(expected instanceof Document key){
            if(!(actual instanceof Document value)||!new ArrayList<>(key.keySet()).equals(new ArrayList<>(value.keySet())))return false;
            return key.entrySet().stream().allMatch(entry->exact(entry.getValue(),value.get(entry.getKey())));
        }
        if(expected instanceof List<?> left){
            if(!(actual instanceof List<?> right)||left.size()!=right.size())return false;
            for(int i=0;i<left.size();i++)if(!exact(left.get(i),right.get(i)))return false;
            return true;
        }
        if(expected==null)return actual==null;
        return expected.equals(actual);
    }
}
