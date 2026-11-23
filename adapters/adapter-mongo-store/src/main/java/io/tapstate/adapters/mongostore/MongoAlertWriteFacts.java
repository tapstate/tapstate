package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import java.util.*;
import org.bson.Document;

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
    static MongoAlertWriteFacts read(MongoDatabase database, ClientSession session) {
        var stages=new ArrayList<Document>();
        stages.add(new Document("$match",new Document("_id","cluster")));
        stages.add(new Document("$project",new Document("_id",0).append("identity",MongoStartConfigRead.boundedValue())));
        var tables=new ArrayList<>(ARTIFACTS);
        tables.addAll(List.of(SystemCollections.STORAGE_SETUP,SystemCollections.STORAGE_BINDING,SystemCollections.STORAGE_RECOVERY,
                SystemCollections.SCHEMA_MIGRATIONS,SystemCollections.CONFIGURATION_VERSION,SystemCollections.PDK_NOTE_SCOPES,
                SystemCollections.ALERT_INSTANCES,SystemCollections.ALERT_EPISODES,SystemCollections.ALERT_TRANSITIONS,SystemCollections.CLUSTER_KEYS));
        for(var table:tables) stages.add(new Document("$lookup",new Document("from",table.collectionName())
                .append("as",table.collectionName()).append("pipeline",List.of(new Document("$limit",2),
                        new Document("$project",new Document("_id",0).append("value",MongoStartConfigRead.boundedValue()))))));
        var row=SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).aggregate(session,stages).first();
        return row==null||!(row.get("identity") instanceof Document)?null:new MongoAlertWriteFacts(row,database,session);
    }
    Document identity(){return captured.get("identity",Document.class);}
    boolean singletonRecovery(){
        var values=captured.getList(SystemCollections.STORAGE_RECOVERY.collectionName(),Document.class);
        return values.size()==1&&values.getFirst().get("value") instanceof Document row&&"cluster".equals(row.get("_id"));
    }
    boolean empty(SystemCollections table){return captured.getList(table.collectionName(),Document.class).isEmpty();}
    Document row(SystemCollections table,Object id) {
        var values=captured.getList(table.collectionName(),Document.class);
        if(values.isEmpty())return null;
        if(values.size()==1&&values.getFirst().get("value") instanceof Document row&&exact(id,row.get("_id")))return row;
        // A populated ambiguous namespace evaluates its predicate under its own collation.
        return table.on(database).find(session,new Document("_id",id)).first();
    }
    private static boolean exact(Object expected,Object actual){
        if(expected instanceof Document key)return actual instanceof Document value&&new ArrayList<>(key.keySet()).equals(new ArrayList<>(value.keySet()))&&key.equals(value);
        return expected.equals(actual);
    }
}
