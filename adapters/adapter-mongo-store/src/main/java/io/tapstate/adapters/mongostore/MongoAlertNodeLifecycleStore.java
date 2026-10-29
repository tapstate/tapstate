package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import org.bson.Document;

/** Rule-independent CONFIG node facts; lease reads precede these commands and their guards are local. */
final class MongoAlertNodeLifecycleStore implements AlertNodeLifecycleStore {
    private final MongoConfigurationDomain domain;
    private final MongoDatabase db;
    private final SourceConfigCipherProvider keys;
    private final MongoStorageBindingStore bindings;
    private final AlarmKey.Scope scope;
    private final StorageSetupScope setup;
    private final Runnable active;
    MongoAlertNodeLifecycleStore(MongoConfigurationDomain domain,MongoDatabase db,SourceConfigCipherProvider keys,
            MongoStorageBindingStore bindings,AlarmKey.Scope scope,StorageSetupScope setup,Runnable active) {
        this.domain=domain;this.db=db;this.keys=keys;this.bindings=bindings;this.scope=scope;this.setup=setup;this.active=active;
        if(!scope.clusterId().equals(setup.clusterId()))throw invalid("scope");
    }
    private <T>T tx(Function<ClientSession,T> action) {
        active.run();
        if(domain.alertSchemaVersion()!=MongoAlertStorageSchema.VERSION)throw unreadable("schema-capability");
        return domain.alertTransaction(new MongoReadBudget(Duration.ofSeconds(10)),session->{
            active.run();currentHost(session);T result=action.apply(session);active.run();return result;
        });
    }
    private CacheAuthScope currentHost(ClientSession session) {
        var row=SystemCollections.STORAGE_SETUP.on(db).find(session,new Document("_id","cluster")).first();
        if(row==null || !setup.clusterId().equals(row.get("clusterId")) || !setup.installationId().equals(row.get("installationId"))
                || !setup.operationId().equals(row.get("operationId")) || !Long.valueOf(setup.generation()).equals(row.get("generation")))throw unreadable("host-installation");
        var binding=bindings.loadForTransaction(session).orElseThrow(()->unreadable("binding"));
        if(!setup.equals(binding.setup()) || binding.mode()==StorageBinding.Mode.OP && !scope.authorityId().equals(setup.installationId()))throw unreadable("host-binding");
        var prepared=bindings.cachePreparationForTransaction(session).orElseThrow(()->unreadable("cache-preparation"));
        var schema=SystemCollections.SCHEMA_MIGRATIONS.on(db).find(session,new Document("_id","config")).first();
        var version=SystemCollections.CONFIGURATION_VERSION.on(db).find(session,new Document("_id",ConfigurationVersion.ID)).first();
        MongoAlertStorageSchema.verifyAuthorityState(schema,version,scope.clusterId(),keys.currentForWrite().activeKeyId());
        return prepared.authScope();
    }
    private void writeFence(ClientSession session) {
        if(!keys.fenceWrite(new SourceConfigWriteScope(session),keys.currentForWrite().activeKeyId()))throw invalid("configuration-key");
        ConfigurationVersion.advanceAlert(session,SystemCollections.CONFIGURATION_VERSION.on(db));
    }
    private void original(ClientSession session,Registration original,Runnable local) {
        local.run();requireScope(original.key());
        if(!setup.equals(original.setup()) || !currentHost(session).equals(original.cache()))throw invalid("original-binding");
    }
    @Override public Row register(Registration original,Instant at,Runnable local) {
        Objects.requireNonNull(at);Objects.requireNonNull(local);local.run();
        return tx(session->{
            original(session,original,local);
            var retained=load(session,original.key());
            if(retained.isPresent()) {
                var before=retained.orElseThrow();var previous=before.registration();
                if(before.phase()!=Phase.EXPECTED)throw invalid("retired-original-boot");
                if(sameOwner(previous,original)) {local.run();return before;}
                throw invalid("original-node-boot-rebound");
            }
            var after=new Row(original,1,at,Phase.EXPECTED,null,null);var encoded=encode(after);
            writeFence(session);
            long count=SystemCollections.ALERT_NODE_LIFECYCLES.on(db).countDocuments(session,scopeFilter(),
                    new com.mongodb.client.model.CountOptions().limit(MAX_ROWS+1));
            if(count>=MAX_ROWS)throw invalid("capacity");
            local.run();SystemCollections.ALERT_NODE_LIFECYCLES.on(db).insertOne(session,encoded);local.run();return after;
        });
    }
    private static boolean sameOwner(Registration a,Registration b) {
        return a.key().equals(b.key()) && a.setup().equals(b.setup()) && a.cache().equals(b.cache())
                && a.session().equals(b.session()) && a.memberId().equals(b.memberId());
    }
    @Override public Row retire(Registration original,long revision,String command,Instant at,Runnable local) {
        Objects.requireNonNull(command);Objects.requireNonNull(at);Objects.requireNonNull(local);local.run();
        return tx(session->{
            original(session,original,local);
            var before=load(session,original.key()).orElseThrow(()->unreadable("missing-original-node"));
            if(!before.registration().equals(original))throw invalid("foreign-retirement");
            if(before.phase()==Phase.RETIRING) {
                if(!command.equals(before.commandId()) || !at.equals(before.requestedAt()))throw invalid("retirement-command");
                local.run();return before;
            }
            if(before.revision()!=revision)throw invalid("retirement-revision");
            var after=new Row(original,nextRevision(revision),before.registeredAt(),Phase.RETIRING,command,at);
            var encoded=encode(after);writeFence(session);local.run();
            if(SystemCollections.ALERT_NODE_LIFECYCLES.on(db).replaceOne(session,
                    new Document("_id",id(original.key())).append("revision",revision).append("phase","EXPECTED"),encoded).getModifiedCount()!=1)throw invalid("retirement-conflict");
            local.run();return after;
        });
    }
    @Override public Optional<Row> read(Key key){requireScope(key);return tx(session->load(session,key));}
    @Override public List<Row> list(){return tx(session->list(session));}
    List<Row> list(ClientSession session) {
        var values=new ArrayList<Row>();
        try(var cursor=SystemCollections.ALERT_NODE_LIFECYCLES.on(db).find(session,scopeFilter()).batchSize(1).limit(MAX_ROWS+1).iterator()) {
            while(cursor.hasNext()){if(values.size()>=MAX_ROWS)throw unreadable("row-budget");values.add(decodeScoped(cursor.next()));}
        }
        return List.copyOf(values);
    }
    Optional<Row> load(ClientSession session,Key key) {
        requireScope(key);var raw=SystemCollections.ALERT_NODE_LIFECYCLES.on(db).find(session,new Document("_id",id(key))).first();
        if(raw==null)return Optional.empty();var value=decodeScoped(raw);
        if(!value.registration().key().equals(key))throw unreadable("identity-hash");return Optional.of(value);
    }
    Optional<Row> successor(ClientSession session,Row original) {
        var r=original.registration();
        var filter=scopeFilter().append("nodeId",r.key().nodeId()).append("phase",new Document("$in",List.of("EXPECTED","RETIRING")))
                .append("setup",new Document("installationId",r.setup().installationId()).append("operationId",r.setup().operationId()).append("generation",r.setup().generation()))
                .append("cache",new Document("bindingVersion",r.cache().bindingVersion()).append("mode",r.cache().mode().name())
                        .append("cacheIncarnation",r.cache().cacheIncarnation()).append("authGeneration",r.cache().authGeneration()))
                .append("session.claimGeneration",new Document("$gt",r.session().claimGeneration()));
        var found=SystemCollections.ALERT_NODE_LIFECYCLES.on(db).find(session,filter).limit(1).first();
        return Optional.ofNullable(found).map(this::decodeScoped);
    }
    boolean currentRegistration(ClientSession session,Registration registration) {
        return setup.equals(registration.setup()) && currentHost(session).equals(registration.cache());
    }
    private Row decodeScoped(Document row){var value=decode(row);requireScope(value.registration().key());return value;}
    private void requireScope(Key key){if(!scope.equals(key.scope()))throw invalid("scope");}
    private Document scopeFilter(){return new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId());}
    static String id(Key key) {
        try {
            var digest=java.security.MessageDigest.getInstance("SHA-256");
            for(String part:List.of(key.scope().authorityId(),key.scope().clusterId(),key.nodeId(),key.bootId())) {
                byte[] bytes=part.getBytes(StandardCharsets.UTF_8);digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        }catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
    }
    static Document encode(Row value) {
        var r=value.registration();var k=r.key();var setup=r.setup();var cache=r.cache();var fence=r.session();
        var row=new Document("_id",id(k)).append("formatVersion",1).append("authorityId",k.scope().authorityId()).append("clusterId",k.scope().clusterId())
                .append("nodeId",k.nodeId()).append("bootId",k.bootId()).append("setup",new Document("installationId",setup.installationId())
                        .append("operationId",setup.operationId()).append("generation",setup.generation()))
                .append("cache",new Document("bindingVersion",cache.bindingVersion()).append("mode",cache.mode().name())
                        .append("cacheIncarnation",cache.cacheIncarnation()).append("authGeneration",cache.authGeneration()))
                .append("session",new Document("claimGeneration",fence.claimGeneration()).append("executionGeneration",fence.executionGeneration())
                        .append("topologyRevision",fence.topologyRevision()))
                .append("memberId",r.memberId()).append("membershipRevision",r.membershipRevision()).append("revision",value.revision())
                .append("registeredAt",value.registeredAt().toString()).append("phase",value.phase().name());
        if(value.phase()==Phase.RETIRING)row.append("reason","ORDERLY_SHUTDOWN").append("commandId",value.commandId()).append("requestedAt",value.requestedAt().toString());
        if(MongoAlertConfigurationCodec.size(row)>MAX_ROW_BYTES)throw invalid("row-size");return row;
    }
    static Row decode(Document row) {
        try {
            var expected=new HashSet<>(Set.of("_id","formatVersion","authorityId","clusterId","nodeId","bootId","setup","cache","session",
                    "memberId","membershipRevision","revision","registeredAt","phase"));
            if("RETIRING".equals(row.get("phase")))expected.addAll(Set.of("reason","commandId","requestedAt"));
            if(!row.keySet().equals(expected) || !Integer.valueOf(1).equals(row.get("formatVersion")) || MongoAlertConfigurationCodec.size(row)>MAX_ROW_BYTES)throw unreadable("format");
            var key=new Key(new AlarmKey.Scope(row.getString("authorityId"),row.getString("clusterId")),row.getString("nodeId"),row.getString("bootId"));
            var setup=row.get("setup",Document.class);var cache=row.get("cache",Document.class);var session=row.get("session",Document.class);
            if(!setup.keySet().equals(Set.of("installationId","operationId","generation"))
                    || !cache.keySet().equals(Set.of("bindingVersion","mode","cacheIncarnation","authGeneration"))
                    || !session.keySet().equals(Set.of("claimGeneration","executionGeneration","topologyRevision")))throw unreadable("binding-format");
            var original=new Registration(key,new StorageSetupScope(key.scope().clusterId(),setup.getString("installationId"),setup.getString("operationId"),setup.getLong("generation")),
                    new CacheAuthScope(key.scope().clusterId(),cache.getLong("bindingVersion"),StorageBinding.Mode.valueOf(cache.getString("mode")),cache.getString("cacheIncarnation"),cache.getLong("authGeneration")),
                    new WorkloadClaimFence(new WorkloadClaimKey(key.scope().clusterId(),WorkloadClaimType.NODE_SESSION,key.nodeId()),new WorkloadOwner(key.nodeId(),key.bootId()),
                            session.getLong("claimGeneration"),session.getLong("executionGeneration"),session.getLong("topologyRevision")),row.getString("memberId"),row.getLong("membershipRevision"));
            var phase=Phase.valueOf(row.getString("phase"));
            if(phase==Phase.RETIRING && !"ORDERLY_SHUTDOWN".equals(row.get("reason")))throw unreadable("retirement-reason");
            var value=new Row(original,row.getLong("revision"),Instant.parse(row.getString("registeredAt")),phase,row.getString("commandId"),
                    row.get("requestedAt")==null?null:Instant.parse(row.getString("requestedAt")));
            if(!encode(value).equals(row))throw unreadable("canonical-node");return value;
        }catch(IllegalArgumentException|ClassCastException|NullPointerException malformed){throw unreadable("node-format");}
    }
    /** Explicit component preparation only, after its original PREPARING marker has been committed. */
    static void prepareNamespace(MongoDatabase database) {
        var collection=SystemCollections.ALERT_NODE_LIFECYCLES;
        var names=database.listCollections().filter(new Document("name",collection.collectionName())).into(new ArrayList<>());
        if(names.isEmpty())try {collection.prepareCollectionOn(database);}
        catch(com.mongodb.MongoCommandException concurrent){if(concurrent.getErrorCode()!=48)throw concurrent;}
        var index=collection.indexes().getFirst();var keys=new Document();index.keys().forEach(key->keys.append(key,1));
        collection.on(database).createIndex(keys,new com.mongodb.client.model.IndexOptions().name(index.indexName()).unique(false));
    }
    private static long nextRevision(long current){if(current>=Long.MAX_VALUE-1)throw unreadable("revision-exhausted");return current+1;}
    private static TapstateException invalid(String reason){return new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","nodeLifecycle","reason",reason),null);}
    private static TapstateException unreadable(String field){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-node-lifecycle","field",field),null);}
}
