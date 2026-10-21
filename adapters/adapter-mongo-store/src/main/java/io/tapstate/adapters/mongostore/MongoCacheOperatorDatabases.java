package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.OperatorDatabaseBinding;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** A target's operator identity and schema are independent of the default cache's runtime authority. */
final class MongoCacheOperatorDatabases {
    private static final List<SystemCollections> TABLES = List.of(SystemCollections.CACHE_IDENTITY,
            SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.CACHE_NEST_STATE, SystemCollections.CACHE_NEST_SHAPES);
    private MongoCacheOperatorDatabases() {}

    static String prepare(ClientSession session, MongoDatabase control, MongoDatabase target, OperatorDatabaseBinding binding, String operationId, Runnable guard) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("operator database preparation requires its original transaction");
        if (operationId == null || !UUID.fromString(operationId).toString().equals(operationId))
            throw new IllegalArgumentException("operator preparation requires a fixed UUID");
        guard.run();
        var names = target.listCollectionNames().into(new ArrayList<>());
        var history = MongoOperatorInventory.databaseHistory(session, control, binding.scope(), target.getName());
        if (!names.isEmpty()) {
            String held = require(session, target, binding);
            if (history.isPresent() && !history.get().equals(held)) throw unavailable();
            guard.run(); return held;
        }
        if (history.isPresent()) throw unavailable();
        for (var table : TABLES) {
            guard.run(); table.prepareCollectionOn(target, session);
            for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(target), index);
        }
        SystemCollections.CACHE_IDENTITY.on(target).insertOne(session, identity(target, binding));
        SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(target).insertOne(session, new Document("_id", "operator")
                .append("installedVersion", 1).append("phase", "READY").append("epoch", 1L).append("operationId", operationId));
        guard.run();
        return operationId;
    }

    static void inspectRecovery(ClientSession session,MongoDatabase target,OperatorDatabaseBinding binding,
            io.tapstate.spi.store.PipelineIntentScope previous) {
        if(target.listCollectionNames().into(new ArrayList<>()).isEmpty())return;
        var current=SystemCollections.CACHE_IDENTITY.on(target).find(session,new Document("_id","operator")).first();
        if(identity(target,binding).equals(current)){require(session,target,binding);return;}
        require(session,target,previousBinding(binding,previous));
    }

    /** Rebinding preserves all old payload. Only newly scoped keys can be used by the recovered run. */
    static String recover(ClientSession session,MongoDatabase control,MongoDatabase target,OperatorDatabaseBinding binding,
            io.tapstate.spi.store.PipelineIntentScope previous,String operationId,Runnable guard) {
        if(target.listCollectionNames().into(new ArrayList<>()).isEmpty())return prepare(session,control,target,binding,operationId,guard);
        var identities=SystemCollections.CACHE_IDENTITY.on(target);
        var current=identities.find(session,new Document("_id","operator")).first();
        if(identity(target,binding).equals(current))return prepare(session,control,target,binding,operationId,guard);
        var old=previousBinding(binding,previous);String oldOperation=require(session,target,old);
        if(MongoOperatorInventory.databaseHistory(session,control,binding.scope(),target.getName()).isPresent())throw unavailable();
        guard.run();
        if(identities.replaceOne(session,identity(target,old),identity(target,binding)).getMatchedCount()!=1)throw unavailable();
        if(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(target).updateOne(session,new Document("_id","operator").append("operationId",oldOperation),
                new Document("$set",new Document("operationId",operationId))).getMatchedCount()!=1)throw unavailable();
        guard.run();return require(session,target,binding);
    }
    private static OperatorDatabaseBinding previousBinding(OperatorDatabaseBinding current,io.tapstate.spi.store.PipelineIntentScope previous) {
        if(previous==null || !previous.clusterId().equals(current.scope().clusterId()) || previous.bindingVersion()!=current.scope().bindingVersion()
                || previous.cacheIncarnation().equals(current.scope().cacheIncarnation()))throw unavailable();
        return new OperatorDatabaseBinding(new io.tapstate.spi.store.CacheBusinessScope(previous.clusterId(),previous.bindingVersion(),previous.cacheIncarnation()),
                current.cacheDatabase(),current.configurationDatabase(),current.masterDatabase());
    }

    static String require(ClientSession session, MongoDatabase target, OperatorDatabaseBinding binding) {
        if (binding == null) throw MongoCachePipelineAccess.fenced();
        var structure = new MongoCacheMetadataStructure(target, MongoRateHistoryStore.DEFAULT_RETENTION);
        for (var table : TABLES) structure.verifyFacet(table);
        var identities = SystemCollections.CACHE_IDENTITY.on(target);
        if (!identity(target, binding).equals(identities.find(session, new Document("_id", "operator")).first())
                || identities.countDocuments(session) != 1) throw unavailable();
        var versions = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(target);
        Document version = versions.find(session, new Document("_id", "operator")).first();
        if (version == null || !version.keySet().equals(Set.of("_id", "installedVersion", "phase", "epoch", "operationId"))
                || !Integer.valueOf(1).equals(version.get("installedVersion")) || !"READY".equals(version.get("phase"))
                || !Long.valueOf(1).equals(version.get("epoch")) || !(version.get("operationId") instanceof String operationId)
                || versions.countDocuments(session) != 1) throw unavailable();
        try { if (!UUID.fromString(operationId).toString().equals(operationId)) throw unavailable(); }
        catch (IllegalArgumentException bad) { throw unavailable(); }
        return operationId;
    }
    private static Document identity(MongoDatabase target, OperatorDatabaseBinding binding) {
        var scope = binding.scope();
        return new Document("_id", "operator").append("formatVersion", 1).append("layoutId", MongoConfigurationDomain.LAYOUT)
                .append("purpose", "OPERATOR").append("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                .append("cacheIncarnation", scope.cacheIncarnation()).append("cacheDatabase", binding.cacheDatabase()).append("database", target.getName());
    }
    private static TapstateException unavailable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,
            Map.of("id", "operator", "field", "operator-database"), null); }
}
