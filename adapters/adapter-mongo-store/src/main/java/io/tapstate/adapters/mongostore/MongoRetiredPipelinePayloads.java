package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;
import java.util.function.Function;

/** Bounded physical reclamation uses immutable retired inventories, never a successor's mutable claim. */
final class MongoRetiredPipelinePayloads implements RetiredPipelinePayloads {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final CacheBusinessScope business;
    private final Runnable active;private final OperatorDatabaseBinding operatorDatabases;
    MongoRetiredPipelinePayloads(MongoClient client,MongoDatabase database,CacheBusinessScope business,Runnable active,OperatorDatabaseBinding operatorDatabases) {
        this.client=Objects.requireNonNull(client);this.database=Objects.requireNonNull(database);this.business=Objects.requireNonNull(business);
        this.active=Objects.requireNonNull(active);this.operatorDatabases=operatorDatabases;
    }
    @Override public Optional<Original> find(PipelineIntentScope scope) {
        requireScope(scope);return transaction(session -> {
            var row=SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session,new Document("_id",MongoPipelinePreparationFacts.scope(scope))).first();
            if(row==null || !row.containsKey("retirement"))return Optional.empty();
            return Optional.of(MongoOperatorInventory.frozen(session,database,scope,row));
        });
    }
    @Override public List<Original> list(String pipelineId,long afterGeneration,int limit) {
        if(pipelineId==null || pipelineId.isBlank() || afterGeneration<0 || limit<=0 || limit>128)throw new IllegalArgumentException("invalid retired inventory page");
        return transaction(session -> {
            var filter=new Document("_id.clusterId",business.clusterId()).append("_id.bindingVersion",business.bindingVersion())
                    .append("_id.cacheIncarnation",business.cacheIncarnation()).append("_id.pipelineId",pipelineId)
                    .append("_id.stateGeneration",new Document("$gt",afterGeneration)).append("retirement",new Document("$exists",true));
            var rows=SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session,filter).sort(new Document("_id.stateGeneration",1)).limit(limit);
            var result=new ArrayList<Original>();
            for(var row:rows)result.add(MongoOperatorInventory.frozen(session,database,MongoPipelinePreparationFacts.scope(row.get("_id",Document.class)),row));
            return List.copyOf(result);
        });
    }
    @Override public long reclaim(Original original,WorkloadClaimFence currentNode) {
        Objects.requireNonNull(original);requireScope(original.scope());Objects.requireNonNull(currentNode);
        return transaction(session -> {
            MongoCachePipelinePreparations.proveNode(session,database,business,currentNode);
            var old=original.scope();var scope=MongoPipelinePreparationFacts.scope(old);
            var inventory=SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session,new Document("_id",scope)).first();
            if(!original.equals(MongoOperatorInventory.frozen(session,database,old,inventory)))throw MongoPipelineRetirementFacts.fenced();
            long deleted=0;
                    // A shared extra database keeps its identity and every other Pipeline's rows.
                    for(var location:inventory.getList("placements",Document.class)) {
                        var target=client.getDatabase(location.getString("database"));
                        if(!target.getName().equals(database.getName())) {
                            String held=MongoCacheOperatorDatabases.require(session,target,operatorDatabases);
                            MongoOperatorInventory.requireDatabase(inventory,target.getName(),held,old);
                        }
                        var table=MongoCacheOperatorStateStore.table(OperatorPlacement.Resource.valueOf(location.getString("resource")));
                        var address=new Document("scope",scope).append("database",target.getName()).append("resource",location.getString("resource"))
                                .append("stepId",location.getString("stepId")).append("pathId",location.getString("pathId")).append("namespace",location.getString("namespace"));
                        deleted+=reclaimRows(session,table.on(target),new Document("_id.placement",address),256-(int)deleted);
                        if(deleted>=256)break;
                    }
                    if(deleted<256)for(var shape:inventory.getList("shapes",Document.class)) {
                        var target=client.getDatabase(shape.getString("database"));
                        if(!target.getName().equals(database.getName())) {
                            String held=MongoCacheOperatorDatabases.require(session,target,operatorDatabases);
                            MongoOperatorInventory.requireDatabase(inventory,target.getName(),held,old);
                        }
                        deleted+=reclaimRows(session,SystemCollections.CACHE_NEST_SHAPES.on(target),
                                new Document("_id.scope",scope).append("_id.stepId",shape.getString("stepId")),256-(int)deleted);
                        if(deleted>=256)break;
                    }
                    // A clear preserves samples under their existing TTL. Logical deletion authorizes
                    // history cleanup only for each separately verified, frozen original scope.
                    if (deleted < 256 && historyRetired(session, old, inventory))
                        deleted += reclaimRows(session, SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(database),
                                new Document("scope", scope), 256 - (int) deleted);
                    var payloads=List.of(
                            java.util.Map.entry(SystemCollections.DIRECT_BATCHES,new Document("_id.channel.scope",scope)),
                            java.util.Map.entry(SystemCollections.DIRECT_CHANNEL_OFFSETS,new Document("_id.scope",scope)),
                            java.util.Map.entry(SystemCollections.CACHE_SINK_PREPARATIONS,MongoSinkPreparationStore.scopeFilter(old)),
                            java.util.Map.entry(SystemCollections.CACHE_DERIVED_SCHEMAS,new Document("scope",scope)),
                            java.util.Map.entry(SystemCollections.CACHE_SNAPSHOT_COUNTS,new Document("_id.scope",scope)),
                            java.util.Map.entry(SystemCollections.CACHE_SNAPSHOT_ORDERS,new Document("_id",scope)));
                    if(deleted<256)for(var payload:payloads) {
                        deleted+=reclaimRows(session,payload.getKey().on(database),payload.getValue(),256-(int)deleted);
                        if(deleted>=256)break;
                    }
            active.run();return deleted;
        });
    }
    private boolean historyRetired(ClientSession session, PipelineIntentScope original, Document inventory) {
        if ("DELETE".equals(inventory.get("retirement", Document.class).get("purpose"))) return true;
        var filter = new Document("_id.clusterId", original.clusterId()).append("_id.bindingVersion", original.bindingVersion())
                .append("_id.cacheIncarnation", original.cacheIncarnation()).append("_id.pipelineId", original.pipelineId())
                .append("_id.stateGeneration", new Document("$gt", original.stateGeneration())).append("retirement.purpose", "DELETE");
        var deletion = SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session, filter)
                .sort(new Document("_id.stateGeneration", 1)).first();
        if (deletion == null) return false;
        MongoOperatorInventory.frozen(session, database,
                MongoPipelinePreparationFacts.scope(deletion.get("_id", Document.class)), deletion);
        return true;
    }

    @Override public List<PipelineIntentScope> listAfter(PipelineIntentScope after, int limit) {
        if (limit <= 0 || limit > 128) throw new IllegalArgumentException("invalid retired inventory page");
        if (after != null) requireScope(after);
        return transaction(session -> {
            var filter = new Document("_id.clusterId", business.clusterId()).append("_id.bindingVersion", business.bindingVersion())
                    .append("_id.cacheIncarnation", business.cacheIncarnation()).append("retirement", new Document("$exists", true));
            if (after != null) filter.append("_id", new Document("$gt", MongoPipelinePreparationFacts.scope(after)));
            var result = new ArrayList<PipelineIntentScope>();
            for (var row : SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session, filter)
                    .sort(new Document("_id", 1)).limit(limit)) {
                try { result.add(MongoPipelinePreparationFacts.scope(row.get("_id", Document.class))); }
                catch (IllegalArgumentException | ClassCastException | NullPointerException malformed) {
                    throw new io.tapstate.core.common.TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), malformed);
                }
            }
            return List.copyOf(result);
        });
    }
    private static long reclaimRows(ClientSession session,MongoCollection<Document> table,Document filter,int remaining) {
        if(remaining<=0)return 0;var ids=new ArrayList<Object>();
        for(var row:table.find(session,filter).projection(new Document("_id",1)).limit(remaining))ids.add(row.get("_id"));
        if(ids.isEmpty())return 0;
        return table.deleteMany(session,new Document("$and",List.of(filter,new Document("_id",new Document("$in",ids))))).getDeletedCount();
    }
    private void requireScope(PipelineIntentScope scope) {
        if(!business.clusterId().equals(scope.clusterId()) || business.bindingVersion()!=scope.bindingVersion() || !business.cacheIncarnation().equals(scope.cacheIncarnation()))throw MongoPipelineRetirementFacts.fenced();
    }
    private <T>T transaction(Function<ClientSession,T> action) {
        return StoreIo.call(() -> {active.run();new MongoCacheMetadataStructure(database,MongoRateHistoryStore.DEFAULT_RETENTION).verifyFacet(SystemCollections.CACHE_OPERATOR_CLEANUP);
            try(var session=client.startSession()) {return session.withTransaction(() -> {
                active.run();new MongoCacheBusinessIdentity(database,business).requireAdmissionState(session);T result=action.apply(session);active.run();return result;
            },TRANSACTION);}
        });
    }
}
