package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;

/** Fixed-scope snapshot facts use the same original CACHE claim transaction as the protected checkpoint. */
final class MongoSnapshotStateStore implements SnapshotStateStore {
    private final MongoCachePipelineAccess access;
    MongoSnapshotStateStore(MongoCachePipelineAccess access) { this.access = Objects.requireNonNull(access,"access"); }

    @Override public long order() {
        return access.metadataTransaction(SystemCollections.CACHE_SNAPSHOT_ORDERS,false,session -> { access.verifyOriginalClaim(session); return order(session,access.database,access.scope).getLong("order"); });
    }
    @Override public long advanceOrder(long expected,long floor,Map<String,CaptureCacheState> originals) {
        counter(expected); counter(floor); var captures = Map.copyOf(originals);
        return access.metadataTransaction(SystemCollections.CACHE_SNAPSHOT_ORDERS,true,session -> {
            var row = order(session,access.database,access.scope);
            var selected = MongoPipelineSourcePreparations.retained(session,access.database,access.scope).selectedTables();
            var required = new TreeSet<String>(); selected.forEach((source,tables) -> { if (!tables.isEmpty()) required.add(source); });
            if (!required.equals(captures.keySet()) || required.isEmpty()) throw fenced();
            var held = new ArrayList<Document>();
            for (String source : required) {
                var original = captures.get(source); var preparation = original.preparation(); var capture = original.claim();
                if (preparation.mode()!=CapturePreparation.Mode.SNAPSHOT_ONLY || original.chain().isPresent()
                        || !preparation.scope().equals(business()) || !preparation.captureId().equals(capture.key().resourceId())
                        || !preparation.captureId().equals(SourceCaptureIdentity.snapshotCapture(preparation.chainId(),selected.get(source)))) throw fenced();
                var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(access.database);
                var actual = table.find(session,new Document("_id",MongoWorkloadClaimStore.id(capture.key()))).first();
                if (actual==null || !preparation.equals(MongoCaptureCachePreparations.requirePreparation(actual,capture.key(),business()))) throw fenced();
                new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(actual);
                if (table.updateOne(session,WorkloadClaimDocuments.live(capture),new Document("$inc",new Document("fencedSnapshotWrites",1L))).getMatchedCount()!=1) throw fenced();
                held.add(new Document("source",source).append("capture",WorkloadClaimDocuments.stored(capture)));
            }
            var command = WorkloadClaimDocuments.stored(access.originalClaim());
            if (command.equals(row.get("command"))) {
                if (!Long.valueOf(expected).equals(row.get("previousOrder")) || !Long.valueOf(floor).equals(row.get("retainedFloor"))
                        || !held.equals(row.get("captures"))) throw fenced();
                return row.getLong("order");
            }
            if (row.getLong("order")!=expected || Math.max(expected,floor)>=Long.MAX_VALUE-1) throw fenced();
            long next = Math.max(expected,floor)+1;
            var updated = new Document("_id",id(access.scope)).append("formatVersion",1).append("order",next)
                    .append("previousOrder",expected).append("retainedFloor",floor).append("command",command).append("captures",held);
            if (SystemCollections.CACHE_SNAPSHOT_ORDERS.on(access.database).replaceOne(session,row,updated).getMatchedCount()!=1) throw fenced();
            return next;
        });
    }
    @Override public void recordCount(String source,String table,long epoch,long rows) {
        if (epoch<1 || epoch==Long.MAX_VALUE || rows<0) throw new IllegalArgumentException("invalid snapshot measurement");
        access.metadataTransaction(SystemCollections.CACHE_SNAPSHOT_COUNTS,true,session -> {
            requireTable(session,source,table); var key = countId(access.scope,source,table);
            var collection = SystemCollections.CACHE_SNAPSHOT_COUNTS.on(access.database); var prior = collection.find(session,new Document("_id",key)).first();
            if (prior!=null) { requireCount(prior,key); if (prior.getLong("snapshotEpoch")>epoch) throw fenced(); }
            var row = new Document("_id",key).append("formatVersion",1).append("snapshotEpoch",epoch).append("rows",rows);
            if (prior==null) collection.insertOne(session,row);
            else if (collection.replaceOne(session,prior,row).getMatchedCount()!=1) throw fenced();
            return null;
        });
    }
    @Override public OptionalLong count(String source,String table,long epoch) {
        if (epoch<1 || epoch==Long.MAX_VALUE) throw new IllegalArgumentException("invalid snapshot epoch");
        return access.metadataTransaction(SystemCollections.CACHE_SNAPSHOT_COUNTS,false,session -> {
            access.verifyOriginalClaim(session);
            requireTable(session,source,table); var key = countId(access.scope,source,table);
            var row = SystemCollections.CACHE_SNAPSHOT_COUNTS.on(access.database).find(session,new Document("_id",key)).first();
            if (row==null) return OptionalLong.empty(); requireCount(row,key);
            return row.getLong("snapshotEpoch")==epoch?OptionalLong.of(row.getLong("rows")):OptionalLong.empty();
        });
    }
    private void requireTable(ClientSession session,String source,String table) {
        var tables = MongoPipelineSourcePreparations.retained(session,access.database,access.scope).selectedTables().get(source);
        if (tables==null || !tables.contains(table)) throw fenced();
    }
    private CacheBusinessScope business() { return new CacheBusinessScope(access.scope.clusterId(),access.scope.bindingVersion(),access.scope.cacheIncarnation()); }
    static void seedInitial(ClientSession session,MongoDatabase database,PipelineIntentScope scope) {
        if (SystemCollections.CACHE_SNAPSHOT_ORDERS.on(database).find(session,new Document("_id",id(scope))).first()!=null
                || SystemCollections.CACHE_SNAPSHOT_COUNTS.on(database).find(session,new Document("_id.scope",id(scope))).first()!=null) throw unreadable();
        SystemCollections.CACHE_SNAPSHOT_ORDERS.on(database).insertOne(session,initial(scope));
    }
    static void verifyInitial(ClientSession session,MongoDatabase database,PipelineIntentScope scope) {
        if (!initial(scope).equals(order(session,database,scope))
                || SystemCollections.CACHE_SNAPSHOT_COUNTS.on(database).find(session,new Document("_id.scope",id(scope))).first()!=null) throw unreadable();
    }
    static Document order(ClientSession session,MongoDatabase database,PipelineIntentScope scope) {
        var row = SystemCollections.CACHE_SNAPSHOT_ORDERS.on(database).find(session,new Document("_id",id(scope))).first();
        try {
            if (row==null || !row.keySet().equals(Set.of("_id","formatVersion","order","previousOrder","retainedFloor","command","captures"))
                    || !id(scope).equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !(row.get("order") instanceof Long current) || current<0 || current==Long.MAX_VALUE
                    || !(row.get("previousOrder") instanceof Long prior) || prior<0
                    || !(row.get("retainedFloor") instanceof Long floor) || floor<0 || !(row.get("captures") instanceof List<?> captures)) throw unreadable();
            if (current==0) { if (!initial(scope).equals(row)) throw unreadable(); }
            else {
                if (Math.max(prior,floor)>=Long.MAX_VALUE-1 || current!=Math.max(prior,floor)+1 || captures.isEmpty()) throw unreadable();
                var command = WorkloadClaimDocuments.readPipeline(row.get("command",Document.class));
                if (!scope.clusterId().equals(command.key().clusterId()) || !scope.pipelineId().equals(command.key().resourceId()) || command.executionGeneration()<1) throw unreadable();
                var sources = new TreeSet<String>(); var ordered = new ArrayList<String>();
                for (var value : captures) {
                    if (!(value instanceof Document capture) || !capture.keySet().equals(Set.of("source","capture"))
                            || !(capture.get("source") instanceof String source) || source.isBlank() || !sources.add(source)) throw unreadable();
                    var fence = WorkloadClaimDocuments.readCapture(capture.get("capture",Document.class));
                    if (!scope.clusterId().equals(fence.key().clusterId())) throw unreadable(); ordered.add(source);
                }
                if (!new ArrayList<>(sources).equals(ordered)) throw unreadable();
            }
            return row;
        } catch (IllegalArgumentException|NullPointerException|ClassCastException invalid) { throw unreadable(); }
    }
    private static Document initial(PipelineIntentScope scope) {
        return new Document("_id",id(scope)).append("formatVersion",1).append("order",0L).append("previousOrder",0L)
                .append("retainedFloor",0L).append("command",null).append("captures",List.of());
    }
    private static Document id(PipelineIntentScope scope) { return MongoPipelinePreparationFacts.scope(scope); }
    private static Document countId(PipelineIntentScope scope,String source,String table) {
        return new Document("scope",id(scope)).append("source",OperatorStateText.encode(source)).append("table",OperatorStateText.encode(table));
    }
    private static void requireCount(Document row,Document key) {
        if (!row.keySet().equals(Set.of("_id","formatVersion","snapshotEpoch","rows")) || !key.equals(row.get("_id"))
                || !Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("snapshotEpoch") instanceof Long epoch) || epoch<1 || epoch==Long.MAX_VALUE
                || !(row.get("rows") instanceof Long rows) || rows<0) throw unreadable();
    }
    private static void counter(long value) { if (value<0 || value==Long.MAX_VALUE) throw new IllegalArgumentException("invalid snapshot order"); }
    private static TapstateException fenced() { return MongoCachePipelineAccess.claimFenced(); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","snapshot-state","field","original-facts"),null); }
}
