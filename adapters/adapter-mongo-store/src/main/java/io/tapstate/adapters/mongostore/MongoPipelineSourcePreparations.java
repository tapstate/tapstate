package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;

/** The original CACHE first-creation inputs, retained with the existing Pipeline claim rather than reissued on load. */
final class MongoPipelineSourcePreparations {
    private MongoPipelineSourcePreparations() {}
    static Document encode(PipelinePreparationInputs value) {
        var sources = new ArrayList<Document>(); var selected = new ArrayList<Document>(); var direct = new ArrayList<Document>();
        for (String source : sorted(value.sourceHashes().keySet())) {
            sources.add(new Document("source",source).append("hash",value.sourceHashes().get(source)));
            selected.add(new Document("source",source).append("tables",sorted(value.selectedTables().get(source))));
        }
        for (var channel : value.directChannels()) direct.add(new Document("context",MongoDirectChannelRecords.id(channel.context()))
                .append("tables",sorted(channel.tables())).append("readMode",channel.readMode().yaml()).append("sourceIdentity",channel.sourceIdentity()));
        var row = new Document("formatVersion",1).append("pipelineHash",value.pipelineHash()).append("sourceHashes",sources).append("selectedTables",selected).append("directChannels",direct);
        return row.append("contentHash",CanonicalHash.ofText(row.toJson()));
    }
    static PipelinePreparationInputs decode(Object value,PipelineIntentScope scope) {
        try {
            if (!(value instanceof Document row) || !row.keySet().equals(Set.of("formatVersion","pipelineHash","sourceHashes","selectedTables","directChannels","contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion"))) throw rebuild(scope);
            var hashes = new HashMap<String,String>(); var selections = new HashMap<String,Set<String>>(); var direct = new ArrayList<DirectChannelPreparation>();
            for (var source : documents(row.get("sourceHashes"))) if (!source.keySet().equals(Set.of("source","hash"))
                    || hashes.put(source.getString("source"),source.getString("hash")) != null) throw rebuild(scope);
            for (var source : documents(row.get("selectedTables"))) if (!source.keySet().equals(Set.of("source","tables"))
                    || selections.put(source.getString("source"),strings(source.get("tables"))) != null) throw rebuild(scope);
            for (var channel : documents(row.get("directChannels"))) {
                if (!channel.keySet().equals(Set.of("context","tables","readMode","sourceIdentity"))) throw rebuild(scope);
                ReadMode mode = switch (channel.getString("readMode")) { case "cdc_only" -> ReadMode.CDC_ONLY; case "snapshot_and_cdc" -> ReadMode.SNAPSHOT_AND_CDC; default -> throw rebuild(scope); };
                var context = MongoDirectChannelRecords.context(channel.get("context",Document.class));
                if (!context.scope().equals(scope)) throw rebuild(scope);
                direct.add(new DirectChannelPreparation(context,strings(channel.get("tables")),mode,channel.getString("sourceIdentity")));
            }
            var result = new PipelinePreparationInputs(row.getString("pipelineHash"),hashes,selections,direct);
            if (!encode(result).equals(row)) throw rebuild(scope); return result;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw rebuild(scope); }
    }
    static PipelinePreparationInputs retained(ClientSession session,MongoDatabase database,PipelineIntentScope scope) {
        var key = new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId());
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var row = table.find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
        if (row == null) throw rebuild(scope);
        new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(row);
        try {
            if (!scope.equals(MongoPipelinePreparationFacts.scope(row.get("pipelineScope",Document.class)))) throw rebuild(scope);
            MongoCachePipelinePreparations.phase(row,scope);
            return decode(row.get("pipelineSources"),scope);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw rebuild(scope); }
    }
    static void requireDirect(ClientSession session,MongoDatabase database,DirectChannelContext context,Set<String> tables) {
        var fixed = retained(session,database,context.scope());
        if (fixed.directChannels().stream().noneMatch(channel -> channel.context().equals(context) && channel.tables().equals(tables))) throw identityUnproven(context.scope());
    }
    static void requireDirectTable(ClientSession session,MongoDatabase database,DirectChannelContext context,String table){
        var scope=context.scope();var key=new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId());
        var row=MongoValidationMetadata.read(session,SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database),new Document("_id",MongoWorkloadClaimStore.id(key)),
                MongoValidationMetadata.claimProjection("pipelineScope","pipelinePreparation","pipelineSources"),256*1024);
        if(row==null)throw rebuild(scope);
        new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).inStrictSession(session).decodeStored(row);
        if(!scope.equals(MongoPipelinePreparationFacts.scope(row.get("pipelineScope",Document.class))))throw rebuild(scope);
        MongoCachePipelinePreparations.phase(row,scope);var fixed=decode(row.get("pipelineSources"),scope);
        if(fixed.directChannels().stream().noneMatch(channel->channel.context().equals(context)&&channel.tables().contains(table)))throw identityUnproven(context.scope());
    }
    static void requireEmpty(ClientSession session,MongoDatabase database,PipelineIntentScope scope) {
        if (SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(session,offsets(scope)).first() != null
                || SystemCollections.DIRECT_BATCHES.on(database).find(session,batches(scope)).first() != null) throw rebuild(scope);
    }
    static void seedInitial(ClientSession session,MongoDatabase database,PipelineIntentScope scope,PipelinePreparationInputs inputs) {
        requireEmpty(session,database,scope);
        for (var channel : inputs.directChannels()) {
            if (!channel.context().scope().equals(scope)) throw rebuild(scope);
            SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).insertOne(session,MongoDirectChannelRecords.progress(MongoDirectChannelRecords.initial(channel.context(),channel.tables())));
        }
    }
    static void verifyInitial(ClientSession session,MongoDatabase database,PipelineIntentScope scope,PipelinePreparationInputs inputs) {
        var expected = new HashMap<Document,Document>();
        for (var channel : inputs.directChannels()) expected.put(MongoDirectChannelRecords.id(channel.context()),MongoDirectChannelRecords.progress(MongoDirectChannelRecords.initial(channel.context(),channel.tables())));
        for (var row : SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(session,offsets(scope))) {
            var original = expected.remove(row.get("_id",Document.class)); if (original == null || !original.equals(row)) throw rebuild(scope);
        }
        if (!expected.isEmpty() || SystemCollections.DIRECT_BATCHES.on(database).find(session,batches(scope)).first() != null) throw rebuild(scope);
    }
    static void verifyAll(ClientSession session,MongoClient client,MongoDatabase database,PipelineIntentScope scope,PipelinePreparationInputs inputs,Runnable active) {
        var expected = new HashMap<Document,DirectChannelPreparation>();
        for (var channel : inputs.directChannels()) expected.put(MongoDirectChannelRecords.id(channel.context()),channel);
        var business = new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation());
        var direct = new MongoDirectChannelStore(client,database,business,active,MongoRateHistoryStore.DEFAULT_RETENTION);
        for (var row : SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(session,offsets(scope))) {
            var original = expected.remove(row.get("_id",Document.class)); if (original == null) throw rebuild(scope);
            var actual = MongoDirectChannelRecords.progress(row); if (!original.tables().equals(actual.tables())) throw rebuild(scope);
            direct.requireChannel(session,original.context());
        }
        if (!expected.isEmpty()) throw rebuild(scope);
        // Each channel validates its own complete batch closure; an unlisted channel must not hide here.
        for (var row : SystemCollections.DIRECT_BATCHES.on(database).find(session,batches(scope))) {
            var channel = MongoDirectChannelRecords.batch(row).context();
            if (inputs.directChannels().stream().noneMatch(value -> value.context().equals(channel))) throw rebuild(scope);
        }
    }
    private static Document offsets(PipelineIntentScope scope) { return new Document("_id.scope",MongoPipelinePreparationFacts.scope(scope)); }
    private static Document batches(PipelineIntentScope scope) { return new Document("_id.channel.scope",MongoPipelinePreparationFacts.scope(scope)); }
    private static List<Document> documents(Object value) {
        if (!(value instanceof List<?> values)) throw new IllegalArgumentException("preparation records must be a list"); var result = new ArrayList<Document>();
        for (var entry : values) { if (!(entry instanceof Document row)) throw new IllegalArgumentException("preparation entries must be documents"); result.add(row); } return result;
    }
    private static Set<String> strings(Object value) {
        if (!(value instanceof List<?> values)) throw new IllegalArgumentException("selected tables must be a list"); var result = new TreeSet<String>();
        for (var entry : values) if (!(entry instanceof String name) || name.isBlank() || !result.add(name)) throw new IllegalArgumentException("invalid selected table");
        if (!sorted(result).equals(values)) throw new IllegalArgumentException("selected tables must be canonical"); return Set.copyOf(result);
    }
    private static List<String> sorted(Set<String> values) { return new ArrayList<>(new TreeSet<>(values)); }
    static TapstateException identityUnproven(PipelineIntentScope scope) { return new TapstateException(IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN,Map.of("pipeline",scope.pipelineId()),null); }
    private static TapstateException rebuild(PipelineIntentScope scope) { return MongoCachePipelinePreparations.rebuild(scope); }
}
