package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;

/** Bounded raw rows from one proof snapshot; the original decoders still establish authority. */
record MongoPipelineProofFacts(Document schema, Document checkpoint, Document claim) {
    private static final String ROW = "row";
    private static final int MAX_INLINE_BYTES = 64 * 1024;
    private static final Collation SIMPLE = Collation.builder().locale("simple").build();

    static MongoPipelineProofFacts read(ClientSession session, MongoDatabase database,
            PipelineIntentScope scope, WorkloadClaimKey key) {
        var aggregate = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).aggregate(session, List.of(
                match("business"), inline(),
                lookup(SystemCollections.CACHE_PIPELINE_STATE, "checkpoint", scope.pipelineId(), false),
                lookup(SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", MongoWorkloadClaimStore.id(key), true)));
        var captured = aggregate.collation(SIMPLE).first();
        if (captured == null) return null;
        var schema = captured.get(ROW, Document.class);
        var checkpoint = row(captured, "checkpoint");
        var claim = row(captured, "claim");
        return schema == null || checkpoint == null || claim == null ? null
                : new MongoPipelineProofFacts(schema, checkpoint, claim);
    }

    record Authority(Document identity, Document schema, Document checkpoint, Document claim) {}

    /** The four original authority facts, from a single server snapshot. */
    static Authority readAuthority(MongoDatabase snapshot, PipelineIntentScope scope, WorkloadClaimKey key) {
        var stages = authorityStages(scope, key);
        var captured = SystemCollections.CACHE_IDENTITY.on(snapshot).aggregate(stages).collation(SIMPLE).first();
        return authority(captured);
    }

    record SinkReceipt(Authority authority, Document row) {}

    static SinkReceipt readSinkReceipt(MongoDatabase snapshot, PipelineIntentScope scope, WorkloadClaimKey key,
            Document receiptId) {
        var stages = authorityStages(scope, key);
        stages.add(lookup(SystemCollections.CACHE_SINK_PREPARATIONS, "receipt", receiptId, false));
        var captured = SystemCollections.CACHE_IDENTITY.on(snapshot).aggregate(stages).collation(SIMPLE).first();
        var facts = authority(captured);
        if (facts == null) return null;
        var receipt = row(captured, "receipt");
        return receipt == null ? null : new SinkReceipt(facts, receipt);
    }

    private static List<Document> authorityStages(PipelineIntentScope scope, WorkloadClaimKey key) {
        var identityFilter = new Document("_id", "cluster").append("clusterId", scope.clusterId())
                .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation())
                .append("layoutId", MongoConfigurationDomain.LAYOUT).append("businessPhase", MongoCacheIdentity.READY)
                .append("$expr", new Document("$eq", List.of(new Document("$type", "$bindingVersion"), "long")));
        return new ArrayList<>(List.of(
                new Document("$match", identityFilter), inline(),
                lookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business", false),
                lookup(SystemCollections.CACHE_PIPELINE_STATE, "checkpoint", scope.pipelineId(), false),
                lookup(SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", MongoWorkloadClaimStore.id(key), true)));
    }

    private static Authority authority(Document captured) {
        if (captured == null) return null;
        var identity = captured.get(ROW, Document.class);
        var schema = row(captured, "schema");
        var checkpoint = row(captured, "checkpoint");
        var claim = row(captured, "claim");
        return identity == null || schema == null || checkpoint == null || claim == null
                ? null : new Authority(identity, schema, checkpoint, claim);
    }

    private static Document match(Object id) {
        return new Document("$match", new Document("_id", id));
    }

    private static Document lookup(SystemCollections table, String path, Object id, boolean lease) {
        var stages = new ArrayList<>(List.of(match(id), new Document("$limit", 1)));
        if (lease) stages.add(MongoWorkloadClaimStore.leaseRemaining(true));
        stages.add(inline());
        return new Document("$lookup", new Document("from", table.collectionName())
                .append("pipeline", stages).append("as", path));
    }

    private static Document inline() {
        return new Document("$project", new Document("_id", 0).append(ROW, new Document("$cond",
                Arrays.asList(new Document("$lte", List.of(new Document("$bsonSize", "$$ROOT"), MAX_INLINE_BYTES)),
                        "$$ROOT", null))));
    }

    private static Document row(Document captured, String path) {
        var entries = captured.getList(path, Document.class);
        return entries.isEmpty() ? null : entries.getFirst().get(ROW, Document.class);
    }
}
