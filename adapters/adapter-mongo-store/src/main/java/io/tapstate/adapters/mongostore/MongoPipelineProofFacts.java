package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimFence;
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

    record RenewalProof(Document identity, Document schema, Document checkpoint) {}

    static RenewalProof readRenewal(ClientSession session, MongoDatabase database, PipelineIntentScope scope) {
        return renewalProof(readPipelineAndBusiness(session, database, scope, List.of()), scope);
    }

    record PinProof(RenewalProof state, Document claim) {}

    static PinProof readPin(ClientSession session, MongoDatabase database, PipelineIntentScope scope, WorkloadClaimFence original) {
        var id = MongoWorkloadClaimStore.id(original.key());
        var live = WorkloadClaimDocuments.live(original).append("pipelineScope", MongoPipelinePreparationFacts.scope(scope))
                .append("pipelinePreparation.phase", "PREPARED");
        var captured = readPipelineAndBusiness(session, database, scope, List.of(MongoCaptureStartupRows.lookup(
                SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", id, List.of(new Document("$match", live)))));
        var state = renewalProof(captured, scope);
        if (state == null || MongoCaptureStartupRows.ambiguous(captured, "claim", id)) return null;
        var claim = MongoCaptureStartupRows.get(captured, "claim");
        return claim == null ? null : new PinProof(state, claim);
    }

    private static Document readPipelineAndBusiness(ClientSession session, MongoDatabase database, PipelineIntentScope scope,
            List<Document> extra) {
        var stages = new ArrayList<>(List.of(match("cluster"), inline(),
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business"),
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_PIPELINE_STATE, "checkpoint", scope.pipelineId())));
        stages.addAll(extra);
        return SystemCollections.CACHE_IDENTITY.on(database).aggregate(session, stages).collation(SIMPLE).first();
    }

    private static RenewalProof renewalProof(Document captured, PipelineIntentScope scope) {
        if (captured == null || MongoCaptureStartupRows.ambiguous(captured, "schema", "business")
                || MongoCaptureStartupRows.ambiguous(captured, "checkpoint", scope.pipelineId())) return null;
        var identity = captured.get(ROW, Document.class);
        var schema = MongoCaptureStartupRows.get(captured, "schema");
        var checkpoint = MongoCaptureStartupRows.get(captured, "checkpoint");
        if (identity == null || !"cluster".equals(identity.get("_id")) || schema == null || checkpoint == null) return null;
        return new RenewalProof(identity, schema, checkpoint);
    }

    record Authority(Document identity, Document schema, Document checkpoint, Document claim) {}

    /** The four original authority facts, from a single server snapshot. */
    static Authority readAuthority(MongoDatabase snapshot, PipelineIntentScope scope, WorkloadClaimKey key) {
        var stages = authorityStages(scope, key);
        var captured = SystemCollections.CACHE_IDENTITY.on(snapshot).aggregate(stages).collation(SIMPLE).first();
        return authority(captured);
    }

    record PipelineAuthority(Authority facts, Document node) {}

    /** A node-backed tenure and its original live node are sampled by the same server command. */
    static PipelineAuthority readPipelineAuthority(MongoDatabase snapshot, PipelineIntentScope scope, WorkloadClaimKey key) {
        var stages = authorityStages(scope, key);
        var nodeId = new Document("clusterId", scope.clusterId())
                .append("resourceType", io.tapstate.spi.store.WorkloadClaimType.NODE_SESSION.name()).append("resourceId", "$$owner");
        stages.add(new Document("$lookup", new Document("from", SystemCollections.CACHE_WORKLOAD_CLAIMS.collectionName())
                .append("let", new Document("owner", new Document("$arrayElemAt", List.of("$claim.row.ownerNodeId", 0))))
                .append("pipeline", List.of(new Document("$match", new Document("$expr", new Document("$eq", List.of("$_id", nodeId)))),
                        new Document("$limit", 1), MongoWorkloadClaimStore.leaseRemaining(true), inline()))
                .append("as", "node")));
        var captured = SystemCollections.CACHE_IDENTITY.on(snapshot).aggregate(stages).collation(SIMPLE).first();
        var facts = authority(captured);
        if (facts == null) return null;
        var rows = captured.getList("node", Document.class);
        var node = row(captured, "node");
        // An actual missing node is a lease refusal; a bounded-out row keeps the original point-read path.
        if (!rows.isEmpty() && node == null) return null;
        return new PipelineAuthority(facts, node);
    }

    static Authority readAuthority(ClientSession session, MongoDatabase database, PipelineIntentScope scope, WorkloadClaimKey key) {
        var stages = authorityStages(scope, key);
        return authority(SystemCollections.CACHE_IDENTITY.on(database).aggregate(session, stages).collation(SIMPLE).first());
    }

    record SinkReceipt(Authority authority, Document row) {}

    static SinkReceipt readSinkReceipt(MongoDatabase snapshot, PipelineIntentScope scope, WorkloadClaimKey key,
            Document receiptId) {
        return readSinkReceipt(null, snapshot, scope, key, receiptId);
    }

    static SinkReceipt readSinkReceipt(ClientSession session, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimKey key, Document receiptId) {
        var stages = authorityStages(scope, key);
        stages.add(lookup(SystemCollections.CACHE_SINK_PREPARATIONS, "receipt", receiptId, false));
        var table = SystemCollections.CACHE_IDENTITY.on(database);
        var query = session == null ? table.aggregate(stages) : table.aggregate(session, stages);
        var captured = query.collation(SIMPLE).first();
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
