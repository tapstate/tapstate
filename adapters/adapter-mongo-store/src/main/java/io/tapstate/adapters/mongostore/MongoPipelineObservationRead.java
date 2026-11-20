package io.tapstate.adapters.mongostore;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;

import static io.tapstate.adapters.mongostore.SystemCollections.*;

/** Reads the current execution's observation facts in one CACHE snapshot. */
final class MongoPipelineObservationRead {
    static final List<SystemCollections> COLLECTIONS = List.of(CACHE_IDENTITY, CACHE_SCHEMA_MIGRATIONS, CACHE_PIPELINE_STATE,
            CACHE_WORKLOAD_CLAIMS, SRS_CHAINS, SRS_CONSUMERS, CACHE_SRS_CONSUMER_OFFSETS, SRS_MODELS, SRS_MODEL_CONTEXTS,
            SRS_LOG_BOUNDS, SRS_EVENTS, CACHE_SOURCE_SCHEMAS, CACHE_SNAPSHOT_COUNTS, CACHE_PIPELINE_OBSERVATION);
    private static final int MAX_RELATIONS = 128;
    private static final int MAX_TABLES = 256;
    private static final int ROW_BYTES = 4096;
    private static final String OVERSIZED = "tooLarge";
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoCachePipelineAccess access;

    MongoPipelineObservationRead(MongoClient client, MongoDatabase database, MongoCachePipelineAccess access) {
        this.client = client;
        this.database = database;
        this.access = access;
    }

    Optional<MongoCacheDomain.ObservationFacts> read(Map<SrsConsumerRun, MongoCacheDomain.ObservationSource> sources) {
        var scope = access.scope;
        var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
        var claim = access.originalClaim();
        if (sources.isEmpty() || sources.size() > 4 || sources.keySet().stream().anyMatch(source ->
                !scope.equals(source.scope()) || !claim.equals(source.originalClaim()) || source.sourceId().contains("."))) return Optional.empty();

        List<Document> query = new ArrayList<>();
        query.add(match(new Document("_id", "cluster")));
        query.add(originalRow());
        query.add(one(CACHE_SCHEMA_MIGRATIONS, "schema", "business"));
        query.add(one(CACHE_PIPELINE_STATE, "state", scope.pipelineId()));
        query.add(lease("claim", claim.key()));
        query.add(lease("node", new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.NODE_SESSION, claim.owner().nodeId())));
        List<String> chains = sources.keySet().stream().map(SrsConsumerRun::miningChainId).distinct().toList();
        for (int i = 0; i < chains.size(); i++) {
            query.add(lookup(SRS_CHAINS, "chain" + i, chainQuery(business, chains.get(i))));
        }
        List<String> ids = sources.keySet().stream().map(SrsConsumerRun::sourceId).toList();
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            var tables = lookup(CACHE_SOURCE_SCHEMAS, "tables", List.of(
                    match(new Document("_id", MongoCacheSchemaStore.ownedKeys(id))
                            .append("cacheScope.clusterId", scope.clusterId()).append("cacheScope.bindingVersion", scope.bindingVersion())
                            .append("cacheScope.cacheIncarnation", scope.cacheIncarnation()).append("$expr",
                            new Document("$eq", List.of("$" + MongoSchemaStore.GENERATION, "$$generation")))),
                    new Document("$sort", new Document("order", 1)), new Document("$limit", MAX_TABLES + 1), boundedRow()));
            tables.get("$lookup", Document.class).append("let", new Document("generation", "$body." + MongoSchemaStore.GENERATION));
            query.add(lookup(CACHE_SOURCE_SCHEMAS, "sourceSchema" + i, List.of(
                    match(new Document("_id", id)), boundedRow(), originalRow(), tables)));
        }
        var countIds = sources.entrySet().stream().flatMap(entry -> entry.getValue().tables().stream()
                .map(table -> MongoSnapshotStateStore.countId(scope, entry.getKey().sourceId(), table))).toList();
        if (countIds.size() > MAX_TABLES) return Optional.empty();
        query.add(lookup(CACHE_SNAPSHOT_COUNTS, "counts", bounded(match(in("_id", countIds)), MAX_TABLES)));
        query.add(boundedRow(12 * 1024 * 1024));

        return StoreIo.call(() -> {
            try (var session = client.startSession(ClientSessionOptions.builder().snapshot(true).build())) {
                var root = CACHE_IDENTITY.on(database).aggregate(session, query).first();
                if (root == null || Boolean.TRUE.equals(root.get(OVERSIZED)) || overflow(root, "schema", 1) || overflow(root, "state", 1)
                        || overflow(root, "claim", 1) || overflow(root, "node", 1)
                        || overflow(root, "counts", MAX_TABLES)) return Optional.empty();
                MongoCacheBusinessIdentity.requireObservedAdmission(root.get("body", Document.class), single(root, "schema"), business);
                var state = single(root, "state");
                access.validateState(state);
                var original = single(root, "claim");
                access.requirePreparedClaim(session, original);
                var reading = MongoWorkloadClaimStore.observedPipelineReading(original, single(root, "node"));
                if (!reading.leased() || !claim.equals(WorkloadClaimFence.from(reading.claim()))) throw MongoCachePipelineAccess.claimFenced();
                var inputs = MongoPipelineSourcePreparations.retained(original, scope);
                var cursors = new LinkedHashMap<String, ConsumerOffset>();
                for (int i = 0; i < chains.size(); i++) {
                    var raw = single(root, "chain" + i);
                    if (raw == null) return Optional.empty();
                    var chain = chains.get(i);
                    var view = chainView(business, chain, raw);
                    if (view.isEmpty()) return Optional.empty();
                    for (var source : sources.entrySet()) {
                        var run = source.getKey();
                        if (!chain.equals(run.miningChainId())) continue;
                        String id = run.context().key().consumerId();
                        var member = view.get().members().get(id);
                        var cursor = view.get().cursors().get(id);
                        if (member == null || member.detached() || cursor == null
                                || !run.context().equals(member.context()) || member.revision() != run.membershipRevision()
                                || !run.originalClaim().equals(member.commandFence()) || cursor.value().snapshotEpoch() != source.getValue().snapshotEpoch()
                                || !member.tables().equals(source.getValue().tables()) || !member.tables().equals(inputs.selectedTables().get(run.sourceId()))) return Optional.empty();
                        cursors.put(run.sourceId(), cursor.value());
                    }
                }
                var schemas = new HashMap<String, DiscoveredSourceModel>();
                var schemaStore = new MongoCacheSchemaStore(new MongoCacheMetadataAccess(client, database, business, CACHE_SOURCE_SCHEMAS, () -> {}));
                for (int i = 0; i < ids.size(); i++) {
                    var row = single(root, "sourceSchema" + i);
                    if (row == null || overflow(row, "tables", MAX_TABLES)) return Optional.empty();
                    var envelope = row.get("body", Document.class);
                    if (!ids.get(i).equals(envelope.get("_id"))) return Optional.empty();
                    var tables = rows(row, "tables");
                    var discovered = schemaStore.decodeDiscovered(ids.get(i), envelope, tables.iterator());
                    if (discovered.isEmpty()) return Optional.empty();
                    schemas.put(ids.get(i), discovered.get());
                }
                var counts = counts(root, sources, inputs);
                return Optional.of(new MongoCacheDomain.ObservationFacts(MongoStateStore.toCheckpoint(state), cursors, counts, schemas));
            }
        });
    }

    private Map<String, Map<String, Long>> counts(Document root, Map<SrsConsumerRun, MongoCacheDomain.ObservationSource> sources, PipelinePreparationInputs inputs) {
        var wanted = new HashMap<Document, SrsConsumerRun>();
        var tables = new HashMap<Document, String>();
        for (var source : sources.keySet()) {
            for (String table : inputs.selectedTables().get(source.sourceId())) {
                var id = MongoSnapshotStateStore.countId(access.scope, source.sourceId(), table);
                wanted.put(id, source);
                tables.put(id, table);
            }
        }
        var results = new HashMap<String, Map<String, Long>>();
        for (var row : rows(root, "counts")) {
            var id = row.get("_id", Document.class);
            var source = wanted.get(id);
            if (source == null) continue;
            MongoSnapshotStateStore.requireCount(row, id);
            if (sources.get(source).snapshotEpoch() == row.getLong("snapshotEpoch")) {
                results.computeIfAbsent(source.sourceId(), ignored -> new HashMap<>()).put(tables.get(id), row.getLong("rows"));
            }
        }
        results.replaceAll((source, byTable) -> Map.copyOf(byTable));
        return Map.copyOf(results);
    }

    private Optional<MongoSrsConsumerStore.View> chainView(CacheBusinessScope business, String chainId, Document root) {
        if (overflow(root, "members", MAX_RELATIONS) || overflow(root, "offsets", MAX_RELATIONS)
                || overflow(root, "models", MAX_TABLES) || overflow(root, "contexts", MAX_TABLES)
                || overflow(root, "bounds", MAX_TABLES) || overflow(root, "events", MAX_TABLES)) return Optional.empty();
        var contexts = new MongoSrsModelContexts(database, business);
        var original = root.get("body", Document.class);
        if (!chainId.equals(original.get("_id"))) return Optional.empty();
        var chain = MongoSrsChainStore.decode(original, business, contexts);
        var view = new MongoSrsConsumerStore(client, database, business, () -> {}, () -> {})
                .relations(chainId, original, chain, rows(root, "members").iterator(), rows(root, "offsets").iterator());
        if (!validateReferences(business, original, root, chain, view, contexts)) return Optional.empty();
        return Optional.of(view);
    }

    private boolean validateReferences(CacheBusinessScope business, Document original, Document root, SrsChainState chain,
            MongoSrsConsumerStore.View view, MongoSrsModelContexts contexts) {
        if (chain.pendingFreshFull() != null) return false;
        var needed = new HashSet<SrsModelKey>();
        for (var cursor : view.cursors().values()) {
            if (cursor.snapshot() == null) continue;
            for (var key : cursor.snapshot().models().values()) {
                if (!chain.miningChainId().equals(key.miningChainId()) || !business.cacheIncarnation().equals(key.cacheIncarnation())) return false;
                needed.add(key);
            }
        }
        var checkpoint = chain.checkpoint();
        if (checkpoint != null) for (var table : checkpoint.tables().values()) {
            var key = table.model();
            if (!chain.miningChainId().equals(key.miningChainId()) || !business.cacheIncarnation().equals(key.cacheIncarnation())
                    || key.captureEpoch() != checkpoint.position().order().epoch()) return false;
            needed.add(key);
        }
        if (needed.isEmpty()) return true;

        var controls = new HashMap<Document, MongoSrsModelContexts.Context>();
        for (var row : rows(root, "contexts")) {
            var context = contexts.decode(row);
            if (controls.put(context.id(), context) != null) return false;
        }
        if (controls.size() != needed.size()) return false;
        var bounds = new HashMap<String, SrsLogBounds>();
        for (var row : rows(root, "bounds")) bounds.put(row.getString("_id"), MongoSrsLogStore.decodeSplitBounds(row));

        var models = new MongoSrsModelStore(client, database, business, () -> {}, () -> {});
        var loaded = new HashMap<SrsModelKey, SrsModel>();
        for (var row : rows(root, "models")) {
            var model = models.decode(row);
            if (!needed.contains(model.key()) || loaded.put(model.key(), model) != null) return false;
            var control = controls.get(MongoSrsModelContexts.contextId(model.key()));
            var head = contexts.head(original, model.key().table());
            if (control == null || control.nextId() != null || control.publishedVersion() != model.key().modelVersion()
                    || head == null || !control.id().equals(head.get("contextId"))
                    || !bounds.containsKey(MongoSrsLogStore.boundsId(model.ringName()))) return false;
            contexts.requireAnchor(original, control, control);
            models.requirePublished(control, model);
        }
        if (!loaded.keySet().equals(needed)) return false;
        if (checkpoint != null) {
            var events = new HashMap<Document, Document>();
            for (var row : rows(root, "events")) events.put(row.get("_id", Document.class), row);
            for (var table : checkpoint.tables().values()) {
                var model = loaded.get(table.model());
                var bound = bounds.get(MongoSrsLogStore.boundsId(model.ringName()));
                long sequence = table.sequence();
                if (sequence < model.ddlSequence() - 1 || sequence > bound.largestSequence()) return false;
                if (sequence >= 0 && sequence > bound.trimmedThrough()) {
                    var row = events.get(MongoSrsLogStore.key(model.ringName(), sequence));
                    // A checkpoint just before the model boundary may refer to an older model.
                    if (row == null || sequence < model.ddlSequence()) return false;
                    var event = MongoSrsLogStore.splitRecord(row);
                    var control = controls.get(MongoSrsModelContexts.contextId(model.key()));
                    if (event.epoch() != model.key().captureEpoch() || event.schemaVer() != model.key().modelVersion()
                            || !control.fence().equals(event.captureFence())) return false;
                }
            }
        }
        return true;
    }

    private static List<Document> chainQuery(CacheBusinessScope business, String chain) {
        var key = new MongoSrsConsumerRecords(business).chain(chain);
        var pinKeys = new Document("$reduce", new Document("input", "$offsets").append("initialValue", List.of())
                .append("in", new Document("$setUnion", List.of("$$value",
                        new Document("$ifNull", List.of("$$this.snapshot.models.model", List.of()))))));
        var checkpointKeys = new Document("$ifNull", List.of("$body.checkpoint.tables.model", List.of()));
        var checkpointTables = new Document("$ifNull", List.of("$body.checkpoint.tables", List.of()));
        var matchModel = new Document("$filter", new Document("input", "$models").append("as", "model")
                .append("cond", new Document("$eq", List.of("$$model._id", "$$table.model"))));
        var checkpointEvents = new Document("$map", new Document("input",
                new Document("$filter", new Document("input", checkpointTables).append("as", "table")
                        .append("cond", new Document("$gte", List.of("$$table.sequence", 0)))))
                .append("as", "table").append("in", new Document("$let", new Document("vars",
                        new Document("selected", new Document("$arrayElemAt", List.of(matchModel, 0))))
                        .append("in", new Document("ring", "$$selected.ringName").append("seq", "$$table.sequence")))));
        return List.of(match(new Document("_id", chain)), boundedRow(), originalRow(),
                lookup(SRS_CONSUMERS, "members", bounded(match(key), MAX_RELATIONS)),
                lookup(CACHE_SRS_CONSUMER_OFFSETS, "offsets",
                        List.of(match(key), new Document("$limit", MAX_RELATIONS + 1), boundedRow(16 * 1024))),
                new Document("$set", new Document("modelKeys", new Document("$setUnion", List.of(pinKeys, checkpointKeys)))),
                join(SRS_MODELS, "models", "modelKeys", "_id", List.of(), MAX_TABLES),
                join(SRS_MODEL_CONTEXTS, "contexts", "models.ringName", "ringName",
                        List.of(match(new Document("cacheIncarnation", business.cacheIncarnation()))), MAX_TABLES),
                new Document("$set", new Document("boundKeys", new Document("$map",
                        new Document("input", "$models").append("as", "model").append("in",
                                new Document("$concat", List.of(MongoSrsLogStore.boundsId(""), "$$model.ringName")))))),
                join(SRS_LOG_BOUNDS, "bounds", "boundKeys", "_id", List.of(), MAX_TABLES),
                new Document("$set", new Document("checkpointKeys", checkpointEvents)),
                join(SRS_EVENTS, "events", "checkpointKeys", "_id", List.of(), MAX_TABLES));
    }

    static Document match(Document filter) { return new Document("$match", filter); }
    static Document in(String field, Collection<?> ids) { return new Document(field, new Document("$in", List.copyOf(ids))); }
    static Document lookup(SystemCollections collection, String name, List<Document> query) {
        return new Document("$lookup", new Document("from", collection.collectionName()).append("as", name).append("pipeline", query));
    }
    private static Document join(SystemCollections collection, String name, String local, String foreign, List<Document> query, int limit) {
        var bounded = new ArrayList<>(query);
        bounded.add(new Document("$limit", limit + 1));
        bounded.add(boundedRow());
        return new Document("$lookup", new Document("from", collection.collectionName()).append("as", name)
                .append("localField", local).append("foreignField", foreign).append("pipeline", bounded));
    }
    static Document one(SystemCollections collection, String name, Object id) {
        return lookup(collection, name, bounded(match(new Document("_id", id)), 1));
    }
    static List<Document> bounded(Document filter, int limit) {
        return List.of(filter, new Document("$limit", limit + 1), boundedRow());
    }
    private static Document lease(String name, WorkloadClaimKey key) {
        return lookup(CACHE_WORKLOAD_CLAIMS, name, List.of(match(new Document("_id", MongoWorkloadClaimStore.id(key))),
                new Document("$limit", 2), boundedRow(), MongoWorkloadClaimStore.leaseRemaining(true)));
    }
    static Document originalRow() {
        return new Document("$project", new Document("_id", 0).append("body", "$$ROOT"));
    }

    static Document boundedRow() { return boundedRow(ROW_BYTES); }

    static Document boundedRow(int maxBytes) {
        return new Document("$replaceWith", new Document("$cond", List.of(
                new Document("$lte", List.of(new Document("$bsonSize", "$$ROOT"), maxBytes)), "$$ROOT",
                new Document(OVERSIZED, true))));
    }
    static List<Document> rows(Document result, String name) { return result.getList(name, Document.class, List.of()); }
    static boolean overflow(Document result, String name, int limit) {
        var rows = rows(result, name);
        return rows.size() > limit || rows.stream().anyMatch(row -> Boolean.TRUE.equals(row.get(OVERSIZED)));
    }
    static Document single(Document result, String name) {
        return rows(result, name).size() == 1 && !overflow(result, name, 1) ? rows(result, name).getFirst() : null;
    }
}
