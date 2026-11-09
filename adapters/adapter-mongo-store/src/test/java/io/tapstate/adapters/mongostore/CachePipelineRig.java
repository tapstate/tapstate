package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A cache database prepared the way a running pipeline finds it, shared by separate processes for kill tests:
 * the test prepares it and drives the pipeline claim's lifecycle; each member process attaches to the same
 * database by name, so a member killed or frozen is a real client of the real store with its sessions and
 * transactions open at the server.
 *
 * <p>Each attachment connects as the process it names, so the store can tell its clients apart the way it
 * tells the processes of a deployment apart when one of them goes away.
 */
final class CachePipelineRig implements AutoCloseable {

    static final PipelineIntentScope SCOPE = new PipelineIntentScope("rig-cluster", 1, "rig-cache", "rig", 1);
    static final CacheBusinessScope BUSINESS = new CacheBusinessScope("rig-cluster", 1, "rig-cache");
    static final WorkloadClaimKey KEY = new WorkloadClaimKey("rig-cluster", WorkloadClaimType.PIPELINE_ACTUATION, "rig");
    static final String STEP = "step";
    static final String PATH = "root";

    private final String url;
    private final String database;
    private final MongoClient raw;
    private final MongoCacheDomain domain;

    private CachePipelineRig(String url, String database, String processId) {
        this.url = url;
        this.database = database;
        this.raw = MongoClients.create(url);
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, withDatabase(url, database), database, null);
        this.domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(10), processId);
    }

    /** Joins, as the process {@code processId}, a database another process prepared. */
    static CachePipelineRig attach(String url, String database, String processId) {
        return new CachePipelineRig(url, database, processId);
    }

    /**
     * Prepares a fresh cache database, as the process {@code owner} boots under, and takes the pipeline claim
     * for {@code owner}, advanced to its first execution generation over {@code nodes}.
     */
    static Prepared prepare(String url, WorkloadOwner owner, Duration ttl, Set<String> nodes) {
        String name = "rig_" + UUID.randomUUID().toString().replace("-", "");
        CachePipelineRig rig = new CachePipelineRig(url, name, owner.bootId());
        MongoDatabase database = rig.raw.getDatabase(name);
        Runnable active = () -> { };
        rig.domain.prepareAuth(new CacheAuthScope("rig-cluster", 1, StorageBinding.Mode.OP, "rig-cache", 1), "auth-rig", active);
        try (var session = rig.raw.startSession()) {
            session.withTransaction(() -> {
                new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session, active);
                return null;
            }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
        }
        SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set",
                new Document("businessPhase", "READY").append("businessPreparationId", "business-rig").append("claimAdmissionFence", 0L)));
        SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                .append("phase", "READY").append("epoch", 1L).append("operationId", "business-rig"));
        SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", SCOPE.pipelineId())
                .append("scope", MongoPipelinePreparationFacts.scope(SCOPE)).append("stateJson", "NEW").append("epoch", 0L)
                .append("touchMillis", 0L).append("runtimeWriteFence", 1L));
        var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
        claims.release(claims.acquire(KEY, owner, 1, Duration.ofMinutes(30)).claim());
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(SCOPE))
                        .append("pipelinePreparation", MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(),
                                PipelineCachePreparations.Phase.PREPARED))));
        try (var session = rig.raw.startSession()) {
            session.withTransaction(() -> {
                String preparation = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session).first()
                        .get("pipelinePreparation", Document.class).getString("preparationId");
                MongoOperatorInventory.prepareInitial(session, database, SCOPE, preparation);
                return null;
            });
        }
        WorkloadClaim acquired = rig.claims().acquirePipeline(SCOPE, owner, 1, ttl).claim();
        WorkloadClaim running = rig.claims().advanceExecution(acquired, 1, nodes).orElseThrow();
        rig.domain.nestShapes(SCOPE, name, WorkloadClaimFence.from(running), active).recordIfAbsent(STEP, Set.of(PATH));
        return new Prepared(rig, running);
    }

    record Prepared(CachePipelineRig rig, WorkloadClaim claim) { }

    String database() {
        return database;
    }

    String url() {
        return url;
    }

    /** The pipeline claims, as this attachment's process takes and holds them. */
    WorkloadClaimStore claims() {
        return domain.workloadClaims(BUSINESS);
    }

    /** Milliseconds left on the pipeline claim's lease, on the store's clock; negative once it has lapsed. */
    long leaseRemainingMillis() {
        return claims().readPipeline(SCOPE).orElseThrow().leaseRemaining().toMillis();
    }

    /** The transactions a client of the process {@code processId} holds open at the server. */
    List<Document> transactionsOpenBy(String processId) {
        return raw.getDatabase("admin").aggregate(List.of(
                new Document("$currentOp", new Document("idleSessions", true).append("allUsers", true)),
                new Document("$match", new Document("transaction", new Document("$exists", true))
                        .append("appName", new Document("$regex", java.util.regex.Pattern.quote(MongoConnection.processTag(processId)) + "$")))))
                .into(new ArrayList<>());
    }

    /** The pipeline claim's document as the store holds it, read with majority read concern. */
    Document claimRow() {
        return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(raw.getDatabase(database)).withReadConcern(ReadConcern.MAJORITY)
                .find(new Document("_id", MongoWorkloadClaimStore.id(KEY))).first();
    }

    /** A plain client of the store, connected as the process {@code processId}, for a member to write through. */
    static MongoClient clientOf(String url, String processId) {
        return MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(url))
                .applicationName(MongoConnection.processTag(processId)).build());
    }

    static String withDatabase(String url, String name) {
        int query = url.indexOf('?');
        String base = query < 0 ? url : url.substring(0, query);
        String params = query < 0 ? "" : url.substring(query);
        int slash = base.indexOf('/', "mongodb://".length());
        String host = slash < 0 ? base : base.substring(0, slash);
        return host + "/" + name + params;
    }

    @Override
    public void close() {
        try {
            domain.close();
        } finally {
            raw.close();
        }
    }
}
