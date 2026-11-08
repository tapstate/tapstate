package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.NestReleaseStore;
import io.tapstate.spi.store.OperatorPlacement;
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
import org.bson.types.Binary;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The production transactional cache store prepared the way a running pipeline finds it, split so that
 * separate processes can share it: the controller prepares a database and holds the claim's lifecycle;
 * each member process opens the same operator-state placement under a fence it is handed on its command
 * line, so a member killed with SIGKILL is a real process dying with its connections and its transactions
 * open, not a thread being interrupted.
 */
public final class KillRig implements AutoCloseable {

    public static final PipelineIntentScope SCOPE = new PipelineIntentScope("kill-cluster", 1, "kill-cache", "kill", 1);
    public static final CacheBusinessScope BUSINESS = new CacheBusinessScope("kill-cluster", 1, "kill-cache");
    public static final WorkloadClaimKey KEY = new WorkloadClaimKey("kill-cluster", WorkloadClaimType.PIPELINE_ACTUATION, "kill");
    public static final String STEP = "step";
    public static final String PATH = "root";
    public static final String NAMESPACE = "nest.orders";

    private final String url;
    private final String database;
    private final MongoClient raw;
    private final MongoCacheDomain domain;

    private KillRig(String url, String database) {
        this.url = url;
        this.database = database;
        this.raw = MongoClients.create(url);
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, withDatabase(url, database), database, null);
        this.domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(10), "nest-kill");
    }

    /** Joins a database another process prepared. */
    public static KillRig attach(String url, String database) {
        for (int attempt = 1; ; attempt++) {
            try {
                return new KillRig(url, database);
            } catch (io.tapstate.core.common.TapstateException unreachable) {
                // a member starting while the set elects or the host is busy; the bench is not about this
                if (attempt == 5) throw unreachable;
            }
        }
    }

    /**
     * Prepares a fresh cache database and takes the pipeline claim for {@code owner}, advanced to its
     * first execution generation over {@code nodes}.
     */
    public static Prepared prepare(String url, WorkloadOwner owner, Duration ttl, Set<String> nodes) {
        String name = "kill_" + UUID.randomUUID().toString().replace("-", "");
        KillRig rig = new KillRig(url, name);
        MongoDatabase database = rig.raw.getDatabase(name);
        Runnable active = () -> { };
        rig.domain.prepareAuth(new CacheAuthScope("kill-cluster", 1, StorageBinding.Mode.OP, "kill-cache", 1), "auth-kill", active);
        try (var session = rig.raw.startSession()) {
            session.withTransaction(() -> {
                new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session, active);
                return null;
            }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
        }
        SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set",
                new Document("businessPhase", "READY").append("businessPreparationId", "business-kill").append("claimAdmissionFence", 0L)));
        SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                .append("phase", "READY").append("epoch", 1L).append("operationId", "business-kill"));
        SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "kill")
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

    public record Prepared(KillRig rig, WorkloadClaim claim) { }

    public String database() {
        return database;
    }

    public String url() {
        return url;
    }

    public WorkloadClaimStore claims() {
        return domain.workloadClaims(BUSINESS);
    }

    /** The claim as the store holds it now. */
    public WorkloadClaim current() {
        return claims().readPipeline(SCOPE).orElseThrow().claim();
    }

    /**
     * The operator-state placement a member writes through, fenced by {@code fence}: every mutation proves
     * this fence's claim and execution generation inside its own transaction.
     */
    public NestReleaseStore store(MongoClient client, WorkloadClaimFence fence) {
        OperatorPlacement at = new OperatorPlacement(SCOPE, database, OperatorPlacement.Resource.NEST_STATE, STEP, PATH, NAMESPACE);
        return new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(client, client.getDatabase(database), SCOPE, fence, () -> { }), at);
    }

    /** The name the store expects for the namespace, as the engine would pass it. */
    public String mapName() {
        return new OperatorPlacement(SCOPE, database, OperatorPlacement.Resource.NEST_STATE, STEP, PATH, NAMESPACE).mapName();
    }

    /**
     * Every state row, key to its decoded value, read straight from the collection with read concern
     * majority and no fence: what a new owner would find, and the ground truth the checks compare with.
     */
    public Map<String, String> rows() {
        Map<String, String> out = new LinkedHashMap<>();
        var collection = SystemCollections.CACHE_NEST_STATE.on(raw.getDatabase(database)).withReadConcern(ReadConcern.MAJORITY);
        for (Document row : collection.find()) {
            Document id = row.get("_id", Document.class);
            String key = OperatorStateText.decode(id.getString("key"));
            out.put(key, decode(row.get("state", Binary.class).getData()));
        }
        return out;
    }

    /** The pipeline's runtime write fence as the store holds it. */
    public long runtimeWriteFence() {
        return SystemCollections.CACHE_PIPELINE_STATE.on(raw.getDatabase(database)).find(new Document("_id", "kill")).first()
                .getLong("runtimeWriteFence");
    }

    /** Server-side transactions still open, as {@code $currentOp} reports them. */
    public List<Document> openTransactions() {
        return raw.getDatabase("admin").aggregate(List.of(
                new Document("$currentOp", new Document("idleSessions", true).append("allUsers", true)),
                new Document("$match", new Document("transaction", new Document("$exists", true))))).into(new java.util.ArrayList<>());
    }

    private static String decode(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return String.valueOf(in.readObject());
        } catch (Exception unreadable) {
            return "<unreadable>";
        }
    }

    public static String withDatabase(String url, String name) {
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
