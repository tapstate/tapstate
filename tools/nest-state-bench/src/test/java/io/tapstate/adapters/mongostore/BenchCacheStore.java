package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.NestReleaseStore;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.OperatorStateStore;
import io.tapstate.spi.store.OperatorStateStores;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The production transactional cache store, prepared the way a running pipeline finds it: business
 * structure ready, a pipeline-actuation claim held and advanced to execution, the nest shape recorded.
 * Every nest namespace gets its own placement, exactly as the engine binds them, and every command the
 * store sends to the cache database is counted by name so a run can report round trips per row.
 */
public final class BenchCacheStore implements AutoCloseable {

    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("bench-cluster", 1, "bench-cache", "bench", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("bench-cluster", 1, "bench-cache");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("bench-cluster", WorkloadClaimType.PIPELINE_ACTUATION, "bench");
    private static final WorkloadOwner OWNER = new WorkloadOwner("bench-node", "bench-boot");
    private static final String STEP = "step";
    private static final String PATH = "root";

    private final MongoClient setup;
    private final MongoClient work;
    private final MongoCacheDomain domain;
    private final MongoDatabase database;
    private final WorkloadClaim claim;
    private final Map<String, AtomicLong> commands = new ConcurrentHashMap<>();
    private final Map<String, KeyedStateStore> byNamespace = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> calls = new ConcurrentHashMap<>();

    private BenchCacheStore(String setupUrl, String workUrl) {
        String name = "bench_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, withDatabase(setupUrl, name), name, null);
        setup = MongoClients.create(setupUrl);
        CommandListener counting = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getDatabaseName().equals(name) || "admin".equals(event.getDatabaseName())) {
                    commands.computeIfAbsent(event.getCommandName(), ignored -> new AtomicLong()).incrementAndGet();
                }
            }
        };
        work = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(workUrl))
                .addCommandListener(counting).build());
        domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(10), "nest-bench");
        database = setup.getDatabase(name);
        Runnable active = () -> { };
        domain.prepareAuth(new CacheAuthScope("bench-cluster", 1, StorageBinding.Mode.OP, "bench-cache", 1), "auth-bench", active);
        try (var session = setup.startSession()) {
            session.withTransaction(() -> {
                new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session, active);
                return null;
            }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
        }
        SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set",
                new Document("businessPhase", "READY").append("businessPreparationId", "business-bench").append("claimAdmissionFence", 0L)));
        SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                .append("phase", "READY").append("epoch", 1L).append("operationId", "business-bench"));
        SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "bench")
                .append("scope", MongoPipelinePreparationFacts.scope(SCOPE)).append("stateJson", "NEW").append("epoch", 0L)
                .append("touchMillis", 0L).append("runtimeWriteFence", 1L));
        var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
        raw.release(raw.acquire(KEY, OWNER, 1, Duration.ofMinutes(30)).claim());
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(SCOPE))
                        .append("pipelinePreparation", MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(),
                                PipelineCachePreparations.Phase.PREPARED))));
        try (var session = setup.startSession()) {
            session.withTransaction(() -> {
                String preparation = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session).first()
                        .get("pipelinePreparation", Document.class).getString("preparationId");
                MongoOperatorInventory.prepareInitial(session, database, SCOPE, preparation);
                return null;
            });
        }
        WorkloadClaim acquired = domain.workloadClaims(BUSINESS).acquirePipeline(SCOPE, OWNER, 1, Duration.ofMinutes(30)).claim();
        claim = domain.workloadClaims(BUSINESS).advanceExecution(acquired, 1, Set.of(OWNER.nodeId())).orElseThrow();
        domain.nestShapes(SCOPE, name, WorkloadClaimFence.from(claim), active).recordIfAbsent(STEP, Set.of(PATH));
    }

    /** Prepares a fresh cache database; setup goes straight to Mongo, the measured store through workUrl. */
    public static BenchCacheStore open(String setupUrl, String workUrl) {
        return new BenchCacheStore(setupUrl, workUrl);
    }

    /** The stores a member is bound to: one transactional placement per nest namespace. */
    public OperatorStateStores stores() {
        KeyedStateStore routing = new Routing();
        OperatorStateStore state = new OperatorStateStore(routing, new NestDeadLetterStore() {
            @Override public void record(NestDeadLetterRecord record) { }
            @Override public List<NestDeadLetterRecord> read(String namespace, int limit) { return List.of(); }
            @Override public void dropNamespace(String namespace) { }
        });
        return new OperatorStateStores() {
            @Override public String defaultDatabase() { return database.getName(); }
            @Override public OperatorStateStore inDatabase(String ignored) { return state; }
        };
    }

    /** Commands sent by the measured store so far, by command name. */
    public Map<String, Long> commands() {
        Map<String, Long> out = new TreeMap<>();
        commands.forEach((name, count) -> out.put(name, count.get()));
        return out;
    }

    /** Store calls by operation and namespace suffix, each one a transaction (or more, past 256 keys). */
    public Map<String, Long> calls() {
        Map<String, Long> out = new TreeMap<>();
        calls.forEach((name, count) -> out.put(name, count.get()));
        return out;
    }

    private void call(String operation, String namespace) {
        String level = namespace.substring(namespace.lastIndexOf('.') + 1);
        if (namespace.endsWith(".refs") || namespace.endsWith(".parking")) {
            String trimmed = namespace.substring(0, namespace.lastIndexOf('.'));
            level = trimmed.substring(trimmed.lastIndexOf('.') + 1) + "." + level;
        }
        calls.computeIfAbsent(operation + ":" + level, ignored -> new AtomicLong()).incrementAndGet();
    }

    /** Stored nest state rows, read directly. */
    public long storedRows() {
        return SystemCollections.CACHE_NEST_STATE.on(database).countDocuments();
    }

    @Override
    public void close() {
        try {
            database.drop();
        } finally {
            work.close();
            setup.close();
            domain.close();
        }
    }

    private KeyedStateStore placed(String namespace) {
        return byNamespace.computeIfAbsent(namespace, ns -> {
            OperatorPlacement at = new OperatorPlacement(SCOPE, database.getName(), OperatorPlacement.Resource.NEST_STATE, STEP, PATH, ns);
            return new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(work, work.getDatabase(database.getName()), SCOPE,
                    WorkloadClaimFence.from(claim), () -> { }), at);
        });
    }


    private static String withDatabase(String url, String name) {
        int query = url.indexOf('?');
        String base = query < 0 ? url : url.substring(0, query);
        String params = query < 0 ? "" : url.substring(query);
        int slash = base.indexOf('/', "mongodb://".length());
        String host = slash < 0 ? base : base.substring(0, slash);
        return host + "/" + name + params;
    }

    /** Routes each nest namespace to its own placement, translating the name the engine uses into the placement's. */
    private final class Routing implements NestReleaseStore {
        private String name(String namespace) {
            return new OperatorPlacement(SCOPE, database.getName(), OperatorPlacement.Resource.NEST_STATE, STEP, PATH, namespace).mapName();
        }
        @Override public Optional<byte[]> load(String namespace, String key) { call("load", namespace); return placed(namespace).load(name(namespace), key); }
        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) { call("loadAll", namespace); return placed(namespace).loadAll(name(namespace), keys); }
        @Override public void save(String namespace, String key, byte[] state) { call("save", namespace); placed(namespace).save(name(namespace), key, state); }
        @Override public void saveAll(String namespace, Map<String, byte[]> states) { call("saveAll", namespace); placed(namespace).saveAll(name(namespace), states); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) { call("saveIfAbsent", namespace); return placed(namespace).saveIfAbsent(name(namespace), key, state); }
        @Override public void delete(String namespace, String key) { call("delete", namespace); placed(namespace).delete(name(namespace), key); }
        @Override public void dropNamespace(String namespace) { placed(namespace).dropNamespace(name(namespace)); }
        @Override public long count(String namespace) { return placed(namespace).count(name(namespace)); }
        @Override public void saveWithRelease(String namespace, String key, byte[] state, byte[] routingKey) {
            ((NestReleaseStore) placed(namespace)).saveWithRelease(name(namespace), key, state, routingKey);
        }
        @Override public List<PendingKey> pendingReleases(String namespace, String afterKey) {
            return ((NestReleaseStore) placed(namespace)).pendingReleases(name(namespace), afterKey);
        }
    }
}
