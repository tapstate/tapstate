package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPIKE (throwaway, not for merge): one transaction per vertex group across every namespace the vertex wrote,
 * on top of lease validation. Runs against a hand-started replica set: -Dspike.mongo=mongodb://localhost:27017/?replicaSet=rs0
 */
@EnabledIfSystemProperty(named = "spike.mongo", matches = ".+")
class StateSessionGroupSpikeIT {
    private static final String URL = System.getProperty("spike.mongo", "");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};
    private static final List<String> NAMESPACES = List.of("lookup.customers", "reverse.customers", "assembler.orders");

    @AfterEach void reset() { MongoCacheOperatorStateStore.afterPart = i -> {}; }

    @Test void threeNamespacesCommitAsOneTransactionOfThreeCommands() {
        withFixture(f -> {
            var commands = new CopyOnWriteArrayList<String>();
            try (var client = observed(commands, f.database.getName())) {
                var stores = f.session(client);
                stores.forEach(MongoCacheOperatorStateStore::openLease); commands.clear();
                MongoCacheOperatorStateStore.commitGroup(parts(stores, "a", 80, 64));
                System.out.println("SPIKE group commands: " + commands);
                assertThat(commands).containsExactly("update:workload_claims", "find:operator_cleanup", "update:nest_state");
                assertThat(f.stored()).isEqualTo(240);
                commands.clear();
                for (int i = 0; i < stores.size(); i++) stores.get(i).saveAll(stores.get(i).placementName(), keys("b", 80, 64));
                System.out.println("SPIKE per-namespace commands: " + commands);
                assertThat(commands).hasSize(9);
                commands.clear();
                var mixed = new ArrayList<MongoCacheOperatorStateStore.GroupPart>();
                mixed.add(new MongoCacheOperatorStateStore.GroupPart(stores.get(0), keys("c", 10, 64), List.of("a1", "a2")));
                mixed.add(new MongoCacheOperatorStateStore.GroupPart(stores.get(1), Map.of(), List.of("a3")));
                MongoCacheOperatorStateStore.commitGroup(mixed);
                System.out.println("SPIKE mixed commands: " + commands);
            }
        });
    }

    @Test void aFencedGroupLandsInNoNamespace() {
        withFixture(f -> {
            var stores = f.session(f.client);
            MongoCacheOperatorStateStore.commitGroup(parts(stores, "a", 10, 64)); long before = f.stored();
            f.onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
            code(() -> MongoCacheOperatorStateStore.commitGroup(parts(stores, "b", 200, 64)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
            code(() -> stores.get(2).loadAll(stores.get(2).placementName(), List.of("a1")), IoError.WORKLOAD_CLAIM_FENCED);
        });
    }

    /** A takeover rewrites the claim and every vertex fence of the pipeline in one transaction; the old session's next group is refused. */
    @Test @EnabledIfSystemProperty(named = "spike.fence.vertex", matches = "true") void aTakeoverRewritingEveryVertexFenceRefusesTheOldGroup() {
        withFixture(f -> {
            var a = f.session(f.client, "a"); var b = f.session(f.client, "b");
            MongoCacheOperatorStateStore.commitGroup(parts(a, "x", 10, 64)); MongoCacheOperatorStateStore.commitGroup(parts(b, "x", 10, 64)); long before = f.stored();
            try (var session = f.client.startSession()) { session.withTransaction(() -> {
                f.claims().updateOne(session, new Document("_id", MongoWorkloadClaimStore.id(KEY)), new Document("$inc", new Document("claimGeneration", 1L)));
                f.database.getCollection("spike_fences").updateMany(session, new Document("_id", new Document("$regex", "^orders/")), new Document("$inc", new Document("claimGeneration", 1L)));
                return null; }); }
            code(() -> MongoCacheOperatorStateStore.commitGroup(parts(a, "y", 50, 64)), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> MongoCacheOperatorStateStore.commitGroup(parts(b, "y", 50, 64)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void aFailureInsideTheTransactionLeavesThePreviousGroupWhole() {
        withFixture(f -> {
            var stores = f.session(f.client);
            MongoCacheOperatorStateStore.commitGroup(parts(stores, "a", 50, 64)); long before = f.stored();
            MongoCacheOperatorStateStore.afterPart = i -> { throw new IllegalStateException("crash after the payload, before commit"); };
            assertThatThrownBy(() -> MongoCacheOperatorStateStore.commitGroup(parts(stores, "b", 256, 64))).isInstanceOf(IllegalStateException.class);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void anOlderInventoryRestoredUnderOneNamespaceRefusesTheWholeGroup() {
        withFixture(f -> {
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database); Document older = inventory.find().first();
            var stores = f.session(f.client);
            MongoCacheOperatorStateStore.commitGroup(parts(stores, "a", 10, 64)); long before = f.stored();
            inventory.replaceOne(new Document(), older);
            code(() -> MongoCacheOperatorStateStore.commitGroup(parts(stores, "b", 10, 64)), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    /** Vertices committing at once on the same claim: one group per vertex against one transaction per namespace. */
    @Test @EnabledIfSystemProperty(named = "spike.bench", matches = "true") void bench() {
        int keys = Integer.getInteger("spike.keys", 64), bytes = Integer.getInteger("spike.bytes", 1024), groups = Integer.getInteger("spike.groups", 200);
        for (String mode : System.getProperty("spike.modes", "split,group").split(",")) for (int vertices : new int[]{1, 2, 4, 6}) withFixture(f -> {
            var sessions = new ArrayList<List<MongoCacheOperatorStateStore>>();
            for (int v = 0; v < vertices; v++) sessions.add(f.session(f.client, "v" + v));
            sessions.forEach(s -> s.forEach(MongoCacheOperatorStateStore::openLease));
            long attempts0 = MongoCacheOperatorStateStore.GROUP_ATTEMPTS.get();
            Document tx0 = f.client.getDatabase("admin").runCommand(new Document("serverStatus", 1)).get("transactions", Document.class);
            var latencies = Collections.synchronizedList(new ArrayList<Long>());
            var threads = new ArrayList<Thread>(); long started = System.nanoTime();
            for (var s : sessions) threads.add(Thread.ofPlatform().start(() -> {
                for (int g = 0; g < groups; g++) {
                    long t = System.nanoTime();
                    if (mode.equals("group")) MongoCacheOperatorStateStore.commitGroup(parts(s, "g" + g + "-", keys / 3, bytes));
                    else for (var store : s) store.saveAll(store.placementName(), keys("g" + g + "-", keys / 3, bytes));
                    latencies.add(System.nanoTime() - t);
                }
            }));
            for (var t : threads) t.join();
            double seconds = (System.nanoTime() - started) / 1e9;
            var sorted = new ArrayList<>(latencies); Collections.sort(sorted);
            long attempts = MongoCacheOperatorStateStore.GROUP_ATTEMPTS.get() - attempts0;
            Document tx1 = f.client.getDatabase("admin").runCommand(new Document("serverStatus", 1)).get("transactions", Document.class);
            System.out.printf("SPIKE server txns committed=%d aborted=%d%n", ((Number) tx1.get("totalCommitted")).longValue() - ((Number) tx0.get("totalCommitted")).longValue(),
                    ((Number) tx1.get("totalAborted")).longValue() - ((Number) tx0.get("totalAborted")).longValue());
            System.out.printf("SPIKE bench mode=%s vertices=%d keys/group=%d bytes=%d groups/s=%.0f keys/s=%.0f p50=%.1fms p95=%.1fms p99=%.1fms attempts/commit=%s%n",
                    mode, vertices, (keys / 3) * 3, bytes, sorted.size() / seconds, sorted.size() * (keys / 3) * 3 / seconds,
                    sorted.get(sorted.size() / 2) / 1e6, sorted.get(sorted.size() * 95 / 100) / 1e6, sorted.get(sorted.size() * 99 / 100) / 1e6,
                    mode.equals("group") ? String.format("%.2f", attempts / (double) sorted.size()) : "n/a");
        });
    }

    /** The byte bound: one group at the ADR's 8 MiB ceiling. */
    @Test @EnabledIfSystemProperty(named = "spike.bench", matches = "true") void anEightMebibyteGroupCommits() {
        withFixture(f -> {
            var stores = f.session(f.client);
            for (int i = 0; i < 3; i++) {
                long t = System.nanoTime();
                MongoCacheOperatorStateStore.commitGroup(List.of(new MongoCacheOperatorStateStore.GroupPart(stores.get(2), keys("big" + i + "-", 256, 32 * 1024), List.of())));
                System.out.printf("SPIKE 8MiB group (256 x 32KiB) committed in %.0f ms%n", (System.nanoTime() - t) / 1e6);
            }
            assertThat(f.stored()).isEqualTo(768);
        });
    }

    private static List<MongoCacheOperatorStateStore.GroupPart> parts(List<MongoCacheOperatorStateStore> stores, String prefix, int perNamespace, int bytes) {
        var out = new ArrayList<MongoCacheOperatorStateStore.GroupPart>();
        for (var store : stores) out.add(new MongoCacheOperatorStateStore.GroupPart(store, keys(prefix, perNamespace, bytes), List.of()));
        return out;
    }

    private static Map<String, byte[]> keys(String prefix, int count, int bytes) {
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) { byte[] v = new byte[bytes]; v[0] = (byte) i; out.put(prefix + i, v); }
        return out;
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code));
    }

    private static MongoClient observed(List<String> commands, String database) {
        CommandListener listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent event) {
            if (event.getDatabaseName().equals(database)) {
                var collection = event.getCommand().get(event.getCommandName());
                commands.add(event.getCommandName() + (collection != null && collection.isString() ? ":" + collection.asString().getValue() : ""));
            }
        }};
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(URL)).addCommandListener(listener).build());
    }

    /** The same prepared cache, pipeline claim and nest shape the operator-state suite starts from. */
    private static void withFixture(Scenario scenario) {
        String name = "session_spike_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, URL.replace("/?", "/" + name + "?"), name, null);
        try (var client = MongoClients.create(URL); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "lease-it")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1), "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session, ACTIVE); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "orders").append("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                        .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", 0L).append("runtimeWriteFence", 1L));
                var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var retained = raw.acquire(KEY, OWNER, 1, Duration.ofMinutes(5)).claim(); raw.release(retained);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), new Document("$set",
                        new Document("pipelineScope", MongoPipelinePreparationFacts.scope(SCOPE)).append("pipelinePreparation",
                                MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), PipelineCachePreparations.Phase.PREPARED))));
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    String preparationId = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session).first().get("pipelinePreparation", Document.class).getString("preparationId");
                    MongoOperatorInventory.prepareInitial(session, database, SCOPE, preparationId); return null;
                }); }
                var claim = domain.workloadClaims(BUSINESS).acquirePipeline(SCOPE, OWNER, 1, Duration.ofMinutes(5)).claim();
                claim = domain.workloadClaims(BUSINESS).advanceExecution(claim, 1, Set.of(OWNER.nodeId())).orElseThrow();
                domain.nestShapes(SCOPE, name, WorkloadClaimFence.from(claim), ACTIVE).recordIfAbsent("step", Set.of("root"));
                long fence = SystemCollections.CACHE_PIPELINE_STATE.on(database).find().first().getLong("runtimeWriteFence");
                var at = new OperatorPlacement(SCOPE, name, OperatorPlacement.Resource.NEST_STATE, "step", "root", "logical.namespace");
                scenario.run(new Fixture(client, database, domain, claim, at, fence));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; } catch (Exception e) { throw new AssertionError(e); }
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, WorkloadClaim claim, OperatorPlacement at,
                           long fenceAfterSetup) {
        KeyedStateStore writer() { return domain.operatorState(at, WorkloadClaimFence.from(claim), ACTIVE); }
        List<MongoCacheOperatorStateStore> session(MongoClient over) { return session(over, ""); }
        /** One vertex instance's session: one access, one store per namespace. */
        List<MongoCacheOperatorStateStore> session(MongoClient over, String vertex) {
            var access = new MongoCachePipelineAccess(over, over.getDatabase(database.getName()), SCOPE, WorkloadClaimFence.from(claim), ACTIVE);
            if (Boolean.getBoolean("spike.fence.vertex")) {
                access.spikeFenceId = "orders/" + vertex;
                database.getCollection("spike_fences").replaceOne(new Document("_id", access.spikeFenceId),
                        new Document("claimGeneration", claim.claimGeneration()).append("writes", 0L), new com.mongodb.client.model.ReplaceOptions().upsert(true));
            }
            var out = new ArrayList<MongoCacheOperatorStateStore>();
            for (String ns : NAMESPACES) out.add(new MongoCacheOperatorStateStore(access,
                    new OperatorPlacement(SCOPE, database.getName(), OperatorPlacement.Resource.NEST_STATE, "step", "root", ns + vertex)));
            return out;
        }
        long stored() { return SystemCollections.CACHE_NEST_STATE.on(database).countDocuments(); }
        MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        void onClaim(Document update) {
            assertThat(claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), update).getMatchedCount()).isEqualTo(1);
        }
        void addTtlIndex() {
            SystemCollections.CACHE_NEST_STATE.on(database).createIndex(new Document("ttl", 1), new IndexOptions().expireAfter(3600L, TimeUnit.SECONDS));
        }
    }

    @FunctionalInterface private interface Scenario { void run(Fixture f) throws Exception; }
}
