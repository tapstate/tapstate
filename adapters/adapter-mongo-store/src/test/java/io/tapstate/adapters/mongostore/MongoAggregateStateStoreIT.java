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
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.LeasedStateStore;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.OrderedStateStore;
import io.tapstate.spi.store.OrderedStateStore.End;
import io.tapstate.spi.store.OrderedStateStore.Entry;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ordered operator store against a real cache: a commit of saves and deletes that lands whole or not
 * at all under the pipeline's claim, keys that sort as their bytes do, the first key of a range read as one
 * entry, and a collection that keeps the two kinds of operator state apart.
 */
@RequiresDocker
class MongoAggregateStateStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};

    @Test void aCommitAppliesItsSavesAndDeletesTogetherAndLoadsAnswerInTheOrderAsked() {
        withFixture(f -> {
            var store = f.groups();
            store.commit(List.of(entry(k(1), 10), entry(k(2), 20), entry(k(3), 30)), List.of());
            store.commit(List.of(entry(k(2), 21), entry(k(4), 40)), List.of(k(1), k(9)));
            var loaded = store.loadAll(List.of(k(4), k(1), k(2), k(3)));
            assertThat(loaded.get(0)).containsExactly(40);
            assertThat(loaded.get(1)).as("a deleted key holds nothing").isNull();
            assertThat(loaded.get(2)).containsExactly(21);
            assertThat(loaded.get(3)).containsExactly(30);
            assertThat(f.stored()).isEqualTo(3);
            assertThat(store.loadAll(List.of())).isEmpty();
        });
    }

    @Test void keysSortAsUnsignedBytesWithAShorterKeyBeforeAnyKeyItBegins() {
        withFixture(f -> {
            var store = f.groups();
            byte[][] ascending = {{}, {0x00}, {0x00, 0x00}, {0x01}, {0x01, (byte) 0xff}, {0x02}, {0x7f}, {(byte) 0x80}, {(byte) 0x80, 0x00}, {(byte) 0xff}};
            List<Entry> saves = new ArrayList<>();
            for (int i = ascending.length - 1; i >= 0; i--) saves.add(entry(ascending[i], i));
            store.commit(saves, List.of());
            var all = store.scan(new byte[0], new byte[]{(byte) 0xff, (byte) 0xff}, null, 200);
            assertThat(all).hasSize(ascending.length);
            for (int i = 0; i < ascending.length; i++) {
                assertThat(all.get(i).key()).as("position %d", i).containsExactly(ascending[i]);
                assertThat(all.get(i).state()).containsExactly(i);
            }
        });
    }

    @Test void theFirstOfARangeIsItsLowestOrHighestKeyAndNothingOutsideTheRangeOrThePlacement() {
        withFixture(f -> {
            var store = f.groups();
            store.commit(List.of(entry(k(1, 5), 1), entry(k(2, 1), 2), entry(k(2, 7), 3), entry(k(2, 9), 4), entry(k(3, 0), 5)), List.of());
            f.members().commit(List.of(entry(k(2, 0), 99), entry(k(2, 200), 98)), List.of());
            assertThat(key(store.first(k(2), k(3), End.LOWEST))).containsExactly(k(2, 1));
            assertThat(key(store.first(k(2), k(3), End.HIGHEST))).containsExactly(k(2, 9));
            assertThat(key(store.first(k(2, 2), k(2, 9), End.HIGHEST))).as("the upper bound is not in the range").containsExactly(k(2, 7));
            assertThat(key(store.first(k(2, 7), k(2, 9), End.LOWEST))).as("the lower bound is").containsExactly(k(2, 7));
            assertThat(store.first(k(4), k(5), End.LOWEST)).isEmpty();
            assertThat(store.first(k(2, 2), k(2, 7), End.HIGHEST)).isEmpty();
        });
    }

    @Test void theFirstOfARangeIsOneReadThatReturnsOneEntryHoweverManyTheRangeHolds() {
        withFixture(f -> {
            List<Entry> saves = new ArrayList<>();
            for (int i = 0; i < 200; i++) saves.add(entry(k(7, i), i));
            f.groups().commit(saves, List.of());
            var commands = new ArrayList<Document>();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getDatabaseName().equals(f.database.getName()) && event.getCommandName().equals("find"))
                    commands.add(Document.parse(event.getCommand().toJson()));
            }};
            try (var client = observed(listener)) {
                var store = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(client, client.getDatabase(f.database.getName()),
                        SCOPE, WorkloadClaimFence.from(f.claim), ACTIVE), f.groupsAt);
                store.openLease(); commands.clear();
                assertThat(key(store.first(k(7), k(8), End.HIGHEST))).containsExactly(k(7, 199));
                assertThat(commands).hasSize(1);
                assertThat(commands.get(0).getString("find")).isEqualTo("aggregate_state");
                assertThat(commands.get(0).getInteger("limit")).isEqualTo(1);
            }
            var rows = SystemCollections.CACHE_AGGREGATE_STATE.on(f.database);
            Document placement = rows.find().first().get("_id", Document.class).get("placement", Document.class);
            var plan = rows.find(new Document("_id", new Document("$gte", new Document("placement", placement).append("key", "07"))
                    .append("$lt", new Document("placement", placement).append("key", "08")))).sort(new Document("_id", -1)).limit(1).explain();
            assertThat(plan.toJson()).as("the range is a range of the id index, never a collection scan or a sort").contains("IXSCAN")
                    .doesNotContain("COLLSCAN").doesNotContain("\"SORT\"");
        });
    }

    @Test void aScanWalksARangeInPagesStartingAfterTheLastKeySeen() {
        withFixture(f -> {
            var store = f.groups();
            List<Entry> saves = new ArrayList<>();
            for (int i = 0; i < 25; i++) saves.add(entry(k(5, i), i));
            saves.add(entry(k(6, 0), 100));
            store.commit(saves, List.of());
            List<Integer> seen = new ArrayList<>(); byte[] after = null;
            for (;;) {
                var page = store.scan(k(5), k(6), after, 10);
                for (Entry entry : page) seen.add((int) entry.state()[0]);
                if (page.size() < 10) break;
                after = page.get(page.size() - 1).key();
            }
            assertThat(seen).hasSize(25).isSorted();
            assertThatThrownBy(() -> store.scan(k(5), k(6), null, 0)).isInstanceOf(IllegalArgumentException.class);
        });
    }

    @Test void aCommitBeyondTheLimitOrNamingAKeyTwiceIsRefusedBeforeAnythingIsWritten() {
        withFixture(f -> {
            var store = f.groups(); var limit = store.commitLimit();
            List<Entry> tooMany = new ArrayList<>();
            for (int i = 0; i <= limit.operations(); i++) tooMany.add(entry(k(i >> 8, i & 0xff), 1));
            assertThatThrownBy(() -> store.commit(tooMany, List.of())).isInstanceOf(IllegalArgumentException.class);
            var heavy = new Entry(k(1), new byte[(int) limit.bytes()]);
            assertThatThrownBy(() -> store.commit(List.of(heavy), List.of())).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.commit(List.of(entry(k(1), 1)), List.of(k(1)))).isInstanceOf(IllegalArgumentException.class);
            assertThat(f.stored()).isZero();
            store.commit(tooMany.subList(0, limit.operations()), List.of());
            assertThat(f.stored()).as("a commit at the limit lands whole").isEqualTo(limit.operations());
        });
    }

    @Test void aClaimTakenOverRefusesTheWholeCommitAndLeavesWhatWasThere() {
        withFixture(f -> {
            var store = f.groups();
            store.commit(List.of(entry(k(1), 10), entry(k(2), 20)), List.of());
            f.onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
            code(() -> store.commit(List.of(entry(k(1), 11), entry(k(3), 30)), List.of(k(2))), IoError.WORKLOAD_CLAIM_FENCED);
            var loaded = store.loadAll(List.of(k(1), k(2), k(3)));
            assertThat(loaded.get(0)).as("the save of the refused commit did not land").containsExactly(10);
            assertThat(loaded.get(1)).as("nor did its delete").containsExactly(20);
            assertThat(loaded.get(2)).isNull();
            code(() -> store.commit(List.of(entry(k(4), 40)), List.of()), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(2);
        });
    }

    @Test void aDroppedLeaseProvesTheClaimAgainBeforeTheNextCommit() {
        withFixture(f -> {
            var store = f.groups();
            store.commit(List.of(entry(k(1), 10)), List.of());
            f.onClaim(new Document("$inc", new Document("pipelineScope.stateGeneration", 1L)));
            ((LeasedStateStore) store).dropLease();
            code(() -> store.commit(List.of(entry(k(2), 20)), List.of()), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(1);
        });
    }

    @Test void theOrderedAndTheKeyedOperationsEachServeOnlyTheirOwnKindOfPlacement() {
        withFixture(f -> {
            var keyedOnAggregate = f.domain.operatorState(f.groupsAt, WorkloadClaimFence.from(f.claim), ACTIVE);
            assertThatThrownBy(() -> keyedOnAggregate.save(f.groupsAt.mapName(), "a", new byte[]{1})).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> keyedOnAggregate.loadAll(f.groupsAt.mapName(), List.of("a"))).isInstanceOf(TapstateException.class);
            var orderedOnNest = f.ordered(f.nest);
            assertThatThrownBy(() -> orderedOnNest.commit(List.of(entry(k(1), 1)), List.of())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> orderedOnNest.first(k(0), k(9), End.LOWEST)).isInstanceOf(TapstateException.class);
            assertThat(f.stored()).isZero();
            assertThat(SystemCollections.CACHE_NEST_STATE.on(f.database).countDocuments()).isZero();
        });
    }

    private static byte[] k(int... bytes) {
        byte[] out = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) out[i] = (byte) bytes[i];
        return out;
    }

    private static Entry entry(byte[] key, int state) { return new Entry(key, new byte[]{(byte) state}); }

    private static byte[] key(Optional<Entry> entry) { return entry.orElseThrow().key(); }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code));
    }

    private static MongoClient observed(CommandListener listener) {
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build());
    }

    /** The same prepared cache and pipeline claim the operator-state suite starts from, with two aggregate placements. */
    private static void withFixture(Scenario scenario) {
        String name = "aggregate_state_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "aggregate-it")) {
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
                var groups = new OperatorPlacement(SCOPE, name, OperatorPlacement.Resource.AGGREGATE_STATE, "totals", "root", "groups");
                var members = new OperatorPlacement(SCOPE, name, OperatorPlacement.Resource.AGGREGATE_STATE, "totals", "root", "members");
                var nest = new OperatorPlacement(SCOPE, name, OperatorPlacement.Resource.NEST_STATE, "step", "root", "logical.namespace");
                scenario.run(new Fixture(client, database, domain, claim, groups, members, nest));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; } catch (Exception e) { throw new AssertionError(e); }
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, WorkloadClaim claim, OperatorPlacement groupsAt,
                           OperatorPlacement membersAt, OperatorPlacement nest) {
        OrderedStateStore groups() { return ordered(groupsAt); }
        OrderedStateStore members() { return ordered(membersAt); }
        /** The store the runtime opens for a placement, which for this resource is also the ordered one. */
        OrderedStateStore ordered(OperatorPlacement at) { return (OrderedStateStore) domain.operatorState(at, WorkloadClaimFence.from(claim), ACTIVE); }
        long stored() { return SystemCollections.CACHE_AGGREGATE_STATE.on(database).countDocuments(); }
        MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        void onClaim(Document update) {
            assertThat(claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), update).getMatchedCount()).isEqualTo(1);
        }
    }

    @FunctionalInterface private interface Scenario { void run(Fixture f) throws Exception; }
}
