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
import io.tapstate.spi.store.LeasedStateStore;
import io.tapstate.spi.store.OperatorPlacement;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a store that validates once per lease still refuses. Each case proves a lease with a first write,
 * changes the cache underneath it the way a takeover, an expiry, a generation switch, an operator restore or
 * an operator's DDL would, and checks the next write: refused as a unit, nothing of it landing, and the store
 * failing closed afterwards where the change is one a proof must see.
 */
@RequiresDocker
class LeaseValidationFencingIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};
    private static final int GROUP = 200;

    @AfterEach void clearProperties() {
        System.clearProperty(MongoCacheOperatorStateStore.VALIDATION_PROPERTY);
        System.clearProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY);
    }

    @Test void aClaimTakenOverWithinTheLeaseRefusesTheNextGroupAsAUnitAndEveryOperationAfterIt() {
        withFixture(f -> {
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10)); long before = f.stored();
            f.onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
            code(() -> store.saveAll(f.at.mapName(), keys("b", GROUP)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).as("nothing of the refused group lands").isEqualTo(before);
            // The refusal ended the lease; every later write proves again and is refused, while reads stay what
            // they always were: a former owner may still read what it wrote, as a successor's recovery needs.
            code(() -> store.save(f.at.mapName(), "c", new byte[]{1}), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(store.loadAll(f.at.mapName(), List.of("a1"))).containsKey("a1");
            assertThat(f.writer().loadAll(f.at.mapName(), List.of("a1"))).as("a store opened after the takeover reads too").containsKey("a1");
            code(() -> store.save(f.at.mapName(), "d", new byte[]{1}), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void anExpiredClaimLeaseRefusesTheNextGroupWhileTheLocalLeaseStillRuns() {
        withFixture(f -> {
            System.setProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY, "600000");
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10)); long before = f.stored();
            f.onClaim(new Document("$set", new Document("leaseUntil", new Date(System.currentTimeMillis() - 60_000))));
            code(() -> store.saveAll(f.at.mapName(), keys("b", GROUP)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void aPipelineScopeMovedToTheNextGenerationRefusesTheNextGroup() {
        withFixture(f -> {
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10)); long before = f.stored();
            f.onClaim(new Document("$inc", new Document("pipelineScope.stateGeneration", 1L)));
            code(() -> store.saveAll(f.at.mapName(), keys("b", GROUP)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void anOlderLegalInventoryRestoredWithinTheLeaseRefusesTheNextGroup() {
        withFixture(f -> {
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database); Document older = inventory.find().first();
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10)); long before = f.stored();
            // The copy taken before this placement registered: legal, and one revision behind the lease's proof.
            inventory.replaceOne(new Document(), older);
            code(() -> store.saveAll(f.at.mapName(), keys("b", GROUP)), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(before);
            code(() -> store.loadAll(f.at.mapName(), List.of("a1")), IoError.DOCUMENT_UNREADABLE);
        });
    }

    @Test void aSiblingRegisteringItsPlacementWithinTheLeaseIsNoReasonToRefuse() {
        withFixture(f -> {
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10));
            long revision = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first().getLong("revision");
            var sibling = new OperatorPlacement(SCOPE, f.database.getName(), OperatorPlacement.Resource.JOIN_FACTS, "step", "root", "sibling");
            f.domain.operatorState(sibling, WorkloadClaimFence.from(f.claim), ACTIVE).save(sibling.mapName(), "s", new byte[]{1});
            assertThat(SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first().getLong("revision")).isGreaterThan(revision);
            store.saveAll(f.at.mapName(), keys("b", GROUP));
            assertThat(f.stored()).isEqualTo(10 + GROUP);
        });
    }

    @Test void aStructureChangeWithinTheLeaseIsSeenAtTheNextProofAndNothingLands() throws Exception {
        withFixture(f -> {
            System.setProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY, "1500");
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10));
            f.addTtlIndex();
            store.saveAll(f.at.mapName(), keys("b", 10));
            assertThat(f.stored()).as("within the lease the structure is not looked at").isEqualTo(20);
            Thread.sleep(1_700);
            code(() -> store.saveAll(f.at.mapName(), keys("c", GROUP)), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(20);
        });
    }

    @Test void aDroppedLeaseProvesAgainBeforeTheNextWrite() {
        withFixture(f -> {
            System.setProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY, "600000");
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10));
            f.addTtlIndex();
            // What the member's claim reading does when a refresh fails or the claim moves.
            ((LeasedStateStore) store).dropLease();
            code(() -> store.saveAll(f.at.mapName(), keys("b", GROUP)), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(10);
        });
    }

    @Test void aFailedWriteProofAlsoEndsTheReadLeaseItWasUpgrading() {
        withFixture(f -> {
            System.setProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY, "600000");
            f.writer().saveAll(f.at.mapName(), keys("a", 10));
            var store = f.writer();
            assertThat(store.loadAll(f.at.mapName(), List.of("a1"))).containsKey("a1");
            f.addTtlIndex();
            code(() -> store.save(f.at.mapName(), "b", new byte[]{1}), IoError.DOCUMENT_UNREADABLE);
            code(() -> store.loadAll(f.at.mapName(), List.of("a1")), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(10);
        });
    }

    @Test void perTransactionValidationRemainsSelectableAndSeesTheStructureChangeAtOnce() {
        withFixture(f -> {
            System.setProperty(MongoCacheOperatorStateStore.VALIDATION_PROPERTY, "transaction");
            var store = f.writer(); store.saveAll(f.at.mapName(), keys("a", 10));
            f.addTtlIndex();
            code(() -> store.saveAll(f.at.mapName(), keys("b", 10)), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(10);
        });
    }

    @Test void aLeasedWriteIsOneClaimCompareAndSetOneInventoryCheckAndThePayload() {
        withFixture(f -> {
            var commands = new ArrayList<String>();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getDatabaseName().equals(f.database.getName())) {
                    var collection = event.getCommand().get(event.getCommandName());
                    commands.add(event.getCommandName() + (collection != null && collection.isString() ? ":" + collection.asString().getValue() : ""));
                }
            }};
            try (var client = observed(listener)) {
                var store = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(client, client.getDatabase(f.database.getName()),
                        SCOPE, WorkloadClaimFence.from(f.claim), ACTIVE), f.at);
                store.openLease(); commands.clear();
                store.saveAll(f.at.mapName(), keys("a", GROUP));
                assertThat(commands).containsExactly("update:workload_claims", "find:operator_cleanup", "update:nest_state");
                commands.clear();
                assertThat(store.loadAll(f.at.mapName(), List.of("a1", "a2"))).hasSize(2);
                assertThat(commands).containsExactly("find:nest_state");
            }
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first().getLong("runtimeWriteFence"))
                    .as("operator-state writes no longer advance the runtime write fence; only the lease proof did")
                    .isEqualTo(f.fenceAfterSetup + 1);
        });
    }

    private static Map<String, byte[]> keys(String prefix, int count) {
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) out.put(prefix + i, new byte[]{(byte) i, 1, 2, 3});
        return out;
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code));
    }

    private static MongoClient observed(CommandListener listener) {
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build());
    }

    /** The same prepared cache, pipeline claim and nest shape the operator-state suite starts from. */
    private static void withFixture(Scenario scenario) {
        String name = "lease_fencing_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "lease-it")) {
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
