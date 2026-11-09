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
import io.tapstate.spi.store.GroupCommitStateStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StateGroup;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One vertex instance's commit group: every namespace it wrote in one transaction, fenced on the instance's
 * own fence row and a read of the claim, never a write to the claim; and the fence rows rewritten by every
 * transaction that changes or ends the tenure.
 */
@RequiresDocker
class StateGroupCommitIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadOwner SUCCESSOR = new WorkloadOwner("node-b", "boot-b");
    private static final Runnable ACTIVE = () -> {};
    private static final List<String> NAMESPACES = List.of("lookup:customers", "references:customers", "state");

    @AfterEach void reset() { MongoStateGroup.afterPayload = () -> {}; }

    @Test void everyNamespaceOfAGroupCommitsInOneTransactionThatNeverWritesTheClaim() {
        withFixture(f -> {
            var commands = new CopyOnWriteArrayList<String>();
            try (var client = observed(commands, f.database.getName())) {
                var group = f.group(client, f.claim, "assembler/0");
                long claimWrites = f.claimWrites();
                commands.clear();
                group.commit(changes("a", 80, 64));
                assertThat(commands).as("fence row, claim read, inventory check, payload")
                        .containsExactly("update:nest_state", "find:workload_claims", "find:operator_cleanup", "update:nest_state");
                assertThat(f.stored()).isEqualTo(240);
                assertThat(f.claimWrites()).as("a commit writes its own fence row, not the claim").isEqualTo(claimWrites);
                commands.clear();
                group.commit(Map.of(NAMESPACES.get(0), new StateGroup.Changes(Map.of(), Map.of(), Set.of("a0", "a1"))));
                assertThat(commands).containsExactly("update:nest_state", "find:workload_claims", "find:operator_cleanup", "delete:nest_state");
                assertThat(f.stored()).isEqualTo(238);
            }
        });
    }

    @Test void aGroupWithNothingInItOpensNoTransaction() {
        withFixture(f -> {
            var commands = new CopyOnWriteArrayList<String>();
            try (var client = observed(commands, f.database.getName())) {
                var group = f.group(client, f.claim, "assembler/0");
                commands.clear();
                group.commit(Map.of());
                group.commit(Map.of(NAMESPACES.get(2), new StateGroup.Changes(Map.of(), Map.of(), Set.of())));
                assertThat(commands).isEmpty();
            }
        });
    }

    @Test void aValueWrittenWithAReleaseKeyCarriesItsMarker() {
        withFixture(f -> {
            var group = f.group(f.client, f.claim, "resolver/0");
            group.commit(Map.of("state", new StateGroup.Changes(Map.of("k", new byte[]{1}), Map.of("k", new byte[]{9}), Set.of())));
            Document row = SystemCollections.CACHE_NEST_STATE.on(f.database).find(new Document("pendingRelease", true)).first();
            assertThat(row).isNotNull();
            assertThat(row.get("releaseKey", org.bson.types.Binary.class).getData()).containsExactly(9);
            group.commit(Map.of("state", new StateGroup.Changes(Map.of("k", new byte[]{2}), Map.of(), Set.of())));
            assertThat(SystemCollections.CACHE_NEST_STATE.on(f.database).countDocuments(new Document("pendingRelease", true))).isZero();
        });
    }

    @Test void aGroupThatFailsPartWayLandsInNoNamespace() {
        withFixture(f -> {
            var group = f.group(f.client, f.claim, "assembler/0");
            group.commit(changes("a", 10, 64)); long before = f.stored();
            var failing = new LinkedHashMap<>(changes("b", 50, 64));
            // The last namespace's last value is over the document limit, so the payload fails after the first
            // namespaces' writes have gone into the transaction.
            var last = new LinkedHashMap<>(failing.get(NAMESPACES.get(2)).writes()); last.put("huge", new byte[17 * 1024 * 1024]);
            failing.put(NAMESPACES.get(2), new StateGroup.Changes(last, Map.of(), Set.of()));
            assertThatThrownBy(() -> group.commit(failing)).isInstanceOf(RuntimeException.class);
            assertThat(f.stored()).as("nothing of the failed group in any namespace").isEqualTo(before);
            group.commit(changes("c", 1, 8));
            assertThat(f.stored()).isEqualTo(before + 3);
        });
    }

    @Test void anAdvancedClaimRefusesTheWholeGroupAndTheGroupFailsClosed() {
        withFixture(f -> {
            var group = f.group(f.client, f.claim, "assembler/0");
            group.commit(changes("a", 10, 64)); long before = f.stored();
            f.onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
            code(() -> group.commit(changes("b", 200, 64)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
            code(() -> group.commit(changes("c", 1, 8)), IoError.WORKLOAD_CLAIM_FENCED);
        });
    }

    @Test void anOlderInventoryRestoredUnderTheGroupRefusesIt() {
        withFixture(f -> {
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database); Document older = inventory.find().first();
            var group = f.group(f.client, f.claim, "assembler/0");
            group.commit(changes("a", 10, 64)); long before = f.stored();
            inventory.replaceOne(new Document(), older);
            code(() -> group.commit(changes("b", 10, 64)), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void aProofRegistersTheFencePlacementAndCreatesOnlyItsOwnRow() {
        withFixture(f -> {
            f.group(f.client, f.claim, "assembler/0");
            f.group(f.client, f.claim, "lookup/1");
            var rows = f.fenceRows();
            assertThat(rows).hasSize(2);
            assertThat(rows).allSatisfy(row -> assertThat(row.getLong("claimGeneration")).isEqualTo(f.claim.claimGeneration()));
            var placements = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first().getList("placements", Document.class);
            assertThat(placements).contains(MongoOperatorInventory.location(MongoCommitFence.placement(SCOPE, f.database)));
            // A store can never be opened on the reserved placement, so no authored step reaches the fence rows.
            assertThatThrownBy(() -> f.domain.operatorState(MongoCommitFence.placement(SCOPE, f.database), WorkloadClaimFence.from(f.claim), ACTIVE))
                    .isInstanceOf(TapstateException.class);
        });
    }

    @Test void aSuccessorsProofRewritesEveryFenceRowSoTheOldGroupsAreRefused() {
        withFixture(f -> {
            var old = f.group(f.client, f.claim, "assembler/0"); var sibling = f.group(f.client, f.claim, "lookup/0");
            old.commit(changes("a", 5, 8)); sibling.commit(changes("s", 5, 8));
            var successor = f.takeOver();
            f.group(f.client, successor, "assembler/0");
            assertThat(f.fenceRows()).allSatisfy(row -> {
                assertThat(row.getString("ownerBootId")).isEqualTo(SUCCESSOR.bootId());
                assertThat(row.getLong("claimGeneration")).isEqualTo(successor.claimGeneration());
            });
            long before = f.stored();
            code(() -> old.commit(changes("b", 5, 8)), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> sibling.commit(changes("b", 5, 8)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.stored()).isEqualTo(before);
        });
    }

    @Test void aReleaseEndsEveryFenceRowsTenure() {
        withFixture(f -> {
            f.group(f.client, f.claim, "assembler/0").commit(changes("a", 1, 8));
            assertThat(f.domain.workloadClaims(BUSINESS).release(f.claim)).isTrue();
            assertThat(f.fenceRows()).isNotEmpty().allSatisfy(row -> assertThat(row.getLong("claimGeneration")).isZero());
        });
    }

    /**
     * An old-tenure commit holding its fence row open: the successor's proof cannot commit until the old group has
     * either committed (and is then visible to the successor) or aborted. Without the rewrite the proof would
     * commit straight away and the old group would land after it.
     */
    @Test void anOldGroupInFlightNeverLandsAfterTheSuccessorsProof() throws Exception {
        withFixture(f -> {
            var old = f.group(f.client, f.claim, "assembler/0");
            old.commit(changes("a", 1, 8));
            var held = new CountDownLatch(1); var release = new CountDownLatch(1);
            MongoStateGroup.afterPayload = () -> {
                held.countDown();
                try { release.await(30, TimeUnit.SECONDS); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            };
            var inFlight = CompletableFuture.runAsync(() -> old.commit(changes("late", 4, 8)));
            assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();
            MongoStateGroup.afterPayload = () -> {};
            var successor = f.takeOver();
            var proved = CompletableFuture.supplyAsync(() -> f.group(f.client, successor, "lookup/0"));
            Thread.sleep(1_000);
            assertThat(proved).as("the successor's proof waits on the old group's fence row").isNotDone();
            release.countDown();
            inFlight.handle((ok, failure) -> null).get(60, TimeUnit.SECONDS);
            proved.get(60, TimeUnit.SECONDS);
            long late = f.storedWithPrefix("late");
            assertThat(late).as("the old group is whole or absent").isIn(0L, 12L);
            // Whatever the successor reads now is final for the old tenure: nothing more of it can land.
            code(() -> old.commit(changes("after", 1, 8)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.storedWithPrefix("late")).isEqualTo(late);
        });
    }

    private static Map<String, StateGroup.Changes> changes(String prefix, int perNamespace, int bytes) {
        Map<String, StateGroup.Changes> out = new LinkedHashMap<>();
        for (String namespace : NAMESPACES) {
            Map<String, byte[]> writes = new LinkedHashMap<>();
            for (int i = 0; i < perNamespace; i++) { byte[] v = new byte[bytes]; v[0] = (byte) i; writes.put(prefix + i, v); }
            out.put(namespace, new StateGroup.Changes(writes, Map.of(), Set.of()));
        }
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
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build());
    }

    /** The same prepared cache, pipeline claim and nest shape the operator-state suites start from. */
    private static void withFixture(Scenario scenario) {
        String name = "group_" + UUID.randomUUID().toString().replace("-", "");
        String url = MONGO.getReplicaSetUrl(name);
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, url, name, null);
        try (var client = MongoClients.create(url); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "group-it")) {
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
                scenario.run(new Fixture(client, database, domain, claim));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; } catch (Exception e) { throw new AssertionError(e); }
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, WorkloadClaim claim) {
        /** One vertex instance's group over three namespaces, each on its own store as a member opens them. */
        StateGroup group(MongoClient over, WorkloadClaim under, String vertexInstance) {
            var named = new ArrayList<GroupCommitStateStore.GroupNamespace>();
            for (String namespace : NAMESPACES) {
                var placement = new OperatorPlacement(SCOPE, database.getName(), OperatorPlacement.Resource.NEST_STATE, "step", "root", namespace + "/" + vertexInstance);
                var access = new MongoCachePipelineAccess(over, over.getDatabase(database.getName()), SCOPE, WorkloadClaimFence.from(under), ACTIVE);
                var store = new MongoCacheOperatorStateStore(access, placement);
                named.add(new GroupCommitStateStore.GroupNamespace(store, placement.mapName()));
            }
            var opened = named.get(0).store().openGroup(vertexInstance, named);
            return changes -> {
                Map<String, StateGroup.Changes> byMap = new LinkedHashMap<>();
                for (int i = 0; i < NAMESPACES.size(); i++) {
                    var part = changes.get(NAMESPACES.get(i));
                    if (part != null) byMap.put(named.get(i).namespace(), part);
                }
                opened.commit(byMap);
            };
        }

        /** The claim expires and another owner takes it over and advances its execution. */
        WorkloadClaim takeOver() {
            onClaim(new Document("$set", new Document("leaseUntil", new Date(0))));
            var claims = domain.workloadClaims(BUSINESS);
            var taken = claims.acquirePipeline(SCOPE, SUCCESSOR, 1, Duration.ofMinutes(5)).claim();
            return claims.advanceExecution(taken, 1, Set.of(SUCCESSOR.nodeId())).orElseThrow();
        }

        long stored() {
            return SystemCollections.CACHE_NEST_STATE.on(database).countDocuments(
                    new Document("_id.placement.stepId", new Document("$ne", OperatorStateText.encode(MongoCommitFence.STEP))));
        }

        long storedWithPrefix(String prefix) {
            long found = 0;
            for (Document row : SystemCollections.CACHE_NEST_STATE.on(database).find()) {
                Document id = row.get("_id", Document.class);
                if (OperatorStateText.encode(MongoCommitFence.STEP).equals(id.get("placement", Document.class).get("stepId"))) continue;
                if (OperatorStateText.decode(id.getString("key")).startsWith(prefix)) found++;
            }
            return found;
        }

        List<Document> fenceRows() {
            return SystemCollections.CACHE_NEST_STATE.on(database).find(
                    new Document("_id.placement.stepId", OperatorStateText.encode(MongoCommitFence.STEP))).into(new ArrayList<>());
        }

        long claimWrites() {
            Document row = claims().find(new Document("_id", MongoWorkloadClaimStore.id(KEY))).first();
            return row.get("fencedPipelineWrites") instanceof Number n ? n.longValue() : 0L;
        }

        MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }

        void onClaim(Document update) {
            assertThat(claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), update).getMatchedCount()).isEqualTo(1);
        }
    }

    @FunctionalInterface private interface Scenario { void run(Fixture f) throws Exception; }
}
