package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Explicit business facts isolate the chain protocol; production capture and consumer wiring are separate. */
@RequiresDocker
class MongoSrsChainStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final String CHAIN = "chain-a";
    private static final String ORDERS = "orders";
    private static final String QUIET = "quiet";

    @Test
    void aSourceCheckpointKeepsExactModelsForQuietTablesAndSurvivesANewCaptureEpoch() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS, QUIET));
            var baseline = checkpoint(1, SourceOrder.SNAPSHOT_SEQ, "opaque-anchor", Map.of(ORDERS, -1L, QUIET, -1L));
            assertThat(f.chains().checkpoint(CHAIN, 0, baseline, f.fence()).orElseThrow().checkpointRevision()).isOne();
            f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            var batch = checkpoint(1, 400, "opaque-batch", Map.of(ORDERS, 0L, QUIET, -1L));
            SrsChainState saved = f.chains().checkpoint(CHAIN, 1, batch, f.fence()).orElseThrow();
            assertThat(saved.checkpoint()).isEqualTo(batch);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThat(new MongoSrsChainStore(other, other.getDatabase(f.database().getName()), SCOPE).load(CHAIN)).contains(saved);
            }
            assertThat(f.chains().openEpoch(CHAIN, 1, f.fence())).hasValue(2);
            assertThat(f.chains().openEpoch(CHAIN, 1, f.fence())).hasValue(2);
            assertThat(f.chains().load(CHAIN).orElseThrow().checkpoint()).isEqualTo(batch);
            f.models().publish(model(ORDERS, 2, 0, 1), f.fence()); f.models().publish(model(QUIET, 2, 0, 0), f.fence());
            assertThat(f.chains().publishServingTables(CHAIN, 2, Set.of(ORDERS, QUIET), f.fence())).isTrue();
            var next = checkpoint(2, SourceOrder.SNAPSHOT_SEQ, "opaque-batch", Map.of(ORDERS, 0L, QUIET, -1L));
            assertThat(f.chains().checkpoint(CHAIN, 2, next, f.fence()).orElseThrow().checkpoint()).isEqualTo(next);
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).find().first().keySet())
                    .doesNotContain("schemaHistory", "consumerOffsets");
        });
    }

    @Test
    void wideningTheRequestStopsAnOldNarrowCallbackFromCertifyingASourcePosition() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS));
            assertThat(f.chains().requestTables(CHAIN, Set.of(QUIET), f.fence()).requestedTables()).containsExactlyInAnyOrder(ORDERS, QUIET);
            Document before = root(f); Document claim = claim(f);
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 0,
                    checkpoint(1, 1, "cannot-cover-quiet", Map.of(ORDERS, -1L)), f.fence())).satisfies(MongoSrsChainStoreIT::unproven);
            assertThat(root(f)).isEqualTo(before); assertThat(claim(f)).isEqualTo(claim);
            assertThat(f.chains().publishServingTables(CHAIN, 1, Set.of(ORDERS, QUIET), f.fence())).isFalse();
            assertThat(f.chains().openEpoch(CHAIN, 1, f.fence())).hasValue(2);
            assertThat(f.chains().publishServingTables(CHAIN, 2, Set.of(ORDERS), f.fence())).isFalse();
        });
    }

    @Test
    void checkpointCasRetainsTheOriginalExpectedRevisionAcrossIndependentClients() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            var a = checkpoint(1, 1, "position-a", Map.of(ORDERS, 0L));
            var b = checkpoint(1, 2, "position-b", Map.of(ORDERS, 0L));
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsChainStore(other, other.getDatabase(f.database().getName()), SCOPE);
                var start = new CountDownLatch(1);
                var firstWrite = executor.submit(() -> { start.await(); return f.chains().checkpoint(CHAIN, 0, a, f.fence()).isPresent(); });
                var secondWrite = executor.submit(() -> { start.await(); return second.checkpoint(CHAIN, 0, b, f.fence()).isPresent(); });
                start.countDown();
                assertThat(List.of(firstWrite.get(15, TimeUnit.SECONDS), secondWrite.get(15, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            }
            SrsChainState winner = f.chains().load(CHAIN).orElseThrow();
            assertThat(winner.checkpointRevision()).isOne();
            assertThat(f.chains().checkpoint(CHAIN, 0, winner.checkpoint(), f.fence())).contains(winner);
            var after = checkpoint(1, 3, "later", Map.of(ORDERS, 0L));
            assertThat(f.chains().checkpoint(CHAIN, 1, after, f.fence())).isPresent();
            assertThat(f.chains().checkpoint(CHAIN, 0, winner.checkpoint(), f.fence())).isEmpty();
            assertThat(f.chains().load(CHAIN).orElseThrow().checkpoint()).isEqualTo(after);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"model", "control", "bounds", "root"})
    void knownFactLossDoesNotAdvanceOrRecreateTheSourceCheckpoint(String loss) {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS));
            var table = switch (loss) {
                case "model" -> SystemCollections.SRS_MODELS;
                case "control" -> SystemCollections.SRS_MODEL_CONTEXTS;
                case "bounds" -> SystemCollections.SRS_LOG_BOUNDS;
                default -> SystemCollections.SRS_CHAINS;
            };
            table.on(f.database()).deleteMany(new Document()); Document before = root(f); Document claim = claim(f);
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 0,
                    checkpoint(1, 1, "must-not-save", Map.of(ORDERS, -1L)), f.fence())).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before); assertThat(claim(f)).isEqualTo(claim);
            if (loss.equals("root")) {
                assertThatThrownBy(() -> f.chains().prepare(CHAIN, "retention", f.fence())).satisfies(MongoSrsChainStoreIT::unproven);
                assertThatThrownBy(() -> f.chains().load(CHAIN)).satisfies(MongoSrsChainStoreIT::unproven);
                assertThat(root(f)).isNull();
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"future", "gap", "old-model", "other-context"})
    void aClaimCannotCertifyAnUnprovenPerTableFrontier(String damage) {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            SrsCaptureCheckpoint attempted;
            switch (damage) {
                case "future" -> attempted = checkpoint(1, 1, "not-persisted", Map.of(ORDERS, 5L));
                case "gap" -> { f.log().store(ring(ORDERS), 2, event(0, 1, f.fence())); attempted = checkpoint(1, 1, "gap", Map.of(ORDERS, 1L)); }
                case "old-model" -> {
                    f.models().publish(model(ORDERS, 1, 1, 1), f.fence()); f.log().store(ring(ORDERS), 1, event(1, 1, f.fence()));
                    attempted = checkpoint(1, 1, "wrong-model", Map.of(ORDERS, 1L));
                }
                default -> attempted = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "wrong-context"),
                        Map.of(ORDERS, new SrsTableCheckpoint(key(ORDERS, 2, 0), 0)));
            }
            Document before = root(f); Document claim = claim(f);
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 0, attempted, f.fence())).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before); assertThat(claim(f)).isEqualTo(claim);
        });
    }

    @Test
    void aRetiredClaimOrAnotherCaptureCannotModifyTheChain() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); Document before = root(f);
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var other = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-b"),
                    new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
            assertThatThrownBy(() -> f.chains().requestTables(CHAIN, Set.of(QUIET), WorkloadClaimFence.from(other))).satisfies(MongoSrsChainStoreIT::fenced);
            assertThat(claims.release(f.claim())).isTrue();
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 0,
                    checkpoint(1, 1, "late", Map.of(ORDERS, -1L)), f.fence())).satisfies(MongoSrsChainStoreIT::fenced);
            assertThat(root(f)).isEqualTo(before);
        });
    }

    @Test
    void losingTheOriginalCallbackGuardRollsBackTheCheckpointAndClaimProof() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); var calls = new AtomicInteger(); Document before = root(f); Document claim = claim(f);
            var guarded = f.chains().guardedBy(() -> { if (calls.incrementAndGet() == 2) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); });
            assertThatThrownBy(() -> guarded.checkpoint(CHAIN, 0, checkpoint(1, 1, "must-rollback", Map.of(ORDERS, -1L)), f.fence()))
                    .isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before); assertThat(claim(f)).isEqualTo(claim);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"checkpoint", "checkpointAt", "quiet-entry", "revision-double", "fence-double", "heads", "epoch-missing"})
    void corruptedControlOrCheckpointClosureIsNotInterpretedAsAnEmptyHistory(String damage) {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS, QUIET));
            f.chains().checkpoint(CHAIN, 0, checkpoint(1, 1, "saved", Map.of(ORDERS, -1L, QUIET, -1L)), f.fence()).orElseThrow();
            Document change = switch (damage) {
                case "checkpoint", "checkpointAt" -> new Document("$unset", new Document(damage, ""));
                case "quiet-entry" -> new Document("$pull", new Document("checkpoint.tables", new Document("table", QUIET)));
                case "revision-double" -> new Document("$set", new Document("checkpointRevision", 1.5D));
                case "fence-double" -> new Document("$set", new Document("captureFence.claimGeneration", 1.5D));
                case "heads" -> new Document("$set", new Document("modelContextHeads", List.of("raw-secret-sentinel")));
                default -> new Document("$unset", new Document("epoch", ""));
            };
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), change); Document before = root(f);
            assertThatThrownBy(() -> f.chains().load(CHAIN)).satisfies(MongoSrsChainStoreIT::unreadable);
            assertThat(root(f)).isEqualTo(before);
        });
    }

    @Test
    void aCheckpointMayUseAnIntactOlderModelWithoutDecodingAnUnreferencedHeadPayload() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            var checkpoint = checkpoint(1, 1, "captured-before-ddl", Map.of(ORDERS, 0L));
            f.models().publish(model(ORDERS, 1, 1, 10), f.fence());
            SystemCollections.SRS_MODELS.on(f.database()).deleteOne(new Document("_id.modelVersion", 1L));
            assertThat(f.chains().checkpoint(CHAIN, 0, checkpoint, f.fence()).orElseThrow().checkpoint()).isEqualTo(checkpoint);
            assertThat(f.chains().load(CHAIN).orElseThrow().checkpoint()).isEqualTo(checkpoint);
        });
    }

    @Test
    void sourceOrderAndEachTableFrontierRemainMonotonicAndCountersDoNotOverflow() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().storeAll(ring(ORDERS), 0, List.of(event(0, 1, f.fence()), event(0, 1, f.fence())));
            f.chains().checkpoint(CHAIN, 0, checkpoint(1, 10, "latest", Map.of(ORDERS, 1L)), f.fence()).orElseThrow();
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 1, checkpoint(1, 9, "rewind-order", Map.of(ORDERS, 1L)), f.fence()))
                    .satisfies(MongoSrsChainStoreIT::unproven);
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 1, checkpoint(1, 11, "rewind-table", Map.of(ORDERS, 0L)), f.fence()))
                    .satisfies(MongoSrsChainStoreIT::unproven);
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelWriteFence", Long.MAX_VALUE - 1)));
            Document before = root(f);
            assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 1, checkpoint(1, 11, "overflow", Map.of(ORDERS, 1L)), f.fence()))
                    .satisfies(MongoSrsChainStoreIT::unproven);
            assertThat(root(f)).isEqualTo(before); assertThat(f.chains().load(CHAIN)).isPresent();
        });
    }

    @Test
    void losingAnUntrimmedCommittedFrontierCannotBeReturnedAsAResumableCheckpoint() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            f.chains().checkpoint(CHAIN, 0, checkpoint(1, 1, "saved", Map.of(ORDERS, 0L)), f.fence()).orElseThrow();
            SystemCollections.SRS_EVENTS.on(f.database()).deleteMany(new Document()); Document before = root(f); Document claim = claim(f);
            assertThatThrownBy(() -> f.chains().load(CHAIN)).satisfies(MongoSrsChainStoreIT::unproven);
            assertThatThrownBy(() -> f.chains().openEpoch(CHAIN, 1, f.fence())).satisfies(MongoSrsChainStoreIT::unproven);
            assertThat(root(f)).isEqualTo(before); assertThat(claim(f)).isEqualTo(claim);
        });
    }

    @Test
    void aLegitimatelyTrimmedFrontierStillUsesItsSavedExactModel() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            var saved = f.chains().checkpoint(CHAIN, 0, checkpoint(1, 1, "saved", Map.of(ORDERS, 0L)), f.fence()).orElseThrow();
            f.log().trim(ring(ORDERS), 0);
            assertThat(f.chains().load(CHAIN)).contains(saved);
            assertThat(f.chains().openEpoch(CHAIN, 1, f.fence())).hasValue(2);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aPositiveNewModelBaselineStillRequiresItsUntrimmedPredecessorEvent(boolean lost) {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 2, event(0, 1, f.fence()));
            f.models().publish(model(ORDERS, 1, 1, 3), f.fence());
            if (lost) SystemCollections.SRS_EVENTS.on(f.database()).deleteMany(new Document());
            var baseline = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "ddl-baseline"),
                    Map.of(ORDERS, new SrsTableCheckpoint(key(ORDERS, 1, 1), 2)));
            if (lost) assertThatThrownBy(() -> f.chains().checkpoint(CHAIN, 0, baseline, f.fence())).satisfies(MongoSrsChainStoreIT::unproven);
            else assertThat(f.chains().checkpoint(CHAIN, 0, baseline, f.fence()).orElseThrow().checkpoint()).isEqualTo(baseline);
        });
    }

    @Test
    void aSavedSourceOrderCannotAdoptAModelFromAnotherCaptureEpoch() {
        withBusiness(f -> {
            prepare(f, Set.of(ORDERS)); f.log().store(ring(ORDERS), 0, event(0, 1, f.fence()));
            f.chains().checkpoint(CHAIN, 0, checkpoint(1, 1, "saved", Map.of(ORDERS, 0L)), f.fence()).orElseThrow();
            f.chains().openEpoch(CHAIN, 1, f.fence()).orElseThrow(); f.models().publish(model(ORDERS, 2, 0, 1), f.fence());
            Document checkpoint = root(f).get("checkpoint", Document.class);
            checkpoint.getList("tables", Document.class).getFirst().put("model", MongoSrsModelStore.key(key(ORDERS, 2, 0)));
            checkpoint.remove("contentHash"); checkpoint.put("contentHash", io.tapstate.core.model.canonical.CanonicalHash.ofText(checkpoint.toJson()));
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("checkpoint", checkpoint)));
            Document before = root(f);
            assertThatThrownBy(() -> f.chains().load(CHAIN)).satisfies(MongoSrsChainStoreIT::unproven);
            assertThat(root(f)).isEqualTo(before);
        });
    }

    private static void prepare(Fixture f, Set<String> tables) {
        assertThat(f.chains().prepare(CHAIN, "retention", f.fence()).epoch()).isZero();
        f.chains().requestTables(CHAIN, tables, f.fence());
        assertThat(f.chains().openEpoch(CHAIN, 0, f.fence())).hasValue(1);
        for (String table : tables) f.models().publish(model(table, 1, 0, 0), f.fence());
        assertThat(f.chains().publishServingTables(CHAIN, 1, tables, f.fence())).isTrue();
    }
    private static SrsCaptureCheckpoint checkpoint(long epoch, long order, String token, Map<String, Long> frontiers) {
        var tables = new HashMap<String, SrsTableCheckpoint>();
        frontiers.forEach((table, sequence) -> tables.put(table, new SrsTableCheckpoint(key(table, epoch, 0), sequence)));
        return new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(epoch, order), token), tables);
    }
    private static SrsModelKey key(String table, long epoch, long version) { return new SrsModelKey(CHAIN, table, SCOPE.cacheIncarnation(), epoch, version); }
    private static SrsModel model(String table, long epoch, long version, long first) {
        return new SrsModel(key(table, epoch, version), ring(table), new SourceTable(table,
                List.of(new SourceField("id-v" + version, "integer")), List.of("id-v" + version), List.of()), first);
    }
    private static SrsLogRecord event(long version, long epoch, WorkloadClaimFence fence) {
        return new SrsLogRecord("opaque-event", Op.INSERT, 1, null, Map.of("id", 1), version, fence, epoch);
    }
    private static String ring(String table) { return "srs." + CHAIN + "." + table; }
    private static Document root(Fixture f) { return SystemCollections.SRS_CHAINS.on(f.database()).find().first(); }
    private static Document claim(Fixture f) { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find().first(); }
    private static void unproven(Throwable failure) { code(failure, IoError.SRS_CHECKPOINT_UNPROVEN); }
    private static void fenced(Throwable failure) { code(failure, IoError.WORKLOAD_CLAIM_FENCED); }
    private static void unreadable(Throwable failure) { code(failure, IoError.DOCUMENT_UNREADABLE); }
    private static void code(Throwable failure, IoError expected) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(expected); assertThat(error.getCause()).isNull();
            assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
        });
    }
    private static void withBusiness(Scenario scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("srs_chain_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.SRS_CHAINS,
                            SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS,
                            SystemCollections.CACHE_WORKLOAD_CLAIMS)) {
                        table.prepareCollectionOn(database, session);
                        for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(database).insertOne(new Document("_id", "cluster").append("clusterId", SCOPE.clusterId())
                        .append("layoutId", MongoConfigurationDomain.LAYOUT).append("bindingVersion", 1L).append("cacheIncarnation", SCOPE.cacheIncarnation())
                        .append("businessPhase", "READY").append("businessPreparationId", "business-a").append("authGeneration", 1L));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                var claim = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).acquire(
                        new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-a"), new WorkloadOwner("node-a", "boot-a"),
                        1, Duration.ofSeconds(30)).claim();
                var models = new MongoSrsModelStore(client, database, SCOPE);
                var log = new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models);
                scenario.run(new Fixture(client, database, new MongoSrsChainStore(client, database, SCOPE), models, log, claim));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoSrsChainStore chains, MongoSrsModelStore models,
                           MongoSrsLogStore log, WorkloadClaim claim) {
        WorkloadClaimFence fence() { return WorkloadClaimFence.from(claim); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
