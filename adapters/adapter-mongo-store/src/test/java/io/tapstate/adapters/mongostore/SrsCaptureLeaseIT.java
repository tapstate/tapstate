package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsCaptureCheckpoint;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SrsTableCheckpoint;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a capture's stores still refuse, and what they stop paying, once their fenced appends and checkpoints
 * validate once per lease. Each case proves a lease with a first append and checkpoint, changes the cache
 * underneath it the way a takeover, an expiry, the owner's own model publication or an operator's index change
 * would, and reads the next write: refused as a unit with nothing landing where the change is one the claim
 * fences, proved again where the owner changed the chain itself, and seen at the next proof otherwise.
 */
@RequiresDocker
class SrsCaptureLeaseIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final String CHAIN = "chain-a";
    private static final String ORDERS = "orders";
    private static final Runnable ACTIVE = () -> {};

    @AfterEach void clearProperties() {
        System.clearProperty(MongoSrsLease.VALIDATION_PROPERTY);
        System.clearProperty(MongoSrsLease.LEASE_PROPERTY);
    }

    @Test void withinALeaseAnAppendAndACheckpointAreOneClaimWriteAndTheirPayloadEach() {
        withCapture(f -> {
            f.append(0); f.checkpoint(0, 1, 0L);
            // The proofs: structure, identity and the model relationships were read by the first append and the first checkpoint.
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes "));
            assertThat(f.commands).anyMatch(command -> command.equals("find cache_identity"));
            f.commands.clear();
            f.append(1);
            assertThat(f.commands).as("a leased append").containsExactly("update workload_claims", "update srs_events", "update srs_log_bounds");
            f.commands.clear();
            f.checkpoint(1, 2, 1L);
            assertThat(f.commands).as("a leased checkpoint").containsExactly("update workload_claims", "find srs_chains", "update srs_chains");
            assertThat(f.events()).isEqualTo(2);
            assertThat(f.chains.load(CHAIN).orElseThrow().checkpoint().tables().get(ORDERS).sequence()).isEqualTo(1L);
        });
    }

    @Test void perTransactionModeKeepsProvingEveryAppendAndCheckpoint() {
        System.setProperty(MongoSrsLease.VALIDATION_PROPERTY, "transaction");
        withCapture(f -> {
            f.append(0); f.checkpoint(0, 1, 0L); f.commands.clear();
            f.append(1);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find srs_models"));
            f.commands.clear();
            f.checkpoint(1, 2, 1L);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find cache_identity"));
        });
    }

    @Test void aClaimTakenOverWithinTheLeaseRefusesTheNextAppendAndCheckpointAsAUnitAndProvesAgainAfterwards() {
        withCapture(f -> {
            f.append(0); f.checkpoint(0, 1, 0L); f.append(1);
            Document root = f.root(); long events = f.events(); Document bounds = f.bounds();
            f.onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
            code(() -> f.append(2), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> f.checkpoint(1, 2, 1L), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.events()).as("nothing of the refused append lands").isEqualTo(events);
            assertThat(f.bounds()).isEqualTo(bounds); assertThat(f.root()).isEqualTo(root);
            // The refusals ended both leases: with the claim restored, the next writes prove again before landing.
            f.onClaim(new Document("$inc", new Document("claimGeneration", -1L))); f.commands.clear();
            f.append(2);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find srs_models"));
            f.commands.clear();
            f.checkpoint(1, 2, 2L);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find cache_identity"));
            assertThat(f.events()).isEqualTo(events + 1);
        });
    }

    @Test void anExpiredClaimLeaseRefusesTheNextWriteWhileTheLocalLeaseStillRuns() {
        System.setProperty(MongoSrsLease.LEASE_PROPERTY, "600000");
        withCapture(f -> {
            f.append(0); f.checkpoint(0, 1, 0L); long events = f.events(); Document root = f.root();
            f.onClaim(new Document("$set", new Document("leaseUntil", new Date(System.currentTimeMillis() - 60_000))));
            code(() -> f.append(1), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> f.checkpoint(1, 2, 0L), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.events()).isEqualTo(events); assertThat(f.root()).isEqualTo(root);
        });
    }

    @Test void theLeaseNeverOutlivesWhatRemainedOfTheClaimWhenItWasProved() {
        withCapture(f -> {
            // Two seconds of claim left at the proof: once they have passed, the next append proves again by itself.
            f.onClaim(new Document("$set", new Document("leaseUntil", new Date(System.currentTimeMillis() + 2_000))));
            f.append(0); f.commands.clear();
            f.append(1);
            assertThat(f.commands).as("still within the proved lease").containsExactly("update workload_claims", "update srs_events", "update srs_log_bounds");
            f.onClaim(new Document("$set", new Document("leaseUntil", new Date(System.currentTimeMillis() + 60_000))));
            Thread.sleep(2_500); f.commands.clear();
            f.append(2);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find srs_models"));
        });
    }

    @Test void theOwnersOwnModelPublicationEndsTheLeaseSoTheNextAppendProvesAgainstTheNewModel() {
        withCapture(f -> {
            f.append(0); f.checkpoint(0, 1, 0L); f.append(1); f.commands.clear();
            // Under the standing lease, an event naming a model nobody published would land unchecked; a lease that
            // did not end on publication would still take it after the publication below.
            f.models.publish(model(ORDERS, 1, 1, 2), f.fence()); f.commands.clear();
            code(() -> f.log.storeAll(ring(ORDERS), 2, List.of(event(5, 1, f.fence()))), IoError.SRS_MODEL_UNAVAILABLE);
            assertThat(f.commands).as("the append after a publication proves").anyMatch(command -> command.equals("find srs_models"));
            assertThat(f.events()).isEqualTo(2);
            f.log.storeAll(ring(ORDERS), 2, List.of(event(1, 1, f.fence()))); f.commands.clear();
            f.log.storeAll(ring(ORDERS), 3, List.of(event(1, 1, f.fence())));
            assertThat(f.commands).as("leased again under the new model").containsExactly("update workload_claims", "update srs_events", "update srs_log_bounds");
            assertThat(f.events()).isEqualTo(4);
        });
    }

    @Test void anIndexChangedWithinTheLeaseIsSeenAtTheNextProofAndNothingLandsThen() {
        withCapture(f -> {
            f.append(0); f.checkpoint(0, 1, 0L);
            SystemCollections.SRS_EVENTS.on(f.database).createIndex(new Document("expiresAt", 1),
                    new IndexOptions().name("expires").expireAfter(60L, TimeUnit.SECONDS));
            // The window the lease accepts: the change is not seen until the next proof.
            f.append(1); assertThat(f.events()).isEqualTo(2);
            f.lease.drop(); long events = f.events(); Document root = f.root();
            code(() -> f.append(2), IoError.DOCUMENT_UNREADABLE);
            code(() -> f.checkpoint(1, 2, 1L), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.events()).isEqualTo(events); assertThat(f.root()).isEqualTo(root);
        });
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(expected));
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

    private static void withCapture(Scenario scenario) {
        var commands = new CopyOnWriteArrayList<String>();
        String name = "srs_lease_" + UUID.randomUUID().toString().replace("-", "");
        var listener = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (!name.equals(event.getDatabaseName())) return;
                var command = event.getCommand();
                var target = command.get(event.getCommandName());
                commands.add(event.getCommandName() + " " + (target != null && target.isString() ? target.asString().getValue() : ""));
            }
        };
        try (var client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
            var database = client.getDatabase(name);
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.SRS_CHAINS,
                            SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS,
                            SystemCollections.SRS_CONSUMERS, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS, SystemCollections.CACHE_WORKLOAD_CLAIMS)) {
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
                CaptureHistoryFixture.shared(database, SCOPE, WorkloadClaimFence.from(claim), CHAIN, "retention");
                // The capture assembly's shape: one lease shared by the three stores, structure verified through the same check.
                var structure = new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION);
                var lease = MongoSrsLease.selected();
                var models = new MongoSrsModelStore(client, database, SCOPE, ACTIVE, structure::verifySrs, lease);
                var chains = new MongoSrsChainStore(client, database, SCOPE, ACTIVE, structure::verifySrs, lease);
                var log = new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models, lease);
                var fixture = new Fixture(database, chains, models, log, lease, claim, commands);
                fixture.prepare();
                commands.clear();
                scenario.run(fixture);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }

    private record Fixture(MongoDatabase database, MongoSrsChainStore chains, MongoSrsModelStore models, MongoSrsLogStore log,
                           MongoSrsLease lease, WorkloadClaim claim, List<String> commands) {
        WorkloadClaimFence fence() { return WorkloadClaimFence.from(claim); }
        void prepare() {
            assertThat(chains.prepare(CHAIN, "retention", fence()).epoch()).isZero();
            chains.requestTables(CHAIN, Set.of(ORDERS), fence());
            assertThat(chains.openEpoch(CHAIN, 0, fence())).hasValue(1);
            models.publish(model(ORDERS, 1, 0, 0), fence());
            assertThat(chains.publishServingTables(CHAIN, 1, Set.of(ORDERS), fence())).isTrue();
        }
        void append(long sequence) { log.storeAll(ring(ORDERS), sequence, List.of(event(0, 1, fence()))); }
        void checkpoint(long expectedRevision, long order, long frontier) {
            var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, order), "token-" + order),
                    Map.of(ORDERS, new SrsTableCheckpoint(key(ORDERS, 1, 0), frontier)));
            assertThat(chains.checkpoint(CHAIN, expectedRevision, checkpoint, fence())).isPresent();
        }
        void onClaim(Document change) { SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document(), change); }
        long events() { return SystemCollections.SRS_EVENTS.on(database).countDocuments(); }
        Document bounds() { return SystemCollections.SRS_LOG_BOUNDS.on(database).find().first(); }
        Document root() { return SystemCollections.SRS_CHAINS.on(database).find().first(); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
