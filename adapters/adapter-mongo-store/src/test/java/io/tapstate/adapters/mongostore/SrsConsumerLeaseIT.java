package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.SrsConsumerMembership;
import io.tapstate.spi.store.SrsConsumerRun;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SrsSnapshotPin;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a consumer's store still refuses, and what it stops paying, once its read verifications and progress
 * writes validate once per lease. Each case proves a lease with a first verification and a first progress
 * write, changes the cache underneath it the way a takeover, the capture owner's publication, the consumer's
 * own snapshot pin or an operator's index change would, and reads the next operation: a verification within the
 * lease costs no round trip and a progress write is the claim compare-and-set, the state fence and its row; whatever the lease
 * cannot vouch for proves, and whatever a proof refuses lands nowhere.
 */
@RequiresDocker
class SrsConsumerLeaseIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final String CHAIN = "chain-a";
    private static final String ORDERS = "orders";
    private static final String PIPELINE = "pipeline-a";
    private static final SrsConsumerContext A = new SrsConsumerContext(new SrsConsumerKey(CHAIN, PIPELINE, "source-a"), 1);
    /** The state row, the claim compare-and-set, the state fence advance, the offsets row and its replace. */
    private static final List<String> LEASED_WRITE = List.of("find pipeline_state", "update workload_claims", "update pipeline_state",
            "find srs_consumer_offsets", "update srs_consumer_offsets");
    private static final Runnable ACTIVE = () -> {};

    @AfterEach void clearProperties() {
        System.clearProperty(MongoSrsLease.VALIDATION_PROPERTY);
        System.clearProperty(MongoSrsLease.LEASE_PROPERTY);
    }

    @Test void withinALeaseAVerificationIsNoRoundTripAndAProgressWriteIsOneClaimWriteAndItsRow() {
        withConsumer(f -> {
            f.verify(0); f.advance(0);
            // The proofs: structure, identity, membership and the model were read by the first verification and the first write.
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes "));
            assertThat(f.commands).anyMatch(command -> command.equals("find cache_identity")).anyMatch(command -> command.equals("find srs_models"));
            var version0 = f.model(0); f.commands.clear();
            assertThat(f.verify(1)).isEqualTo(version0);
            assertThat(f.commands).as("a leased verification").isEmpty();
            f.advance(1);
            assertThat(f.commands).as("a leased read advance").containsExactlyElementsOf(LEASED_WRITE);
            assertThat(f.consumers.beginWriterRun(A, f.member.revision(), "run-a", Map.of(ORDERS, List.of("writer")), f.fence())).isTrue();
            f.commands.clear();
            assertThat(f.consumers.advanceWriter(A, f.member.revision(), "run-a", ORDERS, "writer", point(1, 1, "t1"), f.fence())).isTrue();
            assertThat(f.commands).as("a leased acknowledgement").containsExactlyElementsOf(LEASED_WRITE);
            var progress = f.consumers.progress(A).orElseThrow();
            assertThat(progress.cursor().perTableSeq()).containsEntry(ORDERS, 1L);
            assertThat(progress.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1, 1), "t1"));
        });
    }

    @Test void perTransactionModeKeepsProvingEveryVerificationAndWrite() {
        System.setProperty(MongoSrsLease.VALIDATION_PROPERTY, "transaction");
        withConsumer(f -> {
            f.verify(0); f.advance(0); f.commands.clear();
            f.verify(1);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find srs_models"));
            f.commands.clear();
            f.advance(1);
            assertThat(f.commands).anyMatch(command -> command.startsWith("listIndexes ")).anyMatch(command -> command.equals("find cache_identity"));
        });
    }

    @Test void aClaimTakenOverWithinTheLeaseRefusesTheNextWriteAndTheNextProofAndProvesAgainOnceRestored() {
        withConsumer(f -> {
            f.verify(0); f.advance(0); Document row = f.cursor();
            f.onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
            code(() -> f.advance(1), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.cursor()).as("nothing of the refused write lands").isEqualTo(row);
            // A verification within the lease costs no round trip, so it is the next proof that sees the takeover.
            f.lease.drop();
            code(() -> f.verify(1), IoError.WORKLOAD_CLAIM_FENCED);
            f.onClaim(new Document("$inc", new Document("claimGeneration", -1L))); f.commands.clear();
            f.advance(1);
            assertThat(f.commands).as("the refusal ended the lease").anyMatch(command -> command.startsWith("listIndexes "));
            assertThat(f.cursor().get("progress", Document.class).get("perTableSeq")).isEqualTo(List.of(new Document("table", ORDERS).append("sequence", 1L)));
        });
    }

    @Test void anEventNamingAModelTheLeaseHasNotProvedProvesAndIsRefusedWhenNonePublishedItAndTheRefusalEndsTheLease() {
        withConsumer(f -> {
            f.verify(0); f.commands.clear();
            code(() -> f.verify(1, 5), IoError.SRS_EVENT_MODEL_UNAVAILABLE);
            assertThat(f.commands).as("an unproved model is never taken on the lease's word").anyMatch(command -> command.equals("find srs_models"));
            f.commands.clear(); f.verify(1);
            assertThat(f.commands).as("the refusal ended the lease").anyMatch(command -> command.startsWith("listIndexes "));
        });
    }

    @Test void thePublicationOfANewVersionIsSeenWhenItsFirstEventProvesAndClosesTheRangeTheLeaseHeldOpen() {
        withConsumer(f -> {
            f.verify(0); f.commands.clear();
            // The capture owner publishes version 1 from sequence 100 through its own stores, which share nothing with this lease.
            f.models.publish(model(1, 100), f.captureFence());
            var version0 = f.model(0); var version1 = f.model(1); f.commands.clear();
            assertThat(f.verify(50)).isEqualTo(version0);
            assertThat(f.commands).as("the window the lease accepts: version 0 is still open-ended here").isEmpty();
            assertThat(f.verify(100, 1)).isEqualTo(version1);
            assertThat(f.commands).as("the first event of the new version proves").anyMatch(command -> command.equals("find srs_models"));
            f.commands.clear();
            // That proof read version 0 again, so the lease now holds it closed at 100: the refusal is a proof's, not the lease's.
            code(() -> f.verify(100, 0), IoError.SRS_EVENT_MODEL_UNAVAILABLE);
            assertThat(f.commands).anyMatch(command -> command.equals("find srs_models"));
            // The refusal ended the lease, so nothing carries: each version proves once more, then both are leased.
            f.commands.clear(); f.verify(0);
            assertThat(f.commands).as("the refusal ended the lease").anyMatch(command -> command.startsWith("listIndexes "));
            f.commands.clear(); f.verify(101, 1);
            assertThat(f.commands).as("a version the new lease has not proved proves").anyMatch(command -> command.equals("find srs_models"))
                    .noneMatch(command -> command.startsWith("listIndexes "));
            f.commands.clear();
            assertThat(f.verify(99, 0)).isEqualTo(version0); assertThat(f.verify(101, 1)).isEqualTo(version1);
            assertThat(f.commands).as("both versions leased").isEmpty();
        });
    }

    @Test void theConsumersOwnSnapshotPinEndsTheLeaseSoTheNextOperationsProveAgainstIt() {
        withConsumer(f -> {
            f.verify(0); f.advance(0); long revision = f.consumers.progress(A).orElseThrow().revision();
            assertThat(f.consumers.pinSnapshot(A, f.member.revision(), revision, new SrsSnapshotPin(1, "seam", Map.of(ORDERS, key(0))), f.fence())).isTrue();
            f.commands.clear(); f.verify(1);
            assertThat(f.commands).as("the verification after a pin proves").anyMatch(command -> command.equals("find srs_models"));
            f.commands.clear(); f.advance(1);
            assertThat(f.commands).as("the write after a pin proves").anyMatch(command -> command.startsWith("listIndexes "));
        });
    }

    @Test void anIndexChangedWithinTheLeaseIsSeenAtTheNextProofAndNothingLandsThen() {
        withConsumer(f -> {
            f.verify(0); f.advance(0);
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database).createIndex(new Document("expiresAt", 1),
                    new IndexOptions().name("expires").expireAfter(60L, TimeUnit.SECONDS));
            // The window the lease accepts: the change is not seen until the next proof.
            f.verify(1); f.advance(1);
            f.lease.drop(); Document row = f.cursor();
            code(() -> f.verify(2), IoError.DOCUMENT_UNREADABLE);
            code(() -> f.advance(2), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.cursor()).isEqualTo(row);
        });
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(expected));
    }

    private static SrsModelKey key(long version) { return new SrsModelKey(CHAIN, ORDERS, SCOPE.cacheIncarnation(), 1, version); }
    private static SrsModel model(long version, long first) {
        return new SrsModel(key(version), ring(), new SourceTable(ORDERS, List.of(new SourceField("id-v" + version, "integer")), List.of("id-v" + version), List.of()), first);
    }
    private static String ring() { return "srs." + CHAIN + "." + ORDERS; }
    private static WriterProgress point(long epoch, long sequence, String token) {
        var order = new SourceOrder(epoch, sequence); return new WriterProgress(order, new ChainPosition(order, token));
    }

    private static void withConsumer(Scenario scenario) {
        var commands = new CopyOnWriteArrayList<String>();
        String name = "srs_consumer_lease_" + UUID.randomUUID().toString().replace("-", "");
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
                            SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.SRS_CONSUMERS,
                            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
                        table.prepareCollectionOn(database, session);
                        for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(database).insertOne(new Document("_id", "cluster").append("clusterId", SCOPE.clusterId())
                        .append("layoutId", MongoConfigurationDomain.LAYOUT).append("bindingVersion", 1L).append("cacheIncarnation", SCOPE.cacheIncarnation())
                        .append("businessPhase", "READY").append("businessPreparationId", "business-a").append("authGeneration", 1L));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var capture = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-a"),
                        new WorkloadOwner("node-c", "boot-c"), 1, Duration.ofSeconds(30)).claim();
                var acquired = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
                        new WorkloadOwner("node-a", "boot-a"), 1, Duration.ofSeconds(30)).claim();
                var pipeline = claims.advanceExecution(acquired, 1, Set.of(acquired.owner().nodeId())).orElseThrow();
                preparePipelineScope(database);
                var captureFence = WorkloadClaimFence.from(capture);
                CaptureHistoryFixture.shared(database, SCOPE, captureFence, CHAIN, "retention");
                // The capture's stores, as the owner uses them: nothing they prove is shared with the consumer's lease.
                var chains = new MongoSrsChainStore(client, database, SCOPE); var models = new MongoSrsModelStore(client, database, SCOPE);
                chains.prepare(CHAIN, "retention", captureFence);
                assertThat(chains.openEpoch(CHAIN, 0, captureFence)).hasValue(1);
                models.publish(model(0, 0), captureFence);
                // The consumer assembly's shape: its store built with a lease, structure verified through the same check as the capture's.
                var structure = new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION);
                var lease = MongoSrsLease.selected();
                var consumers = new MongoSrsConsumerStore(client, database, SCOPE, ACTIVE, structure::verifySrs, lease);
                var member = consumers.attach(A, 0, Set.of(ORDERS), WorkloadClaimFence.from(pipeline)).orElseThrow();
                // The reader's arrival, as the coordinator records it before the first read: the table's confirmed floor.
                assertThat(consumers.startReadingAfter(A, member.revision(), 0, ORDERS, -1L, WorkloadClaimFence.from(pipeline))).isTrue();
                var scope = new PipelineIntentScope(SCOPE.clusterId(), SCOPE.bindingVersion(), SCOPE.cacheIncarnation(), PIPELINE, A.stateGeneration());
                var fixture = new Fixture(database, consumers, models, lease, capture, pipeline, member, SrsConsumerRun.of(scope, member), commands);
                commands.clear();
                scenario.run(fixture);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }

    private static void preparePipelineScope(MongoDatabase database) {
        var physical = new PipelineIntentScope(SCOPE.clusterId(), SCOPE.bindingVersion(), SCOPE.cacheIncarnation(), PIPELINE, A.stateGeneration());
        var header = MongoPipelinePreparationFacts.scope(physical);
        SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", PIPELINE).append("scope", header)
                .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", 0L).append("runtimeWriteFence", 1L));
        var key = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(key)),
                new Document("$set", new Document("pipelineScope", header).append("pipelinePreparation",
                        MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
    }

    private record Fixture(MongoDatabase database, MongoSrsConsumerStore consumers, MongoSrsModelStore models, MongoSrsLease lease,
                           WorkloadClaim capture, WorkloadClaim pipeline, SrsConsumerMembership member, SrsConsumerRun run, List<String> commands) {
        WorkloadClaimFence captureFence() { return WorkloadClaimFence.from(capture); }
        WorkloadClaimFence fence() { return WorkloadClaimFence.from(pipeline); }
        SrsModel verify(long sequence) { return verify(sequence, 0); }
        SrsModel verify(long sequence, long version) { return consumers.verifyReadEvent(run, ORDERS, ring(), sequence, 1, version, captureFence()).orElseThrow(); }
        void advance(long sequence) { assertThat(consumers.advanceReadSequence(A, member.revision(), ORDERS, sequence, fence())).isTrue(); }
        SrsModel model(long version) { return models.load(key(version)).orElseThrow(); }
        void onClaim(Document change) {
            var key = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(key)), change);
        }
        Document cursor() {
            return SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(new Document("_id", new MongoSrsConsumerRecords(SCOPE).id(A.key()))).first();
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
