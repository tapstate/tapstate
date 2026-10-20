package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsSnapshotPin;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real transactions over an explicitly prepared business fixture; production capture is not exercised. */
@RequiresDocker
class MongoSrsConsumerStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final String CHAIN = "chain-a";
    private static final SrsConsumerContext A = context("pipeline-a", "source-a", 1);
    private static final SrsConsumerContext B = context("pipeline-b", "source-b", 1);

    @Test void anAttachedSnapshotKeepsItsClosedModelContextUntilItsOwnDetach() {
        withBusiness(f -> {
            var refs=publishSnapshotModels(f,Set.of("orders"));var first=refs.get("orders");
            var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().pinSnapshot(A,member.revision(),0,new SrsSnapshotPin(1,"seam",refs),f.fenceA());
            f.chains().openEpoch(CHAIN,1,f.captureFence()).orElseThrow();
            var next=new SrsModelKey(CHAIN,"orders",SCOPE.cacheIncarnation(),2,0);
            models(f).publishInitial(next,ring("orders"),new SourceTable("orders",List.of(new SourceField("id","int")),List.of(),List.of()),f.captureFence());
            assertThat(models(f).reclaim(CHAIN,f.captureFence())).isZero();assertThat(models(f).require(first).key()).isEqualTo(first);
            f.consumers().detach(A,member.revision(),f.fenceA()).orElseThrow();
            assertThat(models(f).reclaim(CHAIN,f.captureFence())).isOne();assertThat(models(f).require(next).key()).isEqualTo(next);
            assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void independentConsumersKeepSeparateRelationsAndCursorsAcrossReopenAndDetach() {
        withBusiness(f -> {
            var a = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var b = f.consumers().attach(B, 0, Set.of("orders", "quiet"), f.fenceB()).orElseThrow();
            assertThat(f.consumers().attached(CHAIN).revision()).isEqualTo(2);
            assertThat(f.consumers().cursor(A).orElseThrow().perTableSeq()).containsEntry("orders", -1L);
            Document bCursor = cursor(f, B); Document bMember = membership(f, B);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE);
                assertThat(reopened.find(A.key())).contains(a);
                assertThat(reopened.attached(CHAIN).consumers()).containsExactly(a, b);
            }
            var detached = f.consumers().detach(A, 1, f.fenceA()).orElseThrow();
            assertThat(detached.detached()).isTrue(); assertThat(detached.revision()).isEqualTo(2);
            assertThat(f.consumers().cursor(A)).isEmpty();
            assertThat(f.consumers().attached(CHAIN).consumers()).containsExactly(b);
            assertThat(cursor(f, B)).isEqualTo(bCursor); assertThat(membership(f, B)).isEqualTo(bMember);
            assertThat(root(f).getLong("consumerCount")).isOne();
            f.consumers().detach(B, 1, f.fenceB()).orElseThrow();
            assertThat(root(f).getLong("consumerCount")).isZero();
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).countDocuments()).isOne();
        });
    }

    @Test
    void detachedRevisionMarkersFenceLateAttachAndLateDetachEvenWhileTheOriginalClaimIsLive() {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var marker = f.consumers().detach(A, 1, f.fenceA()).orElseThrow();
            assertThat(f.consumers().detach(A, 1, f.fenceA())).contains(marker);
            assertThat(f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isEmpty();
            var next = f.consumers().attach(A, marker.revision(), Set.of("orders"), f.fenceA()).orElseThrow();
            assertThat(next.revision()).isEqualTo(3);
            assertThat(f.consumers().detach(A, 1, f.fenceA())).isEmpty();
            assertThat(f.consumers().cursor(A)).isPresent();
        });
    }

    @Test
    void detachingBeforeAnInitialAttachStillLeavesAnOriginalRevisionBarrier() {
        withBusiness(f -> {
            var marker = f.consumers().detach(A, 0, f.fenceA()).orElseThrow();
            assertThat(marker.detached()).isTrue(); assertThat(marker.revision()).isOne();
            assertThat(f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isEmpty();
            assertThat(f.consumers().attached(CHAIN).consumers()).isEmpty();
        });
    }

    @Test
    void losingTheOnlyDetachedMarkerCannotMakeAnOldInitialAttachNewAgain() {
        withBusiness(f -> {
            f.consumers().detach(A, 0, f.fenceA()).orElseThrow();
            SystemCollections.SRS_CONSUMERS.on(f.database()).deleteMany(new Document()); Document before = root(f);
            assertThatThrownBy(() -> f.consumers().attached(CHAIN)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(root(f)).isEqualTo(before); assertThat(cursor(f, A)).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"member", "cursor", "root"})
    void aLostRequiredFactIsNotAnAbsentConsumerOrAnEmptyChain(String lost) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var resource = switch (lost) { case "member" -> SystemCollections.SRS_CONSUMERS; case "cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS; default -> SystemCollections.SRS_CHAINS; };
            resource.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.consumers().find(A.key())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThatThrownBy(() -> f.consumers().detach(A, 1, f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            if (lost.equals("root")) assertThatThrownBy(() -> f.chains().prepare(CHAIN, "retention", f.captureFence())).isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"srs_consumers", "srs_consumer_offsets"})
    void aLostEmptyCollectionCannotBeImplicitlyRecreatedByOrdinaryAttach(String collection) {
        withBusiness(f -> {
            if (collection.equals("srs_consumers")) SystemCollections.SRS_CONSUMERS.on(f.database()).drop();
            else SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).drop();
            Document before = root(f);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before);
            assertThat(f.database().listCollectionNames()).doesNotContain(collection);
        });
    }

    @Test
    void concurrentClientsCannotApplyDifferentSelectionsAtTheSameOriginalRevision() {
        withBusiness(f -> {
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).isPresent(); });
                var b = executor.submit(() -> { start.await(); return second.attach(A, 0, Set.of("quiet"), f.fenceA()).isPresent(); }); start.countDown();
                assertThat(List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            }
            assertThat(f.consumers().attached(CHAIN).consumers()).hasSize(1); assertThat(root(f).getLong("consumerCount")).isOne();
        });
    }

    @Test
    void aStateGenerationChangeRequiresExplicitDetachAndCannotReturnTheOldCursor() {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow(); var next = new SrsConsumerContext(A.key(), 2);
            assertThatThrownBy(() -> f.consumers().attach(next,1,Set.of("orders"),f.fenceA())).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
            f.consumers().detach(A,1,f.fenceA()).orElseThrow(); replacePipelineScope(f,next);
            f.consumers().attach(next, 2, Set.of("orders"), f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().cursor(A)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThatThrownBy(() -> f.consumers().detach(A,1,f.fenceA())).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
            assertThat(f.consumers().cursor(next)).isPresent();
        });
    }

    @Test
    void theOriginalPipelineClaimCannotBeReplacedByCaptureOrAnotherPipelineAuthorization() {
        withBusiness(f -> {
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.captureFence())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceB())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).release(f.pipelineA())).isTrue();
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(SystemCollections.SRS_CONSUMERS.on(f.database()).countDocuments()).isZero();
        });
    }

    @Test
    void anActuationClaimWithoutAnAllocatedExecutionCannotCreateMembership() {
        withBusiness(f -> {
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var unused = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, "unused"),
                    new WorkloadOwner("node-u", "boot-u"), 1, Duration.ofSeconds(30)).claim();
            assertThatThrownBy(() -> f.consumers().attach(context("unused", "source-u", 1), 0, Set.of("orders"), WorkloadClaimFence.from(unused)))
                    .satisfies(MongoSrsConsumerStoreIT::fenced);
        });
    }

    @Test
    void callbackGuardLossRollsBackBothResourcesTheChainAndTheClaimProof() {
        withBusiness(f -> {
            Document checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first();
            Document root = root(f); Document claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())
                    .find(new Document("_id.resourceId", A.key().pipelineId())).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_chains")) {
                var guarded = new MongoSrsConsumerStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.attach(A, 0, Set.of("orders"), f.fenceA())).satisfies(failure -> code(failure,IoError.CACHE_BUSINESS_FENCED));
                witness.requireCommand();
            }
            assertThat(root(f)).isEqualTo(root); assertThat(membership(f,A)).isNull(); assertThat(cursor(f,A)).isNull();
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first()).isEqualTo(checkpoint);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id.resourceId", A.key().pipelineId())).first()).isEqualTo(claim);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aFreshSameStateCommandStillFencesAnEarlierCapturedOppositeCommand(boolean attached) {
        withBusiness(f -> {
            if (attached) {
                f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
                long capturedDetachRevision = 1;
                f.consumers().attach(A, 1, Set.of("orders"), f.fenceA()).orElseThrow();
                assertThat(f.consumers().detach(A, capturedDetachRevision, f.fenceA())).isEmpty();
                assertThat(f.consumers().cursor(A)).isPresent();
            } else {
                f.consumers().detach(A, 0, f.fenceA()).orElseThrow();
                long capturedAttachRevision = 1;
                f.consumers().detach(A, 1, f.fenceA()).orElseThrow();
                assertThat(f.consumers().attach(A, capturedAttachRevision, Set.of("orders"), f.fenceA())).isEmpty();
                assertThat(f.consumers().cursor(A)).isEmpty();
            }
        });
    }

    @Test
    void reassertingMembershipPreservesRetainedReadAckAndSnapshotProgress() {
        withBusiness(f -> {
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var position = new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1, 7), "saved-token");
            var progress = new io.tapstate.spi.store.ConsumerOffset(A.key().consumerId(), Map.of("orders", 7L), position,
                    List.of("orders"), "saved-seam", 1, Map.of("orders", position), io.tapstate.spi.store.ConsumerProgressKind.SRS);
            var codec = new MongoSrsConsumerRecords(SCOPE);
            var pins = publishSnapshotModels(f, Set.of("orders"));
            var pin = new SrsSnapshotPin(1, "saved-seam", pins);
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).replaceOne(id(A), codec.cursor(member, progress, 11, pin));
            var next = f.consumers().attach(A, 1, Set.of("orders", "quiet"), f.fenceA()).orElseThrow();
            var expected = new io.tapstate.spi.store.ConsumerOffset(A.key().consumerId(), Map.of("orders", 7L, "quiet", -1L), position,
                    List.of("orders"), "saved-seam", 1, Map.of("orders", position), io.tapstate.spi.store.ConsumerProgressKind.SRS);
            assertThat(f.consumers().cursor(A)).contains(expected);
            assertThat(cursor(f, A).getLong("progressRevision")).isEqualTo(11);
            assertThat(cursor(f, A).getLong("membershipRevision")).isEqualTo(next.revision());
        });
    }

    @Test
    void aCursorInsertFailureRollsBackTheRelationAndChainDependencyChange() {
        withBusiness(f -> {
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).createIndex(new Document("stateGeneration", 1),
                    new com.mongodb.client.model.IndexOptions().unique(true));
            Document before = root(f); Document retained = cursor(f, B);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before); assertThat(cursor(f, B)).isEqualTo(retained);
            assertThat(membership(f, A)).isNull(); assertThat(cursor(f, A)).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"member-revision", "member-fence", "cursor-generation", "cursor-progress", "cursor-membership", "root-count", "root-revision"})
    void malformedOrInconsistentFactsAreDiagnosedWithoutRepair(String damage) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            switch (damage) {
                case "member-revision" -> SystemCollections.SRS_CONSUMERS.on(f.database()).updateOne(id(A), new Document("$set", new Document("revision", 1.5D)));
                case "member-fence" -> SystemCollections.SRS_CONSUMERS.on(f.database()).updateOne(id(A), new Document("$set", new Document("commandFence.claimGeneration", 1.5D)));
                case "cursor-generation" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$set", new Document("stateGeneration", 1.5D)));
                case "cursor-progress" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$unset", new Document("progress", "")));
                case "cursor-membership" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$set", new Document("membershipRevision", 2L)));
                case "root-count" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("consumerCount", 2L)));
                default -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("consumerRevision", 3L)));
            }
            Document before = root(f); Document member = membership(f, A); Document cursor = cursor(f, A);
            assertThatThrownBy(() -> f.consumers().find(A.key())).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.getCause()).isNull());
            assertThat(root(f)).isEqualTo(before); assertThat(membership(f, A)).isEqualTo(member); assertThat(cursor(f, A)).isEqualTo(cursor);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "ttl"})
    void ordinaryConsumerOperationsDoNotRepairOrAcceptInvalidIndexes(String damage) {
        withBusiness(f -> {
            if (damage.equals("missing")) SystemCollections.SRS_CONSUMERS.on(f.database()).dropIndex("srs_consumers_chain");
            else SystemCollections.SRS_CONSUMERS.on(f.database()).createIndex(new Document("expiresAt", 1),
                    new com.mongodb.client.model.IndexOptions().expireAfter(30L, TimeUnit.SECONDS));
            assertThatThrownBy(() -> f.consumers().find(A.key())).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(SystemCollections.SRS_CONSUMERS.on(f.database()).countDocuments()).isZero();
        });
    }

    private static SrsConsumerContext context(String pipeline, String source, long generation) { return new SrsConsumerContext(new SrsConsumerKey(CHAIN, pipeline, source), generation); }
    private static Document id(SrsConsumerContext context) { return new Document("_id", new MongoSrsConsumerRecords(SCOPE).id(context.key())); }
    private static Document membership(Fixture f, SrsConsumerContext context) { return SystemCollections.SRS_CONSUMERS.on(f.database()).find(id(context)).first(); }
    @Test
    void snapshotPinsAreIndependentAndReadableAcrossReopenWithoutAnyCaptureClaim() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var a = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var b = f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).release(f.capture())).isTrue();
            var before = root(f); var pinA = new SrsSnapshotPin(17, "opaque-seam-a", refs); var pinB = new SrsSnapshotPin(29, "opaque-seam-b", refs);
            assertThat(f.consumers().pinSnapshot(A, a.revision(), 0, pinA, f.fenceA())).isTrue();
            var savedA = cursor(f, A);
            assertThat(f.consumers().pinSnapshot(B, b.revision(), 0, pinB, f.fenceB())).isTrue();
            assertThat(cursor(f, A)).isEqualTo(savedA);
            assertThat(root(f).getLong("consumerRevision")).isEqualTo(before.getLong("consumerRevision"));
            assertThat(root(f).getLong("consumerCount")).isEqualTo(before.getLong("consumerCount"));
            assertThat(root(f).getLong("modelWriteFence")).isEqualTo(before.getLong("modelWriteFence") + 2);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var independent = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE);
                var held = independent.progress(A).orElseThrow();
                assertThat(held.snapshot()).isEqualTo(pinA); assertThat(held.revision()).isOne();
                assertThat(held.cursor().cdcStartPosition()).isEqualTo("opaque-seam-a"); assertThat(held.cursor().snapshotEpoch()).isEqualTo(17);
                assertThat(independent.pinSnapshot(A, a.revision(), 0, pinA, f.fenceA())).isTrue();
                assertThat(independent.progress(A)).contains(held);
                assertThat(independent.progress(B).orElseThrow().snapshot()).isEqualTo(pinB);
            }
        });
    }

    @Test
    void addingASelectedTablePreservesOldPinsAndOnlyExtendsTheSavedReferenceSet() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders", "quiet"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var first = new SrsSnapshotPin(91, "original-seam", Map.of("orders", refs.get("orders")));
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, first, f.fenceA())).isTrue();
            var expanded = f.consumers().attach(A, member.revision(), Set.of("orders", "quiet"), f.fenceA()).orElseThrow();
            assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(first);
            var full = new SrsSnapshotPin(91, "original-seam", refs);
            assertThat(f.consumers().pinSnapshot(A, expanded.revision(), 1, full, f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(full);
            var reduced = f.consumers().attach(A, expanded.revision(), Set.of("quiet"), f.fenceA()).orElseThrow();
            var before = cursor(f, A);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, reduced.revision(), 2,
                    new SrsSnapshotPin(91, "original-seam", Map.of("quiet", refs.get("quiet"))), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(cursor(f, A)).isEqualTo(before); assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(full);
        });
    }

    @Test
    void aSavedSnapshotNeverAdoptsANewerPublishedModelOrSnapshotOrder() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var first = new SrsSnapshotPin(17, "saved-seam", refs); assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, first, f.fenceA())).isTrue();
            var next = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 1);
            models(f).publish(new SrsModel(next, ring("orders"), new SourceTable("orders", List.of(new SourceField("changed", "int")), List.of(), List.of()), 1), f.captureFence());
            var before = cursor(f, A);
            for (var replacement : List.of(new SrsSnapshotPin(18, "saved-seam", refs), new SrsSnapshotPin(17, "new-seam", refs),
                    new SrsSnapshotPin(17, "saved-seam", Map.of("orders", next)))) {
                assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 1, replacement, f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            }
            assertThat(cursor(f, A)).isEqualTo(before);
            SystemCollections.SRS_MODELS.on(f.database()).updateOne(new Document("_id.modelVersion", 1L), new Document("$set", new Document("schema", new Document("bad", "secret-sentinel"))));
            assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(first);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"body", "context", "bounds", "anchor"})
    void losingARequiredSnapshotFactDoesNotUseLatestDiscoveryOrAnEmptyProgress(String missing) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(17, "saved-seam", refs); assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            switch (missing) {
                case "body" -> SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());
                case "context" -> SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).deleteMany(new Document());
                case "bounds" -> SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
                case "anchor" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelContextHeads", List.of())));
                default -> throw new AssertionError(missing);
            }
            var before = cursor(f, A);
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f, A)).isEqualTo(before);
        });
    }

    @Test
    void snapshotCasAndTheOriginalMembershipCannotBeReplacedOnRetry() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs); var before = cursor(f, A);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 7, pin, f.fenceA())).isFalse(); assertThat(cursor(f, A)).isEqualTo(before);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            f.consumers().attach(A, member.revision(), Set.of("orders"), f.fenceA()).orElseThrow(); before = cursor(f, A);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isFalse(); assertThat(cursor(f, A)).isEqualTo(before);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision() + 1, 1, pin, f.fenceB())).satisfies(MongoSrsConsumerStoreIT::fenced);
        });
    }

    @Test
    void independentClientsCannotPinDifferentSnapshotSeamsAtOneOriginalRevision() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam-a", refs), f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return second.pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam-b", refs), f.fenceA()); });
                start.countDown(); assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
                assertThat(f.consumers().progress(A).orElseThrow().revision()).isOne();
            }
        });
    }

    @Test
    void guardLossRollsBackThePinOriginalClaimProofAndChainWriteFence() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var root = root(f); var cursor = cursor(f,A);
            var checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first();
            var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id", WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_consumer_offsets")) {
                var guarded = new MongoSrsConsumerStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam", refs), f.fenceA()))
                        .satisfies(failure -> code(failure,IoError.CACHE_BUSINESS_FENCED));
                witness.requireCommand();
            }
            assertThat(root(f)).isEqualTo(root); assertThat(cursor(f,A)).isEqualTo(cursor);
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first()).isEqualTo(checkpoint);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first()).isEqualTo(claim);
        });
    }

    @Test
    void aLatePinCannotReviveADetachedConsumerOrBorrowAnotherStateGeneration() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs); var detached = f.consumers().detach(A, member.revision(), f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            var next = new SrsConsumerContext(A.key(),2); replacePipelineScope(f,next);
            f.consumers().attach(next,detached.revision(),Set.of("orders"),f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A,detached.revision()+1,0,pin,f.fenceA())).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
            assertThat(f.consumers().progress(next).orElseThrow().snapshot()).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot", "snapshotEpoch", "modelEpoch", "seam"})
    void corruptSnapshotPayloadCannotLoseItsNecessaryReferences(String changed) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs); assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            String path = switch (changed) {
                case "snapshot" -> "snapshot"; case "snapshotEpoch" -> "snapshot.snapshotEpoch";
                case "modelEpoch" -> "snapshot.models.0.model.captureContext.epoch"; case "seam" -> "progress.cdcStartPosition";
                default -> throw new AssertionError(changed);
            };
            Object value = changed.equals("snapshot") ? null : changed.equals("seam") ? "changed" : 1.0;
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$set", new Document(path, value)));
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
        });
    }

    @Test
    void counterExhaustionCannotLeaveAPinWithoutItsOriginalChainCompetition() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelWriteFence", Long.MAX_VALUE - 1)));
            var before = cursor(f, A); var root = root(f);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam", refs), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f, A)).isEqualTo(before); assertThat(root(f)).isEqualTo(root);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"SRS_MODELS", "SRS_MODEL_CONTEXTS"})
    void aRequiredModelUniqueIndexCannotBeDowngradedToANonUniqueOne(String resource) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var table = SystemCollections.valueOf(resource); var declared = table.indexes().getFirst();
            assertThat(declared.unique()).isTrue(); table.on(f.database()).dropIndex(declared.indexName());
            var keys = new Document(); declared.keys().forEach(key -> keys.put(key, 1));
            table.on(f.database()).createIndex(keys, new com.mongodb.client.model.IndexOptions().name(declared.indexName()).unique(false));
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        });
    }

    @Test
    void writerAccountingSurvivesReopenAndOnlyTheSlowestWriterAcknowledgesItsOwnConsumer() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var roster = Map.of("orders",List.of("fast","slow"));
            assertThat(f.consumers().beginWriterRun(A,1,"run-a",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().advanceWriter(A,1,"run-a","orders","fast",point(1,10,"t10"),f.fenceA())).isTrue();
            assertThat(f.consumers().cursor(A).orElseThrow().sinkAcked()).isNull();
            var root = root(f);
            assertThat(f.consumers().advanceWriter(A,1,"run-a","orders","slow",point(1,6,"t6"),f.fenceA())).isTrue();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",20,f.fenceA())).isTrue();
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1,6),"t6"));
            assertThat(held.cursor().perTableSeq()).containsEntry("orders",20L);
            assertThat(held.writerRun().commandFence()).isEqualTo(f.fenceA());
            assertThat(held.writerRun().membershipRevision()).isOne();
            assertThat(f.consumers().beginWriterRun(A,1,"run-a",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().advanceWriter(A,1,"run-a","orders","fast",point(1,3,"late"),f.fenceA())).isTrue();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",2,f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A)).contains(held); assertThat(root(f)).isEqualTo(root);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE);
                assertThat(reopened.progress(A)).contains(held);
                assertThat(reopened.advanceWriter(A,1,"run-a","orders","slow",point(1,7,"t7"),f.fenceA())).isTrue();
                assertThat(reopened.cursor(A).orElseThrow().sinkAcked().token()).isEqualTo("t7");
            }
        });
    }

    @Test
    void theOwningSnapshotSeamAndTableLocalCdcAcknowledgementsCommitTogether() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f,Set.of("orders","quiet"));
            f.consumers().attach(A,0,Set.of("orders","quiet"),f.fenceA()).orElseThrow();
            var roster = Map.of("orders",List.of("a"),"quiet",List.of("b"));
            assertThat(f.consumers().beginWriterRun(A,1,"run",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().pinSnapshot(A,1,1,new SrsSnapshotPin(7,"owner-seam",refs),f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A).orElseThrow().writerRun().runId()).isEqualTo("run");
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","a",point(7,SourceOrder.SNAPSHOT_SEQ,"other-consumer"),f.fenceA())).isTrue();
            var partial = f.consumers().cursor(A).orElseThrow();
            assertThat(partial.snapshotCompletedTables()).containsExactly("orders"); assertThat(partial.sinkAcked()).isNull();
            assertThat(f.consumers().advanceWriter(A,1,"run","quiet","b",point(7,4,"quiet-cdc"),f.fenceA())).isTrue();
            var held = f.consumers().cursor(A).orElseThrow();
            assertThat(held.sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"owner-seam"));
            assertThat(held.snapshotCompletedTables()).containsExactly("orders","quiet");
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","a",point(7,500,"orders-cdc"),f.fenceA())).isTrue();
            var advanced = f.consumers().cursor(A).orElseThrow();
            assertThat(advanced.sinkAcked()).isEqualTo(held.sinkAcked());
            assertThat(advanced.sinkAckedByTable().get("orders").token()).isEqualTo("orders-cdc");
            assertThat(advanced.sinkAckedByTable().get("quiet").token()).isEqualTo("quiet-cdc");
        });
    }

    @Test
    void aDurableEpochBoundIsNotDecodedAsAPhysicalResumePosition() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA())).isTrue();
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","writer",new WriterProgress(new SourceOrder(0,Long.MAX_VALUE),null),f.fenceA())).isTrue();
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.cursor().sinkAcked()).isNull();
            assertThat(held.cursor().sinkAckedByTable()).containsEntry("orders",new ChainPosition(new SourceOrder(0,Long.MAX_VALUE),null));
            assertThat(held.writerRun().progress().get("orders").get("writer").durableThrough()).isEqualTo(new SourceOrder(0,Long.MAX_VALUE));
        });
    }

    @Test
    void theSameExecutionCannotReplaceItsOriginalRunRosterOrBorrowANewerMembership() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var roster = Map.of("orders",List.of("writer"));
            assertThat(f.consumers().beginWriterRun(A,1,"run",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("replacement")),f.fenceA())).isFalse();
            assertThat(f.consumers().beginWriterRun(A,1,"replacement",roster,f.fenceA())).isFalse();
            assertThat(f.consumers().advanceWriter(A,1,"replacement","orders","writer",point(1,1,"t1"),f.fenceA())).isFalse();
            assertThatThrownBy(() -> f.consumers().advanceWriter(A,1,"run","orders","unknown",point(1,1,"t1"),f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            f.consumers().attach(A,1,Set.of("orders"),f.fenceA()).orElseThrow(); var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.writerRun().membershipRevision()).isOne();
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,1,"t1"),f.fenceA())).isFalse();
            assertThat(f.consumers().advanceWriter(A,2,"run","orders","writer",point(1,1,"t1"),f.fenceA())).isFalse();
            assertThat(f.consumers().beginWriterRun(A,2,"replacement",roster,f.fenceA())).isFalse();
            assertThat(f.consumers().progress(A)).contains(held);
        });
    }

    @Test
    void aNewExecutionReplacesOnlyWriterAccountingAndCannotAcceptLateOriginalReports() {
        withBusiness(f -> {
            var roster = Map.of("orders",List.of("writer"));
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().beginWriterRun(A,1,"old",roster,f.fenceA());
            f.consumers().advanceWriter(A,1,"old","orders","writer",point(1,7,"saved"),f.fenceA());
            f.consumers().advanceReadSequence(A,1,"orders",8,f.fenceA()); var before = f.consumers().progress(A).orElseThrow();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var next = WorkloadClaimFence.from(claims.advanceExecution(f.pipelineA(),1,Set.of(f.pipelineA().owner().nodeId())).orElseThrow());
            var current = f.consumers().attach(A,1,Set.of("orders"),next).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A,current.revision(),"old",roster,next)).isFalse();
            assertThat(f.consumers().beginWriterRun(A,current.revision(),"new",roster,next)).isTrue();
            var after = f.consumers().progress(A).orElseThrow();
            assertThat(after.cursor()).isEqualTo(before.cursor()); assertThat(after.writerRun().progress()).isEmpty();
            assertThat(after.writerRun().commandFence()).isEqualTo(next);
            assertThatThrownBy(() -> f.consumers().advanceWriter(A,1,"old","orders","writer",point(1,99,"late"),f.fenceA())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(f.consumers().advanceWriter(A,current.revision(),"old","orders","writer",point(1,99,"late"),next)).isFalse();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",99,next)).isFalse();
            assertThat(f.consumers().progress(A)).contains(after);
        });
    }

    @Test
    void concurrentOriginalWritersFoldWithoutLosingEitherReportOrInventingAFasterMinimum() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("a","b")),f.fenceA());
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().advanceWriter(A,1,"run","orders","a",point(1,10,"t10"),f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return second.advanceWriter(A,1,"run","orders","b",point(1,6,"t6"),f.fenceA()); });
                start.countDown(); assertThat(List.of(a.get(15,TimeUnit.SECONDS),b.get(15,TimeUnit.SECONDS))).containsOnly(true);
            }
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.writerRun().progress().get("orders")).containsKeys("a","b");
            assertThat(held.cursor().sinkAcked().token()).isEqualTo("t6"); assertThat(held.revision()).isEqualTo(3);
        });
    }

    @Test
    void anUnaffectedConsumerCanKeepReadingAndAcknowledgingWhenAnotherPrivateCursorIsLost() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            f.consumers().beginWriterRun(B,1,"b",Map.of("orders",List.of("writer")),f.fenceB());
            f.consumers().startReadingAfter(B,1,1,"orders",-1,f.fenceB());
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().attached(CHAIN)).isInstanceOf(TapstateException.class);
            assertThat(f.consumers().advanceReadSequence(B,1,"orders",5,f.fenceB())).isTrue();
            assertThat(f.consumers().advanceWriter(B,1,"b","orders","writer",point(1,5,"b5"),f.fenceB())).isTrue();
            var held = f.consumers().progress(B).orElseThrow();
            assertThat(held.cursor().perTableSeq()).containsEntry("orders",5L); assertThat(held.cursor().sinkAcked().token()).isEqualTo("b5");
        });
    }

    @ParameterizedTest @ValueSource(strings={"missing","null-progress","wrong-revision","bad-fence","bad-bound"})
    void malformedWriterFactsCannotBeTreatedAsANewEmptyRun(String damage) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,3,"t3"),f.fenceA());
            switch (damage) {
                case "missing" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$unset",new Document("writerRun","")));
                case "null-progress" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.progress",null)));
                case "wrong-revision" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.membershipRevision",2L)));
                case "bad-fence" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.commandFence.executionGeneration",1.5D)));
                default -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.progress.0.writers.0.durableThrough.epoch",1.5D)));
            }
            var before = cursor(f,A);
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,4,"t4"),f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    @ParameterizedTest @ValueSource(strings={"begin","ack","read","arrival"})
    void losingTheOriginalGuardAfterARealProgressWriteRollsBackCursorClaimAndCheckpoint(String operation) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            if (!operation.equals("begin")) f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
            if (operation.equals("read")) f.consumers().startReadingAfter(A,1,1,"orders",-1,f.fenceA());
            var before = cursor(f,A); var root = root(f);
            var checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first();
            var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_consumer_offsets")) {
                var guarded = new MongoSrsConsumerStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> {
                    switch (operation) {
                        case "begin" -> guarded.beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
                        case "ack" -> guarded.advanceWriter(A,1,"run","orders","writer",point(1,7,"t7"),f.fenceA());
                        case "arrival" -> guarded.startReadingAfter(A,1,1,"orders",5,f.fenceA());
                        default -> guarded.advanceReadSequence(A,1,"orders",7,f.fenceA());
                    }
                }).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED)); witness.requireCommand();
            }
            assertThat(cursor(f,A)).isEqualTo(before); assertThat(root(f)).isEqualTo(root);
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first()).isEqualTo(checkpoint);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first()).isEqualTo(claim);
        });
    }

    @ParameterizedTest @ValueSource(strings={"begin","read"})
    void hotProgressCannotPassAMissingRequiredSnapshotModel(String operation) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f,Set.of("orders"));
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().pinSnapshot(A,1,0,new SrsSnapshotPin(1,"seam",refs),f.fenceA());
            SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());
            var before = cursor(f,A);
            assertThatThrownBy(() -> {
                if (operation.equals("begin")) f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
                else f.consumers().advanceReadSequence(A,1,"orders",5,f.fenceA());
            }).isInstanceOf(TapstateException.class);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    @Test
    void initialArrivalAndReadCursorCommitTogetherButReadAheadNeverMovesTheRecoveryFloor() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().advanceReadSequence(A,1,"orders",9,f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(f.consumers().startReadingAfter(A,1,4,"orders",10,f.fenceA())).isFalse();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA())).isTrue();
            var arrival = f.consumers().progress(A).orElseThrow();
            assertThat(arrival.ringDoneThrough()).containsEntry("orders",10L); assertThat(arrival.cursor().perTableSeq()).containsEntry("orders",10L);
            assertThat(arrival.cursor().sinkAcked()).isNull();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",99,f.fenceA())).isTrue();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",100,f.fenceA())).isFalse();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA())).isTrue();
            var read = f.consumers().progress(A).orElseThrow();
            assertThat(read.ringDoneThrough()).isEqualTo(arrival.ringDoneThrough()); assertThat(read.cursor().perTableSeq()).containsEntry("orders",99L);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE);
                assertThat(reopened.progress(A)).contains(read);
            }
        });
    }

    @Test
    void actualRingConfirmationSurvivesAnEpochEndBoundAndWriterRunReplacement() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().beginWriterRun(A,1,"old",Map.of("orders",List.of("writer")),f.fenceA());
            f.consumers().advanceWriter(A,1,"old","orders","writer",point(1,12,"t12"),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",12L);
            f.consumers().advanceWriter(A,1,"old","orders","writer",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            var ended = f.consumers().progress(A).orElseThrow();
            assertThat(ended.cursor().sinkAckedByTable().get("orders").order().seq()).isEqualTo(Long.MAX_VALUE);
            assertThat(ended.ringDoneThrough()).containsEntry("orders",12L);
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var next = WorkloadClaimFence.from(claims.advanceExecution(f.pipelineA(),1,Set.of(f.pipelineA().owner().nodeId())).orElseThrow());
            f.consumers().attach(A,1,Set.of("orders"),next).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A,2,"new",Map.of("orders",List.of("writer")),next)).isTrue();
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).isEqualTo(ended.ringDoneThrough());
            assertThat(f.consumers().startReadingAfter(A,2,0,"orders",999,next)).isFalse();
        });
    }

    @ParameterizedTest @ValueSource(booleans={true,false})
    void aFiniteConfirmationArrivingAfterTheEpochEndStillRaisesThePhysicalRingFloor(boolean tokened) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","writer",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,12,tokened ? "t12" : null),f.fenceA());
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.cursor().sinkAckedByTable().get("orders").order().seq()).isEqualTo(Long.MAX_VALUE);
            assertThat(held.ringDoneThrough()).containsEntry("orders",12L);
        });
    }

    @Test
    void aLateFiniteTokenStillWaitsForTheOtherWritersDurableBound() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("fast","slow")),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","fast",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","fast",point(1,12,"t12"),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",10L);
            f.consumers().advanceWriter(A,1,"run","orders","slow",point(1,11,null),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",11L);
            f.consumers().advanceWriter(A,1,"run","orders","slow",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",12L);
        });
    }

    @Test
    void concurrentOriginalArrivalsNeverOverwriteTheFirstCommittedSafePlace() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return second.startReadingAfter(A,1,0,"orders",20,f.fenceA()); }); start.countDown();
                assertThat(a.get(15,TimeUnit.SECONDS)).isTrue(); boolean high = b.get(15,TimeUnit.SECONDS);
                var held = f.consumers().progress(A).orElseThrow(); long floor = held.ringDoneThrough().get("orders");
                assertThat(floor).isIn(10L,20L); assertThat(high).isEqualTo(floor == 20);
                assertThat(held.cursor().perTableSeq()).containsEntry("orders",floor); assertThat(held.revision()).isOne();
            }
        });
    }

    @ParameterizedTest @ValueSource(strings={"missing","fraction","unknown-table"})
    void aMalformedRecoveryFloorCannotFallBackToReadAheadOrADifferentArrival(String fault) {
        withBusiness(f -> {
            var member = f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",5,f.fenceA());
            if (fault.equals("missing")) SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$unset",new Document("ringDoneThrough","")));
            else SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("ringDoneThrough.0."+(fault.equals("fraction") ? "sequence" : "table"),fault.equals("fraction") ? 5.5D : "unknown")));
            var before = cursor(f,A);
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().startReadingAfter(A,member.revision(),1,"orders",99,f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    private static WriterProgress point(long epoch,long sequence,String token) {
        var order = new SourceOrder(epoch,sequence); return new WriterProgress(order,token == null ? null : new ChainPosition(order,token));
    }

    private static Map<String, SrsModelKey> publishSnapshotModels(Fixture f, Set<String> tables) {
        assertThat(f.chains().openEpoch(CHAIN, 0, f.captureFence())).hasValue(1);
        var refs = new java.util.HashMap<String, SrsModelKey>();
        for (String table : tables) {
            var key = new SrsModelKey(CHAIN, table, SCOPE.cacheIncarnation(), 1, 0);
            models(f).publish(new SrsModel(key, ring(table), new SourceTable(table, List.of(new SourceField("id", "int")), List.of(), List.of()), 0), f.captureFence());
            refs.put(table, key);
        }
        return Map.copyOf(refs);
    }
    private static String ring(String table) { return "srs." + CHAIN + "." + table; }
    private static MongoSrsModelStore models(Fixture f) { return new MongoSrsModelStore(f.client(), f.database(), SCOPE); }

    private static Document cursor(Fixture f, SrsConsumerContext context) { return SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).find(id(context)).first(); }
    private static Document root(Fixture f) { return SystemCollections.SRS_CHAINS.on(f.database()).find().first(); }
    private static void unavailable(Throwable failure) { code(failure, IoError.SRS_CONSUMER_UNAVAILABLE); }
    private static void fenced(Throwable failure) { code(failure, IoError.WORKLOAD_CLAIM_FENCED); }
    private static void code(Throwable failure, IoError expected) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> { assertThat(error.code()).isEqualTo(expected); assertThat(error.getCause()).isNull(); });
    }
    @Test void trimmingAnEarlierConfirmedCutCannotDeleteANewConsumersUnconfirmedRows() {
        withBusiness(f -> {
            publishSnapshotModels(f,Set.of("orders"));var log=new MongoSrsLogStore(f.client(),SystemCollections.SRS_EVENTS.on(f.database()),
                    SystemCollections.SRS_LOG_BOUNDS.on(f.database()),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()),models(f));
            var events=new java.util.ArrayList<io.tapstate.spi.store.SrsLogRecord>();
            for(int i=0;i<5;i++)events.add(new io.tapstate.spi.store.SrsLogRecord("t"+i,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",i),0,f.captureFence(),1));
            log.storeAll(ring("orders"),0,events);
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",4,f.fenceA())).isTrue();
            long oldCut=f.consumers().progress(A).orElseThrow().ringDoneThrough().get("orders");
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            log.trim(ring("orders"),oldCut,f.captureFence());
            assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(-1);
            assertThat(log.readBatch(ring("orders"),0,5).records()).hasSize(5);
            assertThat(f.consumers().startReadingAfter(B,1,0,"orders",1,f.fenceB())).isTrue();
            log.trim(ring("orders"),oldCut,f.captureFence());
            assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(1);
            assertThat(log.readBatch(ring("orders"),2,3).records().keySet()).containsExactly(2L,3L,4L);
            assertThat(f.consumers().advanceReadSequence(B,1,"orders",4,f.fenceB())).isTrue();
            log.trim(ring("orders"),oldCut,f.captureFence());assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(1);
            f.consumers().detach(B,1,f.fenceB()).orElseThrow();log.trim(ring("orders"),oldCut,f.captureFence());
            assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(4);assertThat(log.readBatch(ring("orders"),0,5).records()).isEmpty();
        });
    }

    @Test void attachAfterTheTrimSnapshotForcesTheOriginalTransactionToRecheckItsCut() {
        withBusiness(f -> {
            var original=preparedTrimLog(f);f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",4,f.fenceA());
            var paused=new CountDownLatch(1);var proceed=new CountDownLatch(1);var once=new java.util.concurrent.atomic.AtomicBoolean();
            var pending=java.util.concurrent.ConcurrentHashMap.<Integer>newKeySet();
            var listener=new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if(event.getCommandName().equals("find")&&event.getCommand().getString("find").getValue().equals(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.collectionName()))pending.add(event.getRequestId());
                }
                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if(pending.remove(event.getRequestId())&&once.compareAndSet(false,true)) {
                        paused.countDown();try{if(!proceed.await(15,TimeUnit.SECONDS))throw new AssertionError("trim race barrier did not open");}
                        catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                    }
                }
            };
            try(var observed=MongoClients.create(com.mongodb.MongoClientSettings.builder().applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build());
                var workers=Executors.newSingleThreadExecutor()) {
                var database=observed.getDatabase(f.database().getName());var models=new MongoSrsModelStore(observed,database,SCOPE);
                var log=new MongoSrsLogStore(observed,SystemCollections.SRS_EVENTS.on(database),SystemCollections.SRS_LOG_BOUNDS.on(database),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database),models);
                var result=workers.submit(() -> log.trim(ring("orders"),4,f.captureFence()));
                try {
                    assertThat(paused.await(10,TimeUnit.SECONDS)).as("the trim read its original consumer snapshot").isTrue();
                    f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
                } finally {proceed.countDown();}
                result.get(20,TimeUnit.SECONDS);
            }
            assertThat(original.bounds(ring("orders")).trimmedThrough()).isEqualTo(-1);
            assertThat(original.readBatch(ring("orders"),0,5).records()).hasSize(5);
            assertThat(f.consumers().attached(CHAIN).consumers()).hasSize(2);
        });
    }

    @Test void trimCannotInterpretAMissingCursorAsAnUnreferencedLog() {
        withBusiness(f -> {
            var log=preparedTrimLog(f);f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",4,f.fenceA());
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
            var bounds=SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first();var claim=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",MongoWorkloadClaimStore.id(f.captureFence().key()))).first();
            assertThatThrownBy(() -> log.trim(ring("orders"),4,f.captureFence())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first()).isEqualTo(bounds);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isEqualTo(5);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",MongoWorkloadClaimStore.id(f.captureFence().key()))).first()).isEqualTo(claim);
        });
    }
    private static MongoSrsLogStore preparedTrimLog(Fixture f) {
        publishSnapshotModels(f,Set.of("orders"));var log=new MongoSrsLogStore(f.client(),SystemCollections.SRS_EVENTS.on(f.database()),
                SystemCollections.SRS_LOG_BOUNDS.on(f.database()),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()),models(f));
        var events=new java.util.ArrayList<io.tapstate.spi.store.SrsLogRecord>();
        for(int i=0;i<5;i++)events.add(new io.tapstate.spi.store.SrsLogRecord("t"+i,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",i),0,f.captureFence(),1));
        log.storeAll(ring("orders"),0,events);return log;
    }

    @Test void rebuildPreflightRequiresOnlyAConsumerDetachedAndKeepsBUnchanged() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var bMember = membership(f,B); var bCursor = cursor(f,B);
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(failure ->
                    assertThat(((TapstateException)failure).code()).isEqualTo(IoError.PIPELINE_STATE_REBUILD_REQUIRED));
            f.consumers().detach(A,1,f.fenceA()).orElseThrow(); var chain = root(f);
            rebuildPreflight(f);
            assertThat(membership(f,B)).isEqualTo(bMember); assertThat(cursor(f,B)).isEqualTo(bCursor);
            assertThat(root(f)).isEqualTo(chain).containsEntry("consumerCount",1L);
        });
    }
    @Test void rebuildPreflightCannotIgnoreAnOrphanOffsetEvenWithoutAnAMembership() {
        withBusiness(f -> {
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var orphan = new Document(cursor(f,B)); var oldId = orphan.get("_id",Document.class);
            orphan.put("_id",new Document(oldId).append("consumer","unknown-consumer"));
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).insertOne(orphan);
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).countDocuments()).isEqualTo(2);
        });
    }
    @Test void rebuildPreflightCannotInterpretMissingCollectionOrUnclosedCountsAsNoAConsumer() {
        withBusiness(f -> {
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id",CHAIN),new Document("$inc",new Document("consumerCount",1L).append("consumerRevision",1L)));
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            SystemCollections.SRS_CONSUMERS.on(f.database()).drop();
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(failure ->
                    assertThat(((TapstateException)failure).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(f.database().listCollectionNames().into(new java.util.ArrayList<>())).doesNotContain("srs_consumers");
        });
    }
    @Test void rebuildPreflightFindsMembershipsWhoseChainRootWasLost() {
        withBusiness(f -> {
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            SystemCollections.SRS_CHAINS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).countDocuments()).isZero();
        });
    }
    private static void rebuildPreflight(Fixture f) {
        var original = new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
        try (var session = f.client().startSession()) {
            session.withTransaction(() -> { f.consumers().requireDetachedPipeline(session,original); return null; });
        }
    }

    @Test void positionEditsPreserveTruthFenceOldWritersAndAllowANewEditAfterReacquisition() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            f.chains().requestTables(CHAIN, Set.of("orders"), f.captureFence());
            f.chains().publishServingTables(CHAIN, 1, Set.of("orders"), f.captureFence());
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "original"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(refs.get("orders"), -1)));
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            f.consumers().pinSnapshot(A, 1, 0, new SrsSnapshotPin(1, "original", refs), f.fenceA());
            f.consumers().beginWriterRun(A, 1, "old-run", Map.of("orders", List.of("writer")), f.fenceA());
            f.consumers().advanceWriter(A, 1, "old-run", "orders", "writer", point(1, 3, "t3"), f.fenceA());
            var before = f.consumers().progress(A).orElseThrow();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var node = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, "node-edit"),
                    new WorkloadOwner("node-edit", "boot-edit"), 1, Duration.ofSeconds(30)).claim();
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("claimAdmissionFence", 0L)));
            var readers = Map.of(positionScope(A), f.fenceA(), positionScope(B), f.fenceB());
            var edit = f.consumers().positions(WorkloadClaimFence.from(node), java.time.Duration.ofDays(7));
            edit.rewrite(List.of(positionMove(f, "requested", f.captureFence())), readers, java.time.Instant.now());
            var changed = f.chains().load(CHAIN).orElseThrow();
            assertThat(changed.checkpoint()).isEqualTo(checkpoint); assertThat(changed.checkpointRevision()).isOne();
            assertThat(changed.resumeOverride().token()).isEqualTo("requested");
            var after = f.consumers().progress(A).orElseThrow();
            assertThat(after.cursor().sinkAcked()).isNull(); assertThat(after.cursor().sinkAckedByTable()).isEmpty(); assertThat(after.writerRun()).isNull();
            assertThat(after.snapshot()).isEqualTo(before.snapshot()); assertThat(after.ringDoneThrough()).isEqualTo(before.ringDoneThrough());
            assertThat(after.cursor().perTableSeq()).isEqualTo(before.cursor().perTableSeq());
            assertThat(claims.read(f.pipelineA().key()).orElseThrow().leased()).isFalse();
            assertThatThrownBy(() -> f.consumers().beginWriterRun(A, 1, "old-run", Map.of("orders", List.of("writer")), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().advanceWriter(A, 1, "old-run", "orders", "writer", point(1, 4, "late"), f.fenceA())).isInstanceOf(TapstateException.class);
            var a = claims.acquire(f.pipelineA().key(), f.pipelineA().owner(), 1, Duration.ofSeconds(30)).claim();
            var b = claims.acquire(f.pipelineB().key(), f.pipelineB().owner(), 1, Duration.ofSeconds(30)).claim();
            assertThat(a.claimGeneration()).isEqualTo(f.pipelineA().claimGeneration()+1);
            edit.rewrite(List.of(positionMove(f, "second", f.captureFence())),
                    Map.of(positionScope(A), WorkloadClaimFence.from(a), positionScope(B), WorkloadClaimFence.from(b)), java.time.Instant.now());
            assertThat(f.chains().load(CHAIN).orElseThrow().resumeOverride().token()).isEqualTo("second");
            f.chains().openEpoch(CHAIN, 1, f.captureFence()).orElseThrow();
            assertThat(f.chains().load(CHAIN).orElseThrow().resumeOverride().token()).isEqualTo("second");
            assertThat(f.chains().load(CHAIN).orElseThrow().checkpoint()).isEqualTo(checkpoint);
            try (var reopened = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThat(new MongoSrsChainStore(reopened, reopened.getDatabase(f.database().getName()), SCOPE).load(CHAIN).orElseThrow().resumeToken()).isEqualTo("second");
            }
            f.chains().publishServingTables(CHAIN, 2, Set.of("orders"), f.captureFence());
            var nextKey = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 2, 0);
            models(f).publishInitial(nextKey, ring("orders"), new SourceTable("orders", List.of(new SourceField("id", "int")), List.of(), List.of()), f.captureFence());
            var actual = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(2), "second"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(nextKey, -1)));
            var confirmed = f.chains().checkpoint(CHAIN, 1, actual, f.captureFence()).orElseThrow();
            assertThat(confirmed.resumeOverride()).isNull(); assertThat(confirmed.checkpoint()).isEqualTo(actual);
        });
    }
    private static io.tapstate.spi.store.PipelineIntentScope positionScope(SrsConsumerContext context) {
        return new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(), SCOPE.bindingVersion(), SCOPE.cacheIncarnation(), context.key().pipelineId(), context.stateGeneration());
    }
    private static io.tapstate.spi.store.SourcePositionEdits.Logged positionMove(Fixture f, String token, WorkloadClaimFence capture) {
        return new io.tapstate.spi.store.SourcePositionEdits.Logged(f.chains().load(CHAIN).orElseThrow(),
                Map.of(A, f.consumers().progress(A).orElseThrow(), B, f.consumers().progress(B).orElseThrow()), token, capture);
    }

    private static void withBusiness(Scenario scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("srs_consumer_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.SRS_CHAINS,
                            SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS,
                            SystemCollections.CACHE_WORKLOAD_CLAIMS,SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
                        table.prepareCollectionOn(database, session); for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
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
                WorkloadClaim a = pipeline(claims,"pipeline-a"); WorkloadClaim b = pipeline(claims,"pipeline-b");
                preparePipelineScope(database,"pipeline-a",1); preparePipelineScope(database,"pipeline-b",1);
                CaptureHistoryFixture.shared(database,SCOPE,WorkloadClaimFence.from(capture),CHAIN,"retention");
                var chains = new MongoSrsChainStore(client, database, SCOPE); chains.prepare(CHAIN, "retention", WorkloadClaimFence.from(capture));
                scenario.run(new Fixture(client, database, chains, new MongoSrsConsumerStore(client, database, SCOPE), capture, a, b));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    /** Explicit checkpoint fixtures support these transaction cases; native preparation is tested separately. */
    private static void preparePipelineScope(MongoDatabase database,String pipeline,long generation) {
        var physical = new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),pipeline,generation);
        var header = MongoPipelinePreparationFacts.scope(physical);
        SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id",pipeline).append("scope",header)
                .append("stateJson","NEW").append("epoch",0L).append("touchMillis",0L).append("runtimeWriteFence",1L));
        var key = new WorkloadClaimKey(SCOPE.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,pipeline);
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id",MongoWorkloadClaimStore.id(key)),
                new Document("$set",new Document("pipelineScope",header).append("pipelinePreparation",
                        MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(),io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
    }
    private static void replacePipelineScope(Fixture f,SrsConsumerContext context) {
        var physical = new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),context.key().pipelineId(),context.stateGeneration());
        var header = MongoPipelinePreparationFacts.scope(physical);
        SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id",context.key().pipelineId()),new Document("$set",new Document("scope",header)));
        var key = new WorkloadClaimKey(SCOPE.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,context.key().pipelineId());
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id",MongoWorkloadClaimStore.id(key)),new Document("$set",new Document("pipelineScope",header)));
    }
    private static WorkloadClaim pipeline(MongoWorkloadClaimStore claims, String pipeline) {
        var acquired = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline),
                new WorkloadOwner("node-" + pipeline, "boot-" + pipeline), 1, Duration.ofSeconds(30)).claim();
        return claims.advanceExecution(acquired, 1, Set.of(acquired.owner().nodeId())).orElseThrow();
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoSrsChainStore chains, MongoSrsConsumerStore consumers,
                           WorkloadClaim capture, WorkloadClaim pipelineA, WorkloadClaim pipelineB) {
        WorkloadClaimFence captureFence() { return WorkloadClaimFence.from(capture); }
        WorkloadClaimFence fenceA() { return WorkloadClaimFence.from(pipelineA); }
        WorkloadClaimFence fenceB() { return WorkloadClaimFence.from(pipelineB); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
