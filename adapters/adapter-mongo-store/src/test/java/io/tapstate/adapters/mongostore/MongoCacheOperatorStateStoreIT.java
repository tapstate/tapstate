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
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;

/** Actual transactions bind opaque operator bytes to one physical scope and original execution. */
@RequiresDocker
class MongoCacheOperatorStateStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};

    @Test void losingBothKnownShapeAndColdBytesStillCannotBecomeANewFirstShape() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); f.writer(at).save(at.mapName(),"key",new byte[]{1});
            SystemCollections.CACHE_NEST_STATE.on(f.database).deleteMany(new Document());
            SystemCollections.CACHE_NEST_SHAPES.on(f.database).deleteMany(new Document()); var before = f.snapshot();
            code(() -> f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE)
                    .recordIfAbsent("step",Set.of("replacement")),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void losingTheInventoryCannotBeHiddenByEmptyShapeAndStateCollections() {
        withFixture(f -> {
            SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).deleteMany(new Document());
            SystemCollections.CACHE_NEST_SHAPES.on(f.database).deleteMany(new Document()); var before = f.snapshot();
            var shape = f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
            code(() -> shape.read("step"),IoError.DOCUMENT_UNREADABLE);
            code(() -> shape.recordIfAbsent("step",Set.of("root")),IoError.DOCUMENT_UNREADABLE);
            for (var kind : OperatorPlacement.Resource.values()) {
                var at = f.at(kind); code(() -> f.writer(at).load(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE);
                code(() -> f.writer(at).save(at.mapName(),"key",new byte[]{1}),IoError.DOCUMENT_UNREADABLE);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void damagedInventoryCountersOrLocationsNeverAuthorizeAReplacementRecord() {
        withFixture(f -> {
            var table = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database); var saved = table.find().first();
            var at = f.at(OperatorPlacement.Resource.JOIN_FACTS);
            for (var mutation : List.of(new Document("revision",0L),new Document("preparationId",UUID.randomUUID().toString()),
                    new Document("shapes",List.of(new Document("database",f.database.getName()).append("stepId","bad!"))),
                    new Document("placements",List.of(new Document("database","private-sentinel"))))) {
                table.updateOne(new Document(),new Document("$set",mutation)); var before = f.snapshot();
                code(() -> f.writer(at).save(at.mapName(),"key",new byte[]{1}),IoError.DOCUMENT_UNREADABLE);
                code(() -> f.writer(at).load(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE);
                assertThat(f.snapshot()).isEqualTo(before); table.replaceOne(new Document(),saved);
            }
        });
    }

    @Test void aLegalOlderInventoryCannotMakeUnregisteredColdBytesReadable() {
        withFixture(f -> {
            var table = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database); Document original = table.find().first();
            var at = f.at(OperatorPlacement.Resource.JOIN_FACTS); var writer = f.writer(at); writer.save(at.mapName(),"key",new byte[]{1});
            table.replaceOne(new Document(),original); var before = f.snapshot();
            code(() -> writer.load(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE);
            code(() -> writer.loadAll(at.mapName(),List.of("key")),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aLegalOlderInventoryCannotAdoptUnregisteredColdBytesThroughANewWrite() {
        withFixture(f -> {
            var table = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database); Document original = table.find().first();
            var at = f.at(OperatorPlacement.Resource.JOIN_FACTS); var writer = f.writer(at); writer.save(at.mapName(),"key",new byte[]{1});
            table.replaceOne(new Document(),original); var before = f.snapshot();
            code(() -> writer.save(at.mapName(),"key",new byte[]{2}),IoError.DOCUMENT_UNREADABLE);
            code(() -> writer.saveIfAbsent(at.mapName(),"other",new byte[]{2}),IoError.DOCUMENT_UNREADABLE);
            code(() -> writer.delete(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aMissingShapeMustNotAuthorizeANewInterpretationOfExistingColdBytes() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); f.writer(at).save(at.mapName(),"key",new byte[]{1});
            SystemCollections.CACHE_NEST_SHAPES.on(f.database).deleteMany(new Document()); var before = f.snapshot();
            code(() -> f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE)
                    .recordIfAbsent("step",Set.of("another-path")),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void missingOrIncompatibleShapeRefusesMapStoreReadsAndWrites() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); var writer = f.writer(at);
            SystemCollections.CACHE_NEST_SHAPES.on(f.database).deleteMany(new Document()); var before = f.snapshot();
            code(() -> writer.save(at.mapName(),"key",new byte[]{1}),IoError.DOCUMENT_UNREADABLE);
            code(() -> writer.load(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void shapePublicationRetainsOneWinnerAndRoundTripsAllOpaquePaths() {
        withFixture(f -> {
            var shapes = f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
            Set<String> paths = Set.of("root", "root.items\nfield", new String(new char[]{0xd800}), Character.toString(0xfffd));
            assertThat(shapes.recordIfAbsent("another-step",paths)).isEmpty();
            assertThat(shapes.recordIfAbsent("another-step",Set.of("different"))).hasValue(paths);
            assertThat(shapes.read("another-step")).hasValue(paths);
            assertThat(shapes.recordIfAbsent("empty-step",Set.of())).isEmpty();
            assertThat(shapes.recordIfAbsent("empty-step",Set.of("root"))).hasValue(Set.of());
        });
    }

    @Test void concurrentShapePublicationReturnsTheSameDurableWinnerAcrossClients() {
        withFixture(f -> {
            var gate = new CountDownLatch(1);
            try (var other = MongoCacheDomain.connect(f.binding,Duration.ofSeconds(5),"shape-other"); var workers = Executors.newFixedThreadPool(2)) {
                var left = f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
                var right = other.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
                var one = workers.submit(() -> { await(gate); return left.recordIfAbsent("race",Set.of("one")); });
                var two = workers.submit(() -> { await(gate); return right.recordIfAbsent("race",Set.of("two")); });
                gate.countDown(); var first = one.get(15,TimeUnit.SECONDS); var second = two.get(15,TimeUnit.SECONDS);
                assertThat(List.of(first,second).stream().filter(Optional::isEmpty).count()).isOne();
                var actual = left.read("race").orElseThrow(); assertThat(first.orElse(Set.of("one"))).isEqualTo(actual);
                assertThat(second.orElse(Set.of("two"))).isEqualTo(actual);
            }
        });
    }

    @Test void malformedShapesAndMissingShapeStructureNeverBecomeAnEmptyShape() {
        withFixture(f -> {
            var shapes = f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
            var table = SystemCollections.CACHE_NEST_SHAPES.on(f.database); var saved = table.find().first();
            for (var mutation : List.of(new Document("paths",List.of("bad!")),new Document("paths",List.of("AA")),
                    new Document("paths",List.of(OperatorStateText.encode("root"),OperatorStateText.encode("root"))),
                    new Document("formatVersion",1L),new Document("extra","private-sentinel"))) {
                table.updateOne(new Document(),new Document("$set",mutation));
                code(() -> shapes.read("step"),IoError.DOCUMENT_UNREADABLE);
                code(() -> f.writer(f.at(OperatorPlacement.Resource.NEST_STATE)).load(f.at(OperatorPlacement.Resource.NEST_STATE).mapName(),"key"),IoError.DOCUMENT_UNREADABLE);
                table.replaceOne(new Document(),saved);
            }
            table.drop(); code(() -> shapes.recordIfAbsent("another",Set.of("root")),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("nest_shapes");
        });
    }

    @Test void shapeCandidatesAreCapturedBeforeCallbacksAndOldClaimsCannotPublishAnotherShape() {
        withFixture(f -> {
            var paths = new LinkedHashSet<>(Set.of("root")); var once = new AtomicBoolean();
            var captured = new MongoCacheNestShapeStore(new MongoCachePipelineAccess(f.client,f.database,SCOPE,WorkloadClaimFence.from(f.claim),() -> {
                if (once.compareAndSet(false,true)) paths.add("mutated");
            }),f.database.getName());
            captured.recordIfAbsent("captured",paths);
            assertThat(captured.read("captured")).hasValue(Set.of("root"));
            var old = f.domain.nestShapes(SCOPE,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
            f.domain.workloadClaims(BUSINESS).advanceExecution(f.claim,1,Set.of(OWNER.nodeId())).orElseThrow(); var before = f.snapshot();
            code(() -> old.recordIfAbsent("late",Set.of("root")),IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> f.domain.nestShapes(SCOPE,f.database.getName()).recordIfAbsent("readonly",Set.of("root")),IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void resourceTypesUseIndependentCollectionsAndKeyedBatches() {
        withFixture(f -> {
            for (var kind : OperatorPlacement.Resource.values()) {
                var placement = f.at(kind); var store = f.writer(placement);
                store.save(placement.mapName(), "key", new byte[]{(byte)kind.ordinal()});
                assertThat(store.saveIfAbsent(placement.mapName(), "key", new byte[]{99})).hasValue(new byte[]{(byte)kind.ordinal()});
                assertThat(store.saveIfAbsent(placement.mapName(), "other", new byte[]{42})).isEmpty();
                assertThat(store.loadAll(placement.mapName(), List.of("key", "other", "missing"))).containsOnlyKeys("key", "other");
                assertThat(store.load(placement.mapName(), "key")).hasValue(new byte[]{(byte)kind.ordinal()});
                assertThat(store.count(placement.mapName())).isEqualTo(2);
                store.delete(placement.mapName(), "other"); assertThat(store.count(placement.mapName())).isOne();
                assertThat(MongoCacheOperatorStateStore.table(kind).on(f.database).countDocuments()).isOne();
            }
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("operator_state", "pdk_notes", "nest_dead_letters");
        });
    }

    @Test void originalScopeCanReloadAfterReopenWithoutCreatingOrChangingAnything() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); f.writer(at).save(at.mapName(), "key", new byte[]{1,2}); var before = f.snapshot();
            try (var reopened = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "reopen")) {
                assertThat(reopened.operatorState(at).load(at.mapName(), "key")).hasValue(new byte[]{1,2});
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void readOnlyViewsCannotMutateAndAnActiveWriterCannotPerformRetirement() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.JOIN_FACTS); var store = f.domain.operatorState(at); var before = f.snapshot();
            code(() -> store.save(at.mapName(), "key", new byte[]{1}), IoError.CACHE_BUSINESS_FENCED);
            code(() -> store.saveIfAbsent(at.mapName(), "key", new byte[]{1}), IoError.CACHE_BUSINESS_FENCED);
            code(() -> store.delete(at.mapName(), "key"), IoError.CACHE_BUSINESS_FENCED);
            code(() -> f.writer(at).dropNamespace(at.mapName()), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> store.load("join.orders.step.fact", "key"), IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void inputBytesAreCapturedBeforeAnyGuardCallback() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); byte[] bytes = {1,2}; var once = new AtomicBoolean();
            var writer = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(f.client, f.database, SCOPE, WorkloadClaimFence.from(f.claim), () -> {
                if (once.compareAndSet(false,true)) bytes[0] = 99;
            }), at);
            writer.save(at.mapName(), "key", bytes);
            assertThat(f.writer(at).load(at.mapName(), "key")).hasValue(new byte[]{1,2}); assertThat(bytes[0]).isEqualTo((byte)99);
        });
    }

    @Test void anOldExecutionCannotWriteDeleteOrAllocateWithTheAdvancedClaim() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.JOIN_WRITERS); var old = f.writer(at); old.save(at.mapName(), "key", new byte[]{1});
            var advanced = f.domain.workloadClaims(BUSINESS).advanceExecution(f.claim,1,Set.of(OWNER.nodeId())).orElseThrow(); var before = f.snapshot();
            code(() -> old.save(at.mapName(), "key", new byte[]{2}), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> old.saveIfAbsent(at.mapName(), "new", new byte[]{2}), IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> old.delete(at.mapName(), "key"), IoError.WORKLOAD_CLAIM_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            var current = f.domain.operatorState(at,WorkloadClaimFence.from(advanced),ACTIVE); current.save(at.mapName(), "key", new byte[]{3});
            assertThat(current.load(at.mapName(), "key")).hasValue(new byte[]{3});
        });
    }

    @Test void aNewStableGenerationUsesDifferentHotAndColdAddressesAndDoesNotAdoptOldBytes() {
        withFixture(f -> {
            var oldAt = f.at(OperatorPlacement.Resource.NEST_STATE); var old = f.writer(oldAt); old.save(oldAt.mapName(), "key", new byte[]{1});
            var next = new PipelineIntentScope("cluster-a",1,"cache-a","orders",2);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database).updateOne(new Document("_id","orders"),new Document("$set",new Document("scope",MongoPipelinePreparationFacts.scope(next))));
            f.claims().updateOne(new Document("_id",MongoWorkloadClaimStore.id(KEY)),new Document("$set",new Document("pipelineScope",MongoPipelinePreparationFacts.scope(next))));
            var nextAt = new OperatorPlacement(next,f.database.getName(),oldAt.resource(),oldAt.stepId(),oldAt.pathId(),oldAt.namespace());
            f.genesis(next);
            f.domain.nestShapes(next,f.database.getName(),WorkloadClaimFence.from(f.claim),ACTIVE).recordIfAbsent("step",Set.of("root"));
            var current = f.domain.operatorState(nextAt,WorkloadClaimFence.from(f.claim),ACTIVE);
            assertThat(nextAt.mapName()).isNotEqualTo(oldAt.mapName()); assertThat(current.load(nextAt.mapName(),"key")).isEmpty();
            current.save(nextAt.mapName(),"key",new byte[]{2}); var before = f.snapshot();
            code(() -> old.load(oldAt.mapName(),"key"),IoError.CACHE_BUSINESS_FENCED);
            code(() -> old.save(oldAt.mapName(),"key",new byte[]{99}),IoError.CACHE_BUSINESS_FENCED);
            code(() -> old.delete(oldAt.mapName(),"key"),IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before); assertThat(SystemCollections.CACHE_NEST_STATE.on(f.database).countDocuments()).isEqualTo(2);
            assertThat(current.load(nextAt.mapName(),"key")).hasValue(new byte[]{2});
        });
    }

    @Test void missingCollectionsAndIndexesAreUnknownAndNeverRepaired() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); var table = SystemCollections.CACHE_NEST_STATE.on(f.database);
            table.dropIndex("operator_placement"); var before = f.snapshot();
            code(() -> f.writer(at).load(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE); assertThat(f.snapshot()).isEqualTo(before);
            table.drop(); code(() -> f.writer(at).save(at.mapName(),"key",new byte[]{1}),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("nest_state");
        });
    }

    @Test void damagedOpaqueRecordsAreNotConvertedToAnEmptyMapOrLeakedThroughCauses() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.JOIN_DIMENSIONS); var writer = f.writer(at); writer.save(at.mapName(),"key",new byte[]{1});
            var table = MongoCacheOperatorStateStore.table(at.resource()).on(f.database); var saved = table.find().first();
            for (var mutation : List.of(new Document("formatVersion",1L),new Document("state","private-sentinel"),new Document("state",new Binary((byte)0x80,new byte[]{1})))) {
                table.updateOne(new Document(),new Document("$set",mutation));
                code(() -> writer.load(at.mapName(),"key"),IoError.DOCUMENT_UNREADABLE);
                code(() -> writer.loadAll(at.mapName(),List.of("key")),IoError.DOCUMENT_UNREADABLE);
                table.replaceOne(new Document(),saved);
            }
        });
    }

    @Test void losingTheGuardAfterPayloadWriteRollsBackClaimCheckpointAndPayloadTogether() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.JOIN_INDEXES); var before = f.snapshot(); var lost = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("join_indexes")) lost.set(true);
            }};
            try (var observed = observed(listener)) {
                var writer = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(observed,observed.getDatabase(f.database.getName()),SCOPE,WorkloadClaimFence.from(f.claim),() -> {
                    if (lost.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
                }),at);
                code(() -> writer.save(at.mapName(),"key",new byte[]{1}),IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(lost).isTrue(); assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void aDelayedWriteCannotPassItsOriginalClaimAfterAnotherClientAdvancedTheExecution() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); var pause = new CountDownLatch(1); var resume = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("workload_claims") && once.compareAndSet(false,true)) {
                    pause.countDown(); await(resume);
                }
            }};
            try (var observed = observed(listener); var worker = Executors.newSingleThreadExecutor()) {
                var writer = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(observed,observed.getDatabase(f.database.getName()),SCOPE,WorkloadClaimFence.from(f.claim),ACTIVE),at);
                var future = worker.submit(() -> writer.save(at.mapName(),"key",new byte[]{1}));
                try { assertThat(pause.await(10,TimeUnit.SECONDS)).isTrue(); f.domain.workloadClaims(BUSINESS).advanceExecution(f.claim,1,Set.of(OWNER.nodeId())).orElseThrow(); }
                finally { resume.countDown(); }
                assertThatThrownBy(() -> future.get(15,TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                    .satisfies(failure -> assertThat(((TapstateException)failure.getCause()).code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(SystemCollections.CACHE_NEST_STATE.on(f.database).countDocuments()).isZero();
                assertThat(f.claims().find().first().get("fencedPipelineWrites")).isEqualTo(1L);
            }
        });
    }

    @Test void concurrentSaveIfAbsentReturnsOneDurableWinnerToBothCallers() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.JOIN_WRITERS); var gate = new CountDownLatch(1);
            try (var other = MongoCacheDomain.connect(f.binding,Duration.ofSeconds(5),"other"); var workers = Executors.newFixedThreadPool(2)) {
                var one = workers.submit(() -> { await(gate); return f.writer(at).saveIfAbsent(at.mapName(),"key",new byte[]{1}); });
                var two = workers.submit(() -> { await(gate); return other.operatorState(at,WorkloadClaimFence.from(f.claim),ACTIVE).saveIfAbsent(at.mapName(),"key",new byte[]{2}); });
                gate.countDown(); var first = one.get(15,TimeUnit.SECONDS); var second = two.get(15,TimeUnit.SECONDS);
                assertThat(List.of(first,second).stream().filter(Optional::isEmpty).count()).isOne();
                byte[] actual = f.writer(at).load(at.mapName(),"key").orElseThrow();
                assertThat(first.orElse(new byte[]{1})).isEqualTo(actual); assertThat(second.orElse(new byte[]{2})).isEqualTo(actual);
                assertThat(f.writer(at).count(at.mapName())).isOne();
            }
        });
    }

    @Test void aMalformedUtf16KeyCannotOverwriteTheReplacementCharacterKey() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE); var store = f.writer(at);
            String replacement = Character.toString(0xfffd); String unpaired = new String(new char[]{0xd800});
            store.save(at.mapName(),replacement,new byte[]{1}); store.save(at.mapName(),unpaired,new byte[]{2});
            assertThat(store.load(at.mapName(),replacement)).hasValue(new byte[]{1});
            assertThat(store.load(at.mapName(),unpaired)).hasValue(new byte[]{2});
            assertThat(store.loadAll(at.mapName(),List.of(replacement,unpaired))).hasSize(2);
        });
    }

    @Test void malformedUtf16PlacementTextCannotAliasAnotherOperatorsColdAddress() {
        withFixture(f -> {
            var original = f.at(OperatorPlacement.Resource.NEST_STATE);
            var replacement = new OperatorPlacement(SCOPE,f.database.getName(),original.resource(),"step","root",Character.toString(0xfffd));
            var unpaired = new OperatorPlacement(SCOPE,f.database.getName(),original.resource(),"step","root",new String(new char[]{0xd800}));
            f.writer(replacement).save(replacement.mapName(),"key",new byte[]{1}); f.writer(unpaired).save(unpaired.mapName(),"key",new byte[]{2});
            assertThat(f.writer(replacement).load(replacement.mapName(),"key")).hasValue(new byte[]{1});
            assertThat(f.writer(unpaired).load(unpaired.mapName(),"key")).hasValue(new byte[]{2});
            assertThat(SystemCollections.CACHE_NEST_STATE.on(f.database).countDocuments()).isEqualTo(2);
        });
    }

    @Test void aPreparedExtraDatabaseHoldsNestStateAndShapeWhileControlStaysInTheDefaultCache() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            bound.prepareOperatorDatabase(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),UUID.randomUUID().toString(),ACTIVE);
            var shape = bound.nestShapes(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),ACTIVE);
            assertThat(shape.recordIfAbsent("extra-step",Set.of("root"))).isEmpty();
            var at = new OperatorPlacement(SCOPE,target.getName(),OperatorPlacement.Resource.NEST_STATE,"extra-step","root","extra-logical");
            var state = bound.operatorState(at,WorkloadClaimFence.from(f.claim),ACTIVE);
            state.save(at.mapName(),"key",new byte[]{1,2,3});
            assertThat(state.load(at.mapName(),"key")).hasValue(new byte[]{1,2,3});
            assertThat(SystemCollections.CACHE_NEST_STATE.on(target).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CACHE_NEST_SHAPES.on(target).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CACHE_NEST_STATE.on(f.database).countDocuments()).isZero();
            assertThat(target.listCollectionNames()).containsExactlyInAnyOrder("cache_identity","schema_migrations","nest_state","nest_shapes");
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first();
            assertThat(inventory.getList("shapes",Document.class)).contains(new Document("database",target.getName()).append("stepId",OperatorStateText.encode("extra-step")));
            assertThat(inventory.getList("placements",Document.class)).contains(MongoOperatorInventory.location(at));
            assertThatThrownBy(() -> bound.operatorState(new OperatorPlacement(SCOPE,target.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","join-logical")))
                    .isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
        }));
    }
    @Test void ordinaryExtraDatabaseReadsCannotPrepareAnUnknownTarget() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            var at = new OperatorPlacement(SCOPE,target.getName(),OperatorPlacement.Resource.NEST_STATE,"extra-step","root","extra-logical");
            var state = bound.operatorState(at,WorkloadClaimFence.from(f.claim),ACTIVE);
            assertThatThrownBy(() -> state.load(at.mapName(),"key")).isInstanceOf(TapstateException.class);
            assertThat(target.listCollectionNames()).isEmpty();
            assertThatThrownBy(() -> bound.nestShapes(SCOPE,target.getName()).read("extra-step")).isInstanceOf(TapstateException.class);
            assertThat(target.listCollectionNames()).isEmpty();
        }));
    }
    @Test void theSameStateGenerationCannotMoveAnExistingNestStepToAnotherDatabase() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            bound.prepareOperatorDatabase(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),UUID.randomUUID().toString(),ACTIVE);
            var before = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first();
            code(() -> bound.nestShapes(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),ACTIVE).recordIfAbsent("step",Set.of("root")),IoError.CACHE_BUSINESS_FENCED);
            assertThat(SystemCollections.CACHE_NEST_SHAPES.on(target).countDocuments()).isZero();
            assertThat(SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first()).isEqualTo(before);
        }));
    }
    @Test void aReleasedOriginalClaimCannotWriteThePreparedExtraDatabase() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            var fence = WorkloadClaimFence.from(f.claim);
            bound.prepareOperatorDatabase(SCOPE,target.getName(),fence,UUID.randomUUID().toString(),ACTIVE);
            bound.nestShapes(SCOPE,target.getName(),fence,ACTIVE).recordIfAbsent("extra-step",Set.of("root"));
            var at = new OperatorPlacement(SCOPE,target.getName(),OperatorPlacement.Resource.NEST_STATE,"extra-step","root","extra-logical");
            var writer = bound.operatorState(at,fence,ACTIVE); writer.save(at.mapName(),"kept",new byte[]{1});
            assertThat(f.domain.workloadClaims(BUSINESS).release(f.claim)).isTrue();
            var before = SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first();
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first();
            code(() -> writer.save(at.mapName(),"late",new byte[]{2}),IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(SystemCollections.CACHE_NEST_STATE.on(target).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first()).isEqualTo(before);
            assertThat(SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first()).isEqualTo(inventory);
        }));
    }
    @Test void anOccupiedUnknownTargetCannotBeAdoptedOrPartlyPrepareTheControlState() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            try (var session = f.client.startSession()) {
                session.withTransaction(() -> { SystemCollections.CACHE_NEST_STATE.prepareCollectionOn(target,session); return null; },
                        TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
            }
            var before = SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first();
            var claim = f.claims().find().first();
            assertThatThrownBy(() -> bound.prepareOperatorDatabase(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),UUID.randomUUID().toString(),ACTIVE))
                    .isInstanceOf(TapstateException.class);
            assertThat(target.listCollectionNames()).containsExactly("nest_state");
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first()).isEqualTo(before);
            assertThat(f.claims().find().first()).isEqualTo(claim);
        }));
    }
    @Test void trustedConfigurationAndMasterNamesAndSystemDatabasesAreRejectedWithoutRawCauses() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            var before = SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first();
            for (String name : List.of("admin","config","local","configuration","master","bad/private-sentinel")) {
                assertThatThrownBy(() -> bound.prepareOperatorDatabase(SCOPE,name,WorkloadClaimFence.from(f.claim),UUID.randomUUID().toString(),ACTIVE))
                        .isInstanceOfSatisfying(TapstateException.class,error -> {
                            assertThat(error.code()).isEqualTo(StoreError.INVALID_OPERATOR_STATE_DATABASE); assertThat(error.getCause()).isNull();
                            assertThat(error.args().toString()).doesNotContain("private-sentinel");
                        });
            }
            assertThat(target.listCollectionNames()).isEmpty();
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first()).isEqualTo(before);
        }));
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void aKnownExtraDatabaseCannotBeRecreatedAfterTotalLossEvenBeforeItsFirstStateWrite(boolean used) {
        withFixture(f -> withExtra(f,(bound,target) -> {
            var fence = WorkloadClaimFence.from(f.claim); String operation = UUID.randomUUID().toString();
            bound.prepareOperatorDatabase(SCOPE,target.getName(),fence,operation,ACTIVE);
            if (used) {
                bound.nestShapes(SCOPE,target.getName(),fence,ACTIVE).recordIfAbsent("extra-step",Set.of("root"));
                var at = new OperatorPlacement(SCOPE,target.getName(),OperatorPlacement.Resource.NEST_STATE,"extra-step","root","extra-logical");
                bound.operatorState(at,fence,ACTIVE).save(at.mapName(),"key",new byte[]{1});
            }
            target.drop(); var before = f.snapshot();
            code(() -> bound.prepareOperatorDatabase(SCOPE,target.getName(),fence,operation,ACTIVE),IoError.DOCUMENT_UNREADABLE);
            assertThat(target.listCollectionNames()).isEmpty(); assertThat(f.snapshot()).isEqualTo(before);
        }));
    }
    @Test void anotherPipelineCannotAdoptAChildPreparationThatContradictsRetainedHistory() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            bound.prepareOperatorDatabase(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),UUID.randomUUID().toString(),ACTIVE);
            var sibling = new PipelineIntentScope(BUSINESS.clusterId(),BUSINESS.bindingVersion(),BUSINESS.cacheIncarnation(),"billing",1);
            var fence = f.sibling(sibling);
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(target).updateOne(new Document("_id","operator"),
                    new Document("$set",new Document("operationId",UUID.randomUUID().toString())));
            var before = f.snapshot();
            code(() -> bound.prepareOperatorDatabase(sibling,target.getName(),fence,UUID.randomUUID().toString(),ACTIVE),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        }));
    }
    @Test void aSiblingUsesTheOriginalChildPreparationWithoutDependingOnTheOtherPipelinesLiveClaim() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            String first = UUID.randomUUID().toString();
            bound.prepareOperatorDatabase(SCOPE,target.getName(),WorkloadClaimFence.from(f.claim),first,ACTIVE);
            var sibling = new PipelineIntentScope(BUSINESS.clusterId(),BUSINESS.bindingVersion(),BUSINESS.cacheIncarnation(),"billing",1);
            var fence = f.sibling(sibling);
            bound.prepareOperatorDatabase(sibling,target.getName(),fence,UUID.randomUUID().toString(),ACTIVE);
            f.claims().deleteOne(new Document("_id",MongoWorkloadClaimStore.id(KEY)));
            bound.prepareOperatorDatabase(sibling,target.getName(),fence,UUID.randomUUID().toString(),ACTIVE);
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(target).find().first()).containsEntry("operationId",first);
            var own = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find(new Document("_id",MongoPipelinePreparationFacts.scope(sibling))).first();
            assertThat(own.getList("databases",Document.class)).containsExactly(new Document("database",target.getName()).append("operationId",first));
        }));
    }
    @Test void defaultDatabasePreparationVerifiesExistingSchemaWithoutOperatorIdentityOrRegistration() {
        withFixture(f -> {
            var fence = WorkloadClaimFence.from(f.claim);
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first();
            f.domain.prepareOperatorDatabase(SCOPE,f.database.getName(),fence,UUID.randomUUID().toString(),ACTIVE);
            assertThat(SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.database).find().first()).isEqualTo(inventory);
            assertThat(SystemCollections.CACHE_IDENTITY.on(f.database).countDocuments(new Document("_id","operator"))).isZero();
            assertThat(f.domain.workloadClaims(BUSINESS).release(f.claim)).isTrue(); var before = f.snapshot();
            code(() -> f.domain.prepareOperatorDatabase(SCOPE,f.database.getName(),fence,UUID.randomUUID().toString(),ACTIVE),IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void crossDatabasePayloadWriteRollsBackWithDefaultControlAfterTheGuardIsLost() {
        withFixture(f -> withExtra(f,(bound,target) -> {
            var fence = WorkloadClaimFence.from(f.claim);
            bound.prepareOperatorDatabase(SCOPE,target.getName(),fence,UUID.randomUUID().toString(),ACTIVE);
            bound.nestShapes(SCOPE,target.getName(),fence,ACTIVE).recordIfAbsent("extra-step",Set.of("root"));
            var at = new OperatorPlacement(SCOPE,target.getName(),OperatorPlacement.Resource.NEST_STATE,"extra-step","root","logical");
            var before = f.snapshot(); var lost = new AtomicBoolean(); var writing = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getDatabaseName().equals(target.getName()) && event.getCommandName().equals("update")
                            && event.getCommand().getString("update").getValue().equals("nest_state")) writing.set(true);
                }
                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if (event.getCommandName().equals("update") && writing.compareAndSet(true,false)) lost.set(true);
                }
            };
            var policy = new OperatorDatabaseBinding(BUSINESS,f.database.getName(),"configuration","master");
            try (var client = observed(listener)) {
                var writer = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(client,client.getDatabase(f.database.getName()),
                        SCOPE,fence,() -> { if (lost.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); },
                        MongoRateHistoryStore.DEFAULT_RETENTION,policy),at);
                code(() -> writer.save(at.mapName(),"late",new byte[]{1}),IoError.WORKLOAD_CLAIM_FENCED);
            }
            assertThat(lost).isTrue(); assertThat(f.snapshot()).isEqualTo(before);
            assertThat(SystemCollections.CACHE_NEST_STATE.on(target).countDocuments()).isZero();
        }));
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"cache_identity","schema_migrations","nest_state","nest_shapes"})
    void ordinaryAccessAndPreparationCannotRepairMissingExtraDatabaseStructure(String missing) {
        withFixture(f -> withExtra(f,(bound,target) -> {
            var fence = WorkloadClaimFence.from(f.claim); String operation = UUID.randomUUID().toString();
            bound.prepareOperatorDatabase(SCOPE,target.getName(),fence,operation,ACTIVE);
            target.getCollection(missing).drop(); var names = new HashSet<>(target.listCollectionNames().into(new ArrayList<>()));
            var before = f.snapshot();
            code(() -> bound.nestShapes(SCOPE,target.getName()).read("extra-step"),IoError.DOCUMENT_UNREADABLE);
            code(() -> bound.prepareOperatorDatabase(SCOPE,target.getName(),fence,operation,ACTIVE),IoError.DOCUMENT_UNREADABLE);
            assertThat(target.listCollectionNames()).containsExactlyInAnyOrderElementsOf(names);
            assertThat(f.snapshot()).isEqualTo(before);
        }));
    }

    private static void withExtra(Fixture fixture,ExtraScenario scenario) throws Exception {
        String name = fixture.database.getName()+"_nest";
        var policy = new io.tapstate.spi.store.OperatorDatabaseBinding(BUSINESS,fixture.database.getName(),"configuration","master");
        try (var bound = MongoCacheDomain.connect(fixture.binding,Duration.ofSeconds(5),"extra-operator-it",MongoRateHistoryStore.DEFAULT_RETENTION,policy)) {
            scenario.run(bound,fixture.client.getDatabase(name));
        } finally { fixture.client.getDatabase(name).drop(); }
    }
    @FunctionalInterface private interface ExtraScenario { void run(MongoCacheDomain domain,com.mongodb.client.MongoDatabase target) throws Exception; }

    @Test void onlyTheCurrentCacheDatabaseIsAvailableUntilAnExtraPlacementHasBeenPrepared() {
        withFixture(f -> {
            var at = f.at(OperatorPlacement.Resource.NEST_STATE);
            for (String database : List.of("admin","config","local","configuration","master","other")) {
                var foreign = new OperatorPlacement(SCOPE,database,at.resource(),at.stepId(),at.pathId(),at.namespace());
                code(() -> f.domain.operatorState(foreign),IoError.CACHE_BUSINESS_FENCED);
            }
        });
    }

    private static MongoClient observed(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15,TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); } }
    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,e -> {
            assertThat(e.code()).isEqualTo(code); assertThat(e.getCause()).isNull(); assertThat(e.args().toString()).doesNotContain("private-sentinel");
        });
    }
    private static void withFixture(Scenario scenario) {
        String name = "cache_operator_"+UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(name),name,null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding,Duration.ofSeconds(5),"operator-it")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(new CacheAuthScope("cluster-a",1,StorageBinding.Mode.OP,"cache-a",1),"auth-a",ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database,MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session,ACTIVE); return null;
                },TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id","cluster"),new Document("$set",new Document("businessPhase","READY")
                    .append("businessPreparationId","business-a").append("claimAdmissionFence",0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id","business").append("installedVersion",1)
                    .append("phase","READY").append("epoch",1L).append("operationId","business-a"));
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id","orders").append("scope",MongoPipelinePreparationFacts.scope(SCOPE))
                    .append("stateJson","NEW").append("epoch",0L).append("touchMillis",0L).append("runtimeWriteFence",1L));
                var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var retained = raw.acquire(KEY,OWNER,1,Duration.ofMinutes(5)).claim(); raw.release(retained);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id",MongoWorkloadClaimStore.id(KEY)),new Document("$set",
                    new Document("pipelineScope",MongoPipelinePreparationFacts.scope(SCOPE)).append("pipelinePreparation",
                        MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(),PipelineCachePreparations.Phase.PREPARED))));
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    String preparationId = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session).first().get("pipelinePreparation",Document.class).getString("preparationId");
                    MongoOperatorInventory.prepareInitial(session,database,SCOPE,preparationId); return null;
                }); }
                var claim = domain.workloadClaims(BUSINESS).acquirePipeline(SCOPE,OWNER,1,Duration.ofMinutes(5)).claim();
                claim = domain.workloadClaims(BUSINESS).advanceExecution(claim,1,Set.of(OWNER.nodeId())).orElseThrow();
                domain.nestShapes(SCOPE,name,WorkloadClaimFence.from(claim),ACTIVE).recordIfAbsent("step",Set.of("root"));
                scenario.run(new Fixture(client,database,domain,binding,claim));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; } catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding, WorkloadClaim claim) {
        OperatorPlacement at(OperatorPlacement.Resource kind) { return new OperatorPlacement(SCOPE,database.getName(),kind,"step","root","logical.namespace"); }
        KeyedStateStore writer(OperatorPlacement at) { return domain.operatorState(at,WorkloadClaimFence.from(claim),ACTIVE); }
        com.mongodb.client.MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        WorkloadClaimFence sibling(PipelineIntentScope scope) {
            SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id",scope.pipelineId()).append("scope",MongoPipelinePreparationFacts.scope(scope))
                    .append("stateJson","NEW").append("epoch",0L).append("touchMillis",0L).append("runtimeWriteFence",1L));
            var key = new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId());
            var raw = new MongoWorkloadClaimStore(claims()); var initial = raw.acquire(key,OWNER,1,Duration.ofMinutes(5)).claim(); raw.release(initial);
            String preparation = UUID.randomUUID().toString();
            claims().updateOne(new Document("_id",MongoWorkloadClaimStore.id(key)),new Document("$set",new Document("pipelineScope",MongoPipelinePreparationFacts.scope(scope))
                    .append("pipelinePreparation",MongoCachePipelinePreparations.marker(preparation,PipelineCachePreparations.Phase.PREPARED))));
            try (var session = client.startSession()) { session.withTransaction(() -> { MongoOperatorInventory.prepareInitial(session,database,scope,preparation); return null; }); }
            var original = domain.workloadClaims(BUSINESS).acquirePipeline(scope,OWNER,1,Duration.ofMinutes(5)).claim();
            return WorkloadClaimFence.from(domain.workloadClaims(BUSINESS).advanceExecution(original,1,Set.of(OWNER.nodeId())).orElseThrow());
        }
        void genesis(PipelineIntentScope scope) {
            try (var session = client.startSession()) { session.withTransaction(() -> {
                String id = claims().find(session).first().get("pipelinePreparation",Document.class).getString("preparationId");
                MongoOperatorInventory.prepareInitial(session,database,scope,id); return null;
            }); }
        }
        Map<String,List<Document>> snapshot() {
            var out = new TreeMap<String,List<Document>>();
            for (var kind : OperatorPlacement.Resource.values()) { var table = MongoCacheOperatorStateStore.table(kind); out.put(table.collectionName(),table.on(database).find().into(new ArrayList<>())); }
            for (var table : List.of(SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.CACHE_WORKLOAD_CLAIMS,SystemCollections.CACHE_NEST_SHAPES,SystemCollections.CACHE_OPERATOR_CLEANUP)) out.put(table.collectionName(),table.on(database).find().into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture f) throws Exception; }
}
