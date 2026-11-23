package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.PreparedTargetModel;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Independent native clients compete on the original target fact, claim and checkpoint. */
@RequiresDocker
class MongoSinkPreparationStoreIT {
    @Container private static final MongoDBContainer MONGO = ownedMongo();
    private static MongoDBContainer ownedMongo() {
        var mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");String owner=System.getProperty("tapstate.e2e.resource-owner");
        if(owner!=null && !owner.isBlank())mongo.withLabel(System.getProperty("tapstate.e2e.resource-owner-label","io.tapstate.e2e.resource-owner"),owner)
                .withTmpFs(Map.of("/data/db","rw,size=1g","/data/configdb","rw,size=16m"));return mongo;
    }
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofMinutes(2);
    private static final Instant AT = Instant.parse("2026-10-19T03:00:00Z");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a","boot-a");

    @Test void aSingleReadyReceiptAndItsAuthorityUseOneSnapshotCommand() {
        withFixture(f -> {
            var input = f.input("orders");
            var control = f.store();
            control.prepareTargets(List.of(input));
            var ready = control.complete(control.begin(input), new SinkPreparationStore.Result(true));
            var commands = new ArrayList<org.bson.BsonDocument>();
            var settings = com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(new com.mongodb.event.CommandListener() {
                        @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                            commands.add(event.getCommand().clone());
                        }
                    }).build();
            try (var client = MongoClients.create(settings)) {
                client.getDatabase("admin").runCommand(new Document("ping",1));
                var access = new MongoCachePipelineAccess(client,client.getDatabase(f.cache.getName()),f.scope,f.fence,ACTIVE);
                var store = new MongoSinkPreparationStore(access);
                commands.clear();
                assertThat(store.load(input)).isEqualTo(ready);
                assertThat(store.loadMany(List.of(input))).containsExactly(ready);
                assertThat(commands).hasSize(2).allSatisfy(command -> {
                    assertThat(command.getString("aggregate").getValue()).isEqualTo("cache_identity");
                    assertThat(command.getDocument("readConcern").getString("level").getValue()).isEqualTo("snapshot");
                    assertThat(command.containsKey("txnNumber")).isFalse();
                });
                System.out.println("SINK_RECEIPT single=1 single_target_roster=1");
            }
        });
    }

    @Test void readyRetainsTheKnownCreateResultAcrossExecutionAndIndependentClientRestart() {
        withFixture(f -> {
            var input=f.input("orders");var store=f.store();store.prepareTargets(List.of(input));
            assertThat(store.load(input).revision()).isZero();var attempt=store.begin(input);
            assertThat(attempt.original().originalClaim()).isEqualTo(f.fence);
            var ready=store.complete(attempt,new SinkPreparationStore.Result(true));
            assertThat(store.complete(attempt,new SinkPreparationStore.Result(true))).isEqualTo(ready);
            assertCode(() -> store.complete(attempt,new SinkPreparationStore.Result(false)),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            var next=f.domain.workloadClaims(f.business).advanceExecution(f.domain.workloadClaims(f.business).readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow();
            try(var client=MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened=new MongoSinkPreparationStore(new MongoCachePipelineAccess(client,client.getDatabase(f.cache.getName()),f.scope,WorkloadClaimFence.from(next),ACTIVE));
                reopened.prepareTargets(List.of(input));assertThat(reopened.load(input)).isEqualTo(ready);
                assertCode(() -> reopened.begin(input),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            }
            assertThat(f.rows().countDocuments()).isEqualTo(1);
        });
    }
    @Test void independentClientsIssueOnlyOneTargetCall() {
        withFixture(f -> {
            var input=f.input("orders");f.store().prepareTargets(List.of(input));
            try(var client=MongoClients.create(MONGO.getReplicaSetUrl());var pool=Executors.newFixedThreadPool(2)) {
                var second=new MongoSinkPreparationStore(new MongoCachePipelineAccess(client,client.getDatabase(f.cache.getName()),f.scope,f.fence,ACTIVE));
                var latch=new CountDownLatch(1);var issued=new AtomicInteger();var refused=new AtomicInteger();
                List<Future<?>> results=new ArrayList<>();
                for(var store:List.of(f.store(),second)) results.add(pool.submit(() -> {try{latch.await();store.begin(input);issued.incrementAndGet();}catch(TapstateException e){assertThat(e.code()).isEqualTo(IoError.PIPELINE_STATE_REBUILD_REQUIRED);refused.incrementAndGet();}catch(InterruptedException e){throw new AssertionError(e);}}));
                latch.countDown();for(var result:results)result.get(20,TimeUnit.SECONDS);
                assertThat(issued).hasValue(1);assertThat(refused).hasValue(1);assertThat(f.store().load(input).revision()).isEqualTo(1);
            }
        });
    }

    @Test void batchReadsOnlyTheRequestedOriginalReceipts() {
        withFixture(f -> {
            var orders = f.input("orders");
            var customers = f.input("customers");
            var store = f.store();
            store.prepareTargets(List.of(orders, customers));
            var ready = store.complete(store.begin(orders), new SinkPreparationStore.Result(true));
            assertThat(store.loadMany(List.of(customers, orders))).containsExactly(store.load(customers), ready);
            assertCode(() -> store.loadMany(List.of(orders, f.input("absent"))), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        });
    }
    @Test void issuedUnknownResultNeverBecomesANewClearPermissionAfterRestart() {
        withFixture(f -> {
            var input=f.input("orders");var original=f.store();original.prepareTargets(List.of(input));original.begin(input);
            var reopened=f.store();reopened.prepareTargets(List.of(input));var held=f.snapshot();
            assertCode(() -> reopened.begin(input),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(held);
        });
    }
    @Test void lateResultCannotCompleteAfterOriginalClaimAdvances() {
        withFixture(f -> {
            var input=f.input("orders");var store=f.store();store.prepareTargets(List.of(input));var attempt=store.begin(input);
            var current=f.domain.workloadClaims(f.business).readPipeline(f.scope).orElseThrow().claim();
            f.domain.workloadClaims(f.business).advanceExecution(current,1,Set.of(OWNER.nodeId())).orElseThrow();var before=f.snapshot();
            assertCode(() -> store.complete(attempt,new SinkPreparationStore.Result(false)),IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void fabricatedAttemptIsRefusedBeforeAnyAccessorRuns() {
        withFixture(f -> {
            var calls=new AtomicInteger();SinkPreparationStore.Attempt fake=() -> {calls.incrementAndGet();return null;};
            assertCode(() -> f.store().complete(fake,new SinkPreparationStore.Result(false)),IoError.WORKLOAD_CLAIM_FENCED);assertThat(calls).hasValue(0);
        });
    }
    @Test void missingExpectedRowIsNeverReservedAgain() {
        withFixture(f -> {
            var input=f.input("orders");var store=f.store();store.prepareTargets(List.of(input));f.rows().deleteMany(new Document());var before=f.snapshot();
            assertCode(() -> store.prepareTargets(List.of(input)),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertCode(() -> store.load(input),IoError.DOCUMENT_UNREADABLE);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"missing","version","operation","targets","unknown"})
    void damagedOriginalManifestDoesNotAuthorizeAFirstTarget(String damage) {
        withFixture(f -> {
            var update=switch(damage){case "missing" -> new Document("$unset",new Document("sinkPreparations",1));case "version" -> new Document("$set",new Document("sinkPreparations.formatVersion",1L));case "operation" -> new Document("$set",new Document("sinkPreparations.preparationId",UUID.randomUUID().toString()));case "targets" -> new Document("$set",new Document("sinkPreparations.targets","unknown"));default -> new Document("$set",new Document("sinkPreparations.unexpected",true));};
            f.claims().updateOne(new Document("_id.resourceId","flow"),update);var before=f.snapshot();
            assertThatThrownBy(() -> f.store().prepareTargets(List.of(f.input("orders")))).isInstanceOf(TapstateException.class);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"model","policy","mode","roster"})
    void changingOriginalTargetInputsRequiresExplicitFull(String change) {
        withFixture(f -> {
            var input=f.input("orders");var store=f.store();store.prepareTargets(List.of(input));
            var changed=switch(change){case "model" -> new SinkPreparationStore.Input(input.key(),"b".repeat(64),input.policy(),true);case "policy" -> new SinkPreparationStore.Input(input.key(),input.modelHash(),OnFullLoad.APPEND,true);case "mode" -> new SinkPreparationStore.Input(input.key(),input.modelHash(),input.policy(),false);default -> f.input("another");};
            var before=f.snapshot();assertCode(() -> store.prepareTargets(List.of(changed)),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void unlistedAndDifferentlyOrderedDuplicateScopeRowsRefuseManifestReplay() {
        withFixture(f -> {
            var input=f.input("orders");var store=f.store();store.prepareTargets(List.of(input));var extra=f.rows().find().first();
            var key=extra.get("_id",Document.class);var scope=key.get("scope",Document.class);var reversed=new Document();
            var keys=new ArrayList<>(scope.keySet());Collections.reverse(keys);for(var field:keys)reversed.append(field,scope.get(field));key.put("scope",reversed);f.rows().insertOne(extra);
            var before=f.snapshot();assertCode(() -> store.prepareTargets(List.of(input)),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void purposeAndLosslessNodeTextRemainDistinct() {
        withFixture(f -> {
            var left=f.input("orders");var right=new SinkPreparationStore.Input(new SinkPreparationStore.Key(f.scope,PipelineNode.Purpose.VIEW,"sink","orders"),left.modelHash(),left.policy(),true);
            var unusual=new SinkPreparationStore.Input(new SinkPreparationStore.Key(f.scope,PipelineNode.Purpose.SYNC,"sink\ud800","orders\udc00"),left.modelHash(),left.policy(),true);
            var store=f.store();store.prepareTargets(List.of(right,unusual,left));
            assertThat(f.rows().countDocuments()).isEqualTo(3);assertThat(store.load(unusual).input()).isEqualTo(unusual);
            store.complete(store.begin(left),new SinkPreparationStore.Result(false));assertThat(store.load(right).revision()).isZero();
        });
    }
    @ParameterizedTest @ValueSource(strings={"table","index"})
    void ordinaryOpenDoesNotRepairMissingStructure(String damage) {
        withFixture(f -> {
            if(damage.equals("table"))f.rows().drop();else f.rows().dropIndex("sink_preparation_scope");
            assertThatThrownBy(() -> f.store().prepareTargets(List.of(f.input("orders")))).isInstanceOf(TapstateException.class);
            if(damage.equals("table")) assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).doesNotContain("sink_preparations");
            else assertThat(f.rows().listIndexes().into(new ArrayList<>())).noneMatch(index -> "sink_preparation_scope".equals(index.get("name")));
        });
    }
    @Test void guardLossAfterActualReservationRollsBackReceiptClaimAndCheckpoint() {
        withFixture(f -> {
            var before=f.snapshot();
            try(var witness=new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.cache.getName(),"insert","sink_preparations")) {
                var store=new MongoSinkPreparationStore(new MongoCachePipelineAccess(witness.client,witness.database,f.scope,f.fence,witness::refuseAfterCommand));
                assertCode(() -> store.prepareTargets(List.of(f.input("orders"))),IoError.CACHE_BUSINESS_FENCED);witness.requireCommand();
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void guardLossAfterActualReadyWriteRollsBackOriginalAttempt() {
        withFixture(f -> {
            var input=f.input("orders");f.store().prepareTargets(List.of(input));
            var baseline=new AtomicInteger(Integer.MAX_VALUE);
            try(var witness=new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.cache.getName(),"update","sink_preparations")) {
                var store=new MongoSinkPreparationStore(new MongoCachePipelineAccess(witness.client,witness.database,f.scope,f.fence,() -> witness.refuseAfterCommand(baseline.get())));
                var attempt=store.begin(input);var before=f.snapshot();baseline.set(witness.confirmedCommands());
                assertCode(() -> store.complete(attempt,new SinkPreparationStore.Result(false)),IoError.CACHE_BUSINESS_FENCED);assertThat(witness.confirmedCommands()).isGreaterThan(baseline.get());assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }
    @Test void frozenV2BodySurvivesReadyAndIndependentRestartWithChangedSampling() {
        withFixture(f -> {
            var original=prepared(f,"orders","STRING(100)");var store=f.store();var inputs=authoring(f);
            store.prepareModels(List.of(original));var attempt=store.begin(original.input());store.complete(attempt,new SinkPreparationStore.Result(true));
            var row=f.rows().find().first();assertThat(row.getInteger("formatVersion")).isEqualTo(2);
            assertThat(store.hasPreparedModels()).isTrue();
            try(var client=MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened=new MongoSinkPreparationStore(new MongoCachePipelineAccess(client,client.getDatabase(f.cache.getName()),f.scope,f.fence,ACTIVE));
                var changed=prepared(f,"orders","STRING(20480)");
                assertThat(reopened.resolveModels(List.of(changed),inputs)).containsExactly(original);
                reopened.prepareModels(List.of(original));assertThat(f.rows().find().first()).isEqualTo(row);
                assertCode(() -> reopened.begin(original.input()),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                var altered=new PipelinePreparationInputs("c".repeat(64),inputs.sourceHashes(),inputs.selectedTables(),inputs.directChannels());
                assertCode(() -> reopened.resolveModels(List.of(changed),altered),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            }
        });
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void exactLegacyBackfillPreservesReadyOrUnknownOutcomeAndOriginalHash(boolean ready) {
        withFixture(f -> {
            var original=prepared(f,"orders","STRING(100)");var store=f.store();store.prepareTargets(List.of(original.input()));
            var attempt=store.begin(original.input());if(ready)store.complete(attempt,new SinkPreparationStore.Result(false));
            var before=store.load(original.input());var old=f.rows().find().first();
            assertThat(store.retainedModels(List.of(original.input().key()),OnFullLoad.CLEAR,true,
                    new PipelinePreparationInputs("a".repeat(64),Map.of(),Map.of(),List.of()))).isEmpty();
            assertThat(store.resolveModels(List.of(original),null)).containsExactly(original);
            store.prepareModels(List.of(original));var upgraded=f.rows().find().first();
            assertThat(upgraded.getInteger("formatVersion")).isEqualTo(2);
            assertThat(store.load(original.input())).isEqualTo(before);
            for(String field:List.of("_id","input","operationId","revision","phase","originalClaim","result"))assertThat(upgraded).containsEntry(field,old.get(field));
            assertCode(() -> store.begin(original.input()),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        });
    }
    @Test void unmatchedLegacyAndKnownMissingBodiesCannotBeBackfilledOrReplaced() {
        withFixture(f -> {
            var original=prepared(f,"orders","STRING(100)");var store=f.store();store.prepareTargets(List.of(original.input()));
            var before=f.snapshot();var changed=prepared(f,"orders","STRING(20480)");
            assertCode(() -> store.resolveModels(List.of(changed),null),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertCode(() -> store.prepareModels(List.of(changed)),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(before);
            store.prepareModels(List.of(original));var row=f.rows().find().first();row.remove("targetModel");f.rows().replaceOne(new Document("_id",row.get("_id")),row);
            var damaged=f.snapshot();assertCode(() -> store.prepareModels(List.of(original)),IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> store.resolveModels(List.of(original),null),IoError.DOCUMENT_UNREADABLE);assertThat(f.snapshot()).isEqualTo(damaged);
        });
    }
    @ParameterizedTest @ValueSource(strings={"hash","subtype","body-version","row-version","extra"})
    void malformedV2RefusesWithoutOverwritingOriginalFacts(String damage) {
        withFixture(f -> {
            var original=prepared(f,"orders","STRING(100)");var store=f.store();store.prepareModels(List.of(original));
            var row=f.rows().find().first();var body=row.get("targetModel",Document.class);var binary=(org.bson.types.Binary)body.get("canonical");
            switch(damage) {
                case "hash" -> {var bytes=binary.getData().clone();bytes[bytes.length-1]^=1;body.put("canonical",new org.bson.types.Binary(bytes));}
                case "subtype" -> body.put("canonical",new org.bson.types.Binary((byte)1,binary.getData()));
                case "body-version" -> body.put("formatVersion",2);
                case "row-version" -> row.put("formatVersion",3);
                case "extra" -> body.put("unrecognized",true);
                default -> throw new AssertionError();
            }
            f.rows().replaceOne(new Document("_id",row.get("_id")),row);var before=f.snapshot();
            assertCode(() -> store.prepareModels(List.of(original)),IoError.DOCUMENT_UNREADABLE);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void rowOverheadAndCompleteScopeBudgetsRefuseAtomically() {
        withFixture(f -> {
            var before=f.snapshot();var tooLarge=prepared(f,"orders","x".repeat(524240));
            assertCode(() -> f.store().prepareModels(List.of(tooLarge)),IoError.SINK_MODEL_CAPACITY);assertThat(f.snapshot()).isEqualTo(before);
            var many=new ArrayList<SinkPreparationStore.Prepared>();for(int i=0;i<10;i++)many.add(prepared(f,"t"+i,"x".repeat(450000)));
            assertCode(() -> f.store().prepareModels(many),IoError.SINK_MODEL_CAPACITY);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void issuedLegacyBackfillReservesItsOriginalReadyResultBeforePublishingTheBody() {
        var width=new AtomicInteger();
        // Measure the actual issued envelope; a fresh fixture has the same fixed-size identity fields.
        withFixture(f -> {
            var small=prepared(f,"orders","");var store=f.store();store.prepareTargets(List.of(small.input()));store.begin(small.input());
            var row=f.rows().find().first().append("formatVersion",2).append("targetModel",new Document("formatVersion",1)
                    .append("canonical",new org.bson.types.Binary(small.canonical())));
            int bytes=new org.bson.RawBsonDocument(row,new org.bson.codecs.DocumentCodec()).getByteBuffer().remaining();
            width.set((int)(SinkPreparationStore.MAX_MODEL_ROW_BYTES-bytes)/2);
        });
        withFixture(f -> {
            var boundary=prepared(f,"orders","x".repeat(width.get()));var store=f.store();store.prepareTargets(List.of(boundary.input()));
            var attempt=store.begin(boundary.input());var before=f.snapshot();
            var issued=f.rows().find().first().append("formatVersion",2).append("targetModel",new Document("formatVersion",1)
                    .append("canonical",new org.bson.types.Binary(boundary.canonical())));
            assertThat(new org.bson.RawBsonDocument(issued,new org.bson.codecs.DocumentCodec()).getByteBuffer().remaining()).isLessThanOrEqualTo((int)SinkPreparationStore.MAX_MODEL_ROW_BYTES);
            assertCode(() -> store.prepareModels(List.of(boundary)),IoError.SINK_MODEL_CAPACITY);
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(store.complete(attempt,new SinkPreparationStore.Result(true)).phase()).isEqualTo(SinkPreparationStore.Phase.READY);
        });
        withFixture(f -> {
            var fitting=prepared(f,"orders","x".repeat(width.get()-32));var store=f.store();store.prepareTargets(List.of(fitting.input()));
            var attempt=store.begin(fitting.input());store.prepareModels(List.of(fitting));
            assertThat(store.complete(attempt,new SinkPreparationStore.Result(true)).phase()).isEqualTo(SinkPreparationStore.Phase.READY);
            assertThat(store.resolveModels(List.of(fitting),null)).containsExactly(fitting);
        });
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void guardLossAfterActualV2PublicationOrBackfillRollsBackTheWholeScope(boolean backfill) {
        withFixture(f -> {
            var original=prepared(f,"orders","STRING(100)");if(backfill)f.store().prepareTargets(List.of(original.input()));
            var before=f.snapshot();
            try(var witness=new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.cache.getName(),backfill?"update":"insert","sink_preparations")) {
                var store=new MongoSinkPreparationStore(new MongoCachePipelineAccess(witness.client,witness.database,f.scope,f.fence,witness::refuseAfterCommand));
                assertCode(() -> store.prepareModels(List.of(original)),IoError.CACHE_BUSINESS_FENCED);witness.requireCommand();
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void concurrentV2PublicationKeepsOneExactRosterAndBackfillCannotCrossALostFence() {
        withFixture(f -> {
            var original=prepared(f,"orders","STRING(100)");
            try(var worker=Executors.newFixedThreadPool(2)) {
                var a=worker.submit(() -> f.store().prepareModels(List.of(original)));
                var b=worker.submit(() -> f.store().prepareModels(List.of(original)));a.get(10,TimeUnit.SECONDS);b.get(10,TimeUnit.SECONDS);
            }
            assertThat(f.rows().countDocuments()).isOne();var before=f.snapshot();
            var old=f.fence;var wrong=new WorkloadClaimFence(old.key(),old.owner(),old.claimGeneration()+1,old.executionGeneration(),old.topologyRevision());
            var stale=new MongoSinkPreparationStore(new MongoCachePipelineAccess(f.client,f.cache,f.scope,wrong,ACTIVE));
            assertCode(() -> stale.prepareModels(List.of(original)),IoError.WORKLOAD_CLAIM_FENCED);assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void mixedRosterRetainsOnlySamplingBodiesAndCannotHideDeclaredWidthChanges() {
        withFixture(f -> {
            var document=prepared(f,"orders","STRING(100)");var sql=prepared(f,"customers","varchar(40)");var store=f.store();
            store.prepareTargets(List.of(document.input(),sql.input()),List.of(document));
            assertThat(f.rows().find(new Document("_id",MongoSinkPreparationStore.id(document.input().key()))).first().getInteger("formatVersion")).isEqualTo(2);
            assertThat(f.rows().find(new Document("_id",MongoSinkPreparationStore.id(sql.input().key()))).first().getInteger("formatVersion")).isEqualTo(1);
            assertThat(store.hasPreparedModels()).isTrue();var before=f.snapshot();
            var changed=prepared(f,"customers","varchar(99)");
            assertCode(() -> store.prepareTargets(List.of(document.input(),changed.input()),List.of(document)),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test
    void ordinaryStartupCombinesRetainedSourcesAndSinkModelsInOneReadOnlySnapshot() {
        withFixture(f -> {
            var inputs = authoring(f);
            var model = prepared(f, "orders", "STRING(100)");
            var commands = new ArrayList<CommandCall>();
            var settings = com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(new com.mongodb.event.CommandListener() {
                        @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                            commands.add(new CommandCall(event.getCommandName(), event.getCommand().clone()));
                        }
                    }).build();
            try (var client = MongoClients.create(settings)) {
                var database = client.getDatabase(f.cache.getName());
                var preparations = new MongoCachePipelinePreparations(client, database, f.business, ACTIVE);
                var sinks = new MongoSinkPreparationStore(new MongoCachePipelineAccess(client, database, f.scope, f.fence, ACTIVE));
                assertThat(preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE)).isFalse();
                f.store().prepareTargets(List.of(model.input()));
                assertThat(preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE)).isFalse();
                f.store().prepareModels(List.of(model));
                var before = f.snapshot();
                commands.clear();
                preparations.verifySources(f.scope, inputs);
                assertThat(sinks.hasPreparedModels()).isTrue();
                assertThat(commands).hasSize(15);
                assertThat(commands.stream().filter(e -> e.name().equals("commitTransaction"))).hasSize(2);
                commands.clear();
                assertThat(preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE)).isTrue();
                assertThat(commands).hasSize(8);
                assertThat(commands.stream().map(CommandCall::name))
                        .containsExactly("aggregate", "find", "find", "find", "find", "find", "find",
                                "commitTransaction");
                assertThat(commands.stream().filter(e -> finds(e, "workload_claims"))).hasSize(1);
                assertThat(commands.stream().filter(e -> finds(e, "sink_preparations"))).hasSize(1);
                var transaction = commands.getFirst().command();
                assertThat(commands).allSatisfy(event -> {
                    assertThat(event.command().get("lsid")).isEqualTo(transaction.get("lsid"));
                    assertThat(event.command().get("txnNumber")).isEqualTo(transaction.get("txnNumber"));
                });
                assertThat(f.snapshot()).isEqualTo(before);
                System.out.println("Combined retained startup CACHE commands 11 -> 8; one commit");

                var expanded = new PipelinePreparationInputs(inputs.pipelineHash(), inputs.sourceHashes(),
                        Map.of("input", Set.of("orders", "customers")), List.of());
                var orders = SystemCollections.CACHE_SNAPSHOT_ORDERS.on(f.cache);
                var savedOrder = orders.find().first();
                var probeSink = new AtomicInteger();
                commands.clear();
                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, expanded, f.fence,
                        probeSink::incrementAndGet), IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED);
                assertThat(probeSink).hasValue(0);
                assertThat(commands).noneMatch(e -> e.command().toString().contains("sink_preparations"));
                assertThat(commands.getLast().name()).isEqualTo("abortTransaction");

                orders.deleteOne(new Document("_id", savedOrder.get("_id")));
                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence,
                        probeSink::incrementAndGet), IoError.DOCUMENT_UNREADABLE);
                assertThat(probeSink).hasValue(0);
                orders.insertOne(savedOrder);

                var newer = new PipelineIntentScope(f.scope.clusterId(), f.scope.bindingVersion(),
                        f.scope.cacheIncarnation(), f.scope.pipelineId(), f.scope.stateGeneration() + 1);
                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(newer, inputs, f.fence, ACTIVE),
                        IoError.CACHE_BUSINESS_FENCED);

                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence,
                        () -> { throw MongoCachePipelineAccess.claimFenced(); }), IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(f.snapshot()).isEqualTo(before);
                f.rows().deleteMany(new Document());
                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE),
                        IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                var sinkGuards = new AtomicInteger();
                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence,
                        () -> { if (sinkGuards.incrementAndGet() == 2) throw MongoCachePipelineAccess.claimFenced(); }),
                        IoError.WORKLOAD_CLAIM_FENCED);

                f.domain.retireBusiness(f.business, UUID.randomUUID().toString(), ACTIVE);
                assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE),
                        IoError.CACHE_BUSINESS_FENCED);
            }
        });
    }

    @Test
    void retainedStartupUsesTheOriginalPointLookupForANoncanonicalBsonId() {
        withFixture(f -> {
            var inputs = authoring(f);
            var model = prepared(f, "orders", "STRING(100)");
            var plain = prepared(f, "customers", "STRING(100)").input();
            f.store().prepareTargets(List.of(plain, model.input()), List.of(model));
            var modelId = MongoSinkPreparationStore.id(model.input().key());
            var original = f.rows().find(new Document("_id", modelId)).first();
            var reordered = new Document();
            for (String part : new ArrayList<>(modelId.keySet()).reversed()) reordered.put(part, modelId.get(part));

            var commands = new ArrayList<CommandCall>();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) { commands.add(new CommandCall(event.getCommandName(), event.getCommand().clone())); }
            };
            try (var client = observe("retained-bson-order", listener)) {
                var database = client.getDatabase(f.cache.getName());
                var preparations = new MongoCachePipelinePreparations(client, database, f.business, ACTIVE);
                var sinks = new MongoSinkPreparationStore(new MongoCachePipelineAccess(client, database, f.scope, f.fence, ACTIVE));
                assertThat(sinks.hasPreparedModels()).isTrue();
                commands.clear();
                assertThat(preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE)).isTrue();
                assertThat(commands.stream().filter(e -> finds(e, "sink_preparations"))).hasSize(1);

                f.rows().deleteOne(new Document("_id", modelId));
                f.rows().insertOne(new Document(original).append("_id", reordered));
                assertThat(f.rows().find(new Document("_id", modelId)).first()).isNull();
                Throwable previous = catchThrowable(sinks::hasPreparedModels);
                assertThat(previous).isNotNull();
                commands.clear();
                assertThatThrownBy(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, ACTIVE))
                        .isExactlyInstanceOf(previous.getClass());
                assertThat(commands.stream().filter(e -> finds(e, "sink_preparations"))).hasSize(2);
                assertThat(commands.stream().filter(e -> finds(e, "sink_preparations"))
                        .map(e -> e.command().getDocument("filter")).toList().getLast()).containsKey("_id");
            }
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retainedStartupReadsTheClaimAgainOnARealTransactionRetry(boolean revokePass) {
        withFixture(f -> {
            var inputs = authoring(f);
            f.store().prepareModels(List.of(prepared(f, "orders", "STRING(100)")));
            var before = f.snapshot();
            var commands = new ArrayList<CommandCall>();
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var active = new java.util.concurrent.atomic.AtomicBoolean(true);
            String appName = "retained-retry-" + UUID.randomUUID();
            var admin = f.client.getDatabase("admin");
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    commands.add(new CommandCall(event.getCommandName(), event.getCommand().clone()));
                    if (finds(commands.getLast(), "snapshot_orders") && armed.compareAndSet(false, true)) {
                        admin.runCommand(new Document("configureFailPoint", "failCommand")
                                .append("mode", new Document("times", 1))
                                .append("data", new Document("failCommands", List.of("find"))
                                        .append("appName", appName).append("errorCode", 112)
                                        .append("errorLabels", List.of("TransientTransactionError"))));
                    }
                }
                @Override public void commandFailed(com.mongodb.event.CommandFailedEvent event) {
                    if (revokePass && armed.get() && event.getCommandName().equals("find")) active.set(false);
                }
            };
            try (var client = observe(appName, listener)) {
                var database = client.getDatabase(f.cache.getName());
                var preparations = new MongoCachePipelinePreparations(client, database, f.business, ACTIVE);
                Runnable requirePass = () -> { if (!active.get()) throw MongoCachePipelineAccess.claimFenced(); };
                if (revokePass) {
                    assertCode(() -> preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, requirePass),
                            IoError.WORKLOAD_CLAIM_FENCED);
                } else {
                    assertThat(preparations.verifySourcesAndCheckSinkModels(f.scope, inputs, f.fence, requirePass)).isTrue();
                }
                var claimReads = commands.stream().filter(e -> finds(e, "workload_claims")).toList();
                assertThat(claimReads).hasSize(2);
                assertThat(claimReads.getFirst().command().get("lsid")).isEqualTo(claimReads.getLast().command().get("lsid"));
                assertThat(claimReads.getFirst().command().get("txnNumber"))
                        .isNotEqualTo(claimReads.getLast().command().get("txnNumber"));
                assertThat(commands.stream().filter(e -> finds(e, "sink_preparations"))).hasSize(revokePass ? 0 : 1);
                assertThat(commands.stream().filter(e -> e.name().equals("commitTransaction")))
                        .hasSize(revokePass ? 0 : 1);
                assertThat(f.snapshot()).isEqualTo(before);
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        });
    }

    private static MongoClient observe(String appName, com.mongodb.event.CommandListener listener) {
        return MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                .applicationName(appName).addCommandListener(listener).build());
    }

    private record CommandCall(String name, org.bson.BsonDocument command) {}

    private static boolean finds(CommandCall event, String collection) {
        return event.name().equals("find") && event.command().get("find") instanceof org.bson.BsonString found
                && found.getValue().equals(collection);
    }

    private static SinkPreparationStore.Prepared prepared(Fixture f,String target,String type) {
        var model=new PreparedTargetModel(target,List.of(new PreparedTargetModel.Field("id",type,true,null,null,null)),List.of());
        var base=f.input(target);return new SinkPreparationStore.Prepared(new SinkPreparationStore.Input(base.key(),model.modelHash(),base.policy(),base.fullLoad()),model.canonical());
    }
    private static PipelinePreparationInputs authoring(Fixture f) {
        var inputs=new PipelinePreparationInputs("a".repeat(64),Map.of("input","b".repeat(64)),Map.of("input",Set.of("orders")),List.of());
        f.claims().updateOne(new Document("_id",MongoWorkloadClaimStore.id(f.fence.key())),new Document("$set",new Document("pipelineSources",MongoPipelineSourcePreparations.encode(inputs))));return inputs;
    }
    private static void assertCode(Runnable action,TapstateErrorCode expected){assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(expected));}
    private static void withFixture(Scenario scenario) {
        String suffix=UUID.randomUUID().toString().replace("-","");String configName="sink_cfg_"+suffix,cacheName="sink_cache_"+suffix;
        try(var client=MongoClients.create(MONGO.getReplicaSetUrl())) {
            var config=client.getDatabase(configName);var cache=client.getDatabase(cacheName);
            var settings=new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(cacheName),cacheName,null);
            try(var configuration=MongoConfigurationBootstrap.prepare(settings,"cluster-a","installation-a","setup-a",1);var domain=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"sink-test")) {
                var master=new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                configuration.bindings().commit(new StorageSetupScope("cluster-a","installation-a","setup-a",1),StorageBinding.Mode.OP,connection,master);
                var business=configuration.bindings().cachePreparation().orElseThrow().businessScope();
                domain.prepareAuth(new CacheAuthScope(business.clusterId(),business.bindingVersion(),StorageBinding.Mode.OP,business.cacheIncarnation(),1),"auth-a",ACTIVE);
                try(var session=client.startSession()){session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.LOCAL).build());new MongoCacheMetadataStructure(cache,MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session,ACTIVE);session.commitTransaction();}
                SystemCollections.CACHE_IDENTITY.on(cache).updateOne(new Document("_id","cluster"),new Document("$set",new Document("businessPhase","READY").append("businessPreparationId","business-a").append("claimAdmissionFence",0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cache).insertOne(new Document("_id","business").append("installedVersion",1).append("phase","READY").append("epoch",1L).append("operationId","business-a"));
                var node=WorkloadClaimFence.from(domain.workloadClaims(business).acquire(new WorkloadClaimKey("cluster-a",WorkloadClaimType.NODE_SESSION,OWNER.nodeId()),OWNER,0,TTL).claim());
                var scope=new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),"flow",1);
                var pipeline=new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");configuration.artifacts().create(pipeline);
                configuration.intents().compareAndSet(scope,0,new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
                var creation=((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(scope,node)).creation();
                var preparation=domain.pipelinePreparations(business,ACTIVE);var reservation=preparation.reserveFirst(creation,TTL);
                configuration.preparations().complete(preparation.seed(reservation,configuration.preparations(),AT));
                var claims=domain.workloadClaims(business);var original=claims.acquirePipeline(scope,OWNER,1,TTL).claim();
                var fence=WorkloadClaimFence.from(claims.advanceExecution(original,1,Set.of(OWNER.nodeId())).orElseThrow());
                scenario.run(new Fixture(client,cache,domain,business,scope,fence));
            } finally{config.drop();cache.drop();}
        }catch(RuntimeException|Error failure){throw failure;}catch(Exception failure){throw new AssertionError(failure);}
    }
    private record Fixture(MongoClient client,MongoDatabase cache,MongoCacheDomain domain,CacheBusinessScope business,PipelineIntentScope scope,WorkloadClaimFence fence) {
        SinkPreparationStore.Input input(String target){return new SinkPreparationStore.Input(new SinkPreparationStore.Key(scope,PipelineNode.Purpose.SYNC,"sink",target),"a".repeat(64),OnFullLoad.CLEAR,true);}
        SinkPreparationStore store(){return domain.sinkPreparations(scope,fence,ACTIVE);}
        com.mongodb.client.MongoCollection<Document> rows(){return SystemCollections.CACHE_SINK_PREPARATIONS.on(cache);}
        com.mongodb.client.MongoCollection<Document> claims(){return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache);}
        Map<String,List<Document>> snapshot(){var result=new TreeMap<String,List<Document>>();for(var table:List.of(SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.CACHE_WORKLOAD_CLAIMS,SystemCollections.CACHE_SINK_PREPARATIONS))result.put(table.collectionName(),table.on(cache).find().sort(new Document("_id",1)).into(new ArrayList<>()));return result;}
    }
    @FunctionalInterface private interface Scenario{void run(Fixture fixture)throws Exception;}
}
