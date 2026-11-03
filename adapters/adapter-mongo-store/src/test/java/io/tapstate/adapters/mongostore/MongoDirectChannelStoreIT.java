package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/** Real CACHE transactions over explicit prepared facts; native preparation and source callbacks are separate tests. */
@RequiresDocker
class MongoDirectChannelStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster",1,"incarnation");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"incarnation","pipeline",1);
    private static final DirectChannelContext CONTEXT = new DirectChannelContext(SCOPE,"channel","source","capture");
    private static final Duration TTL = Duration.ofSeconds(60);
    @Test void validationControlDoesNotLoadCursorWriterOrPendingBatchBodies(){
        withBusiness(f->{
            assertThat(f.store().control(CONTEXT,"orders")).isEmpty();open(f);
            var offsets=SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database());var filter=new Document("_id",MongoDirectChannelRecords.id(CONTEXT));
            offsets.updateOne(filter,new Document("$set",new Document("cursor",new Document("unreadable","x".repeat(2*1024*1024)))));
            var broken=new Document("_id",MongoDirectChannelRecords.batchId(CONTEXT,1,1)).append("unreadable","x".repeat(512*1024));
            SystemCollections.DIRECT_BATCHES.on(f.database()).insertOne(broken);var before=offsets.find(filter).first();
            var control=f.store().control(CONTEXT,"orders").orElseThrow();assertThat(control.epoch()).isOne();
            assertThat(control.capture()).isEqualTo(f.captureFence());assertThat(control.pipeline()).isEqualTo(f.pipelineFence());assertThat(control.selectedTable()).isTrue();
            assertThatThrownBy(()->f.store().load(CONTEXT)).isInstanceOf(TapstateException.class);assertThat(offsets.find(filter).first()).isEqualTo(before);
            offsets.updateOne(filter,new Document("$set",new Document("current.captureFence.resourceId","wrong")));
            assertThatThrownBy(()->f.store().control(CONTEXT,"orders")).isInstanceOf(TapstateException.class);
        });
    }

    @Test void reopenPreservesPendingHighAndQuietTablesDoNotHoldACompletedBatch() {
        withBusiness(f -> {
            open(f); var batch = batch(1,1,2,Map.of("orders",1L));
            assertThat(f.store().recordBatch(batch,0,f.captureFence(),f.pipelineFence())).isTrue();
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = store(client,client.getDatabase(f.database().getName()));
                assertThat(reopened.pending(CONTEXT,1)).containsExactly(batch);
                assertThat(reopened.load(CONTEXT).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
                assertThat(reopened.advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence())).isTrue();
                assertThat(reopened.load(CONTEXT).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
                reopened.advanceWriter(CONTEXT,1,"run","orders","slow",point(1,1),f.pipelineFence());
                assertThat(reopened.load(CONTEXT).cursor().sinkAcked()).isEqualTo(batch.boundary());
                assertThat(reopened.load(CONTEXT).cursor().sinkAckedByTable()).doesNotContainKey("quiet");
                assertThat(reopened.pending(CONTEXT,1)).isEmpty();
                assertThat(reopened.recordBatch(batch,0,f.captureFence(),f.pipelineFence())).isTrue();
                assertThat(reopened.load(CONTEXT).current().recordedThrough()).isOne();
            }
        });
    }
    @Test void laterCompletedBatchesCannotSkipTheBlockedPrefixAndThenSettleTogether() {
        withBusiness(f -> {
            open(f); var first = batch(1,1,2,Map.of("orders",1L)); var second = batch(1,2,4,Map.of("quiet",3L));
            f.store().recordBatch(first,0,f.captureFence(),f.pipelineFence()); f.store().recordBatch(second,1,f.captureFence(),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","quiet","writer",point(1,3),f.pipelineFence());
            assertThat(f.store().pending(CONTEXT,1)).containsExactly(first,second);
            f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","slow",point(1,1),f.pipelineFence());
            assertThat(f.store().pending(CONTEXT,1)).isEmpty(); assertThat(f.store().load(CONTEXT).cursor().sinkAcked()).isEqualTo(second.boundary());
            assertThat(f.store().recordBatch(first,0,f.captureFence(),f.pipelineFence())).isFalse();
        });
    }
    @Test void writerConfirmationBeforeManifestArrivalStillSettlesAtomicallyWhenItsBoundaryIsRegistered() {
        withBusiness(f -> {
            open(f); f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","slow",point(1,1),f.pipelineFence());
            var batch = batch(1,1,2,Map.of("orders",1L));
            assertThat(f.store().load(CONTEXT).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
            f.store().recordBatch(batch,0,f.captureFence(),f.pipelineFence());
            assertThat(f.store().load(CONTEXT).cursor().sinkAcked()).isEqualTo(batch.boundary()); assertThat(f.store().pending(CONTEXT,1)).isEmpty();
        });
    }
    @ParameterizedTest @ValueSource(strings={"progress","batch","offset-collection","batch-collection"})
    void missingRequiredFactsRefuseResumeWithoutRecreatingAnything(String lost) {
        withBusiness(f -> {
            open(f); f.store().recordBatch(batch(1,1,2,Map.of("orders",1L)),0,f.captureFence(),f.pipelineFence());
            if (lost.equals("progress")) SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database()).deleteMany(new Document());
            else if (lost.equals("batch")) SystemCollections.DIRECT_BATCHES.on(f.database()).deleteMany(new Document());
            else if (lost.equals("offset-collection")) SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database()).drop();
            else SystemCollections.DIRECT_BATCHES.on(f.database()).drop();
            var names = f.database().listCollectionNames().into(new ArrayList<>()); var before = raw(f);
            assertThatThrownBy(() -> f.store().load(CONTEXT)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.store().beginCapture(CONTEXT,0,0,f.captureFence(),f.pipelineFence())).isInstanceOf(TapstateException.class);
            assertThat(raw(f)).isEqualTo(before); assertThat(f.database().listCollectionNames().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(names);
        });
    }
    @Test void anIndividuallyCanonicalManifestStillMustMatchTheOriginalCrossResourceReceipt() {
        withBusiness(f -> {
            open(f); var original = batch(1,1,2,Map.of("orders",1L)); f.store().recordBatch(original,0,f.captureFence(),f.pipelineFence());
            var substituted = batch(1,1,2,Map.of());
            SystemCollections.DIRECT_BATCHES.on(f.database()).replaceOne(new Document("_id",MongoDirectChannelRecords.batchId(CONTEXT,1,1)),MongoDirectChannelRecords.batch(substituted));
            assertThatThrownBy(() -> f.store().load(CONTEXT)).satisfies(MongoDirectChannelStoreIT::unavailable);
            assertThatThrownBy(() -> f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence())).satisfies(MongoDirectChannelStoreIT::unavailable);
            assertThat(raw(f).get("cursor",Document.class).get("sinkAcked",Document.class).getString("token")).isEqualTo("anchor");
        });
    }
    @Test void aCanonicalMiddleManifestSubstitutionCannotPassTheOriginalPendingClosure() {
        withBusiness(f -> {
            open(f); f.store().recordBatch(batch(1,1,2,Map.of("orders",1L)),0,f.captureFence(),f.pipelineFence());
            f.store().recordBatch(batch(1,2,4,Map.of("quiet",3L)),1,f.captureFence(),f.pipelineFence());
            f.store().recordBatch(batch(1,3,6,Map.of("orders",5L)),2,f.captureFence(),f.pipelineFence());
            SystemCollections.DIRECT_BATCHES.on(f.database()).replaceOne(new Document("_id",MongoDirectChannelRecords.batchId(CONTEXT,1,2)),
                    MongoDirectChannelRecords.batch(batch(1,2,4,Map.of())));
            assertThatThrownBy(() -> f.store().load(CONTEXT)).satisfies(MongoDirectChannelStoreIT::unavailable);
        });
    }
    @ParameterizedTest @ValueSource(strings={"claimGeneration","executionGeneration","topologyRevision","clusterId","resourceType","resourceId"})
    void producerProofRejectsMalformedCanonicalCaptureFactsEvenWhenMongoEqualityWouldMatch(String field) {
        withBusiness(f -> {
            open(f); var id = new Document("_id",MongoWorkloadClaimStore.id(f.capture().key()));
            var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(id).first();
            Object invalid = row.get(field) instanceof Number number ? number.doubleValue() : "wrong";
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(id,new Document("$set",new Document(field,invalid)));
            var before = raw(f);
            assertThatThrownBy(() -> f.store().recordBatch(batch(1,1,2,Map.of()),0,f.captureFence(),f.pipelineFence())).isInstanceOf(TapstateException.class);
            assertThat(raw(f)).isEqualTo(before); assertThat(SystemCollections.DIRECT_BATCHES.on(f.database()).countDocuments()).isZero();
        });
    }
    @Test void aPrivateWriterCanSettleAlreadyRecordedBatchesAfterProducerLeaseReleaseButOldProducerCannotWrite() {
        withBusiness(f -> {
            open(f); var batch = batch(1,1,2,Map.of("orders",1L)); f.store().recordBatch(batch,0,f.captureFence(),f.pipelineFence());
            assertThat(f.claims().release(f.capture())).isTrue();
            f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","slow",point(1,1),f.pipelineFence());
            assertThat(f.store().load(CONTEXT).cursor().sinkAcked()).isEqualTo(batch.boundary());
            assertThatThrownBy(() -> f.store().recordBatch(batch(1,2,4,Map.of()),1,f.captureFence(),f.pipelineFence())).satisfies(MongoDirectChannelStoreIT::fenced);
        });
    }
    @Test void replacementRetainsPendingFactsUntilTheActualSourceAcceptsTheSavedConfirmedAnchor() {
        withBusiness(f -> {
            open(f); var batch = batch(1,1,2,Map.of("orders",1L)); f.store().recordBatch(batch,0,f.captureFence(),f.pipelineFence());
            f.claims().release(f.capture()); var next = f.claims().acquire(f.capture().key(),new WorkloadOwner("replacement","boot2"),1,TTL).claim(); var fence = WorkloadClaimFence.from(next);
            var before = f.store().load(CONTEXT);
            assertThat(f.store().beginCapture(CONTEXT,before.revision(),1,fence,f.pipelineFence())).hasValue(2);
            assertThat(f.store().pending(CONTEXT,1)).containsExactly(batch); assertThat(f.store().pending(CONTEXT,2)).isEmpty();
            assertThatThrownBy(() -> f.store().confirmAnchor(CONTEXT,2,"newer-unproven",fence,f.pipelineFence())).satisfies(MongoDirectChannelStoreIT::unavailable);
            assertThat(f.store().pending(CONTEXT,1)).containsExactly(batch);
            assertThat(f.store().confirmAnchor(CONTEXT,2,"anchor",fence,f.pipelineFence())).isTrue();
            assertThat(f.store().load(CONTEXT).retained()).isNull(); assertThat(SystemCollections.DIRECT_BATCHES.on(f.database()).countDocuments()).isZero();
            assertThat(f.store().load(CONTEXT).cursor().sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(2),"anchor"));
            assertThat(f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence())).isFalse();
        });
    }
    @Test void aCrashedUnanchoredFirstProducerDoesNotAuthorizeANewUnknownBaseline() {
        withBusiness(f -> {
            f.store().beginCapture(CONTEXT,0,0,f.captureFence(),f.pipelineFence());
            f.claims().release(f.capture()); var next = f.claims().acquire(f.capture().key(),new WorkloadOwner("replacement","boot2"),1,TTL).claim();
            var before = f.store().load(CONTEXT);
            assertThatThrownBy(() -> f.store().beginCapture(CONTEXT,before.revision(),1,WorkloadClaimFence.from(next),f.pipelineFence())).satisfies(MongoDirectChannelStoreIT::unavailable);
            assertThat(f.store().load(CONTEXT)).isEqualTo(before);
        });
    }
    @Test void concurrentOriginalBatchesCompeteAtOneRecordedHighWater() {
        withBusiness(f -> {
            open(f);
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var other = store(client,client.getDatabase(f.database().getName())); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.store().recordBatch(batch(1,1,2,Map.of("orders",1L)),0,f.captureFence(),f.pipelineFence()); });
                var b = executor.submit(() -> { start.await(); return other.recordBatch(batch(1,1,4,Map.of("orders",3L)),0,f.captureFence(),f.pipelineFence()); }); start.countDown();
                assertThat(List.of(a.get(15,TimeUnit.SECONDS),b.get(15,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
            }
            assertThat(f.store().pending(CONTEXT,1)).hasSize(1); assertThat(f.store().load(CONTEXT).current().recordedThrough()).isOne();
        });
    }
    @Test void snapshotsKeepTheirOriginalSeamAndLegitimateWaitingBoundsDoNotCompleteTheLoad() {
        withBusiness(f -> {
            f.store().beginCapture(CONTEXT,0,0,f.captureFence(),f.pipelineFence()); f.store().confirmAnchor(CONTEXT,1,"anchor",f.captureFence(),f.pipelineFence());
            var before = f.store().load(CONTEXT); assertThat(f.store().pinSnapshot(CONTEXT,before.revision(),1,"anchor",f.pipelineFence())).isTrue();
            f.store().beginWriterRun(CONTEXT,1,"run",roster(),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","fast",new WriterProgress(new SourceOrder(0,10),null),f.pipelineFence());
            assertThat(f.store().load(CONTEXT).cursor().snapshotCompletedTables()).isEmpty();
            f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,SourceOrder.SNAPSHOT_SEQ),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","slow",point(1,SourceOrder.SNAPSHOT_SEQ),f.pipelineFence());
            var pinned = f.store().load(CONTEXT); assertThat(pinned.cursor().snapshotCompletedTables()).containsExactly("orders");
            assertThat(f.store().pinSnapshot(CONTEXT,0,1,"anchor",f.pipelineFence())).isTrue();
            assertThat(f.store().pinSnapshot(CONTEXT,pinned.revision(),1,"other",f.pipelineFence())).isFalse();
            assertThat(f.store().load(CONTEXT)).isEqualTo(pinned);
        });
    }
    @Test void closingTheOriginalOwnerDuringAMutationRollsBackProgressAndClaimCompetition() {
        withBusiness(f -> {
            open(f); var before = raw(f); var state = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find().first(); var checks = new AtomicInteger();
            var guarded = f.store().guardedBy(() -> { if (checks.incrementAndGet() >= 3) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null); });
            assertThatThrownBy(() -> guarded.recordBatch(batch(1,1,2,Map.of()),0,f.captureFence(),f.pipelineFence())).isInstanceOf(TapstateException.class);
            assertThat(raw(f)).isEqualTo(before); assertThat(SystemCollections.DIRECT_BATCHES.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find().first()).isEqualTo(state);
        });
    }
    @Test void losingAuthorityAfterAnExistingBatchDeletionRollsBackTheWholeSettlement() {
        withBusiness(f -> {
            open(f); var batch = batch(1,1,2,Map.of("orders",1L)); f.store().recordBatch(batch,0,f.captureFence(),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence());
            var before = raw(f); var state = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find().first();
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find().into(new ArrayList<>());
            var batches = SystemCollections.DIRECT_BATCHES.on(f.database()).find().into(new ArrayList<>());
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"delete","direct_batches")) {
                var guarded = new MongoDirectChannelStore(witness.client,witness.database,BUSINESS,witness::refuseAfterCommand,MongoRateHistoryStore.DEFAULT_RETENTION);
                assertThatThrownBy(() -> guarded.advanceWriter(CONTEXT,1,"run","orders","slow",point(1,1),f.pipelineFence())).isInstanceOf(TapstateException.class);
                witness.requireCommand();
            }
            assertThat(raw(f)).isEqualTo(before); assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find().first()).isEqualTo(state);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(claims);
            assertThat(SystemCollections.DIRECT_BATCHES.on(f.database()).find().into(new ArrayList<>())).containsExactlyElementsOf(batches);
            assertThat(f.store().load(CONTEXT).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
        });
    }
    @Test void aPersistedCheckpointCrossingItsFirstPendingBoundaryIsACodedDiagnostic() {
        withBusiness(f -> {
            open(f); f.store().recordBatch(batch(1,1,2,Map.of("orders",1L)),0,f.captureFence(),f.pipelineFence());
            f.store().recordBatch(batch(1,2,4,Map.of("quiet",3L)),1,f.captureFence(),f.pipelineFence());
            f.store().recordBatch(batch(1,3,8,Map.of("orders",7L)),2,f.captureFence(),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","fast",point(1,1),f.pipelineFence());
            f.store().advanceWriter(CONTEXT,1,"run","orders","slow",point(1,1),f.pipelineFence());
            var held = f.store().load(CONTEXT); var old = held.cursor(); var pending = f.store().pending(CONTEXT,1);
            var cursor = new ConsumerOffset(old.pipelineId(),Map.of(),new ChainPosition(new SourceOrder(1,5),"impossible-confirmation"),old.snapshotCompletedTables(),
                    old.cdcStartPosition(),old.snapshotEpoch(),old.sinkAckedByTable(),ConsumerProgressKind.DIRECT_SOURCE);
            var inconsistent = new DirectChannelProgress(CONTEXT,held.tables(),held.revision(),held.current(),held.retained(),cursor,held.writerRun());
            SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database()).replaceOne(new Document(),MongoDirectChannelRecords.progress(inconsistent,pending,List.of()));
            var before = raw(f);
            assertThatThrownBy(() -> f.store().load(CONTEXT)).satisfies(MongoDirectChannelStoreIT::unavailable);
            assertThat(raw(f)).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"fraction","unknown-field","hash","unknown-table","extra-epoch"})
    void malformedOrUnreferencedRecordsCannotBeLoadedOrSilentlyRepaired(String fault) {
        withBusiness(f -> {
            open(f); var batch = batch(1,1,2,Map.of("orders",1L)); f.store().recordBatch(batch,0,f.captureFence(),f.pipelineFence());
            if (fault.equals("extra-epoch")) SystemCollections.DIRECT_BATCHES.on(f.database()).insertOne(MongoDirectChannelRecords.batch(batch(2,1,2,Map.of())));
            else if (fault.equals("fraction")) SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database()).updateOne(new Document(),new Document("$set",new Document("revision",1.5)));
            else if (fault.equals("unknown-field")) SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database()).updateOne(new Document(),new Document("$set",new Document("unknown",true)));
            else if (fault.equals("hash")) SystemCollections.DIRECT_BATCHES.on(f.database()).updateOne(new Document(),new Document("$set",new Document("contentHash","invalid")));
            else SystemCollections.DIRECT_BATCHES.on(f.database()).replaceOne(new Document(),MongoDirectChannelRecords.batch(batch(1,1,2,Map.of("unknown",1L))));
            var before = raw(f); assertThatThrownBy(() -> f.store().load(CONTEXT)).isInstanceOf(TapstateException.class); assertThat(raw(f)).isEqualTo(before);
        });
    }
    private static void open(Fixture f) {
        assertThat(f.store().beginCapture(CONTEXT,0,0,f.captureFence(),f.pipelineFence())).hasValue(1);
        assertThat(f.store().confirmAnchor(CONTEXT,1,"anchor",f.captureFence(),f.pipelineFence())).isTrue();
        assertThat(f.store().beginWriterRun(CONTEXT,1,"run",roster(),f.pipelineFence())).isTrue();
    }
    private static Map<String,List<String>> roster() { return Map.of("orders",List.of("fast","slow"),"quiet",List.of("writer")); }
    private static DirectBatch batch(long epoch,long ordinal,long seq,Map<String,Long> targets) { return new DirectBatch(CONTEXT,epoch,ordinal,new ChainPosition(new SourceOrder(epoch,seq),"token-"+seq),targets); }
    private static WriterProgress point(long epoch,long seq) { return new WriterProgress(new SourceOrder(epoch,seq),new ChainPosition(new SourceOrder(epoch,seq),"writer-token")); }
    private static Document raw(Fixture f) { return SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.database()).find().first(); }
    private static void unavailable(Throwable error) { assertThat(error).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.DIRECT_CHANNEL_UNAVAILABLE)); }
    private static void fenced(Throwable error) { assertThat(error).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED)); }
    private static MongoDirectChannelStore store(MongoClient client,MongoDatabase database) { return new MongoDirectChannelStore(client,database,BUSINESS,() -> {},MongoRateHistoryStore.DEFAULT_RETENTION); }
    private static void withBusiness(Scenario scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("direct_"+UUID.randomUUID().toString().replace("-",""));
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_IDENTITY,SystemCollections.CACHE_SCHEMA_MIGRATIONS,SystemCollections.CACHE_WORKLOAD_CLAIMS,
                            SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.DIRECT_CHANNEL_OFFSETS,SystemCollections.DIRECT_BATCHES)) {
                        table.prepareCollectionOn(database,session); for (var index : table.indexes()) IndexEnsure.createInTransaction(session,table.on(database),index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(database).insertOne(new Document("_id","cluster").append("clusterId",BUSINESS.clusterId())
                        .append("layoutId",MongoConfigurationDomain.LAYOUT).append("bindingVersion",1L).append("cacheIncarnation",BUSINESS.cacheIncarnation())
                        .append("businessPhase","READY").append("businessPreparationId","business").append("authGeneration",1L).append("claimAdmissionFence",0L));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id","business").append("installedVersion",1)
                        .append("phase","READY").append("epoch",1L).append("operationId","business"));
                var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var capture = claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("capture-node","boot"),1,TTL).claim();
                var initial = claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("pipeline-node","boot"),1,TTL).claim();
                var pipeline = claims.advanceExecution(initial,1,Set.of(initial.owner().nodeId())).orElseThrow();
                var header = MongoPipelinePreparationFacts.scope(SCOPE);
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id","pipeline").append("scope",header).append("stateJson","NEW")
                        .append("epoch",0L).append("touchMillis",0L).append("runtimeWriteFence",1L));
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id",MongoWorkloadClaimStore.id(pipeline.key())),new Document("$set",new Document("pipelineScope",header)
                        .append("pipelinePreparation",MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(),PipelineCachePreparations.Phase.PREPARED))
                        .append("pipelineSources",MongoPipelineSourcePreparations.encode(new PipelinePreparationInputs("a".repeat(64),Map.of("source","b".repeat(64)),
                                Map.of("source",roster().keySet()),List.of(new DirectChannelPreparation(CONTEXT,roster().keySet(),io.tapstate.core.model.ReadMode.CDC_ONLY,"mc-"+"c".repeat(64))))))));
                SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).insertOne(MongoDirectChannelRecords.progress(MongoDirectChannelRecords.initial(CONTEXT,roster().keySet())));
                scenario.run(new Fixture(client,database,claims,store(client,database),capture,pipeline));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    private record Fixture(MongoClient client,MongoDatabase database,MongoWorkloadClaimStore claims,MongoDirectChannelStore store,WorkloadClaim capture,WorkloadClaim pipeline) {
        WorkloadClaimFence captureFence() { return WorkloadClaimFence.from(capture); }
        WorkloadClaimFence pipelineFence() { return WorkloadClaimFence.from(pipeline); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
