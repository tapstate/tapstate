package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
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
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofMinutes(2);
    private static final Instant AT = Instant.parse("2026-10-19T03:00:00Z");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a","boot-a");

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
