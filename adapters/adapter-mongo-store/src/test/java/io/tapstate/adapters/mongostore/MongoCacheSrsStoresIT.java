package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.SourceResource;
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
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;

/** Native preparation and actual SRS facets share the cache owner, never the configuration client. */
@RequiresDocker
class MongoCacheSrsStoresIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final String CHAIN = "chain", RING = "ring";
    private static final List<SystemCollections> TABLES = List.of(SystemCollections.SRS_CHAINS,SystemCollections.SRS_MODELS,
            SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_EVENTS,SystemCollections.SRS_LOG_BOUNDS,
            SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS,SystemCollections.CACHE_WORKLOAD_CLAIMS);

    @Test void actualModelPublicationAndLogReadsSurviveReopeningOnlyTheCacheOwner() {
        withFixture(f -> {
            var stores = f.domain.srs(f.scope,ACTIVE); var model = f.model();
            stores.chains().prepare(CHAIN,"retention",f.capture); stores.chains().requestTables(CHAIN,Set.of("orders"),f.capture);
            assertThat(stores.chains().openEpoch(CHAIN,0,f.capture)).hasValue(1);
            assertThat(stores.models().publish(model,f.capture)).isEqualTo(model);
            assertThat(stores.chains().publishServingTables(CHAIN,1,Set.of("orders"),f.capture)).isTrue();
            var event = new SrsLogRecord("token",Op.INSERT,1,null,Map.of("id",1),0,f.capture,1);
            stores.log().store(RING,0,event); assertThat(stores.log().load(RING,0)).contains(event);
            assertThat(stores.consumers().find(f.consumer())).isEmpty();
            try (var reopened = MongoCacheDomain.connect(f.endpoint,Duration.ofSeconds(5),"srs-reopen")) {
                var views = reopened.srs(f.scope,ACTIVE);
                assertThat(views.models().require(model.key())).isEqualTo(model);
                assertThat(views.log().load(RING,0)).contains(event); assertThat(views.chains().load(CHAIN).orElseThrow().epoch()).isEqualTo(1);
            }
            assertThat(f.configDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContainAnyElementsOf(TABLES.stream().map(SystemCollections::collectionName).toList());
            code(() -> stores.log().store(RING,1,new SrsLogRecord("wrong-model",Op.INSERT,2,null,Map.of("id",2),1,f.capture,1)),IoError.SRS_EVENT_MODEL_UNAVAILABLE);
            assertThat(stores.log().load(RING,1)).isEmpty();
        });
    }

    @Test void aConsumerCannotInventAStateGenerationDifferentFromItsNativelyPreparedPipeline() {
        withFixture(f -> {
            var prepared = f.pipeline(); var stores = f.domain.srs(f.scope,ACTIVE); stores.chains().prepare(CHAIN,"retention",f.capture);
            var wrong = new SrsConsumerContext(f.consumer(),2); var before = f.snapshot();
            assertThatThrownBy(() -> stores.consumers().attach(wrong,0,Set.of("orders"),prepared.fence())).isInstanceOf(TapstateException.class);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aRetainedConsumerCannotBorrowANewClaimAfterTheCurrentPipelineScopeChanged() {
        withFixture(f -> {
            var prepared = f.pipeline(); var stores = f.domain.srs(f.scope,ACTIVE); stores.chains().prepare(CHAIN,"retention",f.capture);
            var old = new SrsConsumerContext(f.consumer(),1); var held = stores.consumers().attach(old,0,Set.of("orders"),prepared.fence()).orElseThrow();
            var nextScope = new PipelineIntentScope(f.scope.clusterId(),f.scope.bindingVersion(),f.scope.cacheIncarnation(),"pipeline",2);
            // Simulate an already published CACHE generation replacement; this is not a recovery-entrypoint test.
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database).updateOne(new Document("_id","pipeline"),
                    new Document("$set",new Document("scope",MongoPipelinePreparationFacts.scope(nextScope))));
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).updateOne(new Document("_id",MongoWorkloadClaimStore.id(prepared.fence().key())),
                    new Document("$set",new Document("pipelineScope",MongoPipelinePreparationFacts.scope(nextScope))));
            var claims = f.domain.workloadClaims(f.scope); var current = claims.acquirePipeline(nextScope,f.node.owner(),1,Duration.ofMinutes(2)).claim();
            var advanced = claims.advanceExecution(current,1,Set.of(f.node.owner().nodeId())).orElseThrow();
            var next = WorkloadClaimFence.from(advanced); assertThat(next.executionGeneration()).isGreaterThan(prepared.fence().executionGeneration());
            assertThat(f.domain.pipelineStates(nextScope).read("pipeline")).isPresent(); var before = f.snapshot();
            assertThatThrownBy(() -> stores.consumers().attach(old,held.revision(),Set.of("orders"),next)).isInstanceOf(TapstateException.class);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aNewExecutionOfTheSamePreparedScopeCanReassertItsConsumerWithoutResettingProgress() {
        withFixture(f -> {
            var prepared = f.pipeline(); var stores = f.domain.srs(f.scope,ACTIVE); stores.chains().prepare(CHAIN,"retention",f.capture);
            var context = new SrsConsumerContext(f.consumer(),1);
            var first = stores.consumers().attach(context,0,Set.of("orders"),prepared.fence()).orElseThrow();
            assertThat(stores.consumers().beginWriterRun(context,first.revision(),"old-run",Map.of("orders",List.of("writer")),prepared.fence())).isTrue();
            assertThat(stores.consumers().advanceWriter(context,first.revision(),"old-run","orders","writer",
                    new WriterProgress(new io.tapstate.core.event.SourceOrder(1,4),new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1,4),"saved")),prepared.fence())).isTrue();
            assertThat(stores.consumers().advanceReadSequence(context,first.revision(),"orders",7,prepared.fence())).isTrue();
            var before = stores.consumers().progress(context).orElseThrow();
            var claims = f.domain.workloadClaims(f.scope); var retained = claims.acquirePipeline(prepared.scope(),f.node.owner(),1,Duration.ofMinutes(2)).claim();
            var current = WorkloadClaimFence.from(claims.advanceExecution(retained,1,Set.of(f.node.owner().nodeId())).orElseThrow());
            var next = stores.consumers().attach(context,first.revision(),Set.of("orders"),current).orElseThrow();
            assertThat(next.commandFence()).isEqualTo(current); assertThat(next.revision()).isEqualTo(first.revision()+1);
            var progress = stores.consumers().progress(context).orElseThrow();
            assertThat(progress.revision()).isEqualTo(before.revision()); assertThat(progress.cursor()).isEqualTo(before.cursor());
            assertThat(progress.writerRun()).isEqualTo(before.writerRun());
            assertThat(stores.consumers().advanceWriter(context,first.revision(),"old-run","orders","writer",
                    new WriterProgress(new io.tapstate.core.event.SourceOrder(1,9),null),current)).isFalse();
            assertThat(stores.consumers().beginWriterRun(context,next.revision(),"new-run",Map.of("orders",List.of("writer")),current)).isTrue();
            assertThat(stores.consumers().progress(context).orElseThrow().cursor()).isEqualTo(before.cursor());
        });
    }

    @Test void everyConsumerMutationWritesTheOriginalCurrentCheckpointFenceExactlyOnce() {
        withFixture(f -> {
            var prepared = f.pipeline(); var stores = f.domain.srs(f.scope,ACTIVE); stores.chains().prepare(CHAIN,"retention",f.capture);
            assertThat(stores.chains().openEpoch(CHAIN,0,f.capture)).hasValue(1); stores.models().publish(f.model(),f.capture);
            var context = new SrsConsumerContext(f.consumer(),1);
            long before = checkpointFence(f);
            var member = stores.consumers().attach(context,0,Set.of("orders"),prepared.fence()).orElseThrow();
            assertThat(checkpointFence(f)).isEqualTo(before+1); before = checkpointFence(f);
            var pin = new SrsSnapshotPin(1,"seam",Map.of("orders",f.model().key()));
            assertThat(stores.consumers().pinSnapshot(context,member.revision(),0,pin,prepared.fence())).isTrue();
            assertThat(checkpointFence(f)).isEqualTo(before+1); before = checkpointFence(f);
            assertThat(stores.consumers().beginWriterRun(context,member.revision(),"run",Map.of("orders",List.of("writer")),prepared.fence())).isTrue();
            assertThat(checkpointFence(f)).isEqualTo(before+1); before = checkpointFence(f);
            assertThat(stores.consumers().advanceWriter(context,member.revision(),"run","orders","writer",
                    new WriterProgress(io.tapstate.core.event.SourceOrder.snapshotRow(1),null),prepared.fence())).isTrue();
            assertThat(checkpointFence(f)).isEqualTo(before+1); before = checkpointFence(f);
            assertThat(stores.consumers().advanceReadSequence(context,member.revision(),"orders",0,prepared.fence())).isTrue();
            assertThat(checkpointFence(f)).isEqualTo(before+1); before = checkpointFence(f);
            assertThat(stores.consumers().detach(context,member.revision(),prepared.fence())).isPresent();
            assertThat(checkpointFence(f)).isEqualTo(before+1);
        });
    }
    private static long checkpointFence(Fixture f) {
        return SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find(new Document("_id","pipeline")).first().getLong("runtimeWriteFence");
    }

    @Test void allCapturedFacetsAndFurtherFactoriesRefuseAClosedOwner() {
        withFixture(f -> {
            var stores = f.domain.srs(f.scope,ACTIVE); f.domain.close();
            refuseReads(f,stores,IoError.CACHE_BUSINESS_FENCED);
            code(() -> f.domain.srs(f.scope,ACTIVE),IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void callerLossIsRetainedAcrossEveryFacetAndAdditionalGuard() {
        withFixture(f -> {
            var alive = new AtomicBoolean(true);
            var stores = f.domain.srs(f.scope,() -> { if (!alive.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); });
            var models = stores.models().guardedBy(ACTIVE); var before = f.snapshot(); alive.set(false);
            refuseReads(f,stores,IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> models.load(f.model().key()),IoError.WORKLOAD_CLAIM_FENCED);
            code(() -> stores.chains().prepare(CHAIN,"retention",f.capture),IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void retirementFencesRetainedViewsWithoutRetiringAuthentication() {
        withFixture(f -> {
            var stores = f.domain.srs(f.scope,ACTIVE); f.domain.retireBusiness(f.scope,"retire",ACTIVE);
            refuseReads(f,stores,IoError.CACHE_BUSINESS_FENCED);
            code(() -> f.domain.srs(f.scope,ACTIVE),IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.domain.loadAuthScope(f.auth)).isEqualTo(f.auth);
        });
    }

    @ParameterizedTest @ValueSource(strings={"srs_chains","srs_models","srs_model_contexts","srs_events","srs_log_bounds","srs_consumers","srs_consumer_offsets","workload_claims"})
    void losingAnyRequiredCollectionRefusesBothExistingViewsAndOpeningWithoutRecreatingIt(String table) {
        withFixture(f -> {
            var stores = f.domain.srs(f.scope,ACTIVE); f.database.getCollection(table).drop();
            var before = f.snapshot(); refuseReads(f,stores,IoError.DOCUMENT_UNREADABLE);
            code(() -> f.domain.srs(f.scope,ACTIVE),IoError.DOCUMENT_UNREADABLE);
            code(() -> stores.chains().prepare(CHAIN,"retention",f.capture),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database.listCollectionNames()).doesNotContain(table); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @ParameterizedTest @ValueSource(strings={"missing","hidden","ttl","keys"})
    void changedIndexesRemainLoadOnlyFailures(String fault) {
        withFixture(f -> {
            var stores = f.domain.srs(f.scope,ACTIVE); var table = SystemCollections.SRS_MODELS.on(f.database);
            var declared = SystemCollections.SRS_MODELS.indexes().getFirst(); table.dropIndex(declared.indexName());
            var keys = new Document(); for (String key : declared.keys()) keys.put(key,1);
            if (fault.equals("keys")) keys = new Document("wrong",1);
            if (!fault.equals("missing")) {
                var options = new IndexOptions().name(declared.indexName()).unique(declared.unique());
                if (fault.equals("hidden")) options.hidden(true);
                if (fault.equals("ttl")) { keys = new Document("expiresAt",1); options.expireAfter(60L,java.util.concurrent.TimeUnit.SECONDS); }
                table.createIndex(keys,options);
            }
            var before = table.listIndexes().into(new ArrayList<>());
            refuseReads(f,stores,IoError.DOCUMENT_UNREADABLE); code(() -> f.domain.srs(f.scope,ACTIVE),IoError.DOCUMENT_UNREADABLE);
            assertThat(table.listIndexes().into(new ArrayList<>())).isEqualTo(before);
        });
    }

    private static void refuseReads(Fixture f,SrsStores stores,IoError expected) {
        code(() -> stores.chains().load(CHAIN),expected); code(() -> stores.models().load(f.model().key()),expected);
        code(() -> stores.log().bounds(RING),expected); code(() -> stores.consumers().find(f.consumer()),expected);
    }
    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,IoError expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(expected));
    }
    private static void withFixture(java.util.function.Consumer<Fixture> action) {
        String suffix = UUID.randomUUID().toString().replace("-","");
        try (var observer = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = observer.getDatabase("srs_cache_"+suffix); var configDatabase = observer.getDatabase("srs_config_"+suffix);
            var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(),database.getName(),null);
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(configDatabase.getName()),null,Duration.ofSeconds(5));
            try (var configuration = MongoConfigurationBootstrap.prepare(settings,"cluster","installation","setup",1);
                 var domain = MongoCacheDomain.connect(endpoint,Duration.ofSeconds(5),"srs-facets")) {
                var source = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var committed = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster","installation","setup",1),StorageBinding.Mode.OP,endpoint,source);
                domain.prepareFirstAuth(committed.firstCreation().orElseThrow()); var scope = domain.prepareInitialBusiness(committed.preparation(),ACTIVE);
                var claims = domain.workloadClaims(scope); var owner = new WorkloadOwner("node","boot");
                var node = claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),owner,1,Duration.ofMinutes(2)).claim();
                var capture = claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),owner,1,Duration.ofMinutes(2)).claim();
                action.accept(new Fixture(configuration,domain,database,configDatabase,endpoint,scope,committed.preparation().authScope(),WorkloadClaimFence.from(capture),WorkloadClaimFence.from(node)));
            } finally { database.drop(); configDatabase.drop(); }
        }
    }
    private record PreparedPipeline(PipelineIntentScope scope,WorkloadClaimFence fence) {}
    private record Fixture(MongoConfigurationDomain configuration,MongoCacheDomain domain,MongoDatabase database,MongoDatabase configDatabase,
            StorageBinding.Connection endpoint,CacheBusinessScope scope,CacheAuthScope auth,WorkloadClaimFence capture,WorkloadClaimFence node) {
        PreparedPipeline pipeline() {
            var pipeline = new io.tapstate.core.model.PipelineResource("pipeline",null,List.of(),null,null,null,null,null);
            configuration.artifacts().create(pipeline);
            var physical = new PipelineIntentScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),pipeline.id(),1);
            configuration.intents().compareAndSet(physical,0,new io.tapstate.core.lifecycle.DesiredState(pipeline.id(),io.tapstate.core.lifecycle.PipelineState.RUNNING,
                    io.tapstate.core.model.canonical.CanonicalHash.of(pipeline),false));
            var grant = ((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(physical,node)).creation();
            var preparations = domain.pipelinePreparations(scope,ACTIVE); var reservation = preparations.reserveFirst(grant,Duration.ofMinutes(2));
            var receipt = preparations.seed(reservation,configuration.preparations(),java.time.Instant.parse("2026-10-19T00:00:00Z"));
            configuration.preparations().complete(receipt);
            var claims = domain.workloadClaims(scope); var retained = claims.acquirePipeline(physical,node.owner(),1,Duration.ofMinutes(2)).claim();
            var running = claims.advanceExecution(retained,1,Set.of(node.owner().nodeId())).orElseThrow();
            return new PreparedPipeline(physical,WorkloadClaimFence.from(running));
        }
        SrsModel model() { return new SrsModel(new SrsModelKey(CHAIN,"orders",scope.cacheIncarnation(),1,0),RING,
                new SourceTable("orders",List.of(new SourceField("id","integer")),List.of("id"),List.of()),0); }
        SrsConsumerKey consumer() { return new SrsConsumerKey(CHAIN,"pipeline","source"); }
        Map<String,List<Document>> snapshot() {
            var rows = new TreeMap<String,List<Document>>();
            for (String table : database.listCollectionNames()) rows.put(table,database.getCollection(table).find().sort(new Document("_id",1)).into(new ArrayList<>()));
            return rows;
        }
    }
}
