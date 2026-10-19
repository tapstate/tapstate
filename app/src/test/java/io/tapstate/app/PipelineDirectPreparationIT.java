package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

/** Native first-grant and CACHE preparation over separate replica sets; no connector, Jet or target is exercised. */
@RequiresDocker
class PipelineDirectPreparationIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(60);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-19T00:00:00Z"),ZoneOffset.UTC);

    @Test void oneNativeGrantSeedsThePrivateClosureWithoutCopyingTheManifestOrSecretsToConfiguration() {
        withFixture(f -> {
            long version = f.version(); var inputs = f.inputs(); var fixed = inputs.directChannels().getFirst();
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.version()).isEqualTo(version); assertThat(f.configDatabase.getCollection("operator_generations").find().first().toJson())
                    .doesNotContain("pipelineSources","directChannels","sourceHashes");
            var claim = f.claim(); assertThat(claim.get("pipelineSources",Document.class)).isNotNull();
            assertThat(claim.toJson()).doesNotContain("test-secret","mongodb://x.invalid/user");
            assertThat(f.cache.directChannels(f.business(),ACTIVE).load(fixed.context()).tables()).containsExactlyInAnyOrder("orders","quiet");
            assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isOne();
            assertThat(f.cacheDatabase.getCollection("direct_batches").countDocuments()).isZero();
        });
    }
    @Test void aNativeReopenVerifiesTheSameContractWithoutResettingRunningWriterOrPendingFacts() {
        withFixture(f -> {
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var context = inputs.directChannels().getFirst().context(); var claims = f.cache.workloadClaims(f.business());
            var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var execution = WorkloadClaimFence.from(claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow());
            var capture = WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey(f.scope.clusterId(),WorkloadClaimType.CAPTURE,context.captureId()),new WorkloadOwner("capture","boot"),1,TTL).claim());
            var direct = f.cache.directChannels(f.business(),ACTIVE); direct.beginCapture(context,0,0,capture,execution); direct.confirmAnchor(context,1,"anchor",capture,execution);
            direct.beginWriterRun(context,1,"run",Map.of("orders",List.of("writer"),"quiet",List.of("writer")),execution);
            direct.recordBatch(new DirectBatch(context,1,1,new ChainPosition(new SourceOrder(1,2),"high"),Map.of("orders",1L)),0,capture,execution);
            var before = f.cacheSnapshot();
            try (var configuration = MongoConfigurationDomain.open(f.settings(),f.scope.clusterId()); var cache = MongoCacheDomain.connect(f.cacheConnection,Duration.ofSeconds(5),"direct-reopen")) {
                assertThat(f.preparer(configuration,cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                assertThat(cache.directChannels(f.business(),ACTIVE).pending(context,1)).hasSize(1);
                assertThat(cache.directChannels(f.business(),ACTIVE).load(context).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
            }
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"coordinates","reference","srs","selection","read-mode","start"})
    void identityChangesRefuseTheOldGenerationAndLeaveAllRetainedFactsUntouched(String change) {
        withFixture(f -> {
            var old = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,old);
            var source = f.source(); var pipeline = f.pipeline();
            switch (change) {
                case "coordinates" -> f.configuration.artifacts().save(source("input","y",null));
                case "reference" -> {
                    f.configuration.artifacts().create(source("other","x",null));
                    f.configuration.artifacts().save(pipeline("other",false,ReadMode.CDC_ONLY,"latest",null));
                }
                case "srs" -> f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
                case "selection" -> f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),source.config(),source.mode(),List.of(TableRef.literal("orders")),source.srs(),null));
                case "read-mode" -> f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_ONLY,"latest",null));
                case "start" -> f.configuration.artifacts().save(pipeline("input",false,ReadMode.CDC_ONLY,"earliest",null));
                default -> throw new AssertionError(change);
            }
            var requested = f.inputs();
            if (change.equals("coordinates")) {
                assertThat(requested.directChannels().getFirst().context()).isEqualTo(old.directChannels().getFirst().context());
                assertThat(requested.directChannels().getFirst().sourceIdentity()).isNotEqualTo(old.directChannels().getFirst().sourceIdentity());
            }
            var before = f.cacheSnapshot(); var mapping = f.configDatabase.getCollection("operator_generations").find().first();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,requested),IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN);
            assertThat(f.cacheSnapshot()).isEqualTo(before); assertThat(f.configDatabase.getCollection("operator_generations").find().first()).isEqualTo(mapping);
            assertThat(f.configuration.pipelineStates().read("flow").orElseThrow().stateGeneration()).isOne();
        });
    }
    @Test void metadataOnlyEditsUseTheSameRetainedRecoveryContract() {
        withFixture(f -> {
            var original = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,original); var before = f.cacheSnapshot();
            var labels = new Metadata(Map.of("team","test"),"updated display text");
            f.configuration.artifacts().saveAll(List.of(source("input","x",labels),pipeline("input",false,ReadMode.CDC_ONLY,"latest",labels)));
            var next = f.inputs(); assertThat(next).isNotEqualTo(original); assertThat(next.sameRecovery(original)).isTrue();
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,next).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"progress","all-direct","manifest","checkpoint","claim"})
    void completedStatusCannotRecreateLostCacheHistory(String lost) {
        withFixture(f -> {
            var inputs = f.inputs(); var complete = f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            switch (lost) {
                case "progress" -> f.cacheDatabase.getCollection("direct_channel_offsets").deleteMany(new Document());
                case "all-direct" -> { f.cacheDatabase.getCollection("direct_channel_offsets").deleteMany(new Document()); f.cacheDatabase.getCollection("direct_batches").deleteMany(new Document()); }
                case "manifest" -> f.cacheDatabase.getCollection("workload_claims").updateOne(new Document("_id",f.claim().get("_id")),new Document("$unset",new Document("pipelineSources","")));
                case "checkpoint" -> f.cacheDatabase.getCollection("pipeline_state").deleteMany(new Document());
                case "claim" -> f.cacheDatabase.getCollection("workload_claims").deleteOne(new Document("_id",f.claim().get("_id")));
                default -> throw new AssertionError(lost);
            }
            var before = f.cacheSnapshot();
            assertThatThrownBy(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,inputs)).isInstanceOf(TapstateException.class);
            assertThat(f.configuration.preparations().read("flow")).contains(complete);
            assertThat(f.configuration.preparations().issueFirstCreate(f.scope,f.node,inputs)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @Test void anExistingOriginalReservationCanFinishButIssuanceWithoutCacheHistoryCannotStartAgain() {
        withFixture(f -> {
            var inputs = f.inputs(); var grant = ((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(f.scope,f.node,inputs)).creation();
            var original = f.cache.pipelinePreparations(f.business(),ACTIVE).reserveFirst(grant,TTL);
            assertThat(original.phase()).isEqualTo(PipelineCachePreparations.Phase.RESERVED); assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isZero();
            try (var configuration = MongoConfigurationDomain.open(f.settings(),f.scope.clusterId()); var cache = MongoCacheDomain.connect(f.cacheConnection,Duration.ofSeconds(5),"direct-resume")) {
                assertThat(f.preparer(configuration,cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                assertThat(cache.directChannels(f.business(),ACTIVE).load(inputs.directChannels().getFirst().context()).current()).isNull();
            }
        });
    }
    @Test void changedDefinitionsBeforeIssuanceCannotSpendTheFirstGrantForAStaleSnapshot() {
        withFixture(f -> {
            var stale = f.inputs(); f.configuration.artifacts().save(source("input","y",null)); var before = f.cacheSnapshot();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,stale),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs()).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
        });
    }
    @ParameterizedTest @ValueSource(strings={"not-an-instant","+1000000000-12-31T23:59:59.999999999Z","at:2026-10-19T00:00:00Z"})
    void invalidStartsCannotSpendTheIrreversibleConfigurationGrant(String start) {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.CDC_ONLY,start,null)); var before = f.cacheSnapshot();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs()),IoError.DOCUMENT_INVALID);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"capture","channel","missing-direct","selection"})
    void configurationReDerivesPrivateCoordinatesInsteadOfTrustingTheCaller(String fault) {
        withFixture(f -> {
            var original = f.inputs(); var direct = original.directChannels().getFirst(); var context = direct.context();
            var wrong = new DirectChannelContext(f.scope,fault.equals("channel") ? "foreign-channel" : context.channelId(),context.sourceId(),fault.equals("capture") ? "foreign-capture" : context.captureId());
            var channels = fault.equals("missing-direct") ? List.<DirectChannelPreparation>of() : List.of(new DirectChannelPreparation(wrong,direct.tables(),direct.readMode(),direct.sourceIdentity()));
            var inputs = new PipelinePreparationInputs(original.pipelineHash(),original.sourceHashes(),original.selectedTables(),channels);
            if (fault.equals("selection")) {
                var altered = DirectChannelPreparation.of(f.scope,f.source(),ReadMode.CDC_ONLY,"latest",Set.of("foreign"));
                inputs = new PipelinePreparationInputs(original.pipelineHash(),original.sourceHashes(),Map.of("input",Set.of("foreign")),List.of(altered));
            }
            var requested = inputs;
            assertCode(() -> f.configuration.preparations().issueFirstCreate(f.scope,f.node,requested),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
        });
    }
    private static SourceResource source(String id,String database,Metadata metadata) {
        return new SourceResource(id,metadata,"mongodb",Map.of("uri","mongodb://"+database+".invalid/user","password","test-secret"),SourceMode.CDC,
                List.of(TableRef.literal("orders"),TableRef.literal("quiet")),new Srs("explicit-key",null,null,null,false),null);
    }
    private static PipelineResource pipeline(String source,boolean shared,ReadMode mode,String start,Metadata metadata) {
        return new PipelineResource("flow",metadata,List.of(SourceRef.spec(source,shared)),null,null,null,new Settings(null,null,null,null,mode,start),null);
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,IoError expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,failure -> { assertThat(failure.code()).isEqualTo(expected); assertThat(failure.getCause()).isNull(); });
    }
    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-",""); var configName = "direct_cfg_"+suffix; var cacheName = "direct_cache_"+suffix;
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var configDb = configClient.getDatabase(configName); var cacheDb = cacheClient.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(cacheName),cacheName,null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings,"cluster","installation","setup",1);
                    var cache = MongoCacheDomain.connect(cacheConnection,Duration.ofSeconds(5),"direct-native")) {
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var committed = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster","installation","setup",1),StorageBinding.Mode.OP,cacheConnection,master);
                var business = committed.preparation().businessScope(); cache.prepareFirstAuth(committed.firstCreation().orElseThrow()); cache.prepareInitialBusiness(committed.preparation(),ACTIVE);
                configuration.artifacts().create(source("input","x",null)); configuration.artifacts().create(pipeline("input",false,ReadMode.CDC_ONLY,"latest",null));
                var scope = new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),"flow",1);
                var node = WorkloadClaimFence.from(cache.workloadClaims(business).acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,OWNER.nodeId()),OWNER,1,TTL).claim());
                scenario.run(new Fixture(configuration,cache,configDb,cacheDb,cacheConnection,scope,node));
            } finally { configDb.drop(); cacheDb.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain configuration,MongoCacheDomain cache,MongoDatabase configDatabase,MongoDatabase cacheDatabase,
            StorageBinding.Connection cacheConnection,PipelineIntentScope scope,WorkloadClaimFence node) {
        CacheBusinessScope business() { return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()); }
        MongoConnectionSettings settings() { return new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDatabase.getName()),null,Duration.ofSeconds(5)); }
        SourceResource source() { var pipeline = pipeline(); return (SourceResource)configuration.artifacts().get(pipeline.sourceIds().getFirst()).orElseThrow(); }
        PipelineResource pipeline() { return (PipelineResource)configuration.artifacts().get("flow").orElseThrow(); }
        PipelinePreparationInputs inputs() {
            var pipeline = pipeline(); var source = source(); var sources = Map.of(source.id(),source);
            var frozen = SourceModelSnapshot.capture("flow",sources.values(),cache.schemas(business(),ACTIVE));
            return frozen.preparationInputs(scope,pipeline,pipeline,sources);
        }
        PipelineFirstStatePreparer preparer(MongoConfigurationDomain configuration,MongoCacheDomain cache) {
            return new PipelineFirstStatePreparer(business(),configuration.preparations(),cache.pipelinePreparations(business(),ACTIVE),() -> Optional.of(node),ACTIVE,CLOCK,TTL);
        }
        long version() { return configDatabase.getCollection("configuration_version").find().first().getLong("revision"); }
        Document claim() { return cacheDatabase.getCollection("workload_claims").find(new Document("_id.resourceType","PIPELINE_ACTUATION")).first(); }
        Map<String,List<Document>> cacheSnapshot() {
            var out = new TreeMap<String,List<Document>>();
            for (String table : List.of("workload_claims","pipeline_state","direct_channel_offsets","direct_batches","operator_cleanup"))
                out.put(table,cacheDatabase.getCollection(table).find().sort(new Document("_id",1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
