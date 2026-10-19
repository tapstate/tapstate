package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.app.fixture.PreparedPinnedSnapshotSource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.srs.SnapshotPhase;
import io.tapstate.runtime.srs.SrsSnapshotPreparation;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Native storage and the actual isolated PDK host callback; no production connector or Jet job runs. */
@RequiresDocker
class PreparedSrsSnapshotIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Test void exactSessionModelsAndConsumerPinCommitBeforeTheConnectorReadsEitherTable() throws Exception {
        withFixture(f -> {
            var owner = f.owner(); var calls = new AtomicInteger(); var sampled = new AtomicReference<String>(); var port = f.port().withSnapshotPreparation((request,seam,actual) -> {
                calls.incrementAndGet(); sampled.set(seam.token()); assertThat(f.count("reads")).isZero(); f.publish(actual,false);
                owner.prepare(request,seam,actual);
                var pin = f.stores.consumers().progress(f.run.context()).orElseThrow().snapshot();
                assertThat(pin).isEqualTo(owner.accepted());
                f.flag("pinned").set(true);
            });
            var loaded = new ArrayList<String>();
            try (var load = SnapshotPhase.openPrepared(port,f.config(),owner)) {
                assertThat(load.tailSeam()).isEqualTo(sampled.get()); assertThat(load.read(row -> { throw new AssertionError("fixture is empty"); },loaded::add)).isZero();
            }
            assertThat(calls).hasValue(1); assertThat(loaded).containsExactly("t1","t2");
            assertThat(f.count("reads")).isEqualTo(2); assertThat(f.count("earlyReads")).isZero();
            assertThat(f.count("inits")).isOne(); assertThat(f.count("discoveries")).isOne(); assertThat(f.count("stops")).isOne();
            var progress = f.stores.consumers().progress(f.run.context()).orElseThrow();
            assertThat(progress.snapshot().models()).hasSize(2); assertThat(progress.cursor().snapshotCompletedTables()).isEmpty();
            for (var key : progress.snapshot().models().values()) {
                var model = f.stores.models().require(key); assertThat(key.modelVersion()).isZero();
                assertThat(model.table().primaryKey()).containsExactly("session_id");
                assertThat(model.table().indexes()).extracting(SourceIndex::name).containsExactly("idx_session");
            }
        });
    }
    @ParameterizedTest @ValueSource(strings={"schema","revision"})
    void aModelMismatchOrOriginalProgressConflictStopsTheConnectorBeforeItsFirstRow(String refusal) throws Exception {
        withFixture(f -> {
            var owner = f.owner();
            if (refusal.equals("revision")) assertThat(f.stores.consumers().advanceReadSequence(f.run.context(),f.run.membershipRevision(),"t1",1,f.run.originalClaim())).isTrue();
            var port = f.port().withSnapshotPreparation((request,seam,actual) -> {
                f.publish(actual,refusal.equals("schema")); owner.prepare(request,seam,actual); f.flag("pinned").set(true);
            });
            assertThatThrownBy(() -> SnapshotPhase.openPrepared(port,f.config(),owner)).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.SRS_CONSUMER_UNAVAILABLE));
            assertThat(f.count("reads")).isZero(); assertThat(f.count("stops")).isOne();
            assertThat(f.stores.consumers().progress(f.run.context()).orElseThrow().snapshot()).isNull();
        });
    }
    @Test void nativeProducerPublishesActualModelsBeforeWriteThroughAndKeepsQuietTableCheckpoint() throws Exception {
        withFixture(f -> {
            var member = member(f.stores.log());
            try {
                var writer = writer(f,member,1); var health = new io.tapstate.runtime.srs.CaptureHealth();
                try (var source = writer.start(f.port().withStreamPreparation(writer::prepare),f.config(),io.tapstate.spi.capture.CaptureStart.present(),health)) {
                    streamEnded(f,health);
                    var checkpoint = f.stores.chains().load("chain").orElseThrow().checkpoint();
                    assertThat(checkpoint.position().token()).isNotBlank();
                    assertThat(checkpoint.tables().get("t1").sequence()).isZero();
                    assertThat(checkpoint.tables().get("t2").sequence()).isEqualTo(-1);
                    var recorded = f.stores.log().load(io.tapstate.runtime.srs.SrsRingbuffer.ringName("chain","t1"),0).orElseThrow();
                    var key = checkpoint.tables().get("t1").model();
                    var actual = f.stores.models().requireEvent(key,io.tapstate.runtime.srs.SrsRingbuffer.ringName("chain","t1"),0,recorded.captureFence());
                    assertThat(actual.table().fields()).extracting(SourceField::name).containsExactly("session_id");
                    assertThat(recorded.epoch()).isEqualTo(1); assertThat(recorded.schemaVer()).isZero();
                }
                assertThat(f.count("streams")).isOne(); assertThat(f.count("stops")).isOne();
            } finally { member.shutdown(); }
        });
    }
    @Test void nativeProducerReopensAtOriginalSourceTokenAndKeepsOldEventModelBindings() throws Exception {
        withFixture(f -> {
            var member = member(f.stores.log());
            try {
                var first = writer(f,member,1); var health = new io.tapstate.runtime.srs.CaptureHealth();
                try (var source = first.start(f.port().withStreamPreparation(first::prepare),f.config(),io.tapstate.spi.capture.CaptureStart.present(),health)) { streamEnded(f,health); }
                var before = f.stores.chains().load("chain").orElseThrow().checkpoint();
                assertThat(f.stores.chains().openEpoch("chain",1,f.capture).orElseThrow()).isEqualTo(2);
                f.state.put("streamEnded",new java.util.concurrent.CountDownLatch(1));
                var second = writer(f,member,2); var nextHealth = new io.tapstate.runtime.srs.CaptureHealth();
                try (var source = second.start(f.port().withStreamPreparation(second::prepare),f.config(),io.tapstate.spi.capture.CaptureStart.resume(new io.tapstate.spi.capture.SourcePosition(before.position().token())),nextHealth)) {
                    streamEnded(f,nextHealth);
                    assertThat(((AtomicReference<?>) f.state.get("streamOffset")).get()).isEqualTo("stream-offset");
                    var after = f.stores.chains().load("chain").orElseThrow().checkpoint();
                    assertThat(after.tables().get("t1").sequence()).isEqualTo(1);
                    assertThat(after.tables().get("t2").sequence()).isEqualTo(-1);
                    String ring = io.tapstate.runtime.srs.SrsRingbuffer.ringName("chain","t1");
                    var oldEvent = f.stores.log().load(ring,0).orElseThrow(); var newEvent = f.stores.log().load(ring,1).orElseThrow();
                    assertThat(oldEvent.epoch()).isEqualTo(1); assertThat(newEvent.epoch()).isEqualTo(2);
                    assertThat(f.stores.models().requireEvent(before.tables().get("t1").model(),ring,0,oldEvent.captureFence()).key().captureEpoch()).isEqualTo(1);
                    assertThat(f.stores.models().requireEvent(after.tables().get("t1").model(),ring,1,newEvent.captureFence()).ddlSequence()).isEqualTo(1);
                }
            } finally { member.shutdown(); }
        });
    }
    @Test void nativeProducerRejectsChangedDiscoveryBeforeReopeningTheSourceStream() throws Exception {
        withFixture(f -> {
            var member = member(f.stores.log());
            try {
                var first = writer(f,member,1); var health = new io.tapstate.runtime.srs.CaptureHealth();
                try (var source = first.start(f.port().withStreamPreparation(first::prepare),f.config(),io.tapstate.spi.capture.CaptureStart.present(),health)) { streamEnded(f,health); }
                var before = f.stores.chains().load("chain").orElseThrow().checkpoint();
                f.stores.chains().openEpoch("chain",1,f.capture).orElseThrow(); f.flag("changedModel").set(true);
                var second = writer(f,member,2);
                assertThatThrownBy(() -> second.start(f.port().withStreamPreparation(second::prepare),f.config(),
                        io.tapstate.spi.capture.CaptureStart.resume(new io.tapstate.spi.capture.SourcePosition(before.position().token())),new io.tapstate.runtime.srs.CaptureHealth()))
                        .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.SRS_MODEL_UNAVAILABLE));
                assertThat(f.count("streams")).isOne();
                assertThat(f.stores.chains().load("chain").orElseThrow().checkpoint()).isEqualTo(before);
                var retained = f.stores.models().publishedForNewSnapshot("chain",Map.of("t1",io.tapstate.runtime.srs.SrsRingbuffer.ringName("chain","t1"),
                        "t2",io.tapstate.runtime.srs.SrsRingbuffer.ringName("chain","t2"))).orElseThrow();
                assertThat(retained).hasSize(2).allSatisfy((table,model) -> assertThat(model.key().captureEpoch()).isEqualTo(1));
            } finally { member.shutdown(); }
        });
    }
    private static io.tapstate.runtime.srs.SrsCaptureWriter writer(Fixture f,com.hazelcast.core.HazelcastInstance member,long epoch) {
        f.stores.chains().requestTables("chain",Set.of("t1","t2"),f.capture);
        assertThat(f.stores.chains().publishServingTables("chain",epoch,Set.of("t1","t2"),f.capture)).isTrue();
        var scope = new CacheBusinessScope(f.run.scope().clusterId(),f.run.scope().bindingVersion(),f.run.scope().cacheIncarnation());
        return new io.tapstate.runtime.srs.SrsCaptureWriter(scope,f.stores.chains().load("chain").orElseThrow(),f.capture,f.stores,member,() -> {});
    }
    private static void streamEnded(Fixture f,io.tapstate.runtime.srs.CaptureHealth health) throws InterruptedException {
        boolean ended = ((java.util.concurrent.CountDownLatch) f.state.get("streamEnded")).await(10,java.util.concurrent.TimeUnit.SECONDS);
        assertThat(health.failure()).isEmpty(); assertThat(ended).as("the owned source callback completed").isTrue();
    }
    private static com.hazelcast.core.HazelcastInstance member(SrsLogStore log) {
        var config = new com.hazelcast.config.Config().setClusterName("native-pdk-producer-"+UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled","false"); config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.operation.thread.count","2"); config.setProperty("hazelcast.operation.generic.thread.count","2");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false); config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new com.hazelcast.config.RingbufferConfig("srs.*").setCapacity(8).setBackupCount(0)
                .setInMemoryFormat(com.hazelcast.config.InMemoryFormat.OBJECT).setRingbufferStoreConfig(new com.hazelcast.config.RingbufferStoreConfig()
                        .setEnabled(true).setFactoryImplementation(new io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory(log))));
        config.getSerializationConfig().addSerializerConfig(new com.hazelcast.config.SerializerConfig()
                .setTypeClass(io.tapstate.runtime.srs.SrsItem.class).setImplementation(new io.tapstate.runtime.srs.SrsItemSerializer()));
        return com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
    }
    private static void withFixture(Scenario scenario) throws Exception {
        var state = new ConcurrentHashMap<String,Object>();
        for (String key : List.of("reads","earlyReads","inits","discoveries","stops","streams")) state.put(key,new AtomicInteger());
        state.put("streamEnded",new java.util.concurrent.CountDownLatch(1)); state.put("streamOffset",new AtomicReference<>());
        state.put("changedModel",new AtomicBoolean()); state.put("pinned",new AtomicBoolean()); System.getProperties().put(PreparedPinnedSnapshotSource.STATE,state);
        String suffix = UUID.randomUUID().toString().replace("-",""); String config = "prepared_config_"+suffix, cache = "prepared_cache_"+suffix;
        var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(),cache,null);
        var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(config),null,Duration.ofSeconds(5));
        try (var observer = MongoClients.create(MONGO.getReplicaSetUrl())) {
            try (var configuration = MongoConfigurationBootstrap.prepare(settings,"cluster","installation","setup",1);
                 var domain = MongoCacheDomain.connect(endpoint,Duration.ofSeconds(5),"pdk-prepared-it")) {
                var target = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var binding = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster","installation","setup",1),StorageBinding.Mode.OP,endpoint,target);
                domain.prepareFirstAuth(binding.firstCreation().orElseThrow()); var scope = domain.prepareInitialBusiness(binding.preparation(),() -> {});
                var claims = domain.workloadClaims(scope); var owner = new WorkloadOwner("node","boot");
                var node = WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),owner,1,Duration.ofMinutes(2)).claim());
                var pipeline = new PipelineResource("pipeline",null,List.of(),null,null,null,null,null); configuration.artifacts().create(pipeline);
                var physical = new PipelineIntentScope("cluster",scope.bindingVersion(),scope.cacheIncarnation(),"pipeline",1);
                configuration.intents().compareAndSet(physical,0,new DesiredState("pipeline",PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
                var grant = ((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(physical,node)).creation();
                var preparations = domain.pipelinePreparations(scope,() -> {});
                var receipt = preparations.seed(preparations.reserveFirst(grant,Duration.ofMinutes(2)),configuration.preparations(),Instant.parse("2026-10-19T00:00:00Z"));
                configuration.preparations().complete(receipt);
                var held = claims.acquirePipeline(physical,owner,1,Duration.ofMinutes(2)).claim();
                var pipelineFence = WorkloadClaimFence.from(claims.advanceExecution(held,1,Set.of(owner.nodeId())).orElseThrow());
                var capture = WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),owner,1,Duration.ofMinutes(2)).claim());
                var stores = domain.srs(scope,() -> {}); stores.chains().prepare("chain","retention",capture); stores.chains().openEpoch("chain",0,capture).orElseThrow();
                var context = new SrsConsumerContext(new SrsConsumerKey("chain","pipeline","source"),1);
                var member = stores.consumers().attach(context,0,Set.of("t1","t2"),pipelineFence).orElseThrow();
                stores.consumers().startReadingAfter(context,member.revision(),0,"t1",-1,pipelineFence);
                stores.consumers().startReadingAfter(context,member.revision(),1,"t2",-1,pipelineFence);
                scenario.run(new Fixture(domain,stores,SrsConsumerRun.of(physical,member),capture,state));
                assertThat(observer.getDatabase(config).listCollectionNames()).doesNotContain("srs_models","srs_consumer_offsets");
            } finally { observer.getDatabase(cache).drop(); observer.getDatabase(config).drop(); }
        } finally { System.getProperties().remove(PreparedPinnedSnapshotSource.STATE); }
    }
    private record Fixture(MongoCacheDomain domain,SrsStores stores,SrsConsumerRun run,WorkloadClaimFence capture,Map<String,Object> state) {
        SrsSnapshotPreparation owner() { return new SrsSnapshotPreparation(domain,run,Map.of("t1","ring-t1","t2","ring-t2"),7,() -> {}); }
        CaptureConfig config() { return new CaptureConfig("prepared-native",Map.of(),List.of("t1","t2")).at(new PipelineNode("pipeline","source")); }
        int count(String key) { return ((AtomicInteger)state.get(key)).get(); }
        AtomicBoolean flag(String key) { return (AtomicBoolean)state.get(key); }
        PdkCapturePort port() throws Exception {
            Path classes = Path.of(PreparedPinnedSnapshotSource.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return new PdkCapturePort(id -> new ConnectorRef(List.of(classes),PreparedPinnedSnapshotSource.class.getName(),"2.0.8",null));
        }
        void publish(Map<String,SourceTable> actual,boolean wrong) {
            actual.forEach((table,model) -> {
                var shape = wrong && table.equals("t1") ? new SourceTable(table,List.of(new SourceField("wrong","string")),List.of(),List.of()) : model;
                stores.models().publish(new SrsModel(new SrsModelKey("chain",table,run.scope().cacheIncarnation(),1,0),"ring-"+table,shape,0),capture);
            });
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
