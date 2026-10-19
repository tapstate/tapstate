package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual configured and cache owners behind a deserialized member-side factory; no Jet job is submitted. */
@RequiresDocker
class ScopedSrsSinkAckFactoryIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Test void originalWriterCallbacksCommitThroughNativePreparedStoresAndCannotBorrowANewerExecution() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-",""); String configName = "ack_config_"+suffix, cacheName = "ack_cache_"+suffix;
        var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(),cacheName,null);
        var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
        try (var observer = MongoClients.create(MONGO.getReplicaSetUrl())) {
            try (var configuration = MongoConfigurationBootstrap.prepare(settings,"cluster","installation","setup",1);
                 var cache = MongoCacheDomain.connect(endpoint,Duration.ofSeconds(5),"scoped-writer-it")) {
                var target = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var committed = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster","installation","setup",1),StorageBinding.Mode.OP,endpoint,target);
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow()); var business = cache.prepareInitialBusiness(committed.preparation(),() -> {});
                var claims = cache.workloadClaims(business); var owner = new WorkloadOwner("node","boot");
                var node = claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),owner,1,Duration.ofMinutes(2)).claim();
                var source = new SourceResource("source",null,"fixture",Map.of(),io.tapstate.core.model.SourceMode.CDC,
                        Set.of("orders").stream().sorted().<io.tapstate.core.model.TableRef>map(io.tapstate.core.model.TableRef::literal).toList(),new io.tapstate.core.model.Srs("chain","retention",null,null,true),null);
                configuration.artifacts().create(source);
                var pipeline = new PipelineResource("pipeline",null,List.of(io.tapstate.core.model.SourceRef.spec("source",true)),null,null,null,null,null);
                configuration.artifacts().create(pipeline);
                var physical = new PipelineIntentScope("cluster",business.bindingVersion(),business.cacheIncarnation(),"pipeline",1);
                configuration.intents().compareAndSet(physical,0,new DesiredState("pipeline",PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
                var inputs=PipelinePreparationInputs.of(physical,pipeline,Map.of("source",source),Map.of("source",Set.of("orders")));
                var grant = ((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(physical,WorkloadClaimFence.from(node),inputs)).creation();
                var preparations = cache.pipelinePreparations(business,() -> {});
                var receipt = preparations.seed(preparations.reserveFirst(grant,Duration.ofMinutes(2)),configuration.preparations(),Instant.parse("2026-10-19T00:00:00Z"));
                configuration.preparations().complete(receipt);
                var prepared = claims.acquirePipeline(physical,owner,1,Duration.ofMinutes(2)).claim();
                var running = claims.advanceExecution(prepared,1,Set.of(owner.nodeId())).orElseThrow(); var original = WorkloadClaimFence.from(running);
                var captureGrant=((CaptureCreationResult.Issued)configuration.capturePreparations().issueFirstCreate(
                        new CapturePreparationRequest(physical,original,WorkloadClaimFence.from(node),"source",inputs))).creation();
                var firstCapture=cache.capturePreparations(business,() -> {}).createFirst(captureGrant);
                var capture=WorkloadClaimFence.from(claims.acquire(firstCapture.claim().key(),owner,1,Duration.ofMinutes(2)).claim());
                var stores = cache.srs(business,() -> {}); stores.chains().prepare("mck-chain","retention",capture);
                assertThat(stores.chains().openEpoch("mck-chain",0,capture)).hasValue(1);
                var model = new SrsModel(new SrsModelKey("mck-chain","orders",business.cacheIncarnation(),1,0),"ring",
                        new SourceTable("orders",List.of(new SourceField("id","integer")),List.of("id"),List.of()),0);
                stores.models().publish(model,capture);
                var context = new SrsConsumerContext(new SrsConsumerKey("mck-chain","pipeline","source"),1);
                var membership = stores.consumers().attach(context,0,Set.of("orders"),original).orElseThrow();
                assertThat(stores.consumers().pinSnapshot(context,membership.revision(),0,new SrsSnapshotPin(7,"owner-seam",Map.of("orders",model.key())),original)).isTrue();
                assertThat(stores.consumers().startReadingAfter(context,membership.revision(),1,"orders",-1,original)).isTrue();
                var run = SrsConsumerRun.of(physical,membership); var readerFactory = new ScopedSrsReadCursorPublisherFactory(run,"orders"); var factory = copied(new ScopedSrsSinkAckFactory(
                        Map.of("source.orders",new ScopedSrsSinkAckFactory.Source(run,"orders")),"original-run"));
                var member = mock(HazelcastInstance.class); var local = new ConcurrentHashMap<String,Object>(); when(member.getUserContext()).thenReturn(local);
                try (var authorization = ExecutionAuthorization.scoped(business,claims,Duration.ofSeconds(2),System::nanoTime,() -> true)) {
                    local.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization); local.put(SrsStoreViews.USER_CONTEXT_KEY,cache);
                    factory.beginRun(member,Map.of("source.orders",List.of("fast","slow")),original);
                    var reader = readerFactory.resolve(member); reader.accept(99);
                    assertThat(readerFactory.recoverySequence(member)).hasValue(-1);
                    assertThat(readerFactory.confirmedPosition(member)).isEmpty();
                    var unbound = factory.resolve(member); var fast = unbound.forWriter("fast"); var slow = unbound.forWriter("slow");
                    fast.advance("source.orders",new ChainPosition(new SourceOrder(7,9),"fast-9"),original);
                    assertThat(stores.consumers().cursor(context).orElseThrow().sinkAcked()).isNull();
                    slow.advance("source.orders",new ChainPosition(SourceOrder.snapshotRow(7),null),original);
                    var landed = stores.consumers().progress(context).orElseThrow();
                    assertThat(landed.cursor().sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"owner-seam"));
                    assertThat(landed.cursor().snapshotCompletedTables()).containsExactly("orders");
                    slow.advance("source.orders",new ChainPosition(new SourceOrder(7,4),"slow-4"),original);
                    var confirmed = stores.consumers().progress(context).orElseThrow();
                    assertThat(confirmed.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(7,4),"slow-4"));
                    assertThat(readerFactory.recoverySequence(member)).hasValue(4);
                    assertThat(readerFactory.confirmedPosition(member)).contains(confirmed.cursor().sinkAcked());
                    assertThat(stores.chains().load("mck-chain").orElseThrow().checkpoint()).isNull();
                    claims.advanceExecution(running,1,Set.of(owner.nodeId())).orElseThrow();
                    assertThatThrownBy(() -> slow.advance("source.orders",new ChainPosition(new SourceOrder(7,99),"late"),original)).isInstanceOf(TapstateException.class);
                    assertThatThrownBy(() -> reader.accept(100)).isInstanceOf(TapstateException.class);
                    assertThat(stores.consumers().progress(context)).contains(confirmed);
                }
                assertThat(observer.getDatabase(configName).listCollectionNames()).doesNotContain("srs_consumer_offsets","srs_consumers","srs_chains");
            } finally { observer.getDatabase(cacheName).drop(); observer.getDatabase(configName).drop(); }
        }
    }
    private static SinkAckFactory copied(SinkAckFactory original) throws Exception {
        var bytes = new ByteArrayOutputStream(); try (var out = new ObjectOutputStream(bytes)) { out.writeObject(original); }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { return (SinkAckFactory)in.readObject(); }
    }
}
