package io.tapstate.app;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.*;
import io.tapstate.runtime.srs.*;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

/** Real cross-member metadata transport; ordinary ACK stores remain separately authoritative. */
class ValidationPipelineInitialsTest {
    private static volatile CompletableFuture<WriteResult> actualWrite;
    @Test @Timeout(50) void remoteWriterReceiptsCannotMakeTheSelectedOutputReadyUntilEveryWriterLands() throws Exception {
        String cluster = "initial-receipts-" + UUID.randomUUID();
        var owner = Hazelcast.newHazelcastInstance(config(cluster, "owner", null)); HazelcastInstance writer = null;
        try {
            var address = owner.getCluster().getLocalMember().getAddress();
            writer = Hazelcast.newHazelcastInstance(config(cluster, "writer", address.getHost() + ":" + address.getPort()));
            HazelcastInstance second = writer; await(() -> owner.getCluster().getMembers().size() == 2 && second.getCluster().getMembers().size() == 2);
            var scope = new CacheBusinessScope("cluster", 1, "cache"); var fence = fence(); var proved = new AtomicInteger();
            try (var a = new ValidationPipelineInitials(owner, scope, () -> {}, value -> { assertThat(value).isEqualTo(fence); proved.incrementAndGet(); }, value -> { assertThat(value).isEqualTo(fence); proved.incrementAndGet(); }, value -> Optional.of(new ValidationPipelineInitials.Attempt(17,21)));
                 var b = new ValidationPipelineInitials(writer, scope, () -> {}, value -> {}, value -> {})) {
                SinkAckFactory ordinary = member -> (stream, position) -> {};
                var observed = a.decorate(fence, ordinary); var roster = new SinkAckFactory.RunWriters(Map.of("orders", List.of("serve.out#0", "serve.out#1")),
                        Map.of("serve.out", Map.of("orders", List.of("serve.out#0", "serve.out#1"))));
                observed.beginRun(writer, new SinkAckFactory.ExecutionWriters(17, 21, roster), claim());
                a.started(owner, fence, 17, 21, "orders", 0, "SNAPSHOT", 7);
                var first = observed.resolve(writer).forWriter("serve.out#0"); var other = observed.resolve(writer).forWriter("serve.out#1");
                first.started(17, 21, claim()); other.started(17, 21, claim()); first.bounded("orders", SourceOrder.snapshotRow(7), claim());
                await(() -> "initial.snapshot-landing".equals(a.status(fence, Set.of("serve.out")).reason()));
                assertThat(a.status(fence, Set.of("serve.out")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
                other.bounded("orders", SourceOrder.snapshotRow(7), claim());
                await(() -> a.status(fence, Set.of("serve.out")).state() == ValidationInitialLoadState.State.READY);
                assertThat(proved).hasValueGreaterThan(0);
            }
        } finally { if (writer != null) writer.shutdown(); owner.shutdown(); }
    }
    @Test @Timeout(35) void aNewCurrentAttemptNeverFallsBackToThePreviouslyCompletedReadyAttempt() throws Exception {
        var member=Hazelcast.newHazelcastInstance(config("initial-current-"+UUID.randomUUID(),"owner",null));
        var current=new AtomicReference<>(new ValidationPipelineInitials.Attempt(17,21)); var scope=new CacheBusinessScope("cluster",1,"cache");var fence=fence();
        try(var service=new ValidationPipelineInitials(member,scope,()->{},value->{},value->{},value->Optional.of(current.get()))) {
            SinkAckFactory ordinary=m->(stream,position)->{};var factory=service.decorate(fence,ordinary);
            var roster=new SinkAckFactory.RunWriters(Map.of("orders",List.of("serve.out#0")),Map.of("serve.out",Map.of("orders",List.of("serve.out#0"))));
            factory.beginRun(member,new SinkAckFactory.ExecutionWriters(17,21,roster),claim());service.started(member,fence,17,21,"orders",0,"SNAPSHOT",7);
            var ack=factory.resolve(member).forWriter("serve.out#0");ack.started(17,21,claim());ack.bounded("orders",SourceOrder.snapshotRow(7),claim());
            await(()->service.status(fence,Set.of("serve.out")).state()==ValidationInitialLoadState.State.READY);service.ended(17,21,"orders",0,true);
            current.set(new ValidationPipelineInitials.Attempt(17,22));factory.beginRun(member,new SinkAckFactory.ExecutionWriters(17,22,roster),claim());
            assertThat(service.status(fence,Set.of("serve.out")).state()).isNotEqualTo(ValidationInitialLoadState.State.READY);
            service.started(member,fence,17,22,"orders",0,"UNPROVEN",7);service.ended(17,22,"orders",0,false);
            assertThat(service.status(fence,Set.of("serve.out")).state()).isNotEqualTo(ValidationInitialLoadState.State.READY);
        }finally{member.shutdown();}
    }
    @Test @Timeout(35) void knownSupersededPipelineFencesDoNotExhaustCumulativeEntryCapacity() {
        var member=Hazelcast.newHazelcastInstance(config("initial-retire-"+UUID.randomUUID(),"owner",null));var latest=new AtomicReference<ExecutionFence>();
        java.util.function.Consumer<ExecutionFence> proof=value->{if(!value.equals(latest.get()))throw new io.tapstate.core.common.TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",value.pipelineId()),null);};
        try(var service=new ValidationPipelineInitials(member,new CacheBusinessScope("cluster",1,"cache"),()->{},proof,proof)) {
            SinkAckFactory ordinary=m->(stream,position)->{};
            for(int index=1;index<=65;index++) {
                var next=new ExecutionFence(new PipelineIntentScope("cluster",1,"cache","flow",1),new WorkloadClaimFence(claim().key(),claim().owner(),1,index,1));latest.set(next);
                assertThat(service.decorate(next,ordinary)).as("execution %s",index).isNotSameAs(ordinary);
            }
            assertThat(service.usage(latest.get()).entries()).isLessThanOrEqualTo(1);
        }finally{member.shutdown();}
    }
    @Test @Timeout(35) void sequentialKnownCurrentAttemptsReleaseOldEvidenceAndIgnoreLateReceipts() throws Exception {
        var member=Hazelcast.newHazelcastInstance(config("initial-attempt-retire-"+UUID.randomUUID(),"owner",null));
        var current=new AtomicReference<>(new ValidationPipelineInitials.Attempt(17,21));var fence=fence();
        try(var service=new ValidationPipelineInitials(member,new CacheBusinessScope("cluster",1,"cache"),()->{},value->{},value->{},value->Optional.of(current.get()))) {
            SinkAckFactory ordinary=m->(stream,position)->{};var factory=service.decorate(fence,ordinary);
            var roster=new SinkAckFactory.RunWriters(Map.of("orders",List.of("serve.out#0")),Map.of("serve.out",Map.of("orders",List.of("serve.out#0"))));
            for(int index=21;index<=27;index++) {
                current.set(new ValidationPipelineInitials.Attempt(17,index));factory.beginRun(member,new SinkAckFactory.ExecutionWriters(17,index,roster),claim());
                service.started(member,fence,17,index,"orders",0,"NO_INITIAL_LOAD",0);factory.resolve(member).forWriter("serve.out#0").started(17,index,claim());
                await(()->service.status(fence,Set.of("serve.out")).state()==ValidationInitialLoadState.State.READY);
                service.ended(17,index,"orders",0,false);assertThat(service.usage(fence).attempts()).isEqualTo(1);assertThat(service.usage(fence).stateBytes()).isLessThan(131072);
            }
            factory.beginRun(member,new SinkAckFactory.ExecutionWriters(17,21,roster),claim());
            assertThat(service.status(fence,Set.of("serve.out")).state()).isEqualTo(ValidationInitialLoadState.State.READY);
            assertThat(service.usage(fence).attempts()).isEqualTo(1);
        }finally{member.shutdown();}
    }
    @Test void writerInitializationObservationNeverRefreshesAuthorizationOrPropagatesItsRefusal() {
        var authorization=org.mockito.Mockito.mock(ExecutionAuthorization.class);var fence=fence();
        org.mockito.Mockito.when(authorization.require(fence)).thenThrow(new AssertionError("synchronous CACHE access"));
        var noted=new AtomicInteger();SinkAck observed=new SinkAck(){public void advance(String stream,io.tapstate.core.event.ChainPosition position){}public void started(long job,long execution){noted.incrementAndGet();}};
        var guarded=FencedSinkAckFactory.guarded(observed,fence,authorization);
        assertThatCode(()->guarded.started(17,21)).doesNotThrowAnyException();assertThat(noted).hasValue(1);
        org.mockito.Mockito.verifyNoInteractions(authorization);
    }
    @Test @Timeout(35) void aRefusedRefreshIsNotPositiveProofToDiscardAnotherPipelinesEvidence() {
        var member=Hazelcast.newHazelcastInstance(config("initial-unknown-proof-"+UUID.randomUUID(),"owner",null));var unavailable=new AtomicBoolean();var old=fence();
        java.util.function.Consumer<ExecutionFence> proof=value->{if(value.equals(old)&&unavailable.get())throw new io.tapstate.core.common.TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",value.pipelineId()),null);};
        try(var service=new ValidationPipelineInitials(member,new CacheBusinessScope("cluster",1,"cache"),()->{},proof,value->{})) {
            SinkAckFactory ordinary=m->(stream,position)->{};service.decorate(old,ordinary);unavailable.set(true);
            var otherClaim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"other"),claim().owner(),1,1,1);
            service.decorate(new ExecutionFence(new PipelineIntentScope("cluster",1,"cache","other",1),otherClaim),ordinary);
            assertThat(service.usage(old).entries()).isEqualTo(2);
        }finally{member.shutdown();}
    }
    @Test @Timeout(35) void aDelayedStatusLookupCannotPruneAcknowledgedFactsOfTheNewCurrentAttempt() throws Exception {
        var member=Hazelcast.newHazelcastInstance(config("initial-delayed-status-"+UUID.randomUUID(),"owner",null));var current=new AtomicReference<>(new ValidationPipelineInitials.Attempt(17,21));
        var captured=new CountDownLatch(1);var release=new CountDownLatch(1);var block=new AtomicBoolean(true);var fence=fence();
        java.util.function.Function<ExecutionFence,Optional<ValidationPipelineInitials.Attempt>> lookup=value->{
            var sampled=current.get();if(Thread.currentThread().getName().equals("delayed-initial-status")&&block.compareAndSet(true,false)){
                captured.countDown();try{if(!release.await(8,TimeUnit.SECONDS))throw new AssertionError("status gate timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
            }return Optional.of(sampled);
        };
        try(var service=new ValidationPipelineInitials(member,new CacheBusinessScope("cluster",1,"cache"),()->{},value->{},value->{},lookup);
            var worker=Executors.newSingleThreadExecutor(task->new Thread(task,"delayed-initial-status"))) {
            SinkAckFactory ordinary=m->(stream,position)->{};var factory=service.decorate(fence,ordinary);
            var roster=new SinkAckFactory.RunWriters(Map.of("orders",List.of("serve.out#0")),Map.of("serve.out",Map.of("orders",List.of("serve.out#0"))));
            factory.beginRun(member,new SinkAckFactory.ExecutionWriters(17,21,roster),claim());service.started(member,fence,17,21,"orders",0,"NO_INITIAL_LOAD",0);factory.resolve(member).forWriter("serve.out#0").started(17,21,claim());
            await(()->service.status(fence,Set.of("serve.out")).state()==ValidationInitialLoadState.State.READY);
            var delayed=worker.submit(()->service.status(fence,Set.of("serve.out")));assertThat(captured.await(3,TimeUnit.SECONDS)).isTrue();
            current.set(new ValidationPipelineInitials.Attempt(17,22));factory.beginRun(member,new SinkAckFactory.ExecutionWriters(17,22,roster),claim());service.started(member,fence,17,22,"orders",0,"NO_INITIAL_LOAD",0);factory.resolve(member).forWriter("serve.out#0").started(17,22,claim());
            await(()->service.status(fence,Set.of("serve.out")).state()==ValidationInitialLoadState.State.READY);
            release.countDown();delayed.get(3,TimeUnit.SECONDS);
            assertThat(service.status(fence,Set.of("serve.out")).state()).isEqualTo(ValidationInitialLoadState.State.READY);
        }finally{release.countDown();member.shutdown();}
    }
    @Test @Timeout(40) void actualSourceAndBlockedWriterDriveReadinessWithMetricsDisabled() throws Exception {
        var configuration=config("initial-live-"+UUID.randomUUID(),"owner",null);configuration.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        var member=Hazelcast.newHazelcastInstance(configuration);var fence=fence();var buffer=new SnapshotBuffer();
        var auth=org.mockito.Mockito.mock(ExecutionAuthorization.class);org.mockito.Mockito.when(auth.require(fence)).thenReturn(claim());
        member.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY,auth);member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY,buffer);
        String ring="initial-live-buffer";buffer.declareSnapshot("flow",ring);actualWrite=new CompletableFuture<>();
        try(var service=new ValidationPipelineInitials(member,new CacheBusinessScope("cluster",1,"cache"),()->{},value->{},value->{})) {
            var frontier=new FrontierBinding(Map.of("orders","orders"));
            byte sourceAxis=frontier.axes().axisOf("orders");
            var bindings=new DagBindings(source->SrsSourceProcessor.snapshotOnlyMetaSupplier("flow",ring,"orders",7,
                    order->new com.hazelcast.jet.core.Watermark(FrontierOrders.pack("orders",order),sourceAxis),SourcePlacement.anyMember()),
                    step->()->event->List.of(event),sync->BlockedWriter::new,from->List.of("orders"));
            var pipeline=new PipelineResource("flow",null,List.of(SourceRef.bare("orders")),null,null,
                    new ServeBlock.Inline(null,FromRef.literal("orders"),List.of(new SyncElement("out","target",null,null,null)),null,null),null,null);
            var observed=FencedSinkAckFactory.heldTo(service.decorate(fence,SinkAckFactory.NONE),fence);
            var dag=PipelineDagBuilder.build(pipeline,bindings,observed,frontier);
            var job=member.getJet().newJob(dag,new com.hazelcast.jet.config.JobConfig().setName("flow").setMetricsEnabled(false).setArgument(Engine.RUN_IDENTITY_ARGUMENT,fence));
            try {
                buffer.append("flow",ring,Envelope.read(0,"orders",Map.of("id",1),null).withOrder(SourceOrder.snapshotRow(7)));buffer.endSnapshot("flow",ring);
                await(()->"initial.snapshot-landing".equals(service.status(fence,Set.of("serve.out")).reason()));
                assertThat(job.getMetrics().metrics()).isEmpty();assertThat(JetLiveExecution.current(member,job.getId())).isPresent();
                assertThat(service.status(fence,Set.of("serve.out")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
                actualWrite.complete(new WriteResult(1));
                await(()->service.status(fence,Set.of("serve.out")).state()==ValidationInitialLoadState.State.READY);
                job.getFuture().get(5,TimeUnit.SECONDS);
            }finally{actualWrite.complete(new WriteResult(1));job.cancel();}
        }finally{member.getUserContext().remove(ExecutionAuthorization.USER_CONTEXT_KEY);member.getUserContext().remove(SnapshotBuffer.USER_CONTEXT_KEY);member.shutdown();}
    }
    private static final class BlockedWriter implements SinkWriter {
        @Override public java.util.concurrent.CompletionStage<WriteResult> write(List<Envelope> rows){return actualWrite;}
        @Override public void close(){}
    }
    @Test @Timeout(40) void remoteReadDoesNotWaitForAChildLookupQueuedBehindItself() throws Exception {
        String cluster="initial-remote-pool-"+UUID.randomUUID();var first=config(cluster,"owner",null);
        first.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);first.getExecutorConfig(ValidationPipelineInitials.EXECUTOR).setPoolSize(1);
        var a=Hazelcast.newHazelcastInstance(first);HazelcastInstance b=null;
        try {
            var address=a.getCluster().getLocalMember().getAddress();var second=config(cluster,"writer",address.getHost()+":"+address.getPort());second.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);b=Hazelcast.newHazelcastInstance(second);
            HazelcastInstance remote=b;await(()->a.getCluster().getMembers().size()==2&&remote.getCluster().getMembers().size()==2);
            var fence=fence();var scope=new CacheBusinessScope("cluster",1,"cache");
            try(var owner=new ValidationPipelineInitials(a,scope,()->{},value->{},value->{});var requester=new ValidationPipelineInitials(b,scope,()->{},value->{},value->{})) {
                var dag=new com.hazelcast.jet.core.DAG();dag.newVertex("hold",com.hazelcast.jet.core.ProcessorMetaSupplier.of(com.hazelcast.jet.core.ProcessorSupplier.of(Hold::new))).localParallelism(1);
                var job=a.getJet().newJob(dag,new com.hazelcast.jet.config.JobConfig().setName("flow").setAutoScaling(false).setArgument(Engine.RUN_IDENTITY_ARGUMENT,fence));
                try {
                    await(()->JetLiveExecution.current(a,job.getId()).isPresent());long execution=JetLiveExecution.current(a,job.getId()).getAsLong();
                    SinkAckFactory ordinary=m->(stream,position)->{};var factory=owner.decorate(fence,ordinary);
                    var roster=new SinkAckFactory.RunWriters(Map.of("orders",List.of("serve.out#0")),Map.of("serve.out",Map.of("orders",List.of("serve.out#0"))));
                    factory.beginRun(a,new SinkAckFactory.ExecutionWriters(job.getId(),execution,roster),claim());owner.started(a,fence,job.getId(),execution,"orders",0,"NO_INITIAL_LOAD",0);factory.resolve(a).forWriter("serve.out#0").started(job.getId(),execution,claim());
                    await(()->owner.status(fence,Set.of("serve.out")).state()==ValidationInitialLoadState.State.READY);
                    assertThat(requester.read(fence,Set.of("serve.out")).state()).isEqualTo(ValidationInitialLoadState.State.READY);
                }finally{job.cancel();}
            }
        }finally{if(b!=null)b.shutdown();a.shutdown();}
    }
    private static final class Hold extends com.hazelcast.jet.core.AbstractProcessor { @Override public boolean complete(){return false;} }
    private static WorkloadClaimFence claim() { return new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"), new WorkloadOwner("owner", "owner-boot"), 1, 1, 1); }
    private static ExecutionFence fence() { return new ExecutionFence(new PipelineIntentScope("cluster", 1, "cache", "flow", 1), claim()); }
    private static Config config(String cluster, String node, String seed) {
        var config = new Config().setClusterName(cluster);
        config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE, node).setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE, node + "-boot");
        config.addExecutorConfig(new ExecutorConfig(ValidationPipelineInitials.EXECUTOR).setPoolSize(2).setQueueCapacity(32));
        config.addExecutorConfig(new ExecutorConfig(ValidationPipelineInitials.LOOKUP_EXECUTOR).setPoolSize(2).setQueueCapacity(32));
        config.setProperty("hazelcast.logging.type", "none").setProperty("hazelcast.phone.home.enabled", "false").setProperty("hazelcast.shutdownhook.enabled", "false")
                .setProperty("hazelcast.operation.thread.count", "2").setProperty("hazelcast.operation.generic.thread.count", "2")
                .setProperty("hazelcast.io.input.thread.count", "1").setProperty("hazelcast.io.output.thread.count", "1").setProperty("hazelcast.partition.count", "17");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = config.getNetworkConfig().getJoin(); join.getMulticastConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true); if (seed != null) join.getTcpIpConfig().addMember(seed);
        return config;
    }
    private static void await(BooleanSupplier ready) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(12); while (!ready.getAsBoolean() && System.nanoTime() - until < 0) Thread.sleep(10); assertThat(ready.getAsBoolean()).isTrue(); }
}
