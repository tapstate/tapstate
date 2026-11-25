package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.jet.core.*;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

class ValidationJobExecutionTest {
    private static final Map<String,State> STATES=new ConcurrentHashMap<>();
    @Test @Timeout(35) void completedJetDoesNotInventBusinessCompletionOrReaderDrain() throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();state.finish.set(true);var execution=fixture.execution(state);execution.start();
            await(()->state.exited.get());
            assertThat(execution.progress().businessComplete()).isFalse();assertThat(execution.progress().readersReleased()).isFalse();
            state.complete.set(true);state.readers.set(true);assertThat(execution.progress().businessComplete()).isTrue();
            await(()->execution.progress().processorsExited());execution.releaseFactories();
        }
    }
    @Test @Timeout(35) void cancellationSignalsLocallyAndMaintainsOnlyTheOriginalJob() throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();var execution=fixture.execution(state);execution.start();await(()->state.entered.get());
            String original=execution.jobName();execution.cancel();assertThat(state.cancelled).isTrue();
            execution.maintain();await(()->execution.progress().processorsExited());
            assertThat(execution.jobName()).isEqualTo(original);assertThat(execution.progress().readersReleased()).isFalse();
            assertThatThrownBy(execution::releaseFactories).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            state.readers.set(true);execution.releaseFactories();
        }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    @Timeout(35) void anEnteredJetCallbackFailureSurvivesLocalStopBeforeMaintenance(boolean fatal)throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();state.callbackFailure=fatal?new AssertionError("Unexpected original Jet callback failure"):
                    new IllegalStateException("Unexpected original Jet callback failure");
            var execution=fixture.execution(state);
            try {
                execution.start();assertThat(state.callbackEntered.await(8,TimeUnit.SECONDS)).isTrue();execution.cancel();
                assertThat(state.cancelled).isTrue();state.callbackRelease.countDown();
                var job=(com.hazelcast.jet.Job)org.springframework.test.util.ReflectionTestUtils.getField(execution,"originalJob");
                assertThatThrownBy(()->job.getFuture().get(8,TimeUnit.SECONDS)).hasMessageContaining("Unexpected original Jet callback failure");
                assertThat(state.failure()).as("The Work latch must not supply this outer callback diagnosis").isNull();
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(()->assertThat(execution.progress().failure()).isNotNull());
                var observed=execution.progress().failure();
                assertThat(NativeValidationReportFailure.capture(observed,new io.tapstate.core.logging.SecretRedactor()).technical()).contains("Unexpected original Jet callback failure");
                if(fatal)assertThat(observed).isInstanceOf(Error.class);
                await(()->execution.progress().processorsExited());state.readers.set(true);execution.releaseFactories();
            }finally{state.callbackRelease.countDown();}
        }
    }
    @Test @Timeout(35) void stoppedSupplierExitWaitsForTheOriginalJobFailureReceipt(){
        try(var fixture=new Fixture()) {
            var state=new State();state.delayReceipt=true;state.readers.set(true);state.callbackFailure=new IllegalStateException("Original delayed Job failure");
            var execution=fixture.execution(state);
            try {
                execution.start();assertThat(state.callbackEntered.await(8,TimeUnit.SECONDS)).isTrue();execution.cancel();state.callbackRelease.countDown();
                assertThat(state.originalJobEnded.await(8,TimeUnit.SECONDS)).isTrue();await(()->state.exited.get());
                assertThat(state.originalFailure.get()).isNotNull();assertThat(execution.progress().processorsExited()).as("Physical supplier exit does not outrun the original Job diagnosis").isFalse();
                assertThatThrownBy(execution::releaseFactories).isInstanceOf(io.tapstate.core.common.TapstateException.class);
                state.delayedReceipt.completeExceptionally(state.originalFailure.get());await(()->execution.progress().processorsExited());
                assertThat(execution.progress().failure()).isNotNull();execution.releaseFactories();
            }catch(Exception failed){throw new AssertionError(failed);}
            finally{state.callbackRelease.countDown();state.delayedReceipt.completeExceptionally(new CancellationException("Fixture cleanup"));}
        }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    @Timeout(35) void originalFutureCarriersKeepTheirSuppressedCleanupDiagnosis(boolean fatal)throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();state.delayReceipt=true;state.readers.set(true);state.callbackFailure=new IllegalStateException("Original callback before carrier cleanup");
            var execution=fixture.execution(state);
            try {
                execution.start();assertThat(state.callbackEntered.await(8,TimeUnit.SECONDS)).isTrue();execution.cancel();state.callbackRelease.countDown();
                assertThat(state.originalJobEnded.await(8,TimeUnit.SECONDS)).isTrue();
                Throwable cleanup=fatal?new AssertionError("Original carrier cleanup failed"):
                        new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","original-carrier-cleanup"),null);
                var carrier=new com.hazelcast.jet.JetException("Original future delivery",state.originalFailure.get());carrier.addSuppressed(cleanup);
                state.delayedReceipt.completeExceptionally(carrier);var observed=execution.progress().failure();assertThat(observed).isNotNull();
                if(fatal)assertThat(observed).isSameAs(cleanup);
                else assertThat(observed.getSuppressed()).contains(cleanup);
                await(()->execution.progress().processorsExited());execution.releaseFactories();
            }finally{state.callbackRelease.countDown();state.delayedReceipt.completeExceptionally(new CancellationException("Fixture cleanup"));}
        }
    }
    @Test @Timeout(35) void aCancelledFutureCarrierStillRetainsItsCleanupFailure()throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();state.delayReceipt=true;state.finish.set(true);state.readers.set(true);var execution=fixture.execution(state);
            execution.start();assertThat(state.originalJobEnded.await(8,TimeUnit.SECONDS)).isTrue();execution.cancel();
            var cleanup=new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","original-cancelled-carrier"),null);
            var carrier=new com.hazelcast.jet.JetException("Original cancellation delivery",new CancellationException("Original stopped Job"));carrier.addSuppressed(cleanup);
            state.delayedReceipt.completeExceptionally(carrier);assertThat(execution.progress().failure()).isSameAs(cleanup);
            await(()->execution.progress().processorsExited());execution.releaseFactories();
        }
    }
    @Test @Timeout(35) void finalReaderExitPublishesItsDiagnosisInTheSameProgressSnapshot()throws Exception {
        try(var fixture=new Fixture()) {
            var released=new AtomicBoolean();var diagnosis=new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","last-reader-exit"),null);
            var state=new State(){@Override public boolean readersReleased(){released.set(true);return true;}@Override public Throwable failure(){return released.get()?diagnosis:null;}};
            state.finish.set(true);var execution=fixture.execution(state);execution.start();
            var job=(com.hazelcast.jet.Job)org.springframework.test.util.ReflectionTestUtils.getField(execution,"originalJob");job.getFuture().get(8,TimeUnit.SECONDS);
            var snapshot=execution.progress();assertThat(snapshot.readersReleased()).isTrue();assertThat(snapshot.failure()).isSameAs(diagnosis);
            await(()->execution.progress().processorsExited());execution.releaseFactories();
        }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    @Timeout(35) void aFailedOriginalJobRetainsTheLastReaderCleanupDiagnosis(boolean fatal)throws Exception {
        try(var fixture=new Fixture()) {
            var published=new AtomicBoolean();
            var primary=new io.tapstate.core.common.TapstateException(ValidationError.OWNER_NOT_AUTHORIZED,Map.of("pipeline","flow"),null);
            Throwable cleanup=fatal?new AssertionError("Late original reader cleanup"):
                    new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","late-original-reader-cleanup"),null);
            var state=new State(){
                @Override public boolean readersReleased(){if(readers.get())published.set(true);return readers.get();}
                @Override public Throwable failure(){return published.get()?cleanup:null;}
            };
            state.delayReceipt=true;state.callbackFailure=primary;var execution=fixture.execution(state);
            try {
                execution.start();assertThat(state.callbackEntered.await(8,TimeUnit.SECONDS)).isTrue();state.callbackRelease.countDown();
                assertThat(state.originalJobEnded.await(8,TimeUnit.SECONDS)).isTrue();state.delayedReceipt.completeExceptionally(primary);
                var before=execution.progress();assertThat(before.failure()).isSameAs(primary);assertThat(before.readersReleased()).isFalse();
                assertThat(published).isFalse();state.readers.set(true);
                var after=execution.progress();assertThat(after.readersReleased()).isTrue();assertThat(after.businessComplete()).isFalse();
                assertThat(after.failure()).isSameAs(fatal?cleanup:primary);
                assertThat(after.failure().getSuppressed()).containsExactly(fatal?primary:cleanup);
                for(int index=0;index<20;index++)assertThat(execution.progress().failure().getSuppressed()).hasSize(1);
                await(()->execution.progress().processorsExited());execution.releaseFactories();
            }finally{state.readers.set(true);state.callbackRelease.countDown();state.delayedReceipt.completeExceptionally(new CancellationException("Fixture cleanup"));}
        }
    }
    @Test @Timeout(35) void localStopBeforeGraphAuthorizationCreatesNoJobOrFailure()throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();state.readers.set(true);var execution=fixture.execution(state);
            var graph=(ValidationJobGraph)org.springframework.test.util.ReflectionTestUtils.getField(execution,"graph");
            var attempted=(AtomicBoolean)org.springframework.test.util.ReflectionTestUtils.getField(execution,"attempted");
            var thrown=new AtomicReference<Throwable>();var returned=new CountDownLatch(1);
            var starter=Thread.ofPlatform().daemon().unstarted(()->{try{execution.start();}catch(Throwable failed){thrown.set(failed);}finally{returned.countDown();}});
            synchronized(graph){starter.start();await(attempted::get);execution.cancel();}
            assertThat(returned.await(8,TimeUnit.SECONDS)).isTrue();assertThat(thrown.get()).as("A proven unsubmitted local cancellation is control flow").isNull();
            assertThat(fixture.member.getJet().getJobs()).isEmpty();assertThat(execution.knownNoSubmission()).isTrue();
            assertThat(execution.progress().failure()).isNull();assertThat(execution.progress().processorsExited()).isTrue();execution.releaseFactories();
        }
    }
    static class State implements ValidationJobExecution.Work {
        final AtomicBoolean finish=new AtomicBoolean(),entered=new AtomicBoolean(),exited=new AtomicBoolean(),complete=new AtomicBoolean(),readers=new AtomicBoolean(),cancelled=new AtomicBoolean();
        final CountDownLatch callbackEntered=new CountDownLatch(1),callbackRelease=new CountDownLatch(1);
        volatile Throwable callbackFailure;
        boolean delayReceipt;
        final CompletableFuture<Void> delayedReceipt=new CompletableFuture<>();
        final AtomicReference<Throwable> originalFailure=new AtomicReference<>();
        final CountDownLatch originalJobEnded=new CountDownLatch(1);
        final AtomicInteger cancelRequests=new AtomicInteger();
        public boolean businessComplete(){return complete.get();}public boolean readersReleased(){return readers.get();}public Throwable failure(){return null;}public void cancel(){cancelled.set(true);}
    }
    private static final class Probe extends AbstractProcessor {
        final String id;Probe(String id){this.id=id;}
        @Override protected void init(Context context){STATES.get(id).entered.set(true);}
        @Override public boolean complete(){
            var state=STATES.get(id);var failure=state.callbackFailure;
            if(failure!=null){
                state.callbackEntered.countDown();
                try{if(!state.callbackRelease.await(10,TimeUnit.SECONDS))throw new AssertionError("Original callback was not released");}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                if(failure instanceof Error fatal)throw fatal;throw (RuntimeException)failure;
            }
            return state.finish.get();
        }
        @Override public void close(){STATES.get(id).exited.set(true);}
    }
    private static final class Factory implements ProcessorSupplier {
        private static final long serialVersionUID=1L;final String id;Factory(String id){this.id=id;}
        public Collection<? extends Processor> get(int count){return List.of(new Probe(id));}
    }
    private static final class Fixture implements AutoCloseable {
        final HazelcastInstance member;final ValidationProcessorRegistry registry;final List<String> ids=new ArrayList<>();
        Fixture(){
            var config=new Config().setClusterName("validation-job-execution-"+UUID.randomUUID());config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
            config.setProperty("hazelcast.logging.type","none").setProperty("hazelcast.phone.home.enabled","false").setProperty("hazelcast.shutdownhook.enabled","false")
                    .setProperty("hazelcast.operation.thread.count","2").setProperty("hazelcast.operation.generic.thread.count","2").setProperty("hazelcast.io.input.thread.count","1").setProperty("hazelcast.io.output.thread.count","1").setProperty("hazelcast.partition.count","17");
            config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");var join=config.getNetworkConfig().getJoin();join.getAutoDetectionConfig().setEnabled(false);join.getMulticastConfig().setEnabled(false);join.getTcpIpConfig().setEnabled(false);
            member=Hazelcast.newHazelcastInstance(config);registry=new ValidationProcessorRegistry(member,()->{});
        }
        ValidationJobExecution execution(State state){
            String id=UUID.randomUUID().toString();ids.add(id);STATES.put(id,state);
            var identity=new ValidationRunIdentity(new ValidationOwner("cluster","flow","node","boot",1,1,1),"task",id,ValidationMode.FULL,"spec",1,"rules");
            return originalExecution(member,registry,identity,state,()->{});
        }
        public void close(){registry.close();member.shutdown();ids.forEach(STATES::remove);}
    }
    static ValidationJobExecution originalExecution(HazelcastInstance member,ValidationProcessorRegistry registry,ValidationRunIdentity identity,State state,Runnable guard){
        STATES.put(identity.runId(),state);var processors=registry.register(identity,Set.of("check"),guard);var local=member.getCluster().getLocalMember();
        var graph=new ValidationJobGraph(identity,local.getAddress(),local.getUuid(),new ValidationJobGraph.Limits(1,0,1),processors);graph.vertex("check",new Factory(identity.runId()));
        com.hazelcast.jet.JetService jet=member.getJet();
        if(state.delayReceipt){
            var actual=jet;jet=org.mockito.Mockito.mock(com.hazelcast.jet.JetService.class,org.mockito.AdditionalAnswers.delegatesTo(actual));
            org.mockito.Mockito.doAnswer(call->{
                var job=actual.newJob((com.hazelcast.jet.core.DAG)call.getArgument(0),(com.hazelcast.jet.config.JobConfig)call.getArgument(1));
                job.getFuture().whenComplete((ignored,failed)->{state.originalFailure.set(failed);state.originalJobEnded.countDown();});
                var original=org.mockito.Mockito.mock(com.hazelcast.jet.Job.class,org.mockito.AdditionalAnswers.delegatesTo(job));
                org.mockito.Mockito.doReturn(state.delayedReceipt).when(original).getFuture();
                org.mockito.Mockito.doAnswer(ignored->{state.cancelRequests.incrementAndGet();if(state.delayedReceipt.isDone())job.cancel();return null;}).when(original).cancel();
                return original;
            }).when(jet).newJob(org.mockito.ArgumentMatchers.any(com.hazelcast.jet.core.DAG.class),org.mockito.ArgumentMatchers.any(com.hazelcast.jet.config.JobConfig.class));
        }
        return new ValidationJobExecution(identity,graph,processors,jet,state,guard,()->{});
    }
    static void forgetOriginal(ValidationRunIdentity identity){STATES.remove(identity.runId());}
    private static void await(java.util.function.BooleanSupplier ready)throws Exception{long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(!ready.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(10);assertThat(ready.getAsBoolean()).isTrue();}
}
