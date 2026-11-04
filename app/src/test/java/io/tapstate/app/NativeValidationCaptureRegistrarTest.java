package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationRootHintOutbox;
import io.tapstate.runtime.srs.CaptureObservation;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeValidationCaptureRegistrarTest {
    @Test void rejectionOfAuthorityMaintenanceClosesTheBindingWithoutDroppingItsFrame()throws Exception{
        var leave=new CountDownLatch(1);var entered=new CountDownLatch(4);
        try(var f=new Fixture()){
            for(int index=0;index<4;index++)f.process.cleanup(()->{entered.countDown();try{leave.await();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}});
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();for(int index=0;index<32;index++)f.process.cleanup(()->{});
            var lease=f.registrar.register(f.start,f.execution,"logical-right","orders",1024,route->f.binding());
            try{await(()->!lease.accepting());assertThat(lease.failure()).isPresent();}
            finally{leave.countDown();}
            await(()->f.resources.snapshot().metadataFrames()==0);
        }finally{leave.countDown();}
    }
    @Test void aPostPublicationFailureRetainsTheDecoderCallbackEvenWithAnEmptyOutbox()throws Exception{
        var entered=new CountDownLatch(1);var leave=new CountDownLatch(1);
        try(var f=new Fixture();var source=Executors.newSingleThreadExecutor()){
            f.keys=(kind,before,after)->{entered.countDown();try{if(!leave.await(4,TimeUnit.SECONDS))throw new AssertionError("decoder fixture timeout");}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}return new ValidationRootHintBatch.Change(kind,null,new ValidationKey(List.of(ValidationKey.Part.number(1))));};
            org.mockito.Mockito.doAnswer(call->{call.callRealMethod();
                source.submit(()->f.listener.onBatch(List.of(Envelope.insert(0,"orders",Map.of("id",1),Map.of())),Optional.empty()));
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();f.observations.failed(f.opening,"fixture-close",refused());throw refused();
            }).when(f.observations).register(org.mockito.ArgumentMatchers.any(ValidationCaptureRoute.class),org.mockito.ArgumentMatchers.any(CaptureObservation.Context.class),org.mockito.ArgumentMatchers.any(ValidationCaptureHintTap.Binding.class),org.mockito.ArgumentMatchers.any());
            try{
                assertThatThrownBy(()->f.registrar.register(f.start,f.execution,"logical-right","orders",1024*1024,route->f.binding())).isInstanceOf(TapstateException.class);
                assertThat(f.outbox.get().drained().toCompletableFuture()).isDone();assertThat(f.resources.snapshot().metadataFrames()).isOne();
            }finally{leave.countDown();}
            await(()->f.resources.snapshot().metadataFrames()==0);
        }finally{leave.countDown();}
    }
    @Test void aSharedPhysicalOpeningAcceptsItsProvenAliasAndRetainsActualPacketOwnership()throws Exception{
        try(var f=new Fixture()){
            var lease=f.registrar.register(f.start,f.execution,"logical-right","orders",1024*1024,route->f.binding());
            f.listener.onBatch(List.of(Envelope.insert(0,"orders",Map.of("id",1),Map.of())),Optional.empty());
            assertThat(f.normal).hasValue(1);assertThat(lease.accepting()).isTrue();assertThat(f.resources.snapshot().metadataFrames()).isOne();
            var packet=f.outbox.get().poll().orElseThrow();assertThat(packet.batch().sender().sourceId()).isEqualTo("logical-right");
            lease.close();assertThat(lease.drained()).isFalse();assertThat(f.resources.snapshot().metadataFrames()).isOne();
            f.listener.onBatch(List.of(Envelope.insert(0,"orders",Map.of("id",2),Map.of())),Optional.empty());assertThat(f.normal).hasValue(2);
            f.outbox.get().settle(packet,null);await(()->f.resources.snapshot().metadataFrames()==0);assertThat(lease.drained()).isTrue();
        }
    }
    @Test void missingBirthNeverConstructsKeysAndLaterLossClosesOnlyTheValidationBinding()throws Exception{
        try(var f=new Fixture()){
            f.born.set(false);
            assertThatThrownBy(()->f.registrar.register(f.start,f.execution,"logical-right","orders",1024,route->{throw new AssertionError("unborn registration must not construct keys");})).isInstanceOf(TapstateException.class);
            assertThat(f.resources.snapshot().metadataFrames()).isZero();f.born.set(true);
            var lease=f.registrar.register(f.start,f.execution,"logical-right","orders",1024,route->f.binding());f.born.set(false);
            await(()->!lease.accepting());assertThat(lease.failure()).isPresent();await(()->f.resources.snapshot().metadataFrames()==0);
            f.listener.onBatch(List.of(Envelope.insert(0,"orders",Map.of("id",3),Map.of())),Optional.empty());assertThat(f.normal).hasValue(1);
        }
    }
    @Test void closeDuringFactoryAdmissionCannotMissTheLateOriginalRegistration()throws Exception{
        var entered=new CountDownLatch(1);var leave=new CountDownLatch(1);
        try(var f=new Fixture();var executor=Executors.newSingleThreadExecutor()){
            var pending=executor.submit(()->f.registrar.register(f.start,f.execution,"logical-right","orders",1024,route->{
                entered.countDown();try{if(!leave.await(3,TimeUnit.SECONDS))throw new AssertionError("factory fixture timeout");}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}return f.binding();}));
            try{assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();f.registrar.close();leave.countDown();
                assertThatThrownBy(()->pending.get(3,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TapstateException.class);
                await(()->f.resources.snapshot().metadataFrames()==0);
            }finally{leave.countDown();}
        }finally{leave.countDown();}
    }
    private static final class Fixture implements AutoCloseable{
        final ValidationMemberResources process=new ValidationMemberResources();final ValidationOwnerTimers timers=new ValidationOwnerTimers();
        final ValidationMemberResources.Member resources=process.bind("cluster","capture","capture-boot",4,()->{});
        final ExecutionFence execution=new ExecutionFence(new PipelineIntentScope("cluster",1,"cache","flow",1),new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("pipeline","pipeline-boot"),1,1,1));
        final WorkloadClaimFence capture=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"physical"),new WorkloadOwner("capture","capture-boot"),1,1,1);
        final ValidationRunIdentity run=new ValidationRunIdentity(new ValidationOwner("cluster","flow","validation","validation-boot",1,1,1),"task",UUID.randomUUID().toString(),ValidationMode.INCREMENTAL,"spec",1,"rules");
        final ValidationReportAdmission.Start start=new ValidationReportAdmission.Start(new ValidationReportAdmission.Task("cluster","flow","task",UUID.randomUUID()),1,1,UUID.randomUUID().toString(),run);
        final CapturePreparation preparation=new CapturePreparation(new CacheBusinessScope("cluster",1,"cache"),CapturePreparation.Mode.SHARED_SRS,"chain","physical","mc-"+"a".repeat(64),UUID.randomUUID().toString());
        final ValidationCaptureRoute route=new ValidationCaptureRoute(execution,"logical-right","orders",preparation,capture,"chain",1,1);
        final ValidationCaptureObservations observations=org.mockito.Mockito.spy(new ValidationCaptureObservations("cluster","capture","capture-boot",new ValidationCaptureObservations.Limits(1,new ValidationCaptureHintRegistry.Limits(2,65536,new ValidationCaptureHintTap.Limits(16,1,1024*1024))),false));
        final CaptureObservation.Context opening=new CaptureObservation.Context("physical",UUID.randomUUID().toString(),"chain","physical-first-alias","physical-pipeline","physical-consumer",1,0,capture,List.of("orders"));
        volatile ValidationCaptureHintTap.Keys keys=(kind,before,after)->new ValidationRootHintBatch.Change(kind,null,new ValidationKey(List.of(ValidationKey.Part.number(1))));
        final AtomicBoolean born=new AtomicBoolean(true);final AtomicInteger normal=new AtomicInteger();final AtomicReference<ValidationRootHintOutbox> outbox=new AtomicReference<>();
        final CaptureListener listener;final NativeValidationCaptureRegistrar registrar;
        Fixture(){
            listener=observations.open(opening).wrap((events,position)->normal.incrementAndGet());
            registrar=new NativeValidationCaptureRegistrar(new NativeValidationCaptureRegistrar.Authority(){
                public void run(ValidationReportAdmission.Start original,ExecutionFence production){assertThat(original).isEqualTo(start);assertThat(production).isEqualTo(execution);if(!born.get())throw refused();}
                public void capture(ValidationCaptureRoute original){assertThat(original).isEqualTo(route);}
            },(original,alias,table,paid)->route,observations,resources,process,timers);
        }
        ValidationCaptureHintTap.Binding binding(){
            var sender=new ValidationRootHintBatch.Sender(UUID.randomUUID().toString(),execution.operatorExecutionKey(),"logical-right","orders","capture","capture-boot",1,1,"route","keys","mapping","out",List.of(new ValidationRootHintBatch.Domain(ValidationKey.Kind.EXACT_NUMBER)));
            var made=new ValidationRootHintOutbox(run,sender,new ValidationRootHintOutbox.Limits(4,65536,8,32768,2,0,TimeUnit.SECONDS.toNanos(5)),()->0);outbox.set(made);
            return new ValidationCaptureHintTap.Binding(made,(kind,before,after)->keys.extract(kind,before,after));
        }
        public void close(){registrar.close();observations.close();process.close();resources.close();timers.close();}
    }
    private static TapstateException refused(){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","test.originalBirth"),null);}
    private static void await(java.util.function.BooleanSupplier fact)throws InterruptedException{long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(4);while(!fact.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(10);assertThat(fact.getAsBoolean()).isTrue();}
}
