package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import io.tapstate.runtime.engine.validation.ValidationCandidateQueue;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class NativeValidationRunsIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @Test void savedDelayIsAppliedToTheOriginalLiveQueueAndStopRetainsItsActualLease() throws Exception {
        var clock=new AtomicLong();var actual=new AtomicReference<ValidationCandidateQueue>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)) {
            var request=f.request("INCREMENTAL",1);var accepted=f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.INCREMENTAL,ValidationMethod.FULL_COLUMN);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);var reservation=f.owners.take(start);
            var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);var control=reservation.access().control();
            runs.start(start,new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}
                public ValidationRunLifecycle.Execution create(){
                    var paid=reservation.resources().budget().reserve(new ValidationLocalBudget.Amount(64*1024,0,0,0));
                    var queue=new ValidationCandidateQueue(identity,new ValidationCandidateQueue.Limits(8,64*1024,8,3,0,TimeUnit.SECONDS.toNanos(8)),clock::get);
                    queue.drained().whenComplete((ignored,failed)->paid.close());actual.set(queue);
                    return new ValidationRunLifecycle.Execution(){
                        public ValidationRunIdentity identity(){return identity;}
                        public void start(){}
                        public ValidationRunLifecycle.Progress progress(){return new ValidationRunLifecycle.Progress(false,true,queue.drained().toCompletableFuture().isDone(),null);}
                        public Optional<ValidationCandidateQueue> candidates(){return Optional.of(queue);}
                        public void cancel(){queue.close();}
                    };
                }
            });
            await(()->actual.get()!=null&&actual.get().delayState().schedulingRevision()==1&&control.delays(identity.owner(),original).applied()!=null);
            var queue=actual.get();assertThat(control.delays(identity.owner(),original).applied().schedulingRevision()).isEqualTo(1);
            var early=root(1);var later=root(2);assertThat(queue.tryOffer(List.of(early))).isEqualTo(ValidationCandidateQueue.Offer.ACCEPTED);
            var saved=f.tasks.read("flow","task").orElseThrow();
            var settings=new ValidationTaskStore.Settings(saved.settings().full(),new ValidationTaskStore.Incremental(100,10));
            assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,saved.revision(),f.comparison,settings,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            await(()->queue.delayState().schedulingRevision()==2&&control.delays(identity.owner(),original).applied()!=null&&control.delays(identity.owner(),original).applied().schedulingRevision()==2);
            assertThat(queue.tryOffer(List.of(later))).isEqualTo(ValidationCandidateQueue.Offer.ACCEPTED);clock.set(TimeUnit.MILLISECONDS.toNanos(100));
            var held=queue.poll().orElseThrow();assertThat(held.observation().root()).isEqualTo(later);assertThat(queue.poll()).isEmpty();
            saved=f.tasks.read("flow","task").orElseThrow();settings=new ValidationTaskStore.Settings(saved.settings().full(),new ValidationTaskStore.Incremental(9000,10));
            assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,saved.revision(),f.comparison,settings,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            await(()->queue.delayState().schedulingRevision()==3&&control.delays(identity.owner(),original).applied()!=null&&control.delays(identity.owner(),original).applied().schedulingRevision()==3);
            assertThat(runs.read(identity).state()).isEqualTo(ValidationRunState.RUNNING);
            runs.stop(identity);assertThat(queue.delayState().closed()).isTrue();
            await(()->f.tasks.read("flow","task").orElseThrow().incremental().permission().phase()==ValidationTaskStore.Phase.CLOSED);
            assertThat(queue.drained().toCompletableFuture()).isNotDone();assertThat(reservation.resources().budget().used().bytes()).isGreaterThanOrEqualTo(64*1024);
            queue.settle(held,ValidationCandidateQueue.Completion.SETTLED);
            await(()->f.resources.snapshot().metadataFrames()==0);assertThat(f.resources.snapshot().processors()).isZero();
            assertThat(f.member.getJet().getJobs()).isEmpty();
        }
    }
    private static ValidationRoot root(int value){return new ValidationRoot("mapping","out",new ValidationKey(List.of(ValidationKey.Part.number(value))),List.of());}
    private static void await(java.util.function.BooleanSupplier fact)throws InterruptedException{long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(!fact.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(10);assertThat(fact.getAsBoolean()).isTrue();}
    @Test void actualNativeRunRegistryWaitsCancelsAndReleasesItsSharedGeneration() throws Exception {
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)) {
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            var activation=new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}
                public ValidationRunLifecycle.Execution create(){throw new AssertionError("initial waiting cannot create a Job");}
            };
            runs.start(start,activation);
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            while(!runs.read(identity).waiting()&&System.nanoTime()-until<0)Thread.sleep(10);
            assertThat(runs.read(identity).waiting()).isTrue();assertThat(f.resources.snapshot().metadataFrames()).isEqualTo(2);
            runs.stop(identity);
            until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            while(f.tasks.read("flow","task").orElseThrow().full().permission().phase()!=ValidationTaskStore.Phase.CLOSED&&System.nanoTime()-until<0)Thread.sleep(10);
            assertThat(f.tasks.read("flow","task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.CLOSED);
            until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            while(f.resources.snapshot().metadataFrames()!=0&&System.nanoTime()-until<0)Thread.sleep(10);
            assertThat(f.resources.snapshot().metadataFrames()).isZero();assertThat(f.resources.snapshot().processors()).isZero();
            assertThat(f.member.getJet().getJobs()).isEmpty();
        }
    }
    @Test void closingDuringAnInflightReadinessCheckContinuesOriginalCleanup() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)) {
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            runs.start(start,new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){entered.countDown();try{if(!release.await(8,TimeUnit.SECONDS))throw new AssertionError("readiness gate timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}
                public ValidationRunLifecycle.Execution create(){throw new AssertionError("cancelled wait cannot activate");}
            });
            assertThat(entered.await(4,TimeUnit.SECONDS)).isTrue();runs.close();release.countDown();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            while(f.tasks.read("flow","task").orElseThrow().full().permission().phase()!=ValidationTaskStore.Phase.CLOSED&&System.nanoTime()-until<0)Thread.sleep(10);
            assertThat(f.tasks.read("flow","task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.CLOSED);
            until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(f.resources.snapshot().metadataFrames()!=0&&System.nanoTime()-until<0)Thread.sleep(10);
            assertThat(f.resources.snapshot().metadataFrames()).isZero();assertThat(f.resources.snapshot().processors()).isZero();
        }finally{release.countDown();}
    }
    @Test void closeCompetesWithAdmissionBeforeTheActorIsInserted() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var worker=Executors.newSingleThreadExecutor(task->new Thread(task,"registry-admission"))) {
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            f.localGuard=()->{if(Thread.currentThread().getName().equals("registry-admission")&&Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(frame->frame.getMethodName().equals("activate")&&frame.getClassName().contains("NativeValidationOwnerReservations"))){entered.countDown();try{if(!release.await(8,TimeUnit.SECONDS))throw new AssertionError("admission gate timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}};
            var admitted=worker.submit(()->runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}public ValidationRunLifecycle.Execution create(){throw new AssertionError("closed admission cannot execute");}}));
            var closing=new Thread(runs::close,"registry-close");
            try {
                assertThat(entered.await(4,TimeUnit.SECONDS)).isTrue();closing.start();
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(4);while(closing.isAlive()&&closing.getState()!=Thread.State.BLOCKED&&System.nanoTime()-until<0)Thread.onSpinWait();
                release.countDown();admitted.get(4,TimeUnit.SECONDS);closing.join(4000);
                until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(f.tasks.read("flow","task").orElseThrow().full().permission()==null&&System.nanoTime()-until<0)Thread.sleep(10);
                until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(f.tasks.read("flow","task").orElseThrow().full().permission().phase()!=ValidationTaskStore.Phase.CLOSED&&System.nanoTime()-until<0)Thread.sleep(10);
                assertThat(f.tasks.read("flow","task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.CLOSED);
            }finally{release.countDown();f.localGuard=()->{};try{runs.stop(identity);}catch(io.tapstate.core.common.TapstateException ended){}closing.join(4000);}
        }finally{release.countDown();}
    }
}
