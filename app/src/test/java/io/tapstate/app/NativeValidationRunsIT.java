package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class NativeValidationRunsIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
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
