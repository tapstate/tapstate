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
    @Test void nativeInitialWaitOwnsOnlyControlAndUpgradesBeforeCreatingExecution()throws Exception{
        var ready=new AtomicReference<>(ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD);var created=new java.util.concurrent.atomic.AtomicInteger();var released=new java.util.concurrent.atomic.AtomicBoolean();
        var reader=new AtomicReference<ValidationLocalBudget.Reservation>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.startup=(saved,spec)->new NativeValidationOwnerReservations.StartupGate(true,ready::get);
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);var reservation=f.owners.take(start);
            runs.start(start,new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}
                public ValidationRunLifecycle.Execution create(){created.incrementAndGet();assertThat(f.resources.snapshot().processReserved().connections()).isEqualTo(2);assertThat(f.resources.snapshot().processors()).isOne();
                    reader.set(reservation.resources().budget().reserve(new ValidationLocalBudget.Amount(1024,0,1,1)));
                    return new ValidationRunLifecycle.Execution(){public ValidationRunIdentity identity(){return identity;}public void start(){}public ValidationRunLifecycle.Progress progress(){return new ValidationRunLifecycle.Progress(false,true,released.get(),null);}public void cancel(){}};
                }
            });
            await(()->runs.read(identity).waiting());assertThat(created).hasValue(0);assertThat(f.resources.snapshot().processors()).isZero();
            assertThat(f.resources.snapshot().processReserved()).isEqualTo(new ValidationLocalBudget.Amount(65536,1,0,0));assertThat(f.member.getJet().getJobs()).isEmpty();
            ready.set(ValidationRunLifecycle.Readiness.UNKNOWN);await(()->runs.read(identity).controlUnknown());assertThat(created).hasValue(0);assertThat(f.resources.snapshot().processReserved().reads()).isZero();
            ready.set(ValidationRunLifecycle.Readiness.READY);await(()->created.get()==1&&runs.read(identity).state()==ValidationRunState.RUNNING);
            runs.stop(identity);await(()->f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
            assertThat(f.resources.snapshot().processReserved().bytes()).isEqualTo(32L*1024*1024);reader.get().close();released.set(true);
            await(()->f.resources.snapshot().metadataFrames()==0);assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
        }finally{if(reader.get()!=null)reader.get().close();released.set(true);}
    }
    @Test void uncertaintyOnTheFinalReadinessCheckKeepsControlOnlyResourcesAndTheSameRun()throws Exception{
        var calls=new java.util.concurrent.atomic.AtomicInteger();var resume=new java.util.concurrent.atomic.AtomicBoolean();var created=new java.util.concurrent.atomic.AtomicInteger();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.startup=(saved,spec)->new NativeValidationOwnerReservations.StartupGate(true,()->calls.incrementAndGet()==1||resume.get()?ValidationRunLifecycle.Readiness.READY:ValidationRunLifecycle.Readiness.UNKNOWN);
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            runs.start(start,new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}
                public ValidationRunLifecycle.Execution create(){created.incrementAndGet();return new ValidationRunLifecycle.Execution(){
                    public ValidationRunIdentity identity(){return identity;}public void start(){}public void cancel(){}
                    public ValidationRunLifecycle.Progress progress(){return new ValidationRunLifecycle.Progress(false,true,true,null);}
                };}
            });
            await(()->runs.read(identity).controlUnknown());var unknown=runs.read(identity);
            assertThat(unknown.state()).isEqualTo(ValidationRunState.STARTING);assertThat(unknown.retired()).isFalse();assertThat(created).hasValue(0);
            assertThat(f.resources.snapshot().processReserved()).isEqualTo(new ValidationLocalBudget.Amount(65536,1,0,0));assertThat(f.resources.snapshot().processors()).isZero();
            resume.set(true);await(()->runs.read(identity).state()==ValidationRunState.RUNNING);assertThat(created).hasValue(1);
            runs.stop(identity);await(()->f.resources.snapshot().metadataFrames()==0);
        }
    }
    @Test void readyCapacityRefusalEndsTheAlreadyChosenOwnerWithoutCreatingExecution()throws Exception{
        var ready=new java.util.concurrent.atomic.AtomicBoolean();var created=new java.util.concurrent.atomic.AtomicInteger();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.startup=(saved,spec)->new NativeValidationOwnerReservations.StartupGate(true,()->ready.get()?ValidationRunLifecycle.Readiness.READY:ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD);
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);var reservation=f.owners.take(start);
            var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);var control=reservation.access().control();
            runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}public ValidationRunLifecycle.Execution create(){created.incrementAndGet();throw new AssertionError("unadmitted execution");}});
            await(()->runs.read(identity).waiting());
            var blockerIdentity=new ValidationRunIdentity(identity.owner(),"blocker",UUID.randomUUID().toString(),ValidationMode.FULL,"blocker",1,"rules");
            var blocker=f.resources.admit(blockerIdentity,new ValidationLocalBudget.Amount(1024*1024,16,16,16),1,()->{});
            try{ready.set(true);await(()->control.read(identity.owner(),original).map(value->value.retired()&&value.state()==ValidationRunState.FAILED).orElse(false));
                assertThat(created).hasValue(0);assertThat(control.read(identity.owner(),original).orElseThrow().run().identity().owner()).isEqualTo(accepted.owner());
                await(()->f.resources.snapshot().metadataFrames()==0);assertThat(f.resources.snapshot().processReserved().connections()).isEqualTo(16);assertThat(f.member.getJet().getJobs()).isEmpty();
            }finally{blocker.close();blocker.processorsExited();}
        }
    }
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
    @Test void terminalMirrorRetriesKeepOnlyBoundedMetadataAndDoNotHoldTheFullSlot()throws Exception{
        var blocked=new java.util.concurrent.atomic.AtomicBoolean(true);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.localGuard=()->{if(blocked.get()&&StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().contains("NativeValidationRuns$Actor")&&frame.getMethodName().equals("mirror"))))
                throw new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","report-fixture"),null);};
            var starts=new ArrayList<ValidationReportAdmission.Start>();var controls=new ArrayList<ValidationControlStore>();var originals=new ArrayList<ValidationControlStore.Run>();var ends=new ArrayList<java.time.Instant>();
            for(int sequence=1;sequence<=2;sequence++){
                var request=f.request("FULL",sequence);var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
                var start=new ValidationReportAdmission.Start(f.task,1,sequence,request.command().toString(),identity);starts.add(start);
                var reservation=f.owners.take(start);var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);originals.add(original);controls.add(reservation.access().control());
                runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}public ValidationRunLifecycle.Execution create(){throw new AssertionError("waiting cannot create a Job");}});
                await(()->runs.read(identity).waiting());runs.stop(identity);
                await(()->controls.getLast().read(identity.owner(),original).map(ValidationControlStore.Status::retired).orElse(false));
                await(()->f.resources.snapshot().processReserved().isEmpty());assertThat(f.resources.snapshot().processors()).isZero();
                ends.add(controls.getLast().read(identity.owner(),original).orElseThrow().endedAt());
            }
            assertThat(f.resources.snapshot().metadataFrames()).isEqualTo(2);assertThat(f.member.getJet().getJobs()).isEmpty();
            blocked.set(false);await(()->f.resources.snapshot().metadataFrames()==0);
            var reports=f.configuration.validationReports(()->{});
            for(int index=0;index<starts.size();index++){
                var start=starts.get(index);var reference=new ValidationReports.Reference("flow","task",ValidationMode.FULL,start.run().runId(),f.task.generation(),start.sequence());
                var report=reports.find(reference).orElseThrow();assertThat(report.terminal()).isEqualTo(ValidationRunState.STOPPED);
                assertThat(report.endedAt()).isEqualTo(ends.get(index));
                assertThat(report.window()).isNull();
            }
        }finally{blocked.set(false);}
    }
    @Test void nativeActivationFailureRetainsTheRedactedFirstCauseAndObservationWindow()throws Exception{
        var redactor=new io.tapstate.core.logging.SecretRedactor();redactor.replace("input",List.of("sentinel-password"));
        var clock=java.time.Clock.fixed(java.time.Instant.EPOCH.plusSeconds(10),java.time.ZoneOffset.UTC);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers,clock,redactor)){
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}
                public ValidationRunLifecycle.Execution create(){redactor.remove("input");throw new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","orders"),new IllegalArgumentException("sentinel-password"));}});
            var reports=f.configuration.validationReports(()->{});var reference=new ValidationReports.Reference("flow","task",ValidationMode.FULL,identity.runId(),f.task.generation(),1);
            await(()->reports.find(reference).map(report->report.terminal()==ValidationRunState.FAILED&&report.failure()!=null).orElse(false));
            await(()->f.resources.snapshot().metadataFrames()==0);var report=reports.find(reference).orElseThrow();
            assertThat(report.failure().code()).isEqualTo(ValidationError.READ_FAILED.code());assertThat(report.failure().technical()).contains("IllegalArgumentException","********").doesNotContain("sentinel-password");
            assertThat(report.window()).isEqualTo(new ValidationReportMetadata.Window(clock.instant(),clock.instant()));
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();assertThat(f.member.getJet().getJobs()).isEmpty();
        }
    }

    @Test void contextDisposalReleasesPendingLocalFramesAfterDrainAndKeepsDurableFailureFacts()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.localGuard=()->{if(StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().contains("NativeValidationRuns$Actor")&&frame.getMethodName().equals("mirror"))))
                throw new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","report-fixture"),null);};
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            var reservation=f.owners.take(start);var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);var control=reservation.access().control();
            runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}
                public ValidationRunLifecycle.Execution create(){throw new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","orders"),null);}});
            await(()->control.read(identity.owner(),original).map(ValidationControlStore.Status::retired).orElse(false));
            await(()->f.resources.snapshot().processReserved().isEmpty());assertThat(f.resources.snapshot().metadataFrames()).isOne();
            var reports=f.configuration.validationReports(()->{});var reference=new ValidationReports.Reference("flow","task",ValidationMode.FULL,identity.runId(),f.task.generation(),1);
            var pending=reports.find(reference).orElseThrow();assertThat(pending.closed()).isTrue();assertThat(pending.terminal()).isNull();assertThat(pending.failure().code()).isEqualTo(ValidationError.READ_FAILED.code());
            f.owners.close();runs.close();await(()->f.resources.snapshot().metadataFrames()==0);
            assertThat(reports.find(reference).orElseThrow().failure()).isEqualTo(pending.failure());assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
        }
    }

    @Test void aLostTerminalReadCanBeRecoveredUnderTheNextTrustedTenure()throws Exception{
        var blocked=new java.util.concurrent.atomic.AtomicBoolean(true);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.localGuard=()->{if(blocked.get()&&StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().contains("NativeValidationOwnerReservations$Reservation")&&frame.getMethodName().equals("retainedTerminal"))))
                throw new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","terminal-receipt-fixture"),null);};
            ValidationOwner previous=null;var starts=new ArrayList<ValidationReportAdmission.Start>();
            for(int sequence=1;sequence<=2;sequence++){
                var request=f.request("FULL",sequence);var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                if(previous!=null)assertThat(accepted.owner().claimGeneration()).isGreaterThan(previous.claimGeneration());previous=accepted.owner();
                var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
                var start=new ValidationReportAdmission.Start(f.task,1,sequence,request.command().toString(),identity);starts.add(start);var reservation=f.owners.take(start);
                var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);var control=reservation.access().control();
                runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}public ValidationRunLifecycle.Execution create(){throw new AssertionError("waiting cannot start a Job");}});
                await(()->runs.read(identity).waiting());runs.stop(identity);await(()->control.read(identity.owner(),original).map(ValidationControlStore.Status::retired).orElse(false));
            }
            blocked.set(false);
            try{await(()->f.resources.snapshot().metadataFrames()==0);}catch(AssertionError failed){
                var details=new ArrayList<Object>();details.add(f.resources.snapshot());
                for(var start:starts){try{details.add(runs.read(start.run()));details.add(runs.reportFailure(start.run()));}catch(io.tapstate.core.common.TapstateException removed){details.add("removed");}}
                failed.addSuppressed(new AssertionError(details.toString()));throw failed;
            }
            var reports=f.configuration.validationReports(()->{});
            for(var start:starts)assertThat(reports.find(new ValidationReports.Reference("flow","task",ValidationMode.FULL,start.run().runId(),f.task.generation(),start.sequence())).orElseThrow()).satisfies(report->{assertThat(report.terminal()).isEqualTo(ValidationRunState.STOPPED);assertThat(report.failure()).isNull();});
        }finally{blocked.set(false);}
    }
    @Test void blockingConfigurationMirrorDoesNotHoldThePipelineControlLock()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            f.localGuard=()->{if(StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().contains("NativeValidationRuns$Actor")&&frame.getMethodName().equals("mirror")))){
                entered.countDown();try{if(!release.await(8,TimeUnit.SECONDS))throw new AssertionError("mirror barrier timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}};
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}public ValidationRunLifecycle.Execution create(){throw new AssertionError("waiting cannot start a Job");}});
            await(()->runs.read(identity).waiting());runs.stop(identity);assertThat(entered.await(4,TimeUnit.SECONDS)).isTrue();
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
            var nextRequest=f.request("FULL",2);var next=f.owners.reserve(nextRequest);assertThat(next.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var nextIdentity=new ValidationRunIdentity(next.owner(),"task",nextRequest.run(),spec.mode(),spec.fingerprint(),1,"rules");
            f.owners.take(new ValidationReportAdmission.Start(f.task,1,2,nextRequest.command().toString(),nextIdentity)).close();
            release.countDown();await(()->f.resources.snapshot().metadataFrames()==0);assertThat(f.member.getJet().getJobs()).isEmpty();
        }finally{release.countDown();}
    }

    @Test void actualDrainAfterOwnerTimersCloseStillReleasesTheOldContextFrames()throws Exception{
        var exited=new java.util.concurrent.atomic.AtomicBoolean();var held=new AtomicReference<ValidationLocalBudget.Reservation>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);var reservation=f.owners.take(start);
            runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.READY;}
                public ValidationRunLifecycle.Execution create(){held.set(reservation.resources().budget().reserve(new ValidationLocalBudget.Amount(1024,0,1,1)));return new ValidationRunLifecycle.Execution(){
                    public ValidationRunIdentity identity(){return identity;}public void start(){}public void cancel(){}
                    public ValidationRunLifecycle.Progress progress(){return new ValidationRunLifecycle.Progress(false,true,exited.get(),null);}
                };}});
            await(()->held.get()!=null&&runs.read(identity).state()==ValidationRunState.RUNNING);runs.stop(identity);
            await(()->f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
            f.owners.close();runs.close();f.timers.close();await(()->runs.controlIdle(identity));assertThat(f.resources.snapshot().metadataFrames()).isGreaterThan(0);
            held.get().close();exited.set(true);await(()->f.resources.snapshot().metadataFrames()==0);
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();assertThat(f.resources.snapshot().processors()).isZero();
        }finally{if(held.get()!=null)held.get().close();exited.set(true);}
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
