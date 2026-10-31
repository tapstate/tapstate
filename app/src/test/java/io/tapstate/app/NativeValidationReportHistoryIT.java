package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Existing native CONFIG/CACHE originals, with no business readers or resumed Job. */
@RequiresDocker
class NativeValidationReportHistoryIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @Test void aDamagedTaskCannotStarveTheNextHealthyHistory()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var healthy=new ValidationReportAdmission.Task("cluster","flow","z-healthy",UUID.randomUUID());
            f.tasks.create(new ValidationTaskStore.Create(healthy,f.comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,1000),UUID.randomUUID()));
            var access=f.configuration.loadValidationAccess("flow",f.scope,f.cache,()->{});var original=ended(f,access,accepted.owner(),healthy,1,1);
            f.raw.getDatabase(f.configName).getCollection("validation_tasks").updateOne(new Document("_id.taskId","task"),new Document("$set",new Document("revision",0L)));
            try(var history=new NativeValidationReportHistory(f.configuration,f.cache,f.scope,()->WorkloadClaimFence.from(f.node),()->{},f.resources,f.process,f.owners,Clock.systemUTC())){
                var reference=new ValidationReports.Reference("flow",healthy.taskId(),ValidationMode.FULL,original.identity().runId(),healthy.generation(),1);
                await(()->access.reports().find(reference).orElseThrow().terminal()==ValidationRunState.COMPLETED);
                assertThat(f.raw.getDatabase(f.configName).getCollection("validation_tasks").find(new Document("_id.taskId","task")).first().getLong("revision")).isZero();
                assertThat(f.member.getJet().getJobs()).isEmpty();
            }
        }
    }
    @Test void expiredLocalOriginalLeavesSiblingQuotaOpenAndItsReservationReapable()throws Exception{
        var blocked=new java.util.concurrent.atomic.AtomicBoolean();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(64,2);var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers)){
            var saved=f.tasks.read("flow","task").orElseThrow();
            f.tasks.update(new ValidationTaskStore.Update(f.task,1,f.comparison,new ValidationTaskStore.Settings(
                    new ValidationTaskStore.Full(ValidationMethod.COUNT,1,null,false,1),saved.settings().incremental()),UUID.randomUUID()));
            var sibling=new ValidationReportAdmission.Task("cluster","flow","sibling",UUID.randomUUID());
            f.tasks.create(new ValidationTaskStore.Create(sibling,f.comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),UUID.randomUUID()));
            var first=waiting(f,runs,f.task,2,ValidationMethod.COUNT);var second=waiting(f,runs,sibling,1,ValidationMethod.FULL_COLUMN);
            assertThat(second.identity().owner()).isEqualTo(first.identity().owner());assertThat(second.countingGeneration()).isEqualTo(first.countingGeneration());
            f.localGuard=()->{if(blocked.get()&&StackWalker.getInstance().walk(frames->frames.anyMatch(frame->
                    frame.getClassName().endsWith("ValidationRunLifecycle")&&frame.getMethodName().equals("advance")
                    ||frame.getClassName().endsWith("NativeValidationOwnerReservations$Reservation")&&frame.getMethodName().equals("retainedTerminal"))))
                throw new io.tapstate.core.common.TapstateException(ValidationError.READ_FAILED,Map.of("table","lost-retirement-reply"),null);};
            blocked.set(true);runs.stop(first.identity());await(()->runs.read(first.identity()).controlUnknown());
            var access=f.configuration.loadValidationAccess("flow",f.scope,f.cache,()->{});var control=access.control();
            control.transition(new ValidationControlStore.Transition(first,ValidationRunState.STARTING,ValidationRunState.STOPPING));
            access.admissions().close(first.admission());control.transition(new ValidationControlStore.Transition(first,ValidationRunState.STOPPING,ValidationRunState.STOPPED));
            control.retire(new ValidationControlStore.Retirement(first,first.report(),ValidationRunState.STOPPED));
            ended(f,access,first.identity().owner(),f.task,2,2);
            NativeValidationReportHistory.reconcile(access,WorkloadClaimFence.from(f.node),f.tasks.read("flow","task").orElseThrow(),Clock.systemUTC(),
                    known->control.expire(new ValidationControlStore.Expiration(first.identity().owner(),known)));
            assertThat(access.history().expired(WorkloadClaimFence.from(f.node),first)).isTrue();blocked.set(false);
            await(()->{try{runs.read(first.identity());return false;}catch(io.tapstate.core.common.TapstateException removed){return true;}});
            var root=new ValidationRoot("mapping","out",new ValidationKey(List.of(ValidationKey.Part.number(1))),List.of());
            assertThat(runs.reserveConfirmed(second.identity(),root).orElseThrow().permit()).isPresent();
            await(()->f.resources.snapshot().metadataFrames()==2);assertThat(runs.read(second.identity()).waiting()).isTrue();
            var third=new ValidationReportAdmission.Task("cluster","flow","third",UUID.randomUUID());
            f.tasks.create(new ValidationTaskStore.Create(third,f.comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,1000),UUID.randomUUID()));
            var request=new ValidationOwnerRouter.Request("flow",third.taskId(),third.generation(),1,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
            var reserved=new java.util.concurrent.atomic.AtomicReference<ValidationOwnerRouter.Result>();
            // The live sibling may own this Pipeline's nonblocking control turn. Acquire the turn,
            // then reserve exactly once; a reservation refusal must not be retried as success.
            await(()->reserved.get()!=null||f.owners.control("flow",()->reserved.set(f.owners.reserve(request))));
            assertThat(reserved.get().outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            runs.stop(second.identity());await(()->f.resources.snapshot().processors()==1);
        }finally{blocked.set(false);}
    }
    @Test void rebuiltContextMirrorsPendingFactsAndNextOwnerExpiresBothDomainsWithoutRebirth()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var old=f.tasks.read("flow","task").orElseThrow();
            var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,null,false,1),old.settings().incremental());
            f.tasks.update(new ValidationTaskStore.Update(f.task,1,f.comparison,settings,UUID.randomUUID()));
            var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
            var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var access=f.configuration.loadValidationAccess("flow",f.scope,f.cache,()->{});var originals=new ArrayList<ValidationControlStore.Run>();
            var owner=accepted.owner();
            for(int sequence=1;sequence<=3;sequence++){
                originals.add(ended(f,access,owner,sequence));
                if(sequence<3){
                    var claims=access.claims();var key=ValidationClaims.key("cluster","flow");var held=claims.read(key).orElseThrow().claim();
                    assertThat(claims.release(held)).isTrue();var next=claims.acquire(key,held.owner(),1,Duration.ofMinutes(5));assertThat(next.acquired()).isTrue();
                    owner=ValidationClaims.owner("flow",claims.advanceExecution(next.claim(),1,Set.of("node")).orElseThrow());
                }
            }
            var pending=access.reports().history("flow","task",ValidationMode.FULL,new ValidationReports.Paging(0,10));
            assertThat(pending.items()).hasSize(3).allSatisfy(report->assertThat(report.terminal()).isNull());
            f.owners.close();f.resources.close();
            var held=access.claims().read(ValidationClaims.key("cluster","flow")).orElseThrow().claim();access.claims().release(held);
            var fresh=f.process.bind("cluster","node","boot",4,()->{});
            try(var replacement=f.owner(fresh);var history=new NativeValidationReportHistory(f.configuration,f.cache,f.scope,
                    ()->WorkloadClaimFence.from(f.node),()->{},fresh,f.process,replacement,Clock.systemUTC())){
                await(()->access.reports().history("flow","task",ValidationMode.FULL,new ValidationReports.Paging(0,10)).items().size()==1);
                var kept=access.reports().history("flow","task",ValidationMode.FULL,new ValidationReports.Paging(0,10)).items().getFirst();
                assertThat(kept.reference().runId()).isEqualTo("original-3");assertThat(kept.terminal()).isEqualTo(ValidationRunState.COMPLETED);
                assertThat(kept.endedAt()).isNotNull();assertThat(f.member.getJet().getJobs()).isEmpty();
                assertThat(f.raw.getDatabase(f.cacheName).getCollection("validation_runs").countDocuments(new Document("_id.kind","run"))).isEqualTo(3);
                assertThat(access.claims().read(ValidationClaims.key("cluster","flow")).orElseThrow().leased()).isFalse();
                var next=replacement.reserve(new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,4,"FULL",UUID.randomUUID(),UUID.randomUUID().toString()));
                assertThat(next.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                assertThat(access.control().snapshot(next.owner()).retainedRuns()).isOne();
                assertThat(access.control().snapshot(next.owner()).countingGeneration()).isEqualTo(3);
                assertThat(f.member.getJet().getJobs()).isEmpty();
                for(int index=0;index<2;index++){
                    var original=originals.get(index);assertThat(access.history().expired(WorkloadClaimFence.from(f.node),original)).isTrue();
                    assertThat(access.admissions().open(new ValidationReportAdmission.Open(original.admission(),original.snapshot())))
                            .isIn(ValidationReportAdmission.Birth.CLOSED,ValidationReportAdmission.Birth.STALE);
                    assertThat(access.reports().mirror(new ValidationReports.TerminalMirror(new ValidationControlStore.Status(original,ValidationRunState.COMPLETED,true,kept.endedAt()),Instant.now(),null,null)))
                            .isEqualTo(ValidationReports.Mirror.CLOSED_OR_EXPIRED);
                }
                assertThat(fresh.snapshot().processors()).isOne();
            }finally{fresh.close();}
        }
    }
    private static ValidationControlStore.Run ended(NativeValidationOwnerReservationsIT.Fixture f,
            io.tapstate.adapters.mongostore.MongoConfigurationDomain.ValidationAccess access,ValidationOwner owner,int sequence){
        return ended(f,access,owner,f.task,2,sequence);
    }
    private static ValidationControlStore.Run ended(NativeValidationOwnerReservationsIT.Fixture f,
            io.tapstate.adapters.mongostore.MongoConfigurationDomain.ValidationAccess access,ValidationOwner owner,ValidationReportAdmission.Task task,long revision,int sequence){
        var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
        var identity=new ValidationRunIdentity(owner,task.taskId(),"original-"+sequence,ValidationMode.FULL,spec.fingerprint(),1,"rules");
        var start=new ValidationReportAdmission.Start(task,revision,sequence,UUID.randomUUID().toString(),identity);
        assertThat(access.admissions().reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
        var ledger=access.control().snapshot(owner);var original=new ValidationControlStore.Run(identity,ledger.countingGeneration()+(ledger.closed()?1:0),start,spec);
        assertThat(access.control().join(original)).isEqualTo(ValidationControlStore.Join.JOINED);
        assertThat(access.admissions().open(new ValidationReportAdmission.Open(start,spec))).isEqualTo(ValidationReportAdmission.Birth.CREATED);
        var read=new ValidationCheckEvent.ReadBoundary("count","v1",Instant.EPOCH,Instant.EPOCH);
        access.results().publishCount(new ValidationResultStore.CountWrite(start.report(),spec,new ValidationCountCheck(identity,"mapping","out",1,1,read,read)));
        access.control().transition(new ValidationControlStore.Transition(original,ValidationRunState.STARTING,ValidationRunState.RUNNING));
        access.admissions().close(start);
        access.control().transition(new ValidationControlStore.Transition(original,ValidationRunState.RUNNING,ValidationRunState.COMPLETED));
        access.control().retire(new ValidationControlStore.Retirement(original,start.report(),ValidationRunState.COMPLETED));return original;
    }
    private static ValidationControlStore.Run waiting(NativeValidationOwnerReservationsIT.Fixture f,NativeValidationRuns runs,
            ValidationReportAdmission.Task task,long revision,ValidationMethod method)throws Exception{
        var request=new ValidationOwnerRouter.Request("flow",task.taskId(),task.generation(),revision,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
        var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
        var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,method);
        var identity=new ValidationRunIdentity(accepted.owner(),task.taskId(),request.run(),ValidationMode.FULL,spec.fingerprint(),1,"rules");
        var start=new ValidationReportAdmission.Start(task,revision,1,request.command().toString(),identity);var reservation=f.owners.take(start);
        var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);
        runs.start(start,new ValidationRunLifecycle.Activation(){public ValidationRunLifecycle.Readiness readiness(){return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;}
            public ValidationRunLifecycle.Execution create(){throw new AssertionError("waiting cannot start a Job");}});
        await(()->runs.read(identity).waiting());return original;
    }
    private static void await(java.util.function.BooleanSupplier condition)throws Exception{
        long until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
        while(!condition.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(20);assertThat(condition.getAsBoolean()).isTrue();
    }
}
