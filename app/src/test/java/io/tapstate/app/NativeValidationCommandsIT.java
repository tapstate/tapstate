package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static io.tapstate.app.ValidationOwnerRouter.*;
import static org.assertj.core.api.Assertions.*;

/** Real RPC, native leases, Jet and CONFIG. Scalar plans are fixture counters, not PDK acceptance. */
@RequiresDocker
class NativeValidationCommandsIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @Test void exactActivationIsIdempotentAndAnOldStopNeverControlsTheNextRun()throws Exception{
        var ready=new AtomicBoolean();var prepared=new AtomicInteger();var counts=new AtomicInteger();var allowDrain=new AtomicBoolean();
        var counting=new java.util.concurrent.CountDownLatch(1);var releaseCount=new java.util.concurrent.CountDownLatch(1);
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var processors=new ValidationProcessorRegistry(f.member,()->{})){
            f.startup=(saved,spec)->new NativeValidationOwnerReservations.StartupGate(true,()->ready.get()?ValidationRunLifecycle.Readiness.READY:ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD);
            NativeValidationFullJob.Factory factory=original->{prepared.incrementAndGet();var closed=new AtomicBoolean();var mapping=original.spec().task().mappings().getFirst();
                return new NativeValidationFullJob.Plan(){
                    public List<ValidationCountScan.Mapping> counts(){return List.of(new ValidationCountScan.Mapping(mapping.mappingId(),guard->{guard.run();counts.incrementAndGet();counting.countDown();
                                try{if(!releaseCount.await(10,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("count gate timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                                return scalar(mapping.source(),9007199254740993L);},
                            guard->{guard.run();counts.incrementAndGet();return scalar(mapping.target(),9007199254740992L);}));}
                    public boolean drained(){return closed.get()&&allowDrain.get();}public void close(){closed.set(true);}
                };};
            var commands=new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory);
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,commands,()->{},Duration.ofSeconds(5))){
                try{
                var request=f.request("FULL",1);var reservation=router.reserve(request);assertThat(reservation.outcome()).isEqualTo(Outcome.ACCEPTED);
                var owner=owner(reservation);var activate=Command.of(Action.ACTIVATE,request,owner);
                try(var concurrent=java.util.concurrent.Executors.newFixedThreadPool(4)){
                    var replies=new ArrayList<java.util.concurrent.Future<CommandReply>>();
                    for(int index=0;index<4;index++)replies.add(concurrent.submit(()->router.command(activate)));
                    var outcomes=new ArrayList<Outcome>();for(var reply:replies)outcomes.add(reply.get(8,java.util.concurrent.TimeUnit.SECONDS).outcome());
                    assertThat(outcomes).contains(Outcome.ACCEPTED).allSatisfy(value->assertThat(value).isIn(Outcome.ACCEPTED,Outcome.UNKNOWN));
                }
                await(()->router.command(Command.of(Action.STATUS,request,owner)).waiting());
                assertThat(router.command(Command.of(Action.STATUS,request,owner)).tables()).singleElement().satisfies(table->assertThat(table.phase()).isEqualTo(ValidationRunLifecycle.ReadPhase.PENDING));
                for(int retry=0;retry<3;retry++)assertThat(router.command(activate).outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(router.command(Command.of(Action.CANCEL_UNUSED,request,owner)).outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(router.command(Command.of(Action.STATUS,request,owner)).waiting()).isTrue();
                assertThat(prepared).hasValue(0);assertThat(f.member.getJet().getJobs()).isEmpty();
                var wrong=new Request(request.pipeline(),request.task(),request.taskGeneration(),1,1,"FULL",UUID.randomUUID(),request.run());
                assertThat(router.command(Command.of(Action.ACTIVATE,wrong,owner)).outcome()).isEqualTo(Outcome.UNKNOWN);
                assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(2);
                ready.set(true);var reference=new ValidationReports.Reference("flow","task",ValidationMode.FULL,request.run(),f.task.generation(),1);
                var reports=f.configuration.validationReports(()->{});
                assertThat(counting.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThat(router.command(Command.of(Action.STATUS,request,owner)).tables()).singleElement().satisfies(table->assertThat(table.phase()).isEqualTo(ValidationRunLifecycle.ReadPhase.READING));
                releaseCount.countDown();await(()->router.command(Command.of(Action.STATUS,request,owner)).tables().stream().anyMatch(table->table.phase()==ValidationRunLifecycle.ReadPhase.READ_COMPLETE));
                assertThat(reports.find(reference).orElseThrow().terminal()).isNull();assertThat(f.resources.snapshot().processReserved().isEmpty()).isFalse();allowDrain.set(true);
                await(()->reports.find(reference).map(value->value.terminal()==ValidationRunState.COMPLETED).orElse(false));
                await(()->f.resources.snapshot().metadataFrames()==0);assertThat(prepared).hasValue(1);assertThat(counts).hasValue(2);assertThat(f.member.getJet().getJobs()).hasSize(1);
                assertThat(reports.find(reference).orElseThrow().counts()).singleElement().satisfies(value->{assertThat(value.sourceCount()).isEqualTo(9007199254740993L);assertThat(value.targetCount()).isEqualTo(9007199254740992L);});
                ready.set(false);var next=f.request("FULL",2);var nextReservation=router.reserve(next);assertThat(nextReservation.outcome()).isEqualTo(Outcome.ACCEPTED);var nextOwner=owner(nextReservation);
                assertThat(router.command(Command.of(Action.ACTIVATE,next,nextOwner)).outcome()).isEqualTo(Outcome.ACCEPTED);
                await(()->router.command(Command.of(Action.STATUS,next,nextOwner)).waiting());
                assertThat(router.command(Command.of(Action.STOP,request,owner)).outcome()).isEqualTo(Outcome.REJECTED);
                assertThat(router.command(Command.of(Action.STATUS,next,nextOwner)).waiting()).isTrue();
                assertThat(router.command(Command.of(Action.STOP,next,nextOwner)).outcome()).isEqualTo(Outcome.ACCEPTED);
                var second=new ValidationReports.Reference("flow","task",ValidationMode.FULL,next.run(),f.task.generation(),2);
                await(()->reports.find(second).map(value->value.terminal()==ValidationRunState.STOPPED).orElse(false));
                await(()->f.resources.snapshot().metadataFrames()==0);assertThat(f.member.getJet().getJobs()).hasSize(1);
                }finally{releaseCount.countDown();allowDrain.set(true);}
            }
        }
    }
    @Test void stoppingAnUnusedOriginalReservationNeverCreatesAPermissionOrJob()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            NativeValidationFullJob.Factory factory=original->{throw new AssertionError("unused stop cannot construct a plan");};
            var commands=new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory);
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,commands,()->{},Duration.ofSeconds(5))){
                var request=f.request("FULL",1);var reserved=router.reserve(request);assertThat(reserved.outcome()).isEqualTo(Outcome.ACCEPTED);var owner=owner(reserved);
                var pending=router.command(Command.of(Action.STATUS,request,owner));assertThat(pending.outcome()).isEqualTo(Outcome.ACCEPTED);assertThat(pending.admitted()).isFalse();
                var stopped=router.command(Command.of(Action.STOP,request,owner));assertThat(stopped.outcome()).isEqualTo(Outcome.ACCEPTED);assertThat(stopped.state()).isEqualTo("STOPPED");
                assertThat(stopped.admitted()).isFalse();assertThat(stopped.retired()).isFalse();assertThat(stopped.drained()).isTrue();
                assertThat(router.command(Command.of(Action.ACTIVATE,request,owner)).outcome()).isNotEqualTo(Outcome.ACCEPTED);
                assertThat(f.tasks.read("flow","task").orElseThrow().full().permission()).isNull();assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(1);
                assertThat(f.member.getJet().getJobs()).isEmpty();assertThat(f.resources.snapshot().metadataFrames()).isZero();
            }
        }
    }
    @Test void cancellingOnlyAnUnusedHandoffCannotStopActivationThatWonItsReservationMonitor()throws Exception{
        var stopped=new AtomicInteger();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{});var caller=java.util.concurrent.Executors.newSingleThreadExecutor()){
            NativeValidationFullJob.Factory factory=original->{throw new AssertionError("handoff race creates no Job");};
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                    new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory),()->{},Duration.ofSeconds(5))){
                var request=f.request("FULL",1);var reserved=router.reserve(request);var owner=owner(reserved);var start=f.owners.original(request,owner);var original=f.owners.take(start);var allocation=original.resources();
                try{
                    java.util.concurrent.Future<CommandReply> cancellation;
                    synchronized(original){
                        cancellation=caller.submit(()->router.command(Command.of(Action.CANCEL_UNUSED,request,owner)));
                        var bean=java.lang.management.ManagementFactory.getThreadMXBean();int identity=System.identityHashCode(original);long holding=Thread.currentThread().threadId();
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(10)).until(()->{
                            for(var info:bean.dumpAllThreads(false,false))if(info.getThreadState()==Thread.State.BLOCKED&&info.getLockOwnerId()==holding&&info.getLockInfo()!=null&&info.getLockInfo().getIdentityHashCode()==identity)return true;return false;
                        });
                        original.activate(stopped::incrementAndGet);
                    }
                    cancellation.get(5,java.util.concurrent.TimeUnit.SECONDS);assertThat(stopped).hasValue(0);
                }finally{original.releaseClosedContextMetadata();allocation.close();allocation.processorsExited();}
            }
        }
    }
    private static ValidationOwner owner(Reply reply){return new ValidationOwner("cluster","flow",reply.receiver().node(),reply.receiver().boot(),reply.claimGeneration(),reply.executionGeneration(),reply.topologyRevision());}
    private static ValidationCountScan.Scalar scalar(ValidationTaskSpec.Table table,long value){return new ValidationCountScan.Scalar(table,value,new ValidationCheckEvent.ReadBoundary("fixture-count","v1",Instant.EPOCH,Instant.EPOCH));}
    private static void await(java.util.function.BooleanSupplier condition)throws Exception{
        long until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while(!condition.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(20);assertThat(condition.getAsBoolean()).isTrue();
    }
}
