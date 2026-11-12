package io.tapstate.app;

import com.hazelcast.core.OperationTimeoutException;
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
    static {
        String owner=System.getProperty("tapstate.test.resourceOwner");
        if(owner!=null){
            var directory=java.nio.file.Path.of(System.getProperty("java.io.tmpdir"),"stop-drain-mongo-"+UUID.randomUUID());
            try{
                java.nio.file.Files.createDirectories(directory.resolve("data"));
                java.nio.file.Files.createDirectories(directory.resolve("config"));
            }catch(java.io.IOException failed){throw new java.io.UncheckedIOException(failed);}
            MONGO.withLabel("io.tapstate.issue-loop.owner",owner);
            MONGO.withFileSystemBind(directory.resolve("data").toString(),"/data/db");
            MONGO.withFileSystemBind(directory.resolve("config").toString(),"/data/configdb");
        }
    }
    @Test void aLostReservationReplyCanBeReadPassivelyBeforeOnlyAnExplicitOriginalRetryActivates()throws Exception{
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
        var reservations=new AtomicInteger();var plans=new AtomicInteger();var receiptsVisible=new AtomicBoolean();
        var captured=new AtomicReference<Request>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            // Prepare the fixed native domains outside the deliberately lost receiver reply.
            f.owners.readOwner("flow");
            Local receiver=new Local(){
                public Result reserve(Request request){
                    reservations.incrementAndGet();captured.set(request);var accepted=f.owners.reserve(request);
                    assertThat(accepted.outcome()).isEqualTo(Outcome.ACCEPTED);entered.countDown();
                    try{if(!release.await(12,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("original reply barrier timeout");}
                    catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                    throw new OperationTimeoutException("lost original reservation reply");
                }
                public LocalReceipt receipt(Request request){return receiptsVisible.get()?f.owners.receipt(request):LocalReceipt.unknown();}
            };
            NativeValidationFullJob.Factory factory=original->{plans.incrementAndGet();var closed=new AtomicBoolean();var mapping=original.spec().task().mappings().getFirst();
                return new NativeValidationFullJob.Plan(){
                    public List<ValidationCountScan.Mapping> counts(){return List.of(new ValidationCountScan.Mapping(mapping.mappingId(),guard->{guard.run();return scalar(mapping.source(),1);},guard->{guard.run();return scalar(mapping.target(),1);}));}
                    public boolean drained(){return closed.get();}public void close(){closed.set(true);}
                };};
            var commands=new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory);
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,receiver,commands,()->{},Duration.ofSeconds(5));
                    var coordinator=new NativeValidationStartCoordinator(f.member,f.configuration,router,f.resources,f.process,()->{})){
                var op=new io.tapstate.runtime.probe.ValidationOperation(coordinator.context(),"flow","task",f.task.generation(),1,1,UUID.randomUUID());
                try{
                    try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    if(!entered.await(5,java.util.concurrent.TimeUnit.SECONDS)){
                        try(var response=coordinator.read(op)){throw new AssertionError("receiver not entered: original="+response.value()+"; reservations="+reservations+"; request="+captured+"; resources="+f.resources.snapshot());}
                    }
                    // The single-member route invokes reserve inline on its paid control worker.
                    release.countDown();
                    await(()->{try(var response=coordinator.read(op)){return response.value().outcome()==io.tapstate.runtime.probe.ValidationOperation.Outcome.UNKNOWN;}});
                    receiptsVisible.set(true);
                    assertThat(f.owners.receipt(captured.get()).result().outcome()).isEqualTo(Outcome.ACCEPTED);
                    try(var response=coordinator.read(op)){
                        assertThat(response.value().outcome()).isEqualTo(io.tapstate.runtime.probe.ValidationOperation.Outcome.ACCEPTED);
                        assertThat(response.value().admitted()).isFalse();assertThat(response.value().state()).isEqualTo("STARTING");
                    }
                    assertThat(reservations).hasValue(1);assertThat(plans).hasValue(0);assertThat(f.member.getJet().getJobs()).isEmpty();
                    assertThat(f.tasks.read("flow","task").orElseThrow().full().permission()).isNull();
                    try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    await(()->{try(var response=coordinator.read(op)){return "COMPLETED".equals(response.value().state())&&response.value().drain()==io.tapstate.runtime.probe.ValidationOperation.Drain.DRAINED;}});
                    assertThat(reservations).hasValue(1);assertThat(plans).hasValue(1);assertThat(f.member.getJet().getJobs()).hasSize(1);
                    assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(2);
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
    @Test void anUnusedRecoveredHandoffRemainsKnownDisposedAfterItsOwnerExpires()throws Exception{
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
        var reservations=new AtomicInteger();var visible=new AtomicBoolean();var now=new AtomicLong(System.nanoTime());var captured=new AtomicReference<Request>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(64,128,now::get);var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            f.owners.readOwner("flow");
            Local receiver=new Local(){
                public Result reserve(Request request){reservations.incrementAndGet();captured.set(request);var accepted=f.owners.reserve(request);
                    assertThat(accepted.outcome()).isEqualTo(Outcome.ACCEPTED);entered.countDown();
                    try{if(!release.await(12,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("unused reply barrier timeout");}
                    catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                    throw new OperationTimeoutException("lost unused reservation reply");}
                public LocalReceipt receipt(Request request){return visible.get()?f.owners.receipt(request):LocalReceipt.unknown();}
            };
            var commands=new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,original->{throw new AssertionError("receipt reads cannot construct a plan");});
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,receiver,commands,()->{},Duration.ofSeconds(5));
                    var coordinator=new NativeValidationStartCoordinator(f.member,f.configuration,router,f.resources,f.process,()->{})){
                var op=new io.tapstate.runtime.probe.ValidationOperation(coordinator.context(),"flow","task",f.task.generation(),1,1,UUID.randomUUID());
                try{
                    try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    await(()->entered.getCount()==0);
                    release.countDown();
                    await(()->{try(var response=coordinator.read(op)){return response.value().outcome()==io.tapstate.runtime.probe.ValidationOperation.Outcome.UNKNOWN;}});
                    visible.set(true);
                    try(var response=coordinator.read(op)){assertThat(response.value().outcome()).isEqualTo(io.tapstate.runtime.probe.ValidationOperation.Outcome.ACCEPTED);assertThat(response.value().admitted()).isFalse();}
                    now.addAndGet(Duration.ofSeconds(31).toNanos());f.owners.expire();
                    await(()->{try(var response=coordinator.read(op)){var value=response.value();return "STOPPED".equals(value.state())&&value.drain()==io.tapstate.runtime.probe.ValidationOperation.Drain.DRAINED;}});
                    try(var response=coordinator.read(op)){assertThat(response.value().admitted()).isFalse();assertThat(response.value().retired()).isFalse();assertThat(response.value().controlUnknown()).isFalse();}
                    assertThat(f.owners.receipt(captured.get()).disposed()).isTrue();assertThat(reservations).hasValue(1);assertThat(f.member.getJet().getJobs()).isEmpty();
                    assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(1);
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
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
    @Test void localMetadataPressureRefusesBeforeAdmissionThenAFreshAttemptSubmitsOneJob()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            var prepared=new AtomicInteger();
            NativeValidationFullJob.Factory factory=original->{prepared.incrementAndGet();var closed=new AtomicBoolean();var mapping=original.spec().task().mappings().getFirst();
                return new NativeValidationFullJob.Plan(){
                    public List<ValidationCountScan.Mapping> counts(){return List.of(new ValidationCountScan.Mapping(mapping.mappingId(),guard->scalar(mapping.source(),1),guard->scalar(mapping.target(),1)));}
                    public boolean drained(){return closed.get();}public void close(){closed.set(true);}
                };};
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                    new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory),()->{},Duration.ofSeconds(5))){
                try(var first=f.resources.admitMetadata();var second=f.resources.admitMetadata();var third=f.resources.admitMetadata();var fourth=f.resources.admitMetadata()){
                    var refused=router.reserve(f.request("FULL",1));assertThat(refused.outcome()).isEqualTo(Outcome.CAPACITY);
                    assertThat(f.tasks.read("flow","task").orElseThrow().full().permission()).isNull();
                    assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(1);
                    assertThat(f.owners.readOwner("flow").filter(WorkloadClaimReading::leased)).isEmpty();assertThat(f.member.getJet().getJobs()).isEmpty();assertThat(prepared).hasValue(0);
                }
                var request=f.request("FULL",1);var reserved=router.reserve(request);assertThat(reserved.outcome()).isEqualTo(Outcome.ACCEPTED);var owner=owner(reserved);
                var reference=new ValidationReports.Reference("flow","task",ValidationMode.FULL,request.run(),f.task.generation(),1);
                assertThat(router.command(Command.of(Action.ACTIVATE,request,owner)).outcome()).isEqualTo(Outcome.ACCEPTED);
                await(()->f.configuration.validationReports(()->{}).find(reference).map(report->report.terminal()==ValidationRunState.COMPLETED).orElse(false));
                assertThat(prepared).hasValue(1);assertThat(f.member.getJet().getJobs()).hasSize(1);
                assertThat(f.configuration.validationReports(()->{}).history("flow","task",ValidationMode.FULL,new ValidationReports.Paging(0,10)).total()).isEqualTo(1);
                await(()->f.resources.snapshot().metadataFrames()==0);
            }
        }
    }
    @Test void stoppingBeforeReportBirthKeepsTheKnownOriginalTerminalAfterActorRemoval()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            NativeValidationFullJob.Factory factory=original->{throw new AssertionError("prebirth stop must not construct readers or Jobs");};
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                    new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory),()->{},Duration.ofSeconds(5))){
                var request=f.request("FULL",1);var reserved=router.reserve(request);var owner=owner(reserved);var identity=f.owners.original(request,owner).run();
                try(var failure=refuseReportBirth(f)){
                    router.command(Command.of(Action.ACTIVATE,request,owner));
                    await(()->runs.read(identity).controlUnknown());
                    router.command(Command.of(Action.STOP,request,owner));
                    await(() -> {
                        var permission = f.tasks.read("flow", "task").orElseThrow().full().permission();
                        return permission != null && permission.phase() == ValidationTaskStore.Phase.CLOSED
                                && runs.original(request, owner).isEmpty();
                    });
                    for(var action:List.of(Action.STATUS,Action.STOP,Action.STATUS)){
                        var known=router.command(Command.of(action,request,owner));assertThat(known.outcome()).isEqualTo(Outcome.ACCEPTED);
                        assertThat(known.state()).isEqualTo("STOPPED");assertThat(known.retired()).isTrue();assertThat(known.drained()).isTrue();assertThat(known.unknown()).isFalse();
                    }
                    var successor=f.request("FULL",2);var next=router.reserve(successor);assertThat(next.outcome()).isEqualTo(Outcome.ACCEPTED);
                    assertThat(f.tasks.read("flow","task").orElseThrow().full().permission().start().commandId()).isEqualTo(request.command().toString());
                    for(var action:List.of(Action.STATUS,Action.STOP)){
                        var retained=router.command(Command.of(action,request,owner));assertThat(retained.outcome()).isEqualTo(Outcome.ACCEPTED);
                        assertThat(retained.state()).isEqualTo("STOPPED");assertThat(retained.drained()).isTrue();
                    }
                    router.command(Command.of(Action.CANCEL_UNUSED,successor,owner(next)));
                    assertThat(f.member.getJet().getJobs()).isEmpty();assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(2);
                    assertThat(f.configuration.validationReports(()->{}).history("flow","task",ValidationMode.FULL,new ValidationReports.Paging(0,10)).total()).isZero();
                }
            }
        }
    }
    @Test void aFullTerminalRegistryCanAdmitItsSuccessorWithoutErasingTheCurrentOriginalReceipt()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(64,2);var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            var saved=f.tasks.read("flow","task").orElseThrow();var other=new ValidationReportAdmission.Task("cluster","flow","other",UUID.randomUUID());
            var third=new ValidationReportAdmission.Task("cluster","flow","third",UUID.randomUUID());
            for(var task:List.of(other,third))assertThat(f.tasks.create(new ValidationTaskStore.Create(task,saved.comparison(),saved.settings(),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            NativeValidationFullJob.Factory factory=original->{throw new AssertionError("capacity-controlled stops never construct a Job");};
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                    new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory),()->{},Duration.ofSeconds(5));
                    var first=f.resources.admitMetadata();var second=f.resources.admitMetadata();var fourth=f.resources.admitMetadata()){
                var originals=List.of(f.request("FULL",1),new Request("flow",other.taskId(),other.generation(),1,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString()));
                var owners=new ArrayList<ValidationOwner>();
                for(var request:originals){
                    var accepted=router.reserve(request);assertThat(accepted.outcome()).isEqualTo(Outcome.ACCEPTED);var owner=owner(accepted);owners.add(owner);
                    stopBeforeBirth(f,runs,router,request,owner);
                }
                var unrelated=new Request("flow",third.taskId(),third.generation(),1,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
                assertThat(router.reserve(unrelated).outcome()).isEqualTo(Outcome.CAPACITY);
                f.startup=(input,spec)->{throw new io.tapstate.core.common.TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","fixture.refusedSuccessor"),null);};
                assertThat(router.reserve(f.request("FULL",2)).outcome()).isEqualTo(Outcome.REJECTED);
                assertThat(router.command(Command.of(Action.STATUS,originals.getFirst(),owners.getFirst())).state()).isEqualTo("STOPPED");
                f.startup=(input,spec)->NativeValidationOwnerReservations.StartupGate.ready();
                var successor=f.request("FULL",2);var accepted=router.reserve(successor);assertThat(accepted.outcome()).isEqualTo(Outcome.ACCEPTED);
                for(int index=0;index<originals.size();index++){
                    var receipt=router.command(Command.of(Action.STATUS,originals.get(index),owners.get(index)));assertThat(receipt.outcome()).isEqualTo(Outcome.ACCEPTED);
                    assertThat(receipt.state()).isEqualTo("STOPPED");assertThat(receipt.drained()).isTrue();
                }
                stopBeforeBirth(f,runs,router,successor,owner(accepted));
                var again=f.request("FULL",3);var repeated=router.reserve(again);assertThat(repeated.outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(router.command(Command.of(Action.STATUS,successor,owner(accepted))).state()).isEqualTo("STOPPED");
                router.command(Command.of(Action.CANCEL_UNUSED,again,owner(repeated)));
                var sameRevision=f.request("FULL",3);var replacement=router.reserve(sameRevision);assertThat(replacement.outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(router.reserve(again).outcome()).isEqualTo(Outcome.REJECTED);
                assertThat(router.command(Command.of(Action.ACTIVATE,again,owner(repeated))).outcome()).isNotEqualTo(Outcome.ACCEPTED);
                router.command(Command.of(Action.CANCEL_UNUSED,sameRevision,owner(replacement)));
                var secondReplacement=f.request("FULL",3);var secondAccepted=router.reserve(secondReplacement);assertThat(secondAccepted.outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(router.reserve(sameRevision).outcome()).isEqualTo(Outcome.REJECTED);
                router.command(Command.of(Action.CANCEL_UNUSED,secondReplacement,owner(secondAccepted)));
                assertThat(router.reserve(f.request("FULL",3)).outcome()).isEqualTo(Outcome.CAPACITY);
                assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,saved.comparison(),saved.settings(),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
                var revised=new Request("flow","task",f.task.generation(),2,3,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
                var current=router.reserve(revised);assertThat(current.outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(router.command(Command.of(Action.STATUS,successor,owner(accepted))).state()).isEqualTo("STOPPED");
                router.command(Command.of(Action.CANCEL_UNUSED,revised,owner(current)));
                assertThat(f.tasks.close(other)).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
                var released=router.reserve(unrelated);assertThat(released.outcome()).isEqualTo(Outcome.ACCEPTED);
                router.command(Command.of(Action.CANCEL_UNUSED,unrelated,owner(released)));
                assertThat(f.member.getJet().getJobs()).isEmpty();assertThat(f.tasks.read("flow","task").orElseThrow().full().nextStartSequence()).isEqualTo(3);
                assertThat(f.configuration.validationReports(()->{}).history("flow","task",ValidationMode.FULL,new ValidationReports.Paging(0,10)).total()).isZero();
            }
        }
    }
    @Test void closingTheTaskReleasesACarrierWhosePredecessorIsStillNamedByConfiguration()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(64,2);var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
            var saved=f.tasks.read("flow","task").orElseThrow();var other=new ValidationReportAdmission.Task("cluster","flow","other",UUID.randomUUID());
            var third=new ValidationReportAdmission.Task("cluster","flow","third",UUID.randomUUID());
            for(var task:List.of(other,third))assertThat(f.tasks.create(new ValidationTaskStore.Create(task,saved.comparison(),saved.settings(),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            NativeValidationFullJob.Factory factory=original->{throw new AssertionError("closed carrier never constructs a Job");};
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                    new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory),()->{},Duration.ofSeconds(5));
                    var first=f.resources.admitMetadata();var second=f.resources.admitMetadata();var fourth=f.resources.admitMetadata()){
                var original=f.request("FULL",1);var accepted=router.reserve(original);stopBeforeBirth(f,runs,router,original,owner(accepted));
                var otherRequest=new Request("flow",other.taskId(),other.generation(),1,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
                var otherAccepted=router.reserve(otherRequest);stopBeforeBirth(f,runs,router,otherRequest,owner(otherAccepted));
                var successor=f.request("FULL",2);var replacement=router.reserve(successor);assertThat(replacement.outcome()).isEqualTo(Outcome.ACCEPTED);
                router.command(Command.of(Action.CANCEL_UNUSED,successor,owner(replacement)));
                assertThat(f.tasks.close(f.task)).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
                var unrelated=new Request("flow",third.taskId(),third.generation(),1,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
                var freed=router.reserve(unrelated);assertThat(freed.outcome()).isEqualTo(Outcome.ACCEPTED);
                router.command(Command.of(Action.CANCEL_UNUSED,unrelated,owner(freed)));assertThat(f.member.getJet().getJobs()).isEmpty();
            }
        }
    }
    @Test @org.junit.jupiter.api.Timeout(90) void threeLiveCountRunsDoNotPreventRetryingALostOriginalStop()throws Exception{
        var reads=new java.util.concurrent.CountDownLatch(3);var stopAttempts=new AtomicInteger();
        var releases=new java.util.concurrent.ConcurrentHashMap<String,java.util.concurrent.CountDownLatch>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(8);var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var processors=new ValidationProcessorRegistry(f.member,()->{})){
            var tasks=new LinkedHashMap<String,ValidationReportAdmission.Task>();tasks.put("flow",f.task);
            for(String id:List.of("count-b","count-c")){
                var pipe=new io.tapstate.core.model.PipelineResource(id,null,List.of(io.tapstate.core.model.SourceRef.spec("input",false)),null,null,
                        new io.tapstate.core.model.ServeBlock.Inline("serving",io.tapstate.core.model.FromRef.literal("orders"),List.of(new io.tapstate.core.model.SyncElement("out","views",null,null,null)),null,null),null,null);
                assertThat(f.configuration.artifacts().create(pipe)).isEqualTo(ArtifactMutation.CREATED);
                var hashes=new TreeMap<>(f.comparison.dependencyHashes());hashes.remove("flow");hashes.put(id,io.tapstate.core.model.canonical.CanonicalHash.of(pipe));
                var task=new ValidationReportAdmission.Task("cluster",id,"task",UUID.randomUUID());tasks.put(id,task);
                assertThat(f.tasks.create(new ValidationTaskStore.Create(task,new ValidationTaskSpec(id,hashes,1,"rules",f.comparison.mappings()),ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,1000),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            }
            NativeValidationFullJob.Factory factory=original->{
                var id=original.spec().task().pipelineId();var release=new java.util.concurrent.CountDownLatch(1);releases.put(id,release);var closed=new AtomicBoolean();var mapping=original.spec().task().mappings().getFirst();
                return new NativeValidationFullJob.Plan(){
                    public List<ValidationCountScan.Mapping> counts(){return List.of(new ValidationCountScan.Mapping(mapping.mappingId(),guard->{
                        reads.countDown();try{assertThat(release.await(30,java.util.concurrent.TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new NativeValidationCancellation("node");}
                        guard.run();return scalar(mapping.source(),1);
                    },guard->{guard.run();return scalar(mapping.target(),1);}));}
                    public boolean drained(){return closed.get();}public void close(){closed.set(true);release.countDown();}
                };
            };
            var commands=new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory);
            Commands delivered=command->{
                if(command.action()==Action.STOP&&command.request().pipeline().equals("flow")&&stopAttempts.incrementAndGet()==1)return commandUnavailable(command,Outcome.UNKNOWN,null);
                return commands.execute(command);
            };
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,delivered,()->{},Duration.ofSeconds(5));
                var coordinator=new NativeValidationStartCoordinator(f.member,f.configuration,router,f.resources,f.process,()->{})){
                var operations=new LinkedHashMap<String,io.tapstate.runtime.probe.ValidationOperation>();
                try{
                    for(var item:tasks.entrySet()){
                        var op=new io.tapstate.runtime.probe.ValidationOperation(coordinator.context(),item.getKey(),"task",item.getValue().generation(),1,1,UUID.randomUUID());operations.put(item.getKey(),op);
                        try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                        int admitted=operations.size();await(()->reads.getCount()==3-admitted&&f.resources.snapshot().metadataFrames()==admitted);
                    }
                    assertThat(reads.await(15,java.util.concurrent.TimeUnit.SECONDS)).isTrue();await(()->f.resources.snapshot().metadataFrames()==3);
                    assertThat(f.resources.snapshot().processors()).isEqualTo(3);assertThat(f.member.getJet().getJobs()).hasSize(3);
                    try(var stopped=coordinator.stop(operations.get("flow"))){assertThat(stopped.value().original()).isEqualTo(operations.get("flow"));}
                    await(()->stopAttempts.get()>=2&&f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
                    try(var foreground=coordinator.read(operations.get("count-b"))){assertThat(foreground.value().state()).isEqualTo("RUNNING");}
                    ReportObservation.await(()->{try(var observed=coordinator.read(operations.get("flow"))){return "STOPPED".equals(observed.value().state())&&observed.value().drain()==io.tapstate.runtime.probe.ValidationOperation.Drain.DRAINED;}});
                    for(String id:tasks.keySet())assertThat(f.tasks.read(id,"task").orElseThrow().full().nextStartSequence()).isEqualTo(2);
                }finally{
                    releases.values().forEach(java.util.concurrent.CountDownLatch::countDown);
                    for(var op:operations.values())try{var original=f.tasks.read(op.pipeline(),op.task()).orElseThrow().full().permission();if(original!=null)runs.stop(original.start().run());}catch(io.tapstate.core.common.TapstateException alreadyReaped){}
                    await(()->f.resources.snapshot().metadataFrames()==0);
                }
            }
        }finally{releases.values().forEach(java.util.concurrent.CountDownLatch::countDown);}
    }

    @Test @org.junit.jupiter.api.Timeout(90) void stopDrainPollingRetriesTransientMetadataFrameContention()throws Exception{
        var controlEntered=new java.util.concurrent.CountDownLatch(1);var releaseControl=new java.util.concurrent.CountDownLatch(1);
        var allowDrain=new AtomicBoolean();
        var reads=new java.util.concurrent.CountDownLatch(3);var stopAttempts=new AtomicInteger();
        var releases=new java.util.concurrent.ConcurrentHashMap<String,java.util.concurrent.CountDownLatch>();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(8);var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var processors=new ValidationProcessorRegistry(f.member,()->{})){
            var tasks=new LinkedHashMap<String,ValidationReportAdmission.Task>();tasks.put("flow",f.task);
            for(String id:List.of("count-b","count-c")){
                var pipe=new io.tapstate.core.model.PipelineResource(id,null,List.of(io.tapstate.core.model.SourceRef.spec("input",false)),null,null,
                        new io.tapstate.core.model.ServeBlock.Inline("serving",io.tapstate.core.model.FromRef.literal("orders"),List.of(new io.tapstate.core.model.SyncElement("out","views",null,null,null)),null,null),null,null);
                assertThat(f.configuration.artifacts().create(pipe)).isEqualTo(ArtifactMutation.CREATED);
                var hashes=new TreeMap<>(f.comparison.dependencyHashes());hashes.remove("flow");hashes.put(id,io.tapstate.core.model.canonical.CanonicalHash.of(pipe));
                var task=new ValidationReportAdmission.Task("cluster",id,"task",UUID.randomUUID());tasks.put(id,task);
                assertThat(f.tasks.create(new ValidationTaskStore.Create(task,new ValidationTaskSpec(id,hashes,1,"rules",f.comparison.mappings()),ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,1000),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            }
            NativeValidationFullJob.Factory factory=original->{
                var id=original.spec().task().pipelineId();var release=new java.util.concurrent.CountDownLatch(1);releases.put(id,release);var closed=new AtomicBoolean();var mapping=original.spec().task().mappings().getFirst();
                return new NativeValidationFullJob.Plan(){
                    public List<ValidationCountScan.Mapping> counts(){return List.of(new ValidationCountScan.Mapping(mapping.mappingId(),guard->{
                        reads.countDown();try{assertThat(release.await(30,java.util.concurrent.TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new NativeValidationCancellation("node");}
                        guard.run();return scalar(mapping.source(),1);
                    },guard->{guard.run();return scalar(mapping.target(),1);}));}
                    public boolean drained(){return closed.get()&&(!id.equals("flow")||allowDrain.get());}public void close(){closed.set(true);release.countDown();}
                };
            };
            var commands=new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,factory);
            Commands delivered=command->{
                if(command.action()==Action.STOP&&command.request().pipeline().equals("flow")&&stopAttempts.incrementAndGet()==1)return commandUnavailable(command,Outcome.UNKNOWN,null);
                return commands.execute(command);
            };
            try(var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,delivered,()->{},Duration.ofSeconds(5));
                var coordinator=new NativeValidationStartCoordinator(f.member,f.configuration,router,f.resources,f.process,()->{})){
                var operations=new LinkedHashMap<String,io.tapstate.runtime.probe.ValidationOperation>();
                try{
                    for(var item:tasks.entrySet()){
                        var op=new io.tapstate.runtime.probe.ValidationOperation(coordinator.context(),item.getKey(),"task",item.getValue().generation(),1,1,UUID.randomUUID());operations.put(item.getKey(),op);
                        try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                        int admitted=operations.size();await(()->reads.getCount()==3-admitted&&f.resources.snapshot().metadataFrames()==admitted);
                    }
                    assertThat(reads.await(15,java.util.concurrent.TimeUnit.SECONDS)).isTrue();await(()->f.resources.snapshot().metadataFrames()==3);
                    assertThat(f.resources.snapshot().processors()).isEqualTo(3);assertThat(f.member.getJet().getJobs()).hasSize(3);
                    try(var stopped=coordinator.stop(operations.get("flow"))){assertThat(stopped.value().original()).isEqualTo(operations.get("flow"));}
                    await(()->stopAttempts.get()>=2&&f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
                    try(var foreground=coordinator.read(operations.get("count-b"))){assertThat(foreground.value().state()).isEqualTo("RUNNING");}
                    // Keep the original native frame until the first drain observation has contended.
                    await(()->f.resources.snapshot().metadataFrames()==3);
                    try(var contention=f.resources.admitControl()){
                        contention.execute(()->{
                            controlEntered.countDown();
                            try{if(!releaseControl.await(30,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("metadata contention barrier timeout");}
                            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                        });
                        assertThat(controlEntered.await(5,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                        contention.close();
                        assertThat(f.resources.snapshot().metadataFrames()).isEqualTo(4);
                        // Logical close cannot return a frame whose physical control body is still blocked.
                        int refusals=ReportObservation.await(()->{try(var observed=coordinator.read(operations.get("flow"))){return "STOPPED".equals(observed.value().state())&&observed.value().drain()==io.tapstate.runtime.probe.ValidationOperation.Drain.DRAINED;}
                            finally{releaseControl.countDown();allowDrain.set(true);}});
                        assertThat(refusals).isGreaterThanOrEqualTo(1);
                    }
                    for(String id:tasks.keySet())assertThat(f.tasks.read(id,"task").orElseThrow().full().nextStartSequence()).isEqualTo(2);
                }finally{
                    releaseControl.countDown();allowDrain.set(true);
                    releases.values().forEach(java.util.concurrent.CountDownLatch::countDown);
                    for(var op:operations.values())try{var original=f.tasks.read(op.pipeline(),op.task()).orElseThrow().full().permission();if(original!=null)runs.stop(original.start().run());}catch(io.tapstate.core.common.TapstateException alreadyReaped){}
                    await(()->f.resources.snapshot().metadataFrames()==0);
                }
            }
        }finally{releaseControl.countDown();allowDrain.set(true);releases.values().forEach(java.util.concurrent.CountDownLatch::countDown);}
    }

    private static AutoCloseable refuseReportBirth(NativeValidationOwnerReservationsIT.Fixture f){
        var admin=f.raw.getDatabase("admin");
        admin.runCommand(new org.bson.Document("configureFailPoint","failCommand").append("mode","alwaysOn")
                .append("data",new org.bson.Document("failCommands",List.of("insert")).append("namespace",f.configName+".validation_reports").append("errorCode",2)));
        return ()->admin.runCommand(new org.bson.Document("configureFailPoint","failCommand").append("mode","off"));
    }
    private static void stopBeforeBirth(NativeValidationOwnerReservationsIT.Fixture f,NativeValidationRuns runs,ValidationOwnerRouter router,Request request,ValidationOwner owner)throws Exception{
        try(var failure=refuseReportBirth(f)){
            var identity=f.owners.original(request,owner).run();router.command(Command.of(Action.ACTIVATE,request,owner));
            await(()->runs.read(identity).controlUnknown());router.command(Command.of(Action.STOP,request,owner));
            await(() -> {
                var permission = f.tasks.read("flow", request.task()).orElseThrow().full().permission();
                return permission != null && permission.phase() == ValidationTaskStore.Phase.CLOSED
                        && runs.original(request, owner).isEmpty();
            });
        }
    }
    private static ValidationOwner owner(Reply reply){return new ValidationOwner("cluster","flow",reply.receiver().node(),reply.receiver().boot(),reply.claimGeneration(),reply.executionGeneration(),reply.topologyRevision());}
    private static ValidationCountScan.Scalar scalar(ValidationTaskSpec.Table table,long value){return new ValidationCountScan.Scalar(table,value,new ValidationCheckEvent.ReadBoundary("fixture-count","v1",Instant.EPOCH,Instant.EPOCH));}
    /** Only report observation can retry this admission refusal; other waits retain their strict behavior. */
    static final class ReportObservation {
        @FunctionalInterface interface Pause {void pause()throws InterruptedException;}
        static int await(java.util.function.BooleanSupplier condition)throws InterruptedException{
            return await(condition,System::nanoTime,()->Thread.sleep(20));
        }
        static int await(java.util.function.BooleanSupplier condition,java.util.function.LongSupplier clock,Pause pause)throws InterruptedException{
            long until=clock.getAsLong()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30);int refusals=0;
            while(clock.getAsLong()-until<0){
                try{if(condition.getAsBoolean())return refusals;}
                catch(io.tapstate.core.common.TapstateException refused){
                    if(refused.code()!=ValidationError.VALUE_LIMIT_EXCEEDED||!"validation.metadataFrames".equals(refused.args().get("limit")))throw refused;
                    refusals++;
                }
                pause.pause();
            }
            throw new AssertionError("Report observation did not succeed within 30 seconds");
        }
    }
    private static void await(java.util.function.BooleanSupplier condition)throws Exception{
        long until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while(!condition.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(20);assertThat(condition.getAsBoolean()).isTrue();
    }
}
