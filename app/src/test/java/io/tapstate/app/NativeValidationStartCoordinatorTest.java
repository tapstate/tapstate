package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.probe.ValidationOperation;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real member routing and paid I/O over bounded CONFIG fixtures; not connector or durable admission acceptance. */
class NativeValidationStartCoordinatorTest {
    private static final UUID GENERATION=UUID.randomUUID();
    @Test @Timeout(30) void liveAndMissingOwnerClaimsCannotOpenASuccessorAttempt() {
        for(boolean live:List.of(true,false))try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            var saved=snapshot("flow","task");var oldOwner=new ValidationOwner("cluster","flow","a","previous-boot",3,4,1);
            var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(oldOwner,"task","old-born-run",ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
            var start=new ValidationReportAdmission.Start(saved.task(),1,1,UUID.randomUUID().toString(),identity);
            var born=new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),1,saved.schedulingCommandId(),false,
                    new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(start,ValidationTaskStore.Phase.BORN)),saved.incremental());
            when(configuration.validationTasks(()->{}).read("flow","task")).thenReturn(Optional.of(born));
            if(live)routing.reading.set(new WorkloadClaimReading(new WorkloadClaim(ValidationClaims.key("cluster","flow"),
                    new WorkloadOwner("a","previous-boot"),3,4,1,java.time.Instant.EPOCH),Duration.ofSeconds(30)));
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                assertThat(coordinator.permissionOwnerLease(born)).isEqualTo(live?ValidationOperation.OwnerLease.LIVE:ValidationOperation.OwnerLease.UNKNOWN);
                var next=new ValidationOperation(coordinator.context(),"flow","task",GENERATION,1,2,UUID.randomUUID());
                try(var response=coordinator.start(next)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.REJECTED);}
                assertThat(routing.calls.values()).allSatisfy(count->assertThat(count).hasValue(0));
                assertThat(resources.snapshot().metadataFrames()).isZero();
            }
        }
    }
    @Test @Timeout(30) void aSuccessorAttemptReachesTheFencedOwnerBeforeObsoletePermissionClosure() {
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            var saved=snapshot("flow","task");var oldOwner=new ValidationOwner("cluster","flow","a","previous-boot",3,4,1);
            routing.reading.set(new WorkloadClaimReading(new WorkloadClaim(ValidationClaims.key("cluster","flow"),
                    new WorkloadOwner("a","previous-boot"),3,4,1,java.time.Instant.now().plusSeconds(300)),Duration.ofSeconds(-1)));
            var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
            var oldIdentity=new ValidationRunIdentity(oldOwner,"task","old-born-run",ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
            var oldStart=new ValidationReportAdmission.Start(saved.task(),1,1,UUID.randomUUID().toString(),oldIdentity);
            var born=new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),1,saved.schedulingCommandId(),false,
                    new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(oldStart,ValidationTaskStore.Phase.BORN)),saved.incremental());
            when(configuration.validationTasks(()->{}).read("flow","task")).thenReturn(Optional.of(born));
            for(var node:List.of("a","b"))routing.behaviors.put(node,request->{
                assertThat(request.startSequence()).isEqualTo(2);assertThat(request.run()).isNotEqualTo(oldIdentity.runId());
                return new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.BUSY,null,null);
            });
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var next=new ValidationOperation(coordinator.context(),"flow","task",GENERATION,1,2,UUID.randomUUID());
                try(var response=coordinator.start(next)){
                    assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.BUSY);
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()==1);
                assertThat(configuration.validationTasks(()->{}).read("flow","task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.BORN);
            }
        }
    }
    @Test @Timeout(30) void stoppingAnExhaustedCapacityAttemptReadsItsOriginalDisposedReceipt() {
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});
            for(var node:List.of("a","b")){
                routing.behaviors.put(node,request->ValidationOwnerRouter.Result.capacity());
                routing.receipts.put(node,request->new ValidationOwnerRouter.LocalReceipt(
                        new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.REJECTED,null,null),true));
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());
                try(var started=coordinator.start(op)){
                    assertThat(started.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.CAPACITY);
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->
                        routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()==2&&resources.snapshot().metadataFrames()==0);
                try(var stopped=coordinator.stop(op)){
                    assertThat(stopped.value().original()).isEqualTo(op);
                    assertThat(stopped.value().state()).isEqualTo("STOPPED");
                    assertThat(stopped.value().drain()).isEqualTo(ValidationOperation.Drain.DRAINED);
                }
                assertThat(routing.calls.values()).allSatisfy(count->assertThat(count).hasValue(1));
            }
        }
    }
    @Test @Timeout(30) void lateReservationCannotActivateAfterTheEntryContextWasReplaced()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var activated=new AtomicInteger();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            try(var original=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var op=operation(original.context());
                for(var node:List.of("a","b")){
                    routing.behaviors.put(node,request->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("reserve barrier timeout");}
                        catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
                        return new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null);});
                    routing.commands.put(node,command->{if(command.action()==ValidationOwnerRouter.Action.ACTIVATE)activated.incrementAndGet();
                        return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,true,false,false,false,null);});
                }
                try(var response=original.start(op)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.PENDING);}
                try{
                    assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();original.close();
                    try(var replacement=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                        assertThat(replacement.context().incarnation()).isNotEqualTo(op.context().incarnation());release.countDown();
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->resources.snapshot().processReserved().bytes()==2L*1024*1024);
                        assertThat(activated).hasValue(0);
                    }
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
    @Test @Timeout(30) void aScheduledAttemptBorrowsItsResponseFrameAndReturnsItsAdmissionResult()throws Exception{
        var attempted=new CountDownLatch(1);var reservation=new AtomicReference<ValidationOwnerRouter.Outcome>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});
            for(var node:List.of("a","b")){
                routing.behaviors.put(node,request->{
                    try(var inputs=resources.admitMetadata()){
                        inputs.requireRetained(resources);reservation.set(ValidationOwnerRouter.Outcome.ACCEPTED);
                        return new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null);
                    }catch(io.tapstate.core.common.TapstateException full){
                        assertThat(full.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED);
                        assertThat(full.args()).containsEntry("limit","validation.metadataFrames");
                        reservation.set(ValidationOwnerRouter.Outcome.CAPACITY);return ValidationOwnerRouter.Result.capacity();
                    }finally{attempted.countDown();}
                });
                routing.commands.put(node,command->new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,true,false,false,false,null));
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{});
                    var first=resources.admitMetadata();var second=resources.admitMetadata()){
                try(var response=coordinator.startScheduled(operation(coordinator.context()),java.time.Instant.now().plusSeconds(30))){
                    assertThat(attempted.await(3,TimeUnit.SECONDS)).isTrue();
                    assertThat(reservation.get()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                    assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);
                }
            }
        }
    }
    @Test @Timeout(30) void aScheduledBodyAdmissionRefusalIsRetainedInsteadOfAnUnexecutedPendingMemo(){
        var refuse=new AtomicBoolean(true);var held=new AtomicReference<ValidationMemberResources.Unbound>();
        var member=new AtomicReference<ValidationMemberResources.Member>();var caller=Thread.currentThread();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{
                if(Thread.currentThread()==caller&&StackWalker.getInstance().walk(frames->frames.anyMatch(frame->
                        frame.getClassName().equals(NativeValidationStartCoordinator.class.getName())&&frame.getMethodName().equals("scheduled")))
                        &&refuse.compareAndSet(true,false))
                    held.set(member.get().reserve(new io.tapstate.runtime.engine.validation.ValidationLocalBudget.Amount(1,64,0,0),0));
            });member.set(resources);
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());
                try{
                    try(var response=coordinator.startScheduled(op,java.time.Instant.now().plusSeconds(30))){
                        assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);
                        assertThat(response.value().reason()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED.code());
                    }
                }finally{if(held.get()!=null)held.get().close();}
                try(var response=coordinator.read(op)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);}
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isZero();
            }
        }finally{if(held.get()!=null)held.get().close();}
    }
    @Test @Timeout(60) void expiredUnsubmittedScheduledRefusalsCannotExhaustTheUnchangedTaskSequence()throws Exception{
        var refuse=new AtomicBoolean(true);var held=new AtomicReference<ValidationMemberResources.Unbound>();
        var bound=new AtomicReference<ValidationMemberResources.Member>();var caller=Thread.currentThread();
        var now=new AtomicReference<java.time.Instant>(java.time.Instant.parse("2026-11-01T00:00:00Z"));
        var clock=new java.time.Clock(){
            public java.time.ZoneId getZone(){return java.time.ZoneOffset.UTC;}
            public java.time.Clock withZone(java.time.ZoneId zone){return this;}
            public java.time.Instant instant(){return now.get();}
        };
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{
                if(Thread.currentThread()==caller&&StackWalker.getInstance().walk(frames->frames.anyMatch(frame->
                        frame.getClassName().equals(NativeValidationStartCoordinator.class.getName())&&frame.getMethodName().equals("scheduled")))
                        &&refuse.compareAndSet(true,false))
                    held.set(bound.get().reserve(new io.tapstate.runtime.engine.validation.ValidationLocalBudget.Amount(1,64,0,0),0));
            });bound.set(resources);
            for(String node:List.of("a","b")){
                routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null));
                routing.commands.put(node,command->new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,true,false,false,false,null));
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{},Duration.ofSeconds(5),clock)){
                var latest=now.get().plusSeconds(30);ValidationOperation last=null;
                for(int index=0;index<128;index++){
                    refuse.set(true);last=operation(coordinator.context());
                    try(var response=coordinator.startScheduled(last,latest)){
                        assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);
                    }finally{var lease=held.getAndSet(null);if(lease!=null)lease.close();}
                }
                try(var response=coordinator.read(last)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);}
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isZero();
                now.set(latest.plusSeconds(1));refuse.set(false);var fresh=operation(coordinator.context());
                try(var response=coordinator.start(fresh)){assertThat(response.value().outcome()).isNotEqualTo(ValidationOperation.Outcome.CAPACITY);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(()->{
                    try(var response=coordinator.read(fresh)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);}
                });
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                assertThat(fresh.sequence()).isEqualTo(1);
            }
        }finally{var lease=held.get();if(lease!=null)lease.close();}
    }

    @Test @Timeout(30) void expiredScheduledUnknownHandoffsWaitForTheirOriginalDisposedReceipt()throws Exception{
        var now=new AtomicReference<java.time.Instant>(java.time.Instant.parse("2026-11-01T00:00:00Z"));
        var clock=new java.time.Clock(){public java.time.ZoneId getZone(){return java.time.ZoneOffset.UTC;}public java.time.Clock withZone(java.time.ZoneId zone){return this;}public java.time.Instant instant(){return now.get();}};
        var disposed=new AtomicBoolean();var reads=new AtomicInteger();var activations=new AtomicInteger();var original=new AtomicReference<ValidationOperation>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});
            for(String node:List.of("a","b")){
                routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null));
                routing.receipts.put(node,request->{assertThat(request.command()).isEqualTo(original.get().command());reads.incrementAndGet();
                    return disposed.get()?new ValidationOwnerRouter.LocalReceipt(new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.REJECTED,null,null),true):ValidationOwnerRouter.LocalReceipt.unknown();});
                routing.commands.put(node,command->{activations.incrementAndGet();throw new AssertionError("Uncertain/disposed starts cannot activate");});
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{},Duration.ofSeconds(5),clock)){
                var op=operation(coordinator.context());original.set(op);var latest=now.get().plusSeconds(5);
                try(var response=coordinator.startScheduled(op,latest)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.UNKNOWN);}
                now.set(latest.plusSeconds(1));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->reads.get()>0);
                assertThat(entryCount(coordinator)).isOne();assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                disposed.set(true);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->entryCount(coordinator)==0&&resources.snapshot().metadataFrames()==0);
                assertThat(activations).hasValue(0);assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
            }
        }
    }
    @Test @Timeout(30) void routineScheduledReceiptRecoveryPreservesForegroundControlHeadroom()throws Exception{
        var now=new AtomicReference<java.time.Instant>(java.time.Instant.parse("2026-11-01T00:00:00Z"));
        var clock=new java.time.Clock(){public java.time.ZoneId getZone(){return java.time.ZoneOffset.UTC;}public java.time.Clock withZone(java.time.ZoneId zone){return this;}public java.time.Instant instant(){return now.get();}};
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});
            for(String node:List.of("a","b")){
                routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null));
                routing.receipts.put(node,request->{entered.countDown();while(release.getCount()!=0)try{release.await();}catch(InterruptedException interrupted){/* The original body retains its frame until exit. */}return ValidationOwnerRouter.LocalReceipt.unknown();});
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{},Duration.ofSeconds(5),clock)){
                var latest=now.get().plusSeconds(5);
                try{
                    for(int index=0;index<4;index++)try(var response=coordinator.startScheduled(operation(coordinator.context()),latest)){
                        assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.UNKNOWN);
                    }
                    now.set(latest.plusSeconds(1));org.springframework.test.util.ReflectionTestUtils.invokeMethod(coordinator,"dispatchStops");
                    assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
                    assertThat(resources.snapshot().metadataFrames()).isLessThanOrEqualTo(3);
                    try(var foreground=resources.admitControl()){
                        var completed=new CountDownLatch(1);foreground.execute(completed::countDown);
                        assertThat(completed.await(2,TimeUnit.SECONDS)).isTrue();
                    }
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }

    private static int entryCount(NativeValidationStartCoordinator coordinator){
        var entries=(Map<?,?>)org.springframework.test.util.ReflectionTestUtils.getField(coordinator,"entries");
        synchronized(entries){return entries.size();}
    }

    @Test @Timeout(30) void aScheduledTimeoutRetainsItsFrameUntilTheClosedContextBodyActuallyExits()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var lookups=new AtomicInteger();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources();var caller=Executors.newSingleThreadExecutor()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            when(configuration.validationTasks(()->{}).read("flow","task")).thenAnswer(ignored->{
                if(lookups.incrementAndGet()==2){entered.countDown();
                    while(release.getCount()!=0)try{release.await();}catch(InterruptedException interrupted){/* Physical native completion owns the frame. */}}
                return Optional.of(snapshot("flow","task"));
            });
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{},Duration.ofMillis(100))){
                var op=operation(coordinator.context());var answer=caller.submit(()->{
                    try(var response=coordinator.startScheduled(op,java.time.Instant.now().plusSeconds(30))){return response.value();}
                });
                try{
                    assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();assertThat(answer.get(2,TimeUnit.SECONDS).outcome()).isEqualTo(ValidationOperation.Outcome.UNKNOWN);
                    assertThat(resources.snapshot().metadataFrames()).isOne();coordinator.close();
                    assertThat(resources.snapshot().metadataFrames()).isOne();release.countDown();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->resources.snapshot().metadataFrames()==0);
                    assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isZero();
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
    @Test @Timeout(30) void aStopDuringScheduledReservationSendsOnlyTheOriginalOwnerStop()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var commands=new java.util.concurrent.CopyOnWriteArrayList<ValidationOwnerRouter.Command>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources();var caller=Executors.newSingleThreadExecutor()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});
            for(var node:List.of("a","b")){
                routing.behaviors.put(node,request->{entered.countDown();
                    try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("scheduled reserve barrier timeout");}
                    catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
                    return new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null);
                });
                routing.commands.put(node,command->{commands.add(command);
                    return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STOPPED",false,false,false,true,true,null);});
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());var answer=caller.submit(()->{
                    try(var response=coordinator.startScheduled(op,java.time.Instant.now().plusSeconds(30))){return response.value();}
                });
                try{
                    assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                    try(var stop=coordinator.stop(op)){assertThat(stop.value().original()).isEqualTo(op);}
                    release.countDown();assertThat(answer.get(3,TimeUnit.SECONDS).outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);
                    assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                    assertThat(commands).isNotEmpty().allSatisfy(command->{assertThat(command.action()).isEqualTo(ValidationOwnerRouter.Action.STOP);
                        assertThat(command.request().command()).isEqualTo(op.command());assertThat(command.request().startSequence()).isEqualTo(op.sequence());});
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
    @Test @Timeout(30) void aStaleStopLookupStillStopsTheKnownPermissionFoundDuringScheduledAdvance()throws Exception{
        var stopLookup=new CountDownLatch(1);var releaseLookup=new CountDownLatch(1);var activating=new CountDownLatch(1);
        var releaseActivation=new CountDownLatch(1);var stopped=new CountDownLatch(1);var reads=new AtomicInteger();
        var commands=new java.util.concurrent.CopyOnWriteArrayList<ValidationOwnerRouter.Command>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources();var callers=Executors.newFixedThreadPool(2)){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());var saved=snapshot("flow","task");var owner=new ValidationOwner("cluster","flow","a","boot-a",3,4,1);
                var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
                var identity=new ValidationRunIdentity(owner,"task",NativeValidationStartCoordinator.runId(op),ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
                var start=new ValidationReportAdmission.Start(saved.task(),1,1,op.command().toString(),identity);
                var opened=new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),1,saved.schedulingCommandId(),false,
                        new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(start,ValidationTaskStore.Phase.OPEN)),saved.incremental());
                when(configuration.validationTasks(()->{}).read("flow","task")).thenAnswer(ignored->{
                    int read=reads.incrementAndGet();if(read==1){stopLookup.countDown();assertThat(releaseLookup.await(5,TimeUnit.SECONDS)).isTrue();return Optional.of(saved);}
                    return Optional.of(read==2?saved:opened);
                });
                routing.commands.put("a",command->{commands.add(command);
                    if(command.action()==ValidationOwnerRouter.Action.ACTIVATE){activating.countDown();
                        try{if(!releaseActivation.await(5,TimeUnit.SECONDS))throw new AssertionError("known activation barrier timeout");}
                        catch(InterruptedException interrupted){throw new AssertionError(interrupted);}}
                    if(command.action()==ValidationOwnerRouter.Action.STOP)stopped.countDown();
                    boolean terminal=command.action()==ValidationOwnerRouter.Action.STOP;
                    return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,terminal?"STOPPED":"STARTING",false,false,false,terminal,terminal,null);
                });
                var stop=callers.submit(()->{try(var response=coordinator.stop(op)){return response.value();}});
                try{
                    assertThat(stopLookup.await(2,TimeUnit.SECONDS)).isTrue();
                    var scheduled=callers.submit(()->{try(var response=coordinator.startScheduled(op,java.time.Instant.now().plusSeconds(30))){return response.value();}});
                    assertThat(activating.await(2,TimeUnit.SECONDS)).isTrue();releaseLookup.countDown();
                    assertThat(stop.get(2,TimeUnit.SECONDS).original()).isEqualTo(op);releaseActivation.countDown();
                    assertThat(scheduled.get(3,TimeUnit.SECONDS).outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);
                    assertThat(stopped.await(2,TimeUnit.SECONDS)).isTrue();
                    assertThat(commands).allSatisfy(command->{assertThat(command.owner()).isEqualTo(owner);assertThat(command.request().command()).isEqualTo(op.command());});
                    assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isZero();
                }finally{releaseLookup.countDown();releaseActivation.countDown();}
            }
        }finally{releaseLookup.countDown();releaseActivation.countDown();}
    }
    @Test @Timeout(30) void aStaleEntryIncarnationCannotReadReceiptsOrStopTheCurrentMemo(){
        var reads=new AtomicInteger();var commands=new java.util.concurrent.CopyOnWriteArrayList<ValidationOwnerRouter.Command>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});
            for(var node:List.of("a","b")){
                routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null));
                routing.receipts.put(node,request->{reads.incrementAndGet();return new ValidationOwnerRouter.LocalReceipt(
                        new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null),false);});
                routing.commands.put(node,command->{commands.add(command);return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null);});
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{})){
                var current=operation(coordinator.context());try(var response=coordinator.start(current)){assertThat(response.value().original()).isEqualTo(current);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==0);
                var context=current.context();var staleContext=new ValidationOperation.Context(context.cluster(),context.node(),context.boot(),context.member(),UUID.randomUUID());
                var stale=new ValidationOperation(staleContext,current.pipeline(),current.task(),current.taskGeneration(),current.revision(),current.sequence(),current.command());
                try(var response=coordinator.read(stale)){assertThat(response.value().original()).isEqualTo(stale);assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.REJECTED,ValidationOperation.Outcome.UNKNOWN);}
                try(var response=coordinator.stop(stale)){assertThat(response.value().original()).isEqualTo(stale);}
                assertThat(reads).hasValue(0);assertThat(commands).isEmpty();
                try(var response=coordinator.read(current)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);}
                assertThat(commands).isNotEmpty().allSatisfy(command->assertThat(command.action()).isEqualTo(ValidationOwnerRouter.Action.STATUS));
            }
        }
    }
    @Test @Timeout(30) void anotherEntryMemberRecoversOnlyTheOriginalReceiptWithoutStartingFromProgress()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var visible=new AtomicBoolean();var activations=new AtomicInteger();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofMillis(150),1);var process=new ValidationMemberResources()){
            var aResources=process.bind("cluster","a","boot-a",2,()->{});var bResources=process.bind("cluster","b","boot-b",2,()->{});var configuration=configuration();
            for(var node:List.of("a","b")){
                routing.behaviors.put(node,request->{entered.countDown();try{assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}
                    catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
                    return new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null);});
                routing.receipts.put(node,request->visible.get()?new ValidationOwnerRouter.LocalReceipt(
                        new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null),false):ValidationOwnerRouter.LocalReceipt.unknown());
                routing.commands.put(node,command->{if(command.action()==ValidationOwnerRouter.Action.ACTIVATE)activations.incrementAndGet();
                    return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null);});
            }
            try(var a=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,aResources,process,()->{});
                    var b=new NativeValidationStartCoordinator(routing.b,configuration,routing.right,bResources,process,()->{})){
                var op=operation(a.context());
                try{
                    try(var response=b.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->{try(var response=b.read(op)){return response.value().outcome()==ValidationOperation.Outcome.UNKNOWN;}});
                    release.countDown();visible.set(true);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->{try(var response=b.read(op)){return response.value().outcome()==ValidationOperation.Outcome.ACCEPTED;}});
                    assertThat(activations).hasValue(0);assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                    try(var response=b.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->activations.get()==1);
                    assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
    @Test @Timeout(30) void anotherMemberCannotDropTheKnownOriginalEntryReceiverPin(){
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var aResources=process.bind("cluster","a","boot-a",2,()->{});var bResources=process.bind("cluster","b","boot-b",2,()->{});var configuration=configuration();
            var saved=snapshot("flow","task");var current=new AtomicReference<>(saved);var original=new AtomicReference<ValidationOperation>();var replacedCommands=new AtomicInteger();
            when(configuration.validationTasks(()->{}).read("flow","task")).thenAnswer(ignored->Optional.of(current.get()));
            routing.behaviors.put("b",request->ValidationOwnerRouter.Result.capacity());
            routing.commands.put("a",command->{
                var op=original.get();var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
                var identity=new ValidationRunIdentity(command.owner(),"task",NativeValidationStartCoordinator.runId(op),ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
                var start=new ValidationReportAdmission.Start(saved.task(),1,1,op.command().toString(),identity);
                current.set(new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),1,saved.schedulingCommandId(),false,
                        new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(start,ValidationTaskStore.Phase.OPEN)),saved.incremental()));
                return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null);
            });
            try(var a=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,aResources,process,()->{});
                    var b=new NativeValidationStartCoordinator(routing.b,configuration,routing.right,bResources,process,()->{})){
                var op=operation(a.context());original.set(op);try(var response=b.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->current.get()!=saved&&aResources.snapshot().metadataFrames()==0);
                int reservations=routing.calls.values().stream().mapToInt(AtomicInteger::get).sum();routing.left.close();
                routing.commands.put("a",command->{replacedCommands.incrementAndGet();return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STOPPED",false,false,false,true,true,null);});
                try(var replacement=routing.router(routing.a,routing.ga,"a",Duration.ofSeconds(5))){
                    try(var response=b.stop(op)){assertThat(response.value().original()).isEqualTo(op);}
                    try(var response=b.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->aResources.snapshot().metadataFrames()==0);
                    assertThat(replacedCommands).hasValue(0);assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(reservations);
                }
            }
        }
    }
    @Test @Timeout(30) void aLocalOriginalRetrySelectedWithAReceiptReadIsNotLostBehindThatRead()throws Exception{receiptRetryRace(false);}
    @Test @Timeout(30) void aLocalOriginalRetrySelectedBeforeOwnerRecoveryStillActivatesAfterRecovery()throws Exception{receiptRetryRace(true);}
    private void receiptRetryRace(boolean readFinishesFirst)throws Exception{
        var readSelected=new CountDownLatch(1);var startSelected=new CountDownLatch(1);var releaseRead=new CountDownLatch(1);var releaseStart=new CountDownLatch(1);
        var receiptEntered=new CountDownLatch(1);var releaseReceipt=new CountDownLatch(1);var readGate=new AtomicBoolean();var startGate=new AtomicBoolean();var activations=new AtomicInteger();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources();var callers=Executors.newFixedThreadPool(2)){
            var resources=process.bind("cluster","a","boot-a",2,()->{
                boolean selected=StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().equals(NativeValidationStartCoordinator.class.getName())&&frame.getMethodName().equals("originalReceipt")));
                try{
                    if(selected&&Thread.currentThread().getName().equals("local-receipt-read")&&readGate.compareAndSet(false,true)){
                        readSelected.countDown();assertThat(releaseRead.await(5,TimeUnit.SECONDS)).isTrue();}
                    if(selected&&Thread.currentThread().getName().equals("local-receipt-start")&&startGate.compareAndSet(false,true)){
                        startSelected.countDown();assertThat(releaseStart.await(5,TimeUnit.SECONDS)).isTrue();}
                }catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
            });
            for(var node:List.of("a","b")){
                routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null));
                routing.receipts.put(node,request->{receiptEntered.countDown();try{assertThat(releaseReceipt.await(5,TimeUnit.SECONDS)).isTrue();}
                    catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
                    return new ValidationOwnerRouter.LocalReceipt(new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null),false);});
                routing.commands.put(node,command->{if(command.action()==ValidationOwnerRouter.Action.ACTIVATE)activations.incrementAndGet();
                    return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null);});
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration(),routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==0);
                var reading=callers.submit(()->{Thread.currentThread().setName("local-receipt-read");try(var response=coordinator.read(op)){return response.value();}});
                try{
                    assertThat(readSelected.await(2,TimeUnit.SECONDS)).isTrue();
                    var starting=callers.submit(()->{Thread.currentThread().setName("local-receipt-start");try(var response=coordinator.start(op)){return response.value();}});
                    assertThat(startSelected.await(2,TimeUnit.SECONDS)).isTrue();releaseRead.countDown();assertThat(receiptEntered.await(2,TimeUnit.SECONDS)).isTrue();
                    if(readFinishesFirst){releaseReceipt.countDown();assertThat(reading.get(3,TimeUnit.SECONDS).outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);assertThat(activations).hasValue(0);}
                    releaseStart.countDown();assertThat(starting.get(2,TimeUnit.SECONDS).original()).isEqualTo(op);
                    releaseReceipt.countDown();assertThat(reading.get(3,TimeUnit.SECONDS).outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->activations.get()==1);
                    assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                }finally{releaseRead.countDown();releaseStart.countDown();releaseReceipt.countDown();}
            }
        }finally{releaseRead.countDown();releaseStart.countDown();releaseReceipt.countDown();}
    }
    @Test void wrappedTransportLossIsUnknownRatherThanAProgrammerFailure(){
        var op=operation(new ValidationOperation.Context("cluster","a","boot-a",UUID.randomUUID(),UUID.randomUUID()));
        for(var failure:List.of(new RejectedExecutionException("transport queue"),new com.hazelcast.core.OperationTimeoutException("transport timeout"),
                mock(com.hazelcast.spi.exception.TargetDisconnectedException.class),mock(com.hazelcast.spi.exception.TargetNotMemberException.class))){
            assertThat(NativeValidationStartCoordinator.failed(op,failure).outcome()).isEqualTo(ValidationOperation.Outcome.UNKNOWN);
        }
    }
    @Test @Timeout(30) void aBornPermissionWithoutItsOriginalReportNeverActivates(){
        var calls=new AtomicInteger();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());var saved=snapshot("flow","task");
                var owner=new ValidationOwner("cluster","flow","a","boot-a",3,4,1);var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
                var identity=new ValidationRunIdentity(owner,"task",NativeValidationStartCoordinator.runId(op),ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
                var start=new ValidationReportAdmission.Start(saved.task(),1,1,op.command().toString(),identity);
                var born=new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),1,saved.schedulingCommandId(),false,
                        new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(start,ValidationTaskStore.Phase.BORN)),saved.incremental());
                when(configuration.validationTasks(()->{}).read("flow","task")).thenReturn(Optional.of(born));
                routing.commands.put("a",command->{calls.incrementAndGet();return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null);});
                assertThatThrownBy(()->coordinator.start(op)).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining("entry.bornReportMissing");
                assertThat(calls).hasValue(0);
            }
        }
    }
    @Test @Timeout(30) void aDifferentEntryMemberForwardsTheOriginalToOneSingleWorkerRegistry()throws Exception{
        var activated=new AtomicInteger();var statuses=new AtomicInteger();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resourcesA=process.bind("cluster","a","boot-a",2,()->{});var resourcesB=process.bind("cluster","b","boot-b",2,()->{});var configuration=configuration();
            for(var node:List.of("a","b"))routing.commands.put(node,command->{
                if(command.action()==ValidationOwnerRouter.Action.ACTIVATE)activated.incrementAndGet();
                if(command.action()==ValidationOwnerRouter.Action.STATUS)statuses.incrementAndGet();
                return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,true,false,false,false,null);
            });
            try(var a=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resourcesA,process,()->{});
                    var b=new NativeValidationStartCoordinator(routing.b,configuration,routing.right,resourcesB,process,()->{})){
                // This case observes foreground forwarding and its returned allocations. Passive
                // maintenance has separate coverage and can otherwise both satisfy STATUS and
                // occupy the process budget between the foreground receipt and its assertion.
                for(var coordinator:List.of(a,b))((java.util.concurrent.ScheduledFuture<?>)org.springframework.test.util.ReflectionTestUtils.getField(coordinator,"stops")).cancel(false);
                ((java.util.concurrent.ScheduledThreadPoolExecutor)org.springframework.test.util.ReflectionTestUtils.getField(process,"controlTicks")).submit(()->{}).get(5,TimeUnit.SECONDS);
                var op=operation(a.context());
                try(var response=b.start(op)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.ACCEPTED);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->activated.get()==1&&resourcesA.snapshot().processReserved().bytes()==4L*1024*1024);
                int reservations=routing.calls.values().stream().mapToInt(AtomicInteger::get).sum();assertThat(reservations).isOne();
                assertThat(statuses).as("Only the explicit forwarded read may request STATUS in this case").hasValue(0);
                try(var response=b.read(op)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->statuses.get()>0&&resourcesA.snapshot().processReserved().bytes()==4L*1024*1024);
                assertThat(activated).hasValue(1);assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                try(var response=b.start(op)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->activated.get()==2&&resourcesA.snapshot().processReserved().bytes()==4L*1024*1024);
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
            }
        }
    }
    @Test @Timeout(30) void anHttpLookupUsesItsOwnFourthFrameRatherThanAdmittingAFifth(){
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{});
                    var first=resources.admitMetadata();var second=resources.admitMetadata();var third=resources.admitMetadata()){
                try(var response=coordinator.read(operation(coordinator.context()))){
                    assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.UNKNOWN);assertThat(resources.snapshot().metadataFrames()).isEqualTo(4);
                }
                assertThat(resources.snapshot().metadataFrames()).isEqualTo(3);assertThat(resources.snapshot().processReserved().bytes()).isEqualTo(2L*1024*1024);
            }
        }
    }
    @Test @Timeout(30) void anHttpTimeoutRetainsItsBorrowedFrameUntilTheOriginalIoActuallyExits()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources();var caller=Executors.newSingleThreadExecutor()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            when(configuration.validationTasks(()->{}).read("flow","task")).thenAnswer(ignored->{entered.countDown();
                while(release.getCount()!=0)try{release.await();}catch(InterruptedException interrupted){/* A native operation need not acknowledge cancellation. */}
                return Optional.of(snapshot("flow","task"));});
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{},Duration.ofMillis(100))){
                var op=operation(coordinator.context());var response=caller.submit(()->coordinator.read(op));
                try{
                    assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();assertThatThrownBy(()->response.get(2,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(io.tapstate.core.common.TapstateException.class);
                    assertThat(resources.snapshot().metadataFrames()).isOne();assertThat(resources.snapshot().processReserved().bytes()).isEqualTo(6L*1024*1024);
                    release.countDown();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->resources.snapshot().metadataFrames()==0&&resources.snapshot().processReserved().bytes()==2L*1024*1024);
                }finally{release.countDown();}
            }
        }finally{release.countDown();}
    }
    @Test @Timeout(30) void monotonicConfigSequenceReclaimsOldEntryMemosWithoutReplayingThem()throws Exception{
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            for(var node:List.of("a","b"))routing.commands.put(node,command->new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null));
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                ValidationOperation first=null;var originals=new ArrayList<ValidationOperation>();
                for(int index=0;index<128;index++){
                    var op=operation(coordinator.context());originals.add(op);if(first==null)first=op;
                    try(var response=coordinator.start(op)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.ACCEPTED);}
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(1)).until(()->resources.snapshot().processReserved().bytes()==2L*1024*1024);
                }
                try(var full=coordinator.start(operation(coordinator.context()))){assertThat(full.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);}
                var saved=snapshot("flow","task");var owner=new ValidationOwner("cluster","flow","a","boot-a",3,4,1);var runSpec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
                var identity=new ValidationRunIdentity(owner,"task",NativeValidationStartCoordinator.runId(first),ValidationMode.FULL,runSpec.fingerprint(),1,"direct-full-v1");
                var start=new ValidationReportAdmission.Start(saved.task(),1,1,first.command().toString(),identity);
                var advanced=new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),1,saved.schedulingCommandId(),false,
                        new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(start,ValidationTaskStore.Phase.CLOSED)),saved.incremental());
                when(configuration.validationTasks(()->{}).read("flow","task")).thenReturn(Optional.of(advanced));
                var next=new ValidationOperation(coordinator.context(),"flow","task",GENERATION,1,2,UUID.randomUUID());
                try(var response=coordinator.start(next)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.ACCEPTED);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(1)).until(()->resources.snapshot().processReserved().bytes()==2L*1024*1024);
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(129);
                try(var old=coordinator.start(originals.get(1))){assertThat(old.value().outcome()).isEqualTo(ValidationOperation.Outcome.REJECTED);}
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(129);
            }
        }
    }
    @Test @Timeout(30) void completedManualTasksCannotPermanentlyExhaustTheSharedEntryRegistry()throws Exception{
        completedDistinctTasks(false,false,Closure.EXACT);
    }
    @Test @Timeout(30) void closedManualStopsWaitForTheirOriginalDrainReceiptBeforeMemoReclamation()throws Exception{
        completedDistinctTasks(true,false,Closure.EXACT);
    }
    @Test @Timeout(30) void closedManualMemosCanBeReclaimedAfterOwnerReceiptReapingWithoutInventingDrain()throws Exception{
        completedDistinctTasks(false,true,Closure.EXACT);
    }
    private enum Closure { EXACT,LATER_TICKET,OTHER_OWNER,UNAVAILABLE }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value=Closure.class,names={"LATER_TICKET","OTHER_OWNER","UNAVAILABLE"})
    @Timeout(30) void aMemoNeedsItsOwnVerifiedClosedTicketAndOwnerBeforeReclamation(Closure closure)throws Exception{
        completedDistinctTasks(false,true,closure);
    }
    private void completedDistinctTasks(boolean waitForDrain,boolean receiptReaped,Closure closure)throws Exception{
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();
            var saved=new ConcurrentHashMap<String,ValidationTaskStore.Snapshot>();var activations=new AtomicInteger();
            var drained=new AtomicBoolean(!waitForDrain);var statusReads=new AtomicInteger();
            var closedLookups=new AtomicInteger();
            when(configuration.validationTasks(()->{}).read(eq("flow"),anyString())).thenAnswer(call->{
                var value=saved.get(call.getArgument(1));
                if(value!=null&&value.full().permission()!=null){
                    closedLookups.incrementAndGet();
                    if(closure==Closure.UNAVAILABLE)throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","fixture.unavailable"),null);
                }
                return Optional.ofNullable(value);
            });
            for(String node:List.of("a","b"))routing.commands.put(node,command->{
                if(command.action()==ValidationOwnerRouter.Action.ACTIVATE){
                    activations.incrementAndGet();var request=command.request();var before=saved.get(request.task());
                    var spec=new ValidationRunSpec(before.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
                    var owner=command.owner();if(closure==Closure.OTHER_OWNER)owner=new ValidationOwner(owner.clusterId(),owner.pipelineId(),owner.nodeId(),owner.bootId(),owner.claimGeneration()+1,owner.executionGeneration(),owner.topologyRevision());
                    boolean later=closure==Closure.LATER_TICKET;long sequence=later?2:1;
                    var identity=new ValidationRunIdentity(owner,request.task(),later?"later-"+request.run():request.run(),ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
                    var start=new ValidationReportAdmission.Start(before.task(),1,sequence,later?UUID.randomUUID().toString():request.command().toString(),identity);
                    saved.put(request.task(),new ValidationTaskStore.Snapshot(before.task(),1,before.commandId(),before.comparison(),before.settings(),1,
                            before.schedulingCommandId(),false,new ValidationTaskStore.Mode(sequence+1,new ValidationTaskStore.Permission(start,waitForDrain?ValidationTaskStore.Phase.OPEN:ValidationTaskStore.Phase.CLOSED)),before.incremental()));
                }else{
                    assertThat(command.action()).isIn(ValidationOwnerRouter.Action.STATUS,ValidationOwnerRouter.Action.STOP);statusReads.incrementAndGet();
                    if(closure==Closure.OTHER_OWNER&&command.owner().equals(saved.get(command.request().task()).full().permission().start().run().owner()))
                        return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"COMPLETED",true,false,false,true,true,null);
                    if(receiptReaped)return ValidationOwnerRouter.commandUnavailable(command,ValidationOwnerRouter.Outcome.REJECTED,null);
                    if(command.action()==ValidationOwnerRouter.Action.STOP){
                        var before=saved.get(command.request().task());
                        saved.put(command.request().task(),new ValidationTaskStore.Snapshot(before.task(),1,before.commandId(),before.comparison(),before.settings(),1,before.schedulingCommandId(),false,
                                new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(before.full().permission().start(),ValidationTaskStore.Phase.CLOSED)),before.incremental()));
                    }
                }
                boolean terminal=!receiptReaped&&saved.get(command.request().task()).full().permission().phase()==ValidationTaskStore.Phase.CLOSED;
                return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,terminal?"COMPLETED":"STARTING",true,false,false,terminal,terminal&&drained.get(),null);
            });
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                ValidationOperation first=null;
                for(int index=0;index<129;index++){
                    String task="task-"+index;saved.put(task,snapshot("flow",task));
                    var op=new ValidationOperation(coordinator.context(),"flow",task,GENERATION,1,1,UUID.randomUUID());if(first==null)first=op;
                    if(index==128&&closure!=Closure.EXACT){
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->closedLookups.get()>0);
                        if(closure==Closure.OTHER_OWNER)try(var foreign=coordinator.read(first)){
                            assertThat(foreign.value().drain()).isEqualTo(ValidationOperation.Drain.DRAINED);
                        }
                        try(var full=coordinator.start(op)){assertThat(full.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);}
                        assertThat(entryCount(coordinator)).isEqualTo(128);assertThat(activations).hasValue(128);
                        assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(128);return;
                    }
                    if(index==128&&waitForDrain){
                        try(var full=coordinator.start(op)){assertThat(full.value().outcome()).isEqualTo(ValidationOperation.Outcome.CAPACITY);}
                        assertThat(activations).hasValue(128);assertThat(entryCount(coordinator)).isEqualTo(128);drained.set(true);
                    }
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).pollInterval(Duration.ofMillis(2)).untilAsserted(()->{
                        try(var response=coordinator.start(op)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.ACCEPTED);}
                    });
                    int expected=index+1;
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).pollInterval(Duration.ofMillis(2)).until(()->
                            activations.get()==expected&&resources.snapshot().metadataFrames()==0&&resources.snapshot().processReserved().bytes()==2L*1024*1024);
                    if(waitForDrain)try(var stopped=coordinator.stop(op)){
                        assertThat(stopped.value().drain()).isEqualTo(drained.get()?ValidationOperation.Drain.DRAINED:ValidationOperation.Drain.NOT_DRAINED);
                    }
                }
                assertThat(activations).hasValue(129);
                try(var replay=coordinator.start(first)){
                    assertThat(replay.value().outcome()).isEqualTo(ValidationOperation.Outcome.ACCEPTED);
                    if(receiptReaped){assertThat(replay.value().state()).isNull();assertThat(replay.value().drain()).isEqualTo(ValidationOperation.Drain.UNKNOWN);}
                    else assertThat(replay.value().state()).isEqualTo("COMPLETED");
                }
                assertThat(activations).hasValue(129);
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(129);
                if(waitForDrain||receiptReaped)assertThat(statusReads).hasPositiveValue();
            }
        }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={true,false})
    @Timeout(30) void stoppingAClosedOriginalAfterMemoEvictionRetainsRetriesUntilDrain(boolean unknownReply)throws Exception{closedOriginalStopRetries(unknownReply,2);}
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={true,false})
    @Timeout(30) void threeCountFramesCannotStarveAnOriginalOwnerStopRetry(boolean unknownReply)throws Exception{closedOriginalStopRetries(unknownReply,3);}
    private void closedOriginalStopRetries(boolean unknownReply,int retainedFrames)throws Exception{
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",retainedFrames==3?8:2,()->{});var configuration=configuration();
            var current=new AtomicReference<>(snapshot("flow","task"));var drained=new AtomicBoolean();
            var forbiddenSnapshot=new AtomicBoolean();var activations=new AtomicInteger();var stops=new AtomicInteger();var original=new AtomicReference<ValidationOperation>();
            when(configuration.validationTasks(any()).read("flow","task")).thenAnswer(call->{
                if(forbiddenSnapshot.get())throw new AssertionError("Automatic scalar STOP cannot materialize a task snapshot");return Optional.of(current.get());});
            for(String node:List.of("a","b"))routing.commands.put(node,command->{
                assertThat(command.request().command()).isEqualTo(original.get().command());
                if(command.action()==ValidationOwnerRouter.Action.ACTIVATE){
                    activations.incrementAndGet();var before=current.get();var spec=new ValidationRunSpec(before.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
                    var identity=new ValidationRunIdentity(command.owner(),"task",command.request().run(),ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");
                    var start=new ValidationReportAdmission.Start(before.task(),1,1,command.request().command().toString(),identity);
                    current.set(new ValidationTaskStore.Snapshot(before.task(),1,before.commandId(),before.comparison(),before.settings(),1,before.schedulingCommandId(),false,
                            new ValidationTaskStore.Mode(2,new ValidationTaskStore.Permission(start,ValidationTaskStore.Phase.CLOSED)),before.incremental()));
                }else if(command.action()==ValidationOwnerRouter.Action.STOP){
                    stops.incrementAndGet();
                    if(unknownReply&&!drained.get())return ValidationOwnerRouter.commandUnavailable(command,ValidationOwnerRouter.Outcome.UNKNOWN,null);
                }else{
                    assertThat(command.action()).isEqualTo(ValidationOwnerRouter.Action.STATUS);
                    return ValidationOwnerRouter.commandUnavailable(command,ValidationOwnerRouter.Outcome.REJECTED,null);
                }
                return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"COMPLETED",true,false,false,true,drained.get(),null);
            });
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());original.set(op);
                try(var response=coordinator.start(op)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.ACCEPTED);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->activations.get()==1&&entryCount(coordinator)==0&&resources.snapshot().metadataFrames()==0);
                var retained=new ArrayList<ValidationMemberResources.Metadata>();for(int frame=0;frame<retainedFrames;frame++)retained.add(resources.admitRetainedMetadata());
                try{
                    try(var response=coordinator.stop(op)){
                        assertThat(response.value().drain()).isEqualTo(unknownReply?ValidationOperation.Drain.UNKNOWN:ValidationOperation.Drain.NOT_DRAINED);
                    }
                    if(retainedFrames==3)forbiddenSnapshot.set(true);
                    assertThat(entryCount(coordinator)).isOne();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->stops.get()>=2);
                    assertThat(entryCount(coordinator)).isOne();
                    assertThat(resources.snapshot().metadataFrames()).isLessThanOrEqualTo(3);
                    try(var foreground=resources.admitControl()){
                        var finished=new CountDownLatch(1);foreground.execute(finished::countDown);
                        assertThat(finished.await(2,TimeUnit.SECONDS)).isTrue();
                    }
                    if(retainedFrames==3){drained.set(true);org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->entryCount(coordinator)==0);assertThat(resources.snapshot().metadataFrames()).isEqualTo(3);}
                }finally{forbiddenSnapshot.set(false);retained.forEach(ValidationMemberResources.Metadata::close);}
                if(retainedFrames==2)assertThat(entryCount(coordinator)).isOne();drained.set(true);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->entryCount(coordinator)==0&&resources.snapshot().metadataFrames()==0);
                assertThat(activations).hasValue(1);
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
            }
        }
    }
    @Test @Timeout(30) void anOwnerlessManualStopRecoversItsOriginalReceiptWithoutMoreClientCalls()throws Exception{ownerlessStopRetries(2);}
    @Test @Timeout(30) void threeCountFramesCannotStarveAnOwnerlessStopReceiptRetry()throws Exception{ownerlessStopRetries(3);}
    private void ownerlessStopRetries(int retainedFrames)throws Exception{
        var visible=new AtomicBoolean();var receiptReads=new AtomicInteger();var stops=new CountDownLatch(1);
        var receivers=ConcurrentHashMap.<String>newKeySet();var original=new AtomicReference<ValidationOperation>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var forbiddenSnapshot=new AtomicBoolean();var configuration=configuration();
            var saved=snapshot("flow","task");when(configuration.validationTasks(any()).read("flow","task")).thenAnswer(call->{if(forbiddenSnapshot.get())throw new AssertionError("Original STOP receipt retry cannot load a task snapshot");return Optional.of(saved);});
            var resources=process.bind("cluster","a","boot-a",retainedFrames==3?8:2,()->{});
            for(String node:List.of("a","b")){
                routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null));
                routing.receipts.put(node,request->{
                    assertThat(request.command()).isEqualTo(original.get().command());receivers.add(node);receiptReads.incrementAndGet();
                    return visible.get()?new ValidationOwnerRouter.LocalReceipt(new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,
                            new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null),false):ValidationOwnerRouter.LocalReceipt.unknown();
                });
                routing.commands.put(node,command->{
                    assertThat(command.action()).isEqualTo(ValidationOwnerRouter.Action.STOP);assertThat(command.request().command()).isEqualTo(original.get().command());
                    stops.countDown();return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STOPPED",false,false,false,true,true,null);
                });
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());original.set(op);
                try(var response=coordinator.start(op)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.UNKNOWN);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()==1&&resources.snapshot().metadataFrames()==0);
                var retained=new ArrayList<ValidationMemberResources.Metadata>();for(int frame=0;frame<retainedFrames;frame++)retained.add(resources.admitRetainedMetadata());
                try{
                    try(var response=coordinator.stop(op)){assertThat(response.value().outcome()).isEqualTo(ValidationOperation.Outcome.UNKNOWN);}
                    assertThat(receiptReads.get()).isPositive();if(retainedFrames==3)forbiddenSnapshot.set(true);visible.set(true);
                    assertThat(stops.await(3,TimeUnit.SECONDS)).as("timer must recover and stop the original without client polling").isTrue();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(()->{
                        var entries=(Map<?,?>)org.springframework.test.util.ReflectionTestUtils.getField(coordinator,"entries");
                        synchronized(entries){
                            assertThat(entries).hasSize(1);var observed=(ValidationOperation.Status)org.springframework.test.util.ReflectionTestUtils.getField(entries.values().iterator().next(),"observed");
                            assertThat(observed.drain()).isEqualTo(ValidationOperation.Drain.DRAINED);
                        }
                    });
                    assertThat(receivers).hasSize(1);assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isOne();
                    assertThat(resources.snapshot().metadataFrames()).isLessThanOrEqualTo(3);
                    try(var foreground=resources.admitControl()){var done=new CountDownLatch(1);foreground.execute(done::countDown);assertThat(done.await(2,TimeUnit.SECONDS)).isTrue();}
                }finally{forbiddenSnapshot.set(false);retained.forEach(ValidationMemberResources.Metadata::close);}
            }
        }
    }
    @Test @Timeout(30) void aBusyStopAttemptCannotRotateItsSuccessorBackBehindIt()throws Exception{
        var aEntered=new CountDownLatch(1);var aRelease=new CountDownLatch(1);var bStopped=new CountDownLatch(1);
        var attempts=new ConcurrentHashMap<UUID,AtomicInteger>();var first=new AtomicReference<ValidationOperation>();var second=new AtomicReference<ValidationOperation>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",8,()->{});var configuration=configuration();
            when(configuration.validationTasks(any()).read(anyString(),eq("task"))).thenAnswer(call->Optional.of(snapshot(call.getArgument(0),"task")));
            for(String node:List.of("a","b"))routing.commands.put(node,command->{
                if(command.action()==ValidationOwnerRouter.Action.ACTIVATE)return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"RUNNING",true,false,false,false,false,null);
                if(command.action()==ValidationOwnerRouter.Action.STATUS)return ValidationOwnerRouter.commandUnavailable(command,ValidationOwnerRouter.Outcome.UNKNOWN,null);
                assertThat(command.action()).isEqualTo(ValidationOwnerRouter.Action.STOP);int number=attempts.computeIfAbsent(command.request().command(),ignored->new AtomicInteger()).incrementAndGet();
                if(number==1)return ValidationOwnerRouter.commandUnavailable(command,ValidationOwnerRouter.Outcome.UNKNOWN,null);
                if(command.request().command().equals(first.get().command())){
                    if(number==2){aEntered.countDown();try{assertThat(aRelease.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}
                    return ValidationOwnerRouter.commandUnavailable(command,ValidationOwnerRouter.Outcome.UNKNOWN,null);
                }
                assertThat(command.request().command()).isEqualTo(second.get().command());bStopped.countDown();
                return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STOPPED",true,false,false,true,true,null);
            });
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                ((ScheduledFuture<?>)org.springframework.test.util.ReflectionTestUtils.getField(coordinator,"stops")).cancel(false);
                first.set(new ValidationOperation(coordinator.context(),"first","task",GENERATION,1,1,UUID.randomUUID()));
                second.set(new ValidationOperation(coordinator.context(),"second","task",GENERATION,1,1,UUID.randomUUID()));
                for(var op:List.of(first.get(),second.get())){
                    try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==0&&resources.snapshot().processReserved().bytes()==2L*1024*1024);
                }
                var held=new ArrayList<ValidationMemberResources.Metadata>();for(int frame=0;frame<3;frame++)held.add(resources.admitRetainedMetadata());
                try{
                    for(var op:List.of(first.get(),second.get())){
                        try(var response=coordinator.stop(op)){assertThat(response.value().original()).isEqualTo(op);}
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->attempts.containsKey(op.command())&&resources.snapshot().metadataFrames()==3&&resources.snapshot().processReserved().bytes()==2L*1024*1024);
                    }
                    org.springframework.test.util.ReflectionTestUtils.invokeMethod(coordinator,"dispatchStops");assertThat(aEntered.await(2,TimeUnit.SECONDS)).isTrue();
                    org.springframework.test.util.ReflectionTestUtils.invokeMethod(coordinator,"dispatchStops");aRelease.countDown();
                    var permits=(Semaphore)org.springframework.test.util.ReflectionTestUtils.getField(process,"originalStopSlots");
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->permits.availablePermits()==1&&resources.snapshot().processReserved().bytes()==2L*1024*1024);
                    org.springframework.test.util.ReflectionTestUtils.invokeMethod(coordinator,"dispatchStops");
                    assertThat(bStopped.await(2,TimeUnit.SECONDS)).as("The successor keeps priority through ticks when the original retry permit is busy").isTrue();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->permits.availablePermits()==1&&resources.snapshot().processReserved().bytes()==2L*1024*1024);
                    try(var foreground=coordinator.read(second.get())){assertThat(foreground.value().drain()).isEqualTo(ValidationOperation.Drain.DRAINED);}
                }finally{aRelease.countDown();held.forEach(ValidationMemberResources.Metadata::close);}
            }
        }finally{aRelease.countDown();}
    }

    @Test @Timeout(30) void sequenceAdvancementCannotPruneAnOwnerlessPendingStop()throws Exception {
        var visible=new AtomicBoolean();var oldCalls=new AtomicInteger();var stopped=new CountDownLatch(1);var original=new AtomicReference<ValidationOperation>();
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();var saved=snapshot("flow","task");var sequence=new AtomicLong(1);
            var consumedSpec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
            var consumedIdentity=new ValidationRunIdentity(new ValidationOwner("cluster","flow","a","boot-a",3,4,1),"task","other-consumed-original",ValidationMode.FULL,consumedSpec.fingerprint(),1,"direct-full-v1");
            var consumed=new ValidationReportAdmission.Start(saved.task(),1,1,UUID.randomUUID().toString(),consumedIdentity);
            when(configuration.validationTasks(any()).read("flow","task")).thenAnswer(call->Optional.of(new ValidationTaskStore.Snapshot(saved.task(),saved.revision(),saved.commandId(),saved.comparison(),saved.settings(),
                    saved.schedulingRevision(),saved.schedulingCommandId(),false,new ValidationTaskStore.Mode(sequence.get(),sequence.get()==1?null:new ValidationTaskStore.Permission(consumed,ValidationTaskStore.Phase.CLOSED)),saved.incremental())));
            for(String node:List.of("a","b")){
                routing.behaviors.put(node,request->{if(request.command().equals(original.get().command()))oldCalls.incrementAndGet();return new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null);});
                routing.receipts.put(node,request->request.command().equals(original.get().command())&&visible.get()
                        ?new ValidationOwnerRouter.LocalReceipt(new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.ACCEPTED,new ValidationOwner("cluster","flow",node,"boot-"+node,3,4,1),null),false):ValidationOwnerRouter.LocalReceipt.unknown());
                routing.commands.put(node,command->{assertThat(command.action()).isEqualTo(ValidationOwnerRouter.Action.STOP);assertThat(command.request().command()).isEqualTo(original.get().command());stopped.countDown();
                    return new ValidationOwnerRouter.CommandReply(ValidationOwnerRouter.Outcome.ACCEPTED,command,"STOPPED",false,false,false,true,true,null);});
            }
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var op=operation(coordinator.context());original.set(op);try(var response=coordinator.start(op)){assertThat(response.value().original()).isEqualTo(op);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->oldCalls.get()==1&&resources.snapshot().metadataFrames()==0);
                try(var response=coordinator.stop(op)){assertThat(response.value().drain()).isEqualTo(ValidationOperation.Drain.UNKNOWN);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==0);
                var held=new ArrayList<>(resources.admitRetainedMetadata(2));held.add(resources.admitWorkMetadata());
                try{
                    sequence.set(2);var next=new ValidationOperation(coordinator.context(),"flow","task",GENERATION,1,2,UUID.randomUUID());
                    try(var response=coordinator.start(next)){assertThat(response.value().original()).isEqualTo(next);}
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==3);
                    assertThat(entryCount(coordinator)).as("Unknown original STOP remains pinned when another command consumes the sequence").isEqualTo(2);
                }finally{held.forEach(ValidationMemberResources.Metadata::close);}
                visible.set(true);assertThat(stopped.await(3,TimeUnit.SECONDS)).as("The original receiver receipt still recovers without another client request").isTrue();
                assertThat(oldCalls).hasValue(1);
            }
        }
    }

    @Test @Timeout(30) void aLocallyUnsubmittedStopMemoCanBePrunedAfterSequenceAdvancement()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var paused=new AtomicBoolean();var nextSequence=new AtomicLong(1);
        try(var routing=new ValidationOwnerRouterTest.Fixture(Duration.ofSeconds(5),1);var process=new ValidationMemberResources()){
            var resources=process.bind("cluster","a","boot-a",2,()->{});var configuration=configuration();var saved=snapshot("flow","task");
            var consumedSpec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,ValidationMethod.COUNT);
            var consumedIdentity=new ValidationRunIdentity(new ValidationOwner("cluster","flow","a","boot-a",3,4,1),"task","other-consumed-original",ValidationMode.FULL,consumedSpec.fingerprint(),1,"direct-full-v1");
            var consumed=new ValidationReportAdmission.Start(saved.task(),1,1,UUID.randomUUID().toString(),consumedIdentity);
            when(configuration.validationTasks(any()).read("flow","task")).thenAnswer(call->{
                boolean advancing=StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().equals(NativeValidationStartCoordinator.class.getName())&&frame.getMethodName().equals("advance")));
                if(advancing&&paused.compareAndSet(false,true)){entered.countDown();try{assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}
                return Optional.of(new ValidationTaskStore.Snapshot(saved.task(),1,saved.commandId(),saved.comparison(),saved.settings(),saved.schedulingRevision(),saved.schedulingCommandId(),false,
                        new ValidationTaskStore.Mode(nextSequence.get(),nextSequence.get()==1?null:new ValidationTaskStore.Permission(consumed,ValidationTaskStore.Phase.CLOSED)),saved.incremental()));
            });
            for(var node:List.of("a","b"))routing.behaviors.put(node,request->new ValidationOwnerRouter.Result(ValidationOwnerRouter.Outcome.UNKNOWN,null,null));
            try(var coordinator=new NativeValidationStartCoordinator(routing.a,configuration,routing.left,resources,process,()->{})){
                var old=operation(coordinator.context());try(var response=coordinator.start(old)){assertThat(response.value().original()).isEqualTo(old);}
                try{assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();try(var response=coordinator.stop(old)){assertThat(response.value().original()).isEqualTo(old);}}finally{release.countDown();}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==0);
                assertThat(routing.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isZero();nextSequence.set(2);
                var next=new ValidationOperation(coordinator.context(),"flow","task",GENERATION,1,2,UUID.randomUUID());try(var response=coordinator.start(next)){assertThat(response.value().original()).isEqualTo(next);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->resources.snapshot().metadataFrames()==0);
                assertThat(entryCount(coordinator)).as("Positive local non-submission can be reclaimed; uncertain STOP remains covered separately").isEqualTo(1);
            }
        }finally{release.countDown();}
    }

    static ValidationOperation operation(ValidationOperation.Context context){return new ValidationOperation(context,"flow","task",GENERATION,1,1,UUID.randomUUID());}
    static MongoConfigurationDomain configuration(){
        var configuration=mock(MongoConfigurationDomain.class);var tasks=mock(ValidationTaskStore.class);var reports=mock(ValidationReports.class);
        when(configuration.clusterId()).thenReturn("cluster");when(configuration.validationTasks(any())).thenReturn(tasks);when(configuration.validationReports(any())).thenReturn(reports);
        when(reports.find(any())).thenReturn(Optional.empty());when(tasks.read(anyString(),anyString())).thenAnswer(call->Optional.of(snapshot(call.getArgument(0),call.getArgument(1))));return configuration;
    }
    private static ValidationTaskStore.Snapshot snapshot(String pipeline,String task){
        var table=new ValidationTaskSpec.Table("source","source",null,null,"orders",null,null);var target=new ValidationTaskSpec.Table("out","target",null,null,"orders",null,null);
        var spec=new ValidationTaskSpec(pipeline,Map.of(pipeline,"a".repeat(64),"source","b".repeat(64),"target","c".repeat(64)),1,"direct-full-v1",
                List.of(new ValidationTaskSpec.Mapping("orders",ValidationTaskSpec.OutputKind.SYNC,"out",table,target,null)));
        return new ValidationTaskStore.Snapshot(new ValidationReportAdmission.Task("cluster",pipeline,task,GENERATION),1,UUID.randomUUID(),spec,
                ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,0),1,UUID.randomUUID(),false,new ValidationTaskStore.Mode(1,null),new ValidationTaskStore.Mode(1,null));
    }
}
