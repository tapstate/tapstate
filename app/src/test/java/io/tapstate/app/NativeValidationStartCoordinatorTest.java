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
                var op=operation(a.context());
                try(var response=b.start(op)){assertThat(response.value().outcome()).isIn(ValidationOperation.Outcome.PENDING,ValidationOperation.Outcome.ACCEPTED);}
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->activated.get()==1&&resourcesA.snapshot().processReserved().bytes()==4L*1024*1024);
                int reservations=routing.calls.values().stream().mapToInt(AtomicInteger::get).sum();assertThat(reservations).isOne();
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
