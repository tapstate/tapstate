package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.*;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.probe.ValidationOperation;
import io.tapstate.spi.store.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static io.tapstate.runtime.probe.ValidationOperation.*;

/** Exact entry-context coordination, distinct from the eventual execution owner. No durable intent is added. */
final class NativeValidationStartCoordinator implements AutoCloseable {
    private static final String BINDING="tapstate.validation-start.entry";
    private static final int MAX_ENTRIES=128;
    private static final long MAX_BYTES=2L*1024*1024;
    private enum Verb { START,READ,STOP }
    private record Key(String pipeline,String task,UUID generation,long revision,long sequence,UUID command) {
        static Key of(ValidationOperation op){return new Key(op.pipeline(),op.task(),op.taskGeneration(),op.revision(),op.sequence(),op.command());}
    }
    private record Known(ValidationReportAdmission.Start start,boolean closed,Status passive) {}
    private record Fence(UUID generation,long nextSequence,boolean closed) implements Serializable {}
    private record Lookup(Known known,boolean eligible,Status refusal,Fence fence) {}
    private final HazelcastInstance member;
    private final MongoConfigurationDomain configuration;
    private final ValidationOwnerRouter router;
    private final ValidationMemberResources.Member resources;
    private final Runnable active;private final long ioTimeoutNanos;private final java.time.Clock clock;
    private final Context context;
    private final ValidationMemberResources.Unbound registryBudget;
    private final ScheduledFuture<?> stops;
    private final Map<Key,Entry> entries=new LinkedHashMap<>();
    private long retainedBytes;
    private volatile boolean closed;
    NativeValidationStartCoordinator(HazelcastInstance member,MongoConfigurationDomain configuration,ValidationOwnerRouter router,
            ValidationMemberResources.Member resources,ValidationMemberResources process,Runnable active){
        this(member,configuration,router,resources,process,active,Duration.ofSeconds(12));
    }
    NativeValidationStartCoordinator(HazelcastInstance member,MongoConfigurationDomain configuration,ValidationOwnerRouter router,
            ValidationMemberResources.Member resources,ValidationMemberResources process,Runnable active,Duration ioTimeout){
        this(member,configuration,router,resources,process,active,ioTimeout,java.time.Clock.systemUTC());
    }
    NativeValidationStartCoordinator(HazelcastInstance member,MongoConfigurationDomain configuration,ValidationOwnerRouter router,
            ValidationMemberResources.Member resources,ValidationMemberResources process,Runnable active,Duration ioTimeout,java.time.Clock clock){
        this.clock=Objects.requireNonNull(clock);
        if(ioTimeout==null||ioTimeout.isZero()||ioTimeout.isNegative()||ioTimeout.compareTo(Duration.ofSeconds(30))>0)throw invalid("entry.ioTimeout");
        ioTimeoutNanos=ioTimeout.toNanos();
        this.member=Objects.requireNonNull(member);this.configuration=Objects.requireNonNull(configuration);this.router=Objects.requireNonNull(router);
        this.resources=Objects.requireNonNull(resources);this.active=Objects.requireNonNull(active);
        var local=member.getCluster().getLocalMember();
        context=new Context(configuration.clusterId(),local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),
                local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE),local.getUuid(),UUID.randomUUID());
        registryBudget=resources.reserve(new io.tapstate.runtime.engine.validation.ValidationLocalBudget.Amount(MAX_BYTES,0,0,0),0);
        try{
            if(member.getUserContext().putIfAbsent(BINDING,this)!=null)throw invalid("entry.alreadyBound");
            stops=process.controlTicks(this::dispatchStops);
        }catch(RuntimeException|Error failed){member.getUserContext().remove(BINDING,this);registryBudget.close();throw failed;}
    }
    Context context(){require();return context;}
    Response start(ValidationOperation original){return response(original,Verb.START,null);}
    Response startScheduled(ValidationOperation original,java.time.Instant latest){return response(original,Verb.START,Objects.requireNonNull(latest));}
    Response read(ValidationOperation original){return response(original,Verb.READ,null);}
    Response stop(ValidationOperation original){return response(original,Verb.STOP,null);}

    private Response response(ValidationOperation original,Verb verb,java.time.Instant latest){
        var scope=new ResponseScope();
        try{return new RetainedResponse(scope,route(original,verb,scope,latest));}
        catch(RuntimeException|Error failed){scope.close();throw failed;}
    }
    private final class ResponseScope implements AutoCloseable {
        private final ValidationMemberResources.Metadata frame=resources.admitMetadata();
        private final Set<ValidationMemberResources.Control> controls=new HashSet<>();private boolean retiring,returned;
        synchronized void require(){frame.requireRetained(resources);if(retiring)throw invalid("entry.responseClosed");}
        ValidationMemberResources.Control control(){
            require();var control=resources.admitControl(frame);boolean accept;
            synchronized(this){accept=!retiring;if(accept)controls.add(control);}
            if(!accept){control.close();throw invalid("entry.responseClosed");}
            control.released().whenComplete((ignored,failed)->{synchronized(ResponseScope.this){controls.remove(control);}release();});return control;
        }
        private void release(){
            boolean release;synchronized(this){release=retiring&&!returned&&controls.isEmpty();if(release)returned=true;}
            if(release)frame.close();
        }
        @Override public void close(){
            List<ValidationMemberResources.Control> original;synchronized(this){if(retiring)return;retiring=true;original=List.copyOf(controls);}
            original.forEach(ValidationMemberResources.Control::close);release();
        }
    }
    private static final class RetainedResponse extends Response {
        private final NativeValidationStartCoordinator.ResponseScope scope;private Status value;
        RetainedResponse(NativeValidationStartCoordinator.ResponseScope scope,Status value){this.scope=scope;this.value=value;}
        @Override public Status value(){scope.require();return value;}
        @Override public void close(){value=null;scope.close();}
    }
    private Status route(ValidationOperation original,Verb verb,ResponseScope scope,java.time.Instant latest){
        require();Objects.requireNonNull(original);
        if(!context.cluster().equals(original.context().cluster()))return status(original,Outcome.REJECTED,null);
        // CONFIG is consulted before either a fresh entry or an original command. Missing facts grant nothing.
        var checked=paid(scope,()->lookup(original));
        if(checked.known()==null&&verb==Verb.START&&!checked.eligible())return checked.refusal();
        var target=member.getCluster().getMembers().stream().filter(value->matches(original.context(),value)).findFirst().orElse(null);
        if(target==null)return checked.known()!=null?checked.known().passive():status(original,verb==Verb.START?Outcome.REJECTED:Outcome.UNKNOWN,null);
        if(target.localMember()){
            if(!context.equals(original.context()))return checked.known()!=null?checked.known().passive():status(original,Outcome.REJECTED,null);
            if(checked.known()!=null)return paid(scope,()->original(original,verb,checked.known()));
            if(needsReceipt(original))return originalReceipt(original,verb,scope);
            return verb==Verb.START&&latest!=null?scheduled(original,checked.fence(),latest,scope):accept(original,verb,checked.fence(),latest);
        }
        try{
            var result=member.getExecutorService(ValidationOwnerRouter.EXECUTOR)
                    .submitToMember(new EntryCall(original,verb,checked.fence(),latest,checked.known()!=null),target).get(2,TimeUnit.SECONDS);
            require();
            if(result==null||!original.equals(result.original()))return status(original,Outcome.UNKNOWN,null);
            return checked.known()!=null&&result.outcome()!=Outcome.ACCEPTED?checked.known().passive():result;
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();return status(original,Outcome.UNKNOWN,null);}
        catch(TimeoutException|CancellationException|RejectedExecutionException|HazelcastInstanceNotActiveException|MemberLeftException
                |com.hazelcast.spi.exception.TargetDisconnectedException|com.hazelcast.spi.exception.TargetNotMemberException|OperationTimeoutException unavailable){return status(original,Outcome.UNKNOWN,null);}
        catch(ExecutionException failed){return failed(original,failed.getCause());}
    }
    private Status accept(ValidationOperation original,Verb verb,Fence fence,java.time.Instant latest){
        return accept(original,verb,fence,latest,true);
    }
    private Status accept(ValidationOperation original,Verb verb,Fence fence,java.time.Instant latest,boolean dispatch){
        return accept(original,verb,fence,latest,dispatch,false);
    }
    private Status accept(ValidationOperation original,Verb verb,Fence fence,java.time.Instant latest,boolean dispatch,boolean known){
        require();if(!context.equals(original.context()))return status(original,Outcome.REJECTED,null);
        Entry entry;
        synchronized(entries){
            if(closed)return status(original,Outcome.REJECTED,null);
            if(verb==Verb.START)prune(original,fence);
            entry=entries.get(Key.of(original));
            if(entry==null){
                if(verb!=Verb.START&&!known)return status(original,Outcome.UNKNOWN,null);
                long bytes=bytes(original);if(entries.size()==MAX_ENTRIES||bytes>MAX_BYTES-retainedBytes)return status(original,Outcome.CAPACITY,null);
                entry=new Entry(original,bytes,latest);entries.put(Key.of(original),entry);retainedBytes+=bytes;
            }
            if(!entry.original.equals(original))return status(original,Outcome.REJECTED,null);
            if(verb==Verb.START&&!Objects.equals(entry.latest,latest))return status(original,Outcome.REJECTED,null);
            if(known)entry.reservationAttempted=true;
            if(verb==Verb.START&&entry.inFlight)entry.retryStart=true;
            if(verb==Verb.STOP){entry.stop=true;entry.retryStart=false;}
            if(dispatch&&entry.observed.drain()!=Drain.DRAINED&&(known||verb!=Verb.READ||entry.owner!=null))dispatch(entry,verb,known);return entry.observed;
        }
    }
    private boolean needsReceipt(ValidationOperation original){
        if(!context.equals(original.context()))return false;
        synchronized(entries){var entry=entries.get(Key.of(original));return entry!=null&&entry.original.equals(original)&&!entry.inFlight&&entry.reservationAttempted
                &&entry.owner==null&&entry.receiver!=null&&entry.observed.drain()!=Drain.DRAINED;}
    }
    /** Progress can recover only an existing handoff. It never ranks or reserves a replacement. */
    private Status originalReceipt(ValidationOperation original,Verb verb,ResponseScope scope){
        if(!context.equals(original.context()))return status(original,Outcome.REJECTED,null);
        var body=scope.control();
        try{return paid(body,()->{
            final Entry entry;
            synchronized(entries){
                entry=entries.get(Key.of(original));
                if(entry==null||closed||!context.equals(original.context())||!entry.original.equals(original))return status(original,Outcome.REJECTED,null);
                if(verb==Verb.STOP){entry.stop=true;entry.retryStart=false;}
                if(verb==Verb.START&&(entry.inFlight||entry.owner!=null))entry.retryStart=true;
                if(entry.inFlight||entry.owner!=null||entry.receiver==null)return entry.observed;
                entry.inFlight=true;entry.body=body;
            }
            return resolveReceipt(entry,verb,body);
        });}catch(TapstateException refused){return status(original,Outcome.UNKNOWN,refused.code().code());}
    }
    private Status resolveReceipt(Entry entry,Verb verb,ValidationMemberResources.Control body){
        var original=entry.original;
        try{
            require();body.require();var receipt=router.receipt(request(original),entry.receiver);require();body.require();
            synchronized(entries){
                if(!context.equals(original.context())||entries.get(Key.of(original))!=entry)return status(original,Outcome.UNKNOWN,null);
                if(receipt.disposed()){
                    entry.observed=new Status(original,runId(original),Outcome.ACCEPTED,"STOPPED",false,false,false,false,Drain.DRAINED,receipt.reply().reasonCode(),List.of());
                    return entry.observed;
                }
                var reply=receipt.reply();
                if(reply.outcome()!=ValidationOwnerRouter.Outcome.ACCEPTED){entry.observed=status(original,Outcome.UNKNOWN,reply.reasonCode());return entry.observed;}
                var identity=reply.receiver();var owner=new ValidationOwner(context.cluster(),original.pipeline(),identity.node(),identity.boot(),
                        reply.claimGeneration(),reply.executionGeneration(),reply.topologyRevision());
                if(entry.owner!=null&&!entry.owner.equals(owner))throw invalid("entry.originalOwnerChanged");
                entry.owner=owner;
            }
            advance(entry,verb);synchronized(entries){return entry.observed;}
        }catch(TapstateException refused){publish(entry,status(original,Outcome.UNKNOWN,refused.code().code()));synchronized(entries){return entry.observed;}}
        finally{synchronized(entries){if(entry.body==body){entry.inFlight=false;entry.body=null;}}}
    }
    /** An executor RPC only queues the paid owner-local read; it never waits on nested member RPC. */
    private Status receive(ValidationOperation original,Verb verb,Fence fence,java.time.Instant latest,boolean known){
        require();if(!context.equals(original.context()))return status(original,Outcome.REJECTED,null);
        synchronized(entries){
            var entry=entries.get(Key.of(original));
            if(entry!=null&&!entry.original.equals(original))return status(original,Outcome.REJECTED,null);
            if(!known&&needsReceipt(original)){
                if(verb==Verb.STOP){entry.stop=true;entry.retryStart=false;}
                dispatchReceipt(entry,verb);return entry.observed;
            }
        }
        return accept(original,verb,fence,latest,true,known);
    }
    private void dispatchReceipt(Entry entry,Verb verb){
        if(entry.inFlight||closed||entry.owner!=null||entry.receiver==null)return;
        final ValidationMemberResources.Control body;
        try{body=resources.admitControl();}
        catch(TapstateException refused){entry.observed=status(entry.original,Outcome.UNKNOWN,refused.code().code());return;}
        entry.inFlight=true;entry.body=body;
        try{body.execute(()->resolveReceipt(entry,verb,body));}
        catch(RuntimeException|Error failed){
            if(body.released().toCompletableFuture().isDone()){entry.inFlight=false;entry.body=null;body.close();}
            entry.observed=status(entry.original,Outcome.UNKNOWN,failed instanceof TapstateException coded?coded.code().code():null);
            if(failed instanceof TapstateException||failed instanceof RejectedExecutionException)return;
            throw failed;
        }
    }
    private ValidationOwnerRouter.Receiver receiver(ValidationOperation original){
        synchronized(entries){var entry=entries.get(Key.of(original));return entry==null?null:entry.receiver;}
    }
    /** The timer caller has no later HTTP poll: return its paid attempt's actual admission outcome.
     * Its existing response frame also pays for the worker, so owner admission can retain inputs. */
    private Status scheduled(ValidationOperation original,Fence fence,java.time.Instant latest,ResponseScope scope){
        var initial=accept(original,Verb.START,fence,latest,false);
        if(initial.outcome()!=Outcome.PENDING)return initial;
        final ValidationMemberResources.Control body;
        try{body=scope.control();}
        catch(TapstateException refused){return scheduledRefusal(original,
                refused.code()==ValidationError.VALUE_LIMIT_EXCEEDED?Outcome.CAPACITY:Outcome.UNKNOWN,refused.code().code());}
        try{return paid(body,()->{
            final Entry entry;
            synchronized(entries){
                entry=entries.get(Key.of(original));
                if(entry==null||closed)return status(original,Outcome.REJECTED,null);
                if(entry.inFlight||entry.reservationAttempted&&entry.owner==null)return entry.observed;
                entry.inFlight=true;entry.body=body;
            }
            advance(entry,Verb.START);
            synchronized(entries){return entry.observed;}
        });}catch(TapstateException refused){return scheduledRefusal(original,Outcome.UNKNOWN,refused.code().code());}
    }
    private Status scheduledRefusal(ValidationOperation original,Outcome outcome,String reason){
        synchronized(entries){
            var entry=entries.get(Key.of(original));
            if(entry==null)return status(original,outcome,reason);
            if(entry.observed.outcome()==Outcome.PENDING&&(outcome==Outcome.UNKNOWN||!entry.inFlight&&!entry.reservationAttempted))
                entry.observed=status(original,outcome,reason);
            return entry.observed;
        }
    }
    /** Only routing memos are removed. Native actors and their independent resource receipts are untouched. */
    private void prune(ValidationOperation current,Fence fence){
        if(fence==null)return;var iterator=entries.values().iterator();
        while(iterator.hasNext()){
            var previous=iterator.next();var op=previous.original;
            if(previous.inFlight||!op.pipeline().equals(current.pipeline())||!op.task().equals(current.task())||!op.taskGeneration().equals(fence.generation()))continue;
            if(previous.stop&&previous.owner!=null&&previous.observed.drain()!=Drain.DRAINED)continue;
            if(fence.closed()||op.sequence()<fence.nextSequence()){iterator.remove();retainedBytes-=previous.bytes;}
        }
    }
    /** Called only while holding entries; submission does not execute nested Hazelcast RPC on its worker. */
    private void dispatch(Entry entry,Verb verb){dispatch(entry,verb,false);}
    private void dispatch(Entry entry,Verb verb,boolean known){
        if(entry.inFlight||closed)return;
        if(entry.reservationAttempted&&entry.owner==null&&!known)return;
        ValidationMemberResources.Control body;
        try{body=resources.admitControl();}
        catch(TapstateException refused){entry.observed=status(entry.original,Outcome.CAPACITY,refused.code().code());return;}
        entry.inFlight=true;entry.body=body;
        try{body.execute(()->advance(entry,verb));}
        catch(RuntimeException|Error failed){
            if(body.released().toCompletableFuture().isDone()){entry.inFlight=false;entry.body=null;body.close();}
            entry.observed=status(entry.original,Outcome.UNKNOWN,failed instanceof TapstateException coded?coded.code().code():null);
            if(failed instanceof TapstateException||failed instanceof RejectedExecutionException)return;
            throw failed;
        }
    }
    private void advance(Entry entry,Verb verb){
        try{
            require();entry.body.require();
            if(expired(entry)){
                publish(entry,entry.owner==null?status(entry.original,Outcome.REJECTED,ValidationError.EXECUTION_CANCELLED.code()):
                        project(entry.original,router.command(ValidationOwnerRouter.Command.of(ValidationOwnerRouter.Action.CANCEL_UNUSED,request(entry.original),entry.owner),entry.receiver)));
                return;
            }
            var checked=lookup(entry.original);
            if(checked.known()!=null){
                final Verb action;
                synchronized(entries){
                    if(closed)return;
                    var owner=checked.known().start().run().owner();
                    if(entry.owner!=null&&!entry.owner.equals(owner))throw invalid("entry.originalOwnerChanged");
                    entry.owner=owner;action=entry.stop?Verb.STOP:verb;
                }
                require();entry.body.require();publish(entry,original(entry.original,action,checked.known()));return;
            }
            synchronized(entries){
                if(entry.stop&&!entry.reservationAttempted){entry.observed=status(entry.original,Outcome.REJECTED,ValidationError.EXECUTION_CANCELLED.code());return;}
            }
            if(entry.owner==null){
                if(verb!=Verb.START)return;
                if(!checked.eligible()){publish(entry,checked.refusal());return;}
                synchronized(entries){if(closed||entry.reservationAttempted)return;entry.reservationAttempted=true;}
                var attempt=router.reserveAttempt(request(entry.original));var reply=attempt.reply();
                synchronized(entries){if(attempt.dispatched())entry.receiver=attempt.receiver();}
                require();entry.body.require();
                if(reply.outcome()!=ValidationOwnerRouter.Outcome.ACCEPTED){publish(entry,status(entry.original,outcome(reply.outcome()),reply.reasonCode()));return;}
                var receiver=reply.receiver();
                synchronized(entries){entry.owner=new ValidationOwner(context.cluster(),entry.original.pipeline(),receiver.node(),receiver.boot(),
                        reply.claimGeneration(),reply.executionGeneration(),reply.topologyRevision());}
            }
            require();entry.body.require();
            if(expired(entry)){router.command(ValidationOwnerRouter.Command.of(ValidationOwnerRouter.Action.CANCEL_UNUSED,request(entry.original),entry.owner),entry.receiver);publish(entry,status(entry.original,Outcome.REJECTED,ValidationError.EXECUTION_CANCELLED.code()));return;}
            boolean stop;synchronized(entries){if(closed)return;stop=entry.stop;}
            var command=ValidationOwnerRouter.Command.of(stop?ValidationOwnerRouter.Action.STOP:verb==Verb.READ?ValidationOwnerRouter.Action.STATUS:ValidationOwnerRouter.Action.ACTIVATE,request(entry.original),entry.owner);
            publish(entry,project(entry.original,router.command(command,entry.receiver)));
        }catch(TapstateException refused){publish(entry,status(entry.original,Outcome.UNKNOWN,refused.code().code()));}
        catch(RuntimeException|Error defect){publish(entry,status(entry.original,Outcome.UNKNOWN,null));throw defect;}
        finally{synchronized(entries){entry.inFlight=false;entry.body=null;}}
    }
    private boolean expired(Entry entry){return !entry.stop&&entry.latest!=null&&clock.instant().isAfter(entry.latest);}
    private void publish(Entry entry,Status result){synchronized(entries){entry.observed=compact(result);}}
    private Status original(ValidationOperation operation,Verb verb,Known known){
            var command=ValidationOwnerRouter.Command.of(verb==Verb.STOP?ValidationOwnerRouter.Action.STOP:
                    verb==Verb.START&&!known.closed()?ValidationOwnerRouter.Action.ACTIVATE:ValidationOwnerRouter.Action.STATUS,request(known.start()),known.start().run().owner());
            var response=router.command(command,receiver(operation));
            return response.outcome()==ValidationOwnerRouter.Outcome.ACCEPTED?project(operation,response):known.passive();
    }
    private Lookup lookup(ValidationOperation operation){
        require();var tasks=configuration.validationTasks(this::require);
        var saved=tasks.read(operation.pipeline(),operation.task()).orElse(null);
        if(saved!=null&&saved.task().generation().equals(operation.taskGeneration())){
            var permission=saved.full().permission();
            if(permission!=null&&matches(operation,permission.start())){
                var start=permission.start();var terminal=configuration.validationReports(this::require).find(reference(start)).orElse(null);
                if(permission.phase()==ValidationTaskStore.Phase.BORN&&terminal==null)throw invalid("entry.bornReportMissing");
                var passive=passive(operation,start,permission.phase()==ValidationTaskStore.Phase.CLOSED,terminal);
                return new Lookup(new Known(start,permission.phase()==ValidationTaskStore.Phase.CLOSED,passive),false,null,fence(saved));
            }
        }
        // Historical original reports can survive a later definition or task lifetime; they still grant no replacement start.
        var retained=configuration.validationReports(this::require).find(new ValidationReports.Reference(operation.pipeline(),operation.task(),ValidationMode.FULL,
                runId(operation),operation.taskGeneration(),operation.sequence())).orElse(null);
        if(retained!=null&&matches(operation,retained.admission()))return new Lookup(new Known(retained.admission(),retained.closed(),passive(operation,retained.admission(),retained.closed(),retained)),false,null,fence(saved));
        boolean valid=saved!=null&&!saved.closed()&&saved.task().generation().equals(operation.taskGeneration())&&saved.revision()==operation.revision()
                &&saved.full().nextStartSequence()==operation.sequence()
                &&(saved.full().permission()==null||saved.full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
        return new Lookup(null,valid,status(operation,Outcome.REJECTED,null),fence(saved));
    }
    private static Fence fence(ValidationTaskStore.Snapshot saved){return saved==null?null:new Fence(saved.task().generation(),saved.full().nextStartSequence(),saved.closed());}
    private <T>T paid(ResponseScope scope,java.util.concurrent.Callable<T> command){return paid(scope.control(),command);}
    private <T>T paid(ValidationMemberResources.Control control,java.util.concurrent.Callable<T> command){
        var value=new CompletableFuture<T>();var answer=new CompletableFuture<T>();
        control.released().whenComplete((ignored,failed)->{if(failed!=null)answer.completeExceptionally(failed);
            else value.whenComplete((result,problem)->{if(problem==null)answer.complete(result);else answer.completeExceptionally(problem);});});
        try{
            control.execute(()->{try{value.complete(command.call());}catch(Throwable failed){value.completeExceptionally(failed);if(failed instanceof Error fatal)throw fatal;}});
            return answer.get(ioTimeoutNanos,TimeUnit.NANOSECONDS);
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw cancelled(interrupted);}
        catch(TimeoutException timeout){throw cancelled(timeout);}
        catch(ExecutionException failed){var cause=failed.getCause();if(cause instanceof Error fatal)throw fatal;if(cause instanceof RuntimeException runtime)throw runtime;throw new IllegalStateException("validation control failed",cause);}
        finally{control.close();}
    }
    private void require(){if(closed)throw invalid("entry.closed");active.run();}
    @Override public void close(){
        List<ValidationMemberResources.Control> owned;
        synchronized(entries){if(closed)return;closed=true;owned=entries.values().stream().map(entry->entry.body).filter(Objects::nonNull).toList();entries.clear();retainedBytes=0;}
        stops.cancel(false);member.getUserContext().remove(BINDING,this);owned.forEach(ValidationMemberResources.Control::close);registryBudget.close();
    }
    private static final class Entry {
        final ValidationOperation original;final long bytes;final java.time.Instant latest;volatile Status observed;boolean inFlight,reservationAttempted,stop;
        ValidationOwner owner;ValidationOwnerRouter.Receiver receiver;ValidationMemberResources.Control body;boolean retryStart;
        Entry(ValidationOperation original,long bytes,java.time.Instant latest){this.original=original;this.bytes=bytes;this.latest=latest;observed=status(original,Outcome.PENDING,null);}
    }
    private void dispatchStops(){
        synchronized(entries){if(closed)return;for(var entry:entries.values()){
            if(entry.stop&&entry.owner!=null&&entry.observed.drain()!=Drain.DRAINED)dispatch(entry,Verb.STOP);
            else if(entry.retryStart&&!entry.inFlight){
                // Coalesce only an explicit START that arrived behind another original control body.
                entry.retryStart=false;
                if(entry.stop||entry.observed.drain()==Drain.DRAINED)continue;
                if(entry.owner==null&&entry.receiver!=null)dispatchReceipt(entry,Verb.START);else dispatch(entry,Verb.START);
            }
        }}
    }
    private static final class EntryCall implements Callable<Status>,Serializable,HazelcastInstanceAware {
        private static final long serialVersionUID=1L;
        private final ValidationOperation original;private final Verb verb;private final Fence fence;private final java.time.Instant latest;private final boolean known;private transient HazelcastInstance member;
        private EntryCall(ValidationOperation original,Verb verb,Fence fence,java.time.Instant latest,boolean known){this.original=original;this.verb=verb;this.fence=fence;this.latest=latest;this.known=known;}
        @Override public void setHazelcastInstance(HazelcastInstance value){member=value;}
        @Override public Status call(){
            if(member==null)return status(original,Outcome.REJECTED,null);
            var bound=member.getUserContext().get(BINDING);
            return bound instanceof NativeValidationStartCoordinator coordinator?coordinator.receive(original,verb,fence,latest,known):status(original,Outcome.REJECTED,null);
        }
    }
    private static Status passive(ValidationOperation operation,ValidationReportAdmission.Start start,boolean closed,ValidationReports.Snapshot retained){
        return new Status(operation,start.run().runId(),Outcome.ACCEPTED,retained==null||retained.terminal()==null?null:retained.terminal().name(),retained!=null,
                false,retained==null||retained.terminal()==null,retained!=null&&retained.terminal()!=null,Drain.UNKNOWN,
                retained==null||retained.failure()==null?null:retained.failure().code(),List.of());
    }
    private static Status project(ValidationOperation operation,ValidationOwnerRouter.CommandReply reply){
        if(reply.outcome()!=ValidationOwnerRouter.Outcome.ACCEPTED)return status(operation,outcome(reply.outcome()),reply.reasonCode());
        return new Status(operation,reply.original().request().run(),Outcome.ACCEPTED,reply.state(),reply.admitted(),reply.waiting(),reply.unknown(),reply.retired(),
                reply.drained()?Drain.DRAINED:Drain.NOT_DRAINED,reply.reasonCode(),reply.tables().stream().map(table->new Table(table.mappingId(),table.phase().name())).toList());
    }
    private static Status compact(Status value){return new Status(value.original(),value.run(),value.outcome(),value.state(),value.admitted(),value.waiting(),value.controlUnknown(),value.retired(),value.drain(),value.reason(),List.of());}
    private static Status status(ValidationOperation original,Outcome outcome,String reason){return new Status(original,runId(original),outcome,null,false,false,true,false,Drain.UNKNOWN,reason,List.of());}
    private static Outcome outcome(ValidationOwnerRouter.Outcome outcome){return switch(outcome){case ACCEPTED->Outcome.ACCEPTED;case CAPACITY->Outcome.CAPACITY;case BUSY->Outcome.BUSY;case REJECTED,REDIRECT->Outcome.REJECTED;case UNKNOWN->Outcome.UNKNOWN;};}
    private static boolean matches(Context context,Member member){return !member.isLiteMember()&&context.member().equals(member.getUuid())
            &&context.node().equals(member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE))&&context.boot().equals(member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE));}
    private static boolean matches(ValidationOperation op,ValidationReportAdmission.Start start){return op.context().cluster().equals(start.task().clusterId())&&op.pipeline().equals(start.task().pipelineId())
            &&op.task().equals(start.task().taskId())&&op.taskGeneration().equals(start.task().generation())&&op.revision()==start.configurationRevision()&&op.sequence()==start.sequence()&&op.command().toString().equals(start.commandId());}
    private static ValidationReports.Reference reference(ValidationReportAdmission.Start start){return new ValidationReports.Reference(start.task().pipelineId(),start.task().taskId(),ValidationMode.FULL,start.run().runId(),start.task().generation(),start.sequence());}
    private static ValidationOwnerRouter.Request request(ValidationOperation op){return new ValidationOwnerRouter.Request(op.pipeline(),op.task(),op.taskGeneration(),op.revision(),op.sequence(),"FULL",op.command(),runId(op));}
    private static ValidationOwnerRouter.Request request(ValidationReportAdmission.Start start){return new ValidationOwnerRouter.Request(start.task().pipelineId(),start.task().taskId(),start.task().generation(),start.configurationRevision(),start.sequence(),"FULL",UUID.fromString(start.commandId()),start.run().runId());}
    static String runId(ValidationOperation op){
        try{var hash=MessageDigest.getInstance("SHA-256");try(var out=new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(),hash))){
            for(var value:List.of("validation-full-operation-v1",op.context().cluster(),op.pipeline(),op.task(),op.taskGeneration().toString(),Long.toString(op.revision()),Long.toString(op.sequence()),op.command().toString())){
                var bytes=value.getBytes(StandardCharsets.UTF_8);out.writeInt(bytes.length);out.write(bytes);}
        }return "full-"+HexFormat.of().formatHex(hash.digest());}catch(NoSuchAlgorithmException|IOException impossible){throw new IllegalStateException("validation operation identity unavailable",impossible);}
    }
    private static long bytes(ValidationOperation op){return 4096L+2L*(op.pipeline().length()+op.task().length()+op.context().cluster().length()+op.context().node().length()+op.context().boot().length());}
    static Status failed(ValidationOperation op,Throwable failure){if(failure instanceof Error fatal)throw fatal;if(failure instanceof TapstateException coded)return status(op,Outcome.UNKNOWN,coded.code().code());
        if(failure instanceof RejectedExecutionException||failure instanceof CancellationException||failure instanceof TimeoutException
                ||failure instanceof HazelcastInstanceNotActiveException||failure instanceof MemberLeftException||failure instanceof OperationTimeoutException
                ||failure instanceof com.hazelcast.spi.exception.TargetDisconnectedException||failure instanceof com.hazelcast.spi.exception.TargetNotMemberException)
            return status(op,Outcome.UNKNOWN,null);throw new IllegalStateException("validation entry receiver failed",failure);}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
    private static TapstateException cancelled(Throwable cause){return new TapstateException(ValidationError.READ_CANCELLED,Map.of("table","validation-control"),cause);}
}
