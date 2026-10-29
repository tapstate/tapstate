package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import io.tapstate.runtime.probe.ValidationOperation;
import io.tapstate.spi.store.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Future FULL occurrences only. Discovery, observation loss and process restart never resume an old run. */
final class NativeValidationScheduler implements AutoCloseable {
    private static final int MAX_TASKS=128;
    private static final Duration LATENESS=Duration.ofSeconds(5);
    private record Key(String pipeline,String task){}
    private static final class Entry {
        final ValidationReportAdmission.Task task;final long revision;final ValidationSchedule schedule;Instant next;boolean pending;
        Entry(ValidationTaskStore.Snapshot saved,Instant next){task=saved.task();revision=saved.revision();schedule=saved.settings().full().schedule();this.next=next;}
    }
    private record Due(Entry entry,Instant at,UUID command){}
    private final MongoConfigurationDomain configuration;private final ValidationMemberResources.Member resources;private final ValidationMemberResources process;
    private final Supplier<NativeValidationStartCoordinator> coordinators;private final MemoryValidationSchedules observations;private final Clock clock;
    private final ValidationSchedule.Calculator calculator;private final Runnable active;
    private final Map<Key,Entry> entries=new LinkedHashMap<>();private final AtomicBoolean discovering=new AtomicBoolean();
    private ScheduledFuture<?> polling;private ValidationMemberResources.Unbound allocation;private ThreadPoolExecutor starts;
    private NativeValidationStartCoordinator coordinator;private MemoryValidationSchedules.Writer writer;private ValidationScheduleObservations.Origin origin;
    private String pipeline,afterPipeline,afterTask;private volatile boolean closed;private boolean started;private Instant highWater;private long nextDiscovery;
    NativeValidationScheduler(MongoConfigurationDomain configuration,ValidationMemberResources.Member resources,ValidationMemberResources process,
            Supplier<NativeValidationStartCoordinator> coordinators,MemoryValidationSchedules observations,Clock clock,ValidationSchedule.Calculator calculator,Runnable active){
        this.configuration=Objects.requireNonNull(configuration);this.resources=Objects.requireNonNull(resources);this.process=Objects.requireNonNull(process);
        this.coordinators=Objects.requireNonNull(coordinators);this.observations=Objects.requireNonNull(observations);this.clock=Objects.requireNonNull(clock);
        this.calculator=Objects.requireNonNull(calculator);this.active=Objects.requireNonNull(active);
    }
    synchronized void start(){if(closed||started)return;started=true;polling=process.controlTicks(this::dispatch);}
    private void dispatch(){
        if(closed)return;
        try{active.run();sendDue();}catch(TapstateException unavailable){return;}
        long now=System.nanoTime();if(now-nextDiscovery<0||!discovering.compareAndSet(false,true))return;nextDiscovery=now+TimeUnit.MILLISECONDS.toNanos(100);
        final ValidationMemberResources.Control work;
        try{work=resources.admitControl();}
        catch(TapstateException unavailable){discovering.set(false);return;}
        try{work.execute(()->{try{scan();}catch(TapstateException unavailable){/* Unknown or unprepared storage grants no schedule. */}
            finally{discovering.set(false);}});}
        catch(RejectedExecutionException|TapstateException unavailable){work.close();discovering.set(false);}
    }
    /** One identity per pass avoids ambient definition enumeration and starvation behind a damaged task. */
    private void scan(){
        require();var tasks=configuration.validationTasks(this::require);
        if(pipeline==null){var found=tasks.pipelines(afterPipeline,1);if(found.isEmpty()){afterPipeline=null;return;}pipeline=found.getFirst();afterTask=null;}
        var current=pipeline;List<String> ids;
        try{ids=tasks.taskIds(current,afterTask,1);}catch(TapstateException failed){afterPipeline=pipeline;pipeline=null;afterTask=null;throw failed;}
        if(ids.isEmpty()){afterPipeline=pipeline;pipeline=null;afterTask=null;return;}
        afterTask=ids.getFirst();var saved=tasks.read(current,afterTask).orElse(null);if(saved==null)return;
        var key=new Key(current,afterTask);var full=saved.settings().full();
        synchronized(this){
            require();
            if(saved.closed()||!full.scheduleEnabled()||full.schedule()==null){entries.remove(key);if(writer!=null)writer.forget(saved.task());return;}
            ensureRegistry();var previous=entries.get(key);
            if(previous!=null&&previous.task.equals(saved.task())&&previous.revision==saved.revision())return;
            if(previous==null&&entries.size()==MAX_TASKS){publish(saved.task(),saved.revision(),null,null,ValidationScheduleObservations.State.FAILED,ValidationError.VALUE_LIMIT_EXCEEDED.code(),null,null);return;}
            final Instant next;
            try{next=calculator.nextAfter(full.schedule(),clock.instant(),ValidationSchedule.SearchBudget.DEFAULT);}
            catch(TapstateException invalid){publish(saved.task(),saved.revision(),null,null,ValidationScheduleObservations.State.FAILED,invalid.code().code(),null,null);return;}
            var entry=new Entry(saved,next);entries.put(key,entry);publish(entry.task,entry.revision,next,null,ValidationScheduleObservations.State.WAITING,null,null,null);
        }
    }
    private void ensureRegistry(){
        if(starts!=null)return;require();var reserved=resources.reserve(new ValidationLocalBudget.Amount(8L*1024*1024,1,0,0),0);
        MemoryValidationSchedules.Writer created=null;
        try{
            coordinator=coordinators.get();var entry=coordinator.context();origin=new ValidationScheduleObservations.Origin(entry.node(),entry.boot(),entry.member(),entry.incarnation());
            created=observations.open(origin);writer=created;allocation=reserved;
            starts=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(16),task->{var thread=new Thread(task,"validation-schedule-start");thread.setDaemon(true);return thread;},new ThreadPoolExecutor.AbortPolicy()){
                @Override protected void terminated(){reserved.close();}
            };
        }catch(RuntimeException|Error failed){if(created!=null)created.close();reserved.close();throw failed;}
    }
    private synchronized void sendDue(){
        require();var now=clock.instant();
        if(highWater!=null&&now.isBefore(highWater))return;highWater=now;
        if(starts==null)return;
        for(var entry:entries.values()){
            if(entry.pending||now.isBefore(entry.next))continue;var due=entry.next;
            entry.next=calculator.nextAfter(entry.schedule,now,ValidationSchedule.SearchBudget.DEFAULT);
            if(Duration.between(due,now).compareTo(LATENESS)>0){publish(entry.task,entry.revision,entry.next,due,ValidationScheduleObservations.State.SKIPPED_LATE,null,null,null);continue;}
            var request=new Due(entry,due,command(entry.task,entry.revision,due));entry.pending=true;
            try{starts.execute(()->fire(request));}
            catch(RejectedExecutionException full){entry.pending=false;publish(entry.task,entry.revision,entry.next,due,ValidationScheduleObservations.State.FAILED,ValidationError.VALUE_LIMIT_EXCEEDED.code(),request.command(),null);}
        }
    }
    private void fire(Due due){
        try{
            require();if(late(due)){publish(due.entry(),due,ValidationScheduleObservations.State.SKIPPED_LATE,null,null);return;}
            final long sequence;
            // Materialize the definition inside its own frame and relinquish it before opening the coordinator response.
            try(var frame=resources.admitMetadata()){
                frame.requireRetained(resources);var saved=configuration.validationTasks(this::require).read(due.entry().task.pipelineId(),due.entry().task.taskId()).orElse(null);
                require();
                if(saved==null||saved.closed()||!saved.task().equals(due.entry().task)||saved.revision()!=due.entry().revision||!saved.settings().full().scheduleEnabled())return;
                var mode=saved.full();var permission=mode.permission();
                if(permission!=null&&permission.start().commandId().equals(due.command().toString()))return;
                if(permission!=null&&permission.phase()!=ValidationTaskStore.Phase.CLOSED){publish(due.entry(),due,ValidationScheduleObservations.State.SKIPPED_BUSY,null,null);return;}
                sequence=mode.nextStartSequence();
            }
            require();if(late(due)){publish(due.entry(),due,ValidationScheduleObservations.State.SKIPPED_LATE,null,null);return;}
            var task=due.entry().task;var operation=new ValidationOperation(coordinator.context(),task.pipelineId(),task.taskId(),task.generation(),due.entry().revision,sequence,due.command());
            try(var result=coordinator.startScheduled(operation,due.at().plus(LATENESS))){
                var value=result.value();var state=switch(value.outcome()){
                    case PENDING,ACCEPTED->ValidationScheduleObservations.State.START_REQUESTED;
                    case BUSY->ValidationScheduleObservations.State.SKIPPED_BUSY;
                    case UNKNOWN->ValidationScheduleObservations.State.UNKNOWN;
                    case CAPACITY,REJECTED->ValidationScheduleObservations.State.FAILED;
                };
                publish(task,due.entry().revision,due.entry().next,due.at(),state,value.reason(),due.command(),value.run(),sequence);
            }
        }catch(TapstateException failed){publish(due.entry(),due,ValidationScheduleObservations.State.FAILED,failed.code().code(),null);}
        catch(RuntimeException|Error defect){publish(due.entry(),due,ValidationScheduleObservations.State.FAILED,null,null);throw defect;}
        finally{synchronized(this){due.entry().pending=false;}}
    }
    private boolean late(Due due){var now=clock.instant();return now.isBefore(due.at())||now.isAfter(due.at().plus(LATENESS));}
    private void publish(Entry entry,Due due,ValidationScheduleObservations.State state,String reason,String run){
        publish(entry.task,entry.revision,entry.next,due.at(),state,reason,due.command(),run);
    }
    private synchronized void publish(ValidationReportAdmission.Task task,long revision,Instant next,Instant last,ValidationScheduleObservations.State state,String reason,UUID command,String run){
        publish(task,revision,next,last,state,reason,command,run,0);
    }
    private synchronized void publish(ValidationReportAdmission.Task task,long revision,Instant next,Instant last,ValidationScheduleObservations.State state,String reason,UUID command,String run,long sequence){
        if(closed||writer==null)return;writer.publish(new ValidationScheduleObservations.Fact(task,revision,origin,clock.instant(),next,last,state,reason,command,run,sequence));
    }
    private void require(){if(closed)throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","schedule.contextClosed"),null);active.run();}
    @Override public synchronized void close(){
        if(closed)return;closed=true;if(polling!=null)polling.cancel(false);entries.clear();if(writer!=null)writer.close();if(starts!=null)starts.shutdown();
    }
    static UUID command(ValidationReportAdmission.Task task,long revision,Instant occurrence){
        try(var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes)){
            for(var value:List.of("validation-full-cycle-v1",task.clusterId(),task.pipelineId(),task.taskId(),task.generation().toString(),ValidationSchedule.RULES_VERSION)){
                var encoded=value.getBytes(StandardCharsets.UTF_8);out.writeInt(encoded.length);out.write(encoded);
            }
            out.writeLong(revision);out.writeLong(occurrence.getEpochSecond());out.writeInt(occurrence.getNano());out.flush();return UUID.nameUUIDFromBytes(bytes.toByteArray());
        }catch(IOException impossible){throw new IllegalStateException("validation cycle identity unavailable",impossible);}
    }
}
