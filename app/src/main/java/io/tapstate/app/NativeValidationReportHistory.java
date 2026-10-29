package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Bounded original FULL report maintenance. This never prepares storage or creates a reader, Job or owner. */
final class NativeValidationReportHistory implements AutoCloseable {
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final CacheBusinessScope scope;
    private final Supplier<WorkloadClaimFence> node;
    private final Runnable active;
    private final ValidationMemberResources.Member resources;
    private final ValidationMemberResources process;
    private final NativeValidationOwnerReservations owners;
    private final Clock clock;
    private final AtomicBoolean inFlight=new AtomicBoolean();
    private final ScheduledFuture<?> polling;
    private volatile boolean closed;
    private long nextAttempt;
    private String pipeline,afterPipeline,afterTask;
    NativeValidationReportHistory(MongoConfigurationDomain configuration,MongoCacheDomain cache,CacheBusinessScope scope,
            Supplier<WorkloadClaimFence> node,Runnable active,ValidationMemberResources.Member resources,
            ValidationMemberResources process,NativeValidationOwnerReservations owners,Clock clock){
        this.configuration=Objects.requireNonNull(configuration);this.cache=Objects.requireNonNull(cache);this.scope=Objects.requireNonNull(scope);
        this.node=Objects.requireNonNull(node);this.active=Objects.requireNonNull(active);this.resources=Objects.requireNonNull(resources);
        this.process=Objects.requireNonNull(process);this.owners=Objects.requireNonNull(owners);this.clock=Objects.requireNonNull(clock);
        polling=process.controlTicks(this::dispatch);
    }
    private void dispatch(){
        long now=System.nanoTime();if(closed||now-nextAttempt<0||!inFlight.compareAndSet(false,true))return;
        nextAttempt=now+TimeUnit.SECONDS.toNanos(1);
        process.cleanup(()->{try{scan();}catch(TapstateException unavailable){/* Preserve unknown originals for a later bounded pass. */}
            finally{inFlight.set(false);}}).whenComplete((ignored,failed)->{if(failed!=null)inFlight.set(false);});
    }
    /** One saved task per pass, including after a refused original; no failed Pipeline can starve later Pipelines. */
    synchronized void scan(){
        requireActive();try(var frame=resources.admitMetadata()){
            var tasks=configuration.validationTasks(this::requireActive);
            if(pipeline==null){
                var next=tasks.pipelines(afterPipeline,1);if(next.isEmpty()){afterPipeline=null;return;}
                pipeline=next.getFirst();afterTask=null;
            }
            var current=pipeline;final List<String> ids;
            try{ids=tasks.taskIds(current,afterTask,1);}
            catch(TapstateException unreadable){afterPipeline=pipeline;pipeline=null;afterTask=null;throw unreadable;}
            if(ids.isEmpty()){afterPipeline=pipeline;pipeline=null;afterTask=null;return;}
            afterTask=ids.getFirst();var saved=tasks.read(current,afterTask).orElse(null);if(saved==null)return;
            if(saved.closed())return;
            var access=configuration.loadValidationAccess(current,scope,cache,this::requireActive);
            reconcile(access,node.get(),saved,clock,owners::expireRetained);
        }
    }
    static void reconcile(MongoConfigurationDomain.ValidationAccess access,WorkloadClaimFence node,
            ValidationTaskStore.Snapshot saved,Clock clock,Consumer<ValidationControlStore.Status> expire){
        if(saved.closed())return;
        var history=access.history();var reports=access.reports();
        var selected=history.terminals(node,new ValidationRunHistory.Selection(saved.task(),ValidationMode.FULL,100,28));
        var pending=new LinkedHashMap<ValidationRunIdentity,ValidationControlStore.Status>();
        for(var known:selected.recent())pending.put(known.run().identity(),known);
        for(var known:selected.oldest())pending.putIfAbsent(known.run().identity(),known);
        for(var header:pending.values()){
            var reference=reference(header.run());
            if(reports.find(reference).isEmpty())continue;
            var known=history.terminal(node,header.run());
            if(known.isPresent())reports.mirror(new ValidationReports.TerminalMirror(known.orElseThrow(),clock.instant(),null,null));
        }
        reports.retain(new ValidationReports.Retention(saved.task(),ValidationMode.FULL,saved.settings().full().historyLimit(),List.copyOf(pending.values())));
        for(var known:selected.oldest())if(reports.find(reference(known.run())).isEmpty())expire.accept(known);
    }
    private static ValidationReports.Reference reference(ValidationControlStore.Run run){
        var start=run.admission();return new ValidationReports.Reference(start.task().pipelineId(),start.task().taskId(),run.identity().mode(),
                run.identity().runId(),start.task().generation(),start.sequence());
    }
    private void requireActive(){if(closed)throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","history.contextClosed"),null);active.run();}
    @Override public void close(){closed=true;polling.cancel(false);}
}
