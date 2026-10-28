package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Native original-run control. The server supplies an independently admitted activation, never a caller DTO. */
final class NativeValidationRuns implements AutoCloseable {
    private record Generation(ValidationOwner owner,long counting) {}
    private final NativeValidationOwnerReservations owners;
    private final ValidationMemberResources.Member resources;
    private final ValidationMemberResources process;
    private final ConcurrentMap<ValidationRunIdentity,Actor> runs=new ConcurrentHashMap<>();
    private final Map<Generation,Shared> quotas=new HashMap<>();
    private final ScheduledFuture<?> polling;
    private final AtomicInteger rotation=new AtomicInteger();
    private volatile boolean closed;
    NativeValidationRuns(NativeValidationOwnerReservations owners,ValidationMemberResources.Member resources,
            ValidationMemberResources process,ValidationOwnerTimers timers) {
        this.owners=Objects.requireNonNull(owners);this.resources=Objects.requireNonNull(resources);this.process=Objects.requireNonNull(process);
        polling=timers.deadlines.scheduleWithFixedDelay(this::tick,10,100,TimeUnit.MILLISECONDS);
    }
    ValidationRunLifecycle.Status start(ValidationReportAdmission.Start original,ValidationRunLifecycle.Activation activation) {
        if(closed)throw invalid("runs.contextClosed");
        var existing=runs.get(original.run());if(existing!=null){if(!existing.original.admission().equals(original))throw invalid("runs.originalCommand");return existing.observed;}
        var reservation=owners.take(original);var snapshot=reservation.spec();
        var run=new ValidationControlStore.Run(original.run(),reservation.countingGeneration(),original,snapshot);
        var candidate=new Actor(run,reservation,Objects.requireNonNull(activation));
        synchronized(runs) {
            if(closed)throw invalid("runs.contextClosed");existing=runs.get(original.run());
            if(existing!=null)return existing.observed;if(runs.size()==128)throw capacity();
            reservation.activate(candidate.lifecycle::stop);runs.put(original.run(),candidate);
        }
        var admitted=candidate.observed;candidate.dispatch();return admitted;
    }
    ValidationRunLifecycle.Status read(ValidationRunIdentity original) {var run=runs.get(original);if(run==null)throw invalid("runs.originalUnavailable");return run.observed;}
    void stop(ValidationRunIdentity original) {var run=runs.get(original);if(run==null)throw invalid("runs.originalUnavailable");run.lifecycle.stop();run.dispatch();}
    private void tick(){var ordered=List.copyOf(runs.values());if(ordered.isEmpty()){if(closed)polling.cancel(false);return;}int offset=Math.floorMod(rotation.getAndIncrement(),ordered.size());for(int index=0;index<ordered.size();index++)ordered.get((offset+index)%ordered.size()).dispatch();}
    private Shared shared(Actor actor) {
        var key=new Generation(actor.original.identity().owner(),actor.original.countingGeneration());
        synchronized(quotas) {
            var current=quotas.get(key);if(current!=null)return current;
            var frame=resources.admitMetadata();
            try {current=new Shared(key,frame,new ValidationOwnerQuota(key.owner(),key.counting(),8L*1024*1024,actor.reservation.access().control(),actor.reservation::requireOwner));quotas.put(key,current);return current;}
            catch(RuntimeException|Error failed){frame.close();throw failed;}
        }
    }
    private final class Actor implements ValidationRunLifecycle.Local {
        final ValidationControlStore.Run original;
        final NativeValidationOwnerReservations.Reservation reservation;
        final ValidationMemberResources.Run allocation;
        final ExecutorService control;
        final ValidationRunLifecycle lifecycle;
        final AtomicBoolean inFlight=new AtomicBoolean(),sealed=new AtomicBoolean();
        volatile Shared shared;volatile ValidationRunLifecycle.Execution execution;
        volatile io.tapstate.runtime.engine.validation.ValidationCandidateQueue candidates;
        io.tapstate.runtime.engine.validation.ValidationDelayController delays;
        ValidationControlStore.Delay confirmedDelay;
        volatile ValidationRunLifecycle.Status observed;
        Actor(ValidationControlStore.Run original,NativeValidationOwnerReservations.Reservation reservation,ValidationRunLifecycle.Activation activation) {
            this.original=original;this.reservation=reservation;allocation=reservation.resources();control=allocation.controlExecutor();
            var factory=new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){var nativeReady=reservation.readiness();return nativeReady==ValidationRunLifecycle.Readiness.READY?activation.readiness():nativeReady;}
                public ValidationRunLifecycle.Execution create(){reservation.beginExecution();var created=activation.create();execution=created;return created;}
                public void prepared(ValidationRunLifecycle.Execution created){
                    activation.prepared(created);
                    if(original.identity().mode()==ValidationMode.INCREMENTAL){
                        candidates=created.candidates().orElseThrow(()->invalid("runs.incrementalQueueUnavailable"));
                        if(!original.identity().equals(candidates.run()))throw invalid("runs.incrementalQueueIdentity");
                        delays=new io.tapstate.runtime.engine.validation.ValidationDelayController(reservation.access().control(),original,candidates,Actor.this::requireRun);
                    }
                }
            };
            lifecycle=new ValidationRunLifecycle(original,reservation.access().admissions(),reservation.access().control(),factory,this);
            observed=lifecycle.status();
        }
        void dispatch() {
            if(!inFlight.compareAndSet(false,true))return;
            Runnable action=()->{
                try {
                    owners.control(original.identity().owner().pipelineId(),()->{
                    if(shared!=null)shared.quota.reconcileLimit();
                    if(execution instanceof ValidationJobExecution job)job.maintain();
                    var state=lifecycle.advance();
                    observed=state;
                    if(state.state()==ValidationRunState.RUNNING&&!sealed.get()&&delays!=null)applySavedDelay();
                    if(state.retired()&&state.drained()) {
                        if(execution instanceof ValidationJobExecution job)job.releaseFactories();
                        runs.remove(original.identity(),this);
                    }
                    });
                } catch(TapstateException unresolved) { /* Original operation and actual reservations remain retained. */ }
                catch(RuntimeException|Error failed){lifecycle.fail(failed);throw failed;}
                finally{try{lifecycle.observePhysicalDrain();observed=lifecycle.status();}finally{inFlight.set(false);}}
            };
            if(closed||control.isShutdown())process.cleanup(action).whenComplete((ignored,failed)->{if(failed!=null)inFlight.set(false);});
            else try{control.execute(action);}catch(RejectedExecutionException|TapstateException unavailable){inFlight.set(false);}
        }
        @Override public void joined(){shared=NativeValidationRuns.this.shared(this);shared.quota.joined(original,this::requireRun,lifecycle::stop);shared.members.add(original.identity());}
        @Override public void requireOwner(){reservation.requireOwner();}
        @Override public void requireRun(){reservation.requireReserved();if(sealed.get())throw invalid("runs.publicationClosed");}
        @Override public void sealPublication(){sealed.set(true);var queue=candidates;if(queue!=null)queue.close();var value=shared;if(value!=null)value.quota.seal(original.identity());}
        private void applySavedDelay(){
            var saved=reservation.savedTask();var originalStart=original.admission();
            if(!saved.task().equals(originalStart.task())||saved.closed()){lifecycle.stop();return;}
            var permission=saved.incremental().permission();
            if(permission==null||!permission.start().equals(originalStart)||permission.phase()!=ValidationTaskStore.Phase.BORN){lifecycle.stop();return;}
            var change=new ValidationControlStore.Delay(original,saved.schedulingRevision(),saved.schedulingCommandId(),saved.settings().incremental().firstCheckDelayMillis());
            if(change.equals(confirmedDelay))return;
            if(delays.apply(change)==io.tapstate.runtime.engine.validation.ValidationDelayController.Application.ACKNOWLEDGED)confirmedDelay=change;
        }
        @Override public void retired(ValidationRunState terminal){
            var value=shared;if(value!=null){value.quota.retired(original,terminal);value.members.remove(original.identity());releaseShared(value);}
            reservation.releaseAfterKnownTerminal();
        }
        @Override public void processorsExited(){allocation.processorsExited();}
        @Override public void drained(){allocation.close();}
    }
    private static final class Shared {
        final Generation generation;final ValidationMemberResources.Metadata frame;final ValidationOwnerQuota quota;final Set<ValidationRunIdentity> members=ConcurrentHashMap.newKeySet();
        Shared(Generation generation,ValidationMemberResources.Metadata frame,ValidationOwnerQuota quota){this.generation=generation;this.frame=frame;this.quota=quota;}
    }
    private void releaseShared(Shared shared) {
        synchronized(quotas) {
            if(shared.members.isEmpty()&&quotas.remove(shared.generation,shared))shared.frame.close();
        }
    }
    /** Empty means bounded backpressure, not a lost root or permission to publish. */
    Optional<io.tapstate.runtime.engine.validation.ValidationProblemQuota.Reservation> reserveConfirmed(ValidationRunIdentity original,ValidationRoot root) {
        var actor=runs.get(original);if(actor==null||actor.shared==null)throw invalid("runs.originalUnavailable");
        var result=new AtomicReference<io.tapstate.runtime.engine.validation.ValidationProblemQuota.Reservation>();
        boolean entered=owners.control(original.owner().pipelineId(),()->{actor.requireRun();result.set(actor.shared.quota.reserve(actor.original,root));});
        return entered?Optional.of(result.get()):Optional.empty();
    }
    boolean publish(ValidationRunIdentity original,io.tapstate.runtime.engine.validation.ValidationProblemQuota.PublicationPermit permit,Runnable boundedSubmit) {
        var actor=runs.get(original);if(actor==null||actor.shared==null)return false;actor.requireRun();return actor.shared.quota.publish(permit,boundedSubmit);
    }
    @Override public void close(){
        List<Actor> ending;synchronized(runs){if(closed)return;closed=true;ending=List.copyOf(runs.values());}
        for(var run:ending){run.lifecycle.stop();run.dispatch();}
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
    private static TapstateException capacity(){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.runControls"),null);}
}
