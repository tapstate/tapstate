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
    private final java.time.Clock clock;
    private final io.tapstate.core.logging.SecretRedactor redactor;
    NativeValidationRuns(NativeValidationOwnerReservations owners,ValidationMemberResources.Member resources,
            ValidationMemberResources process,ValidationOwnerTimers timers) {
        this(owners,resources,process,timers,java.time.Clock.systemUTC(),new io.tapstate.core.logging.SecretRedactor());
    }
    NativeValidationRuns(NativeValidationOwnerReservations owners,ValidationMemberResources.Member resources,
            ValidationMemberResources process,ValidationOwnerTimers timers,java.time.Clock clock,io.tapstate.core.logging.SecretRedactor redactor){
        this.clock=Objects.requireNonNull(clock);this.redactor=Objects.requireNonNull(redactor);
        this.owners=Objects.requireNonNull(owners);this.resources=Objects.requireNonNull(resources);this.process=Objects.requireNonNull(process);
        polling=process.controlTicks(this::tick);
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
            reservation.activate(candidate.lifecycle::stop);reservation.retainReportMetadata();runs.put(original.run(),candidate);
        }
        var admitted=candidate.observed;candidate.dispatch();return admitted;
    }
    ValidationRunLifecycle.Status read(ValidationRunIdentity original) {var run=runs.get(original);if(run==null)throw invalid("runs.originalUnavailable");return run.observed;}
    List<ValidationRunLifecycle.Table> tables(ValidationRunIdentity original){
        var actor=runs.get(original);if(actor==null)throw invalid("runs.originalUnavailable");var current=actor.execution;
        if(current!=null)return current.tables();
        return actor.original.snapshot().task().mappings().stream().map(mapping->new ValidationRunLifecycle.Table(mapping.mappingId(),ValidationRunLifecycle.ReadPhase.PENDING)).toList();
    }
    Optional<ValidationReportAdmission.Start> original(ValidationOwnerRouter.Request request,ValidationOwner owner){
        for(var actor:runs.values()){
            var start=actor.original.admission();var identity=start.run();
            if(!identity.owner().equals(owner)||!identity.taskId().equals(request.task())||!identity.runId().equals(request.run())||!identity.mode().name().equals(request.mode()))continue;
            if(!start.task().generation().equals(request.taskGeneration())||start.configurationRevision()!=request.configurationRevision()
                    ||start.sequence()!=request.startSequence()||!start.commandId().equals(request.command().toString()))throw invalid("runs.originalCommand");
            return Optional.of(start);
        }
        return Optional.empty();
    }
    boolean controlIdle(ValidationRunIdentity original){var actor=runs.get(original);return actor==null||!actor.inFlight.get();}
    Optional<ValidationReportMetadata.Failure> reportFailure(ValidationRunIdentity original){var actor=runs.get(original);return actor==null?Optional.empty():Optional.ofNullable(actor.reportFailure);}
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
        final AtomicInteger pendingPublications=new AtomicInteger();
        volatile Shared shared;volatile ValidationRunLifecycle.Execution execution;
        volatile io.tapstate.runtime.engine.validation.ValidationCandidateQueue candidates;
        io.tapstate.runtime.engine.validation.ValidationDelayController delays;
        ValidationControlStore.Delay confirmedDelay;
        volatile ValidationRunLifecycle.Status observed;
        volatile ValidationControlStore.Status terminalKnown;
        java.time.Instant terminalKnownAt,readingStarted,readingFinished;
        boolean reportSettled,retirementReleased,expired;
        volatile ValidationReportMetadata.Failure reportFailure;
        final AtomicBoolean mirrorWaiting=new AtomicBoolean();
        volatile boolean mirrorReady;
        final ValidationReports reports;
        final io.tapstate.core.logging.SecretRedactor reportRedactor;
        Actor(ValidationControlStore.Run original,NativeValidationOwnerReservations.Reservation reservation,ValidationRunLifecycle.Activation activation) {
            this.original=original;this.reservation=reservation;allocation=reservation.resources();control=allocation.controlExecutor();
            reports=reservation.access().reports();var secrets=new ArrayList<String>();var sources=new ArrayList<String>();
            for(var resource:reservation.declarations.artifacts().list())if(resource instanceof io.tapstate.core.model.SourceResource source){sources.add(source.id());secretScalars(source.config(),secrets);}
            reportRedactor=redactor.fork(sources);reportRedactor.replace("original-inputs",secrets);
            var factory=new ValidationRunLifecycle.Activation(){
                public ValidationRunLifecycle.Readiness readiness(){var nativeReady=reservation.readiness();return nativeReady==ValidationRunLifecycle.Readiness.READY?activation.readiness():nativeReady;}
                public ValidationRunLifecycle.Readiness prepare(){
                    var ready=reservation.readiness();if(ready!=ValidationRunLifecycle.Readiness.READY)return ready;
                    ready=activation.prepare();return ready==ValidationRunLifecycle.Readiness.READY?reservation.beginExecution():ready;
                }
                public ValidationRunLifecycle.Execution create(){readingStarted=clock.instant();var created=activation.create();execution=created;return created;}
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
            if(mirrorWaiting.get()&&!mirrorReady)return;
            if(!inFlight.compareAndSet(false,true))return;
            boolean metadata=mirrorReady;
            Runnable action=()->{
                try {
                    if(metadata){
                        mirror(lifecycle.status());reportFailure=null;
                        if(reportSettled)runs.remove(original.identity(),this);
                    }else{
                        if(execution instanceof ValidationJobExecution job){try{job.maintain();}catch(TapstateException cleanupUnknown){if(!lifecycle.status().retired()&&!owners.closed())throw cleanupUnknown;}}
                        owners.control(original.identity().owner().pipelineId(),()->{
                            if(!lifecycle.status().retired()&&(!reservation.originalOwnerAvailable()||observed.controlUnknown()&&observed.admitted())){
                                var known=reservation.retainedTerminal(original);
                                if(known.isPresent()){terminalKnown=known.orElseThrow();terminalKnownAt=clock.instant();lifecycle.knownRetirement(terminalKnown);}
                                else if(reservation.expired(original)){expired=true;lifecycle.stop();}
                            }
                            if(expired)return;
                            if(shared!=null&&!lifecycle.status().retired())shared.quota.reconcileLimit();
                            observed=lifecycle.advance();
                            if(observed.state()==ValidationRunState.RUNNING&&!sealed.get()&&delays!=null)applySavedDelay();
                        });
                    }
                }catch(TapstateException unresolved){
                    if(metadata)reportFailure=NativeValidationReportFailure.capture(unresolved,reportRedactor);
                }catch(RuntimeException|Error failed){lifecycle.fail(failed);throw failed;}
                finally{try{
                    lifecycle.observePhysicalDrain();observed=lifecycle.status();
                    if(expired&&observed.drained())disposeExpiredOriginal();
                    else if(closed&&observed.drained()&&(owners.closed()||observed.retired()&&retirementReleased))disposeClosedContext();
                    else if(!metadata&&observed.retired()&&observed.drained()&&retirementReleased)prepareMirror();
                }finally{inFlight.set(false);}}
            };
            if(metadata||closed||control.isShutdown())process.cleanup(action).whenComplete((ignored,failed)->{if(failed!=null)inFlight.set(false);});
            else try{control.execute(action);}catch(RejectedExecutionException|TapstateException unavailable){inFlight.set(false);}
        }
        private void prepareMirror(){
            if(readingStarted!=null&&readingFinished==null){var finished=clock.instant();if(!finished.isBefore(readingStarted))readingFinished=finished;}
            if(execution instanceof ValidationJobExecution job)job.releaseFactories();
            if(mirrorWaiting.compareAndSet(false,true))allocation.allReleased().whenComplete((ignored,failed)->{
                if(failed==null){mirrorReady=true;dispatch();}
                else reportFailure=NativeValidationReportFailure.capture(failed,reportRedactor);
            });
        }
        private void disposeClosedContext(){
            // This is local context disposal, never a new durable retirement or a completed mirror claim.
            var value=shared;if(value!=null){value.quota.ownerLost();value.members.remove(original.identity());releaseShared(value);}
            reservation.releaseClosedContextMetadata();
            if(execution instanceof ValidationJobExecution job)job.releaseFactories();runs.remove(original.identity(),this);
        }
        private void disposeExpiredOriginal(){
            var value=shared;if(value!=null){value.quota.expired(original.identity());value.members.remove(original.identity());releaseShared(value);}
            reservation.releaseClosedContextMetadata();
            if(execution instanceof ValidationJobExecution job)job.releaseFactories();runs.remove(original.identity(),this);
        }
        private void mirror(ValidationRunLifecycle.Status state){
            var start=original.admission();
            var reference=new ValidationReports.Reference(start.task().pipelineId(),start.task().taskId(),original.identity().mode(),original.identity().runId(),start.task().generation(),start.sequence());
            if(reports.find(reference).isEmpty()){settleReport();return;}
            if(terminalKnown==null){
                var known=reservation.retainedTerminal(original).orElseThrow(()->invalid("runs.terminalUnknown"));
                if(!known.retired()||known.state()!=state.state())throw invalid("runs.terminalUnknown");
                terminalKnown=known;terminalKnownAt=clock.instant();
            }
            var window=readingStarted==null?null:new ValidationReportMetadata.Window(readingStarted,readingFinished);
            var answer=reports.mirror(new ValidationReports.TerminalMirror(terminalKnown,terminalKnownAt,window,NativeValidationReportFailure.capture(lifecycle.executionFailure(),reportRedactor)));
            if(answer==ValidationReports.Mirror.CLOSED_OR_EXPIRED){settleReport();return;}
            if(!state.drained())return;
            var saved=reservation.retainedTask().orElse(null);
            if(saved!=null&&saved.task().equals(start.task())&&!saved.closed()){
                var pending=new ArrayList<ValidationControlStore.Status>();
                for(var actor:runs.values()){
                    var known=actor.terminalKnown;
                    if(known!=null&&known.run().admission().task().equals(start.task())&&known.run().identity().mode()==original.identity().mode())pending.add(known);
                }
                int keep=original.identity().mode()==ValidationMode.FULL?saved.settings().full().historyLimit():saved.settings().incremental().historyLimit();
                reports.retain(new ValidationReports.Retention(start.task(),original.identity().mode(),keep,pending));
            }
            settleReport();
        }
        private void settleReport(){reportSettled=true;reservation.reportSettled();}
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
        @Override public void beforeClosure(){
            if(readingStarted!=null&&readingFinished==null&&(execution==null||execution.progress().readersReleased())){
                var finished=clock.instant();if(!finished.isBefore(readingStarted))readingFinished=finished;
            }
            reports.observe(new ValidationReports.Observation(original.report(),readingStarted==null?null:
                    new ValidationReportMetadata.Window(readingStarted,readingFinished),NativeValidationReportFailure.capture(lifecycle.executionFailure(),reportRedactor)));
        }
        @Override public void retired(ValidationRunState terminal){
            // Obtain the original CACHE receipt while this original tenure is still retained.
            if(terminalKnown==null){
                var known=reservation.retainedTerminal(original).orElse(null);
                if(known!=null){if(!known.retired()||known.state()!=terminal)throw invalid("runs.terminalUnknown");terminalKnown=known;terminalKnownAt=clock.instant();}
            }
            var failure=lifecycle.executionFailure();
            reservation.rememberTerminal(terminal,lifecycle.status().admitted(),failure instanceof TapstateException coded?coded.code().code():null,lifecycle.reportBorn());
            var value=shared;if(value!=null){value.quota.retired(original,terminal);value.members.remove(original.identity());releaseShared(value);}
            reservation.releaseAfterKnownTerminal();retirementReleased=true;
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
    /** Empty means bounded backpressure, not a lost root or permission to publish. Capacity is not timing confirmation. */
    Optional<io.tapstate.runtime.engine.validation.ValidationProblemQuota.Reservation> reserveConfirmed(ValidationRunIdentity original,ValidationRoot root) {
        var actor=runs.get(original);if(actor==null||actor.shared==null)throw invalid("runs.originalUnavailable");
        var result=new AtomicReference<io.tapstate.runtime.engine.validation.ValidationProblemQuota.Reservation>();
        boolean entered=owners.control(original.owner().pipelineId(),()->{actor.requireRun();result.set(actor.shared.quota.reserve(actor.original,root));});
        return entered?Optional.of(result.get()):Optional.empty();
    }
    boolean publish(ValidationRunIdentity original,io.tapstate.runtime.engine.validation.ValidationProblemQuota.PublicationPermit permit,Runnable boundedSubmit) {
        var actor=runs.get(original);if(actor==null||actor.shared==null)return false;actor.requireRun();return actor.shared.quota.publish(permit,boundedSubmit);
    }

    /** Reserve a complete located observation, then enqueue its CONFIG write outside all quota locks. */
    Optional<CompletionStage<ValidationResultStore.Publication>> publishRoot(ValidationResultStore.RootWrite write) {
        Objects.requireNonNull(write);
        var identity=write.report().run();var actor=runs.get(identity);
        if(actor==null||!actor.original.report().equals(write.report())
                ||actor.original.snapshot().method()!=write.check().method()||write.check().observedAgreement())
            throw invalid("runs.originalProblem");
        actor.requireRun();
        var admitted=reserveConfirmed(identity,write.check().observation().root());
        if(admitted.isEmpty())return Optional.empty();
        var permit=admitted.orElseThrow().permit();
        if(permit.isEmpty())return Optional.of(CompletableFuture.failedFuture(cancelled(identity)));
        var pending=new RootPublication(actor,write);
        var attempted=new AtomicBoolean();
        try {
            boolean submitted=publish(identity,permit.orElseThrow(),()->{
                attempted.set(true);
                actor.allocation.executeControl(pending::run,pending::notSubmitted);
            });
            if(!submitted){pending.notSubmitted();pending.result.completeExceptionally(cancelled(identity));}
        }catch(RuntimeException|Error failed){
            // An uncertain executor response may still own the queued action and its payload.
            if(!attempted.get())pending.notSubmitted();
            var reported=failed instanceof RejectedExecutionException
                    ? new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.resultPublications"),failed) : failed;
            pending.result.completeExceptionally(reported);
            if(failed instanceof Error fatal)throw fatal;
        }
        return Optional.of(pending.result.minimalCompletionStage());
    }

    boolean publicationsDrained(ValidationRunIdentity identity) {
        var actor=runs.get(identity);
        return actor==null||actor.pendingPublications.get()==0;
    }

    private static final class RootPublication {
        private final Actor actor;
        private final io.tapstate.runtime.engine.validation.ValidationLocalBudget.Reservation payload;
        private final AtomicBoolean released=new AtomicBoolean();
        private final CompletableFuture<ValidationResultStore.Publication> result=new CompletableFuture<>();
        private ValidationResultStore.RootWrite write;
        RootPublication(Actor actor,ValidationResultStore.RootWrite write) {
            this.actor=actor;this.write=write;
            // Encoder work is separate from the scan/model plan. This is logical admission, not an RSS measurement.
            long bytes=Math.addExact(64L*1024,Math.multiplyExact(6L,write.check().accountedBytes()));
            payload=actor.allocation.budget().reserve(new io.tapstate.runtime.engine.validation.ValidationLocalBudget.Amount(bytes,0,0,0));
            actor.pendingPublications.incrementAndGet();
        }
        void run() {
            ValidationResultStore.Publication answer=null;var failure=new ValidationFailureLatch();
            try{actor.requireRun();answer=actor.reservation.access().results().publish(write);}
            catch(RuntimeException|Error failed){failure.record(failed);}
            finally{try{release();}catch(RuntimeException|Error cleanup){failure.record(cleanup);}}
            if(failure.get()==null)result.complete(answer);else result.completeExceptionally(failure.get());
            if(failure.get() instanceof Error fatal)throw fatal;
        }
        void notSubmitted(){release();}
        private void release(){
            if(released.compareAndSet(false,true)){
                write=null;
                try{payload.close();}finally{actor.pendingPublications.decrementAndGet();}
            }
        }
    }
    private static void secretScalars(Object value,List<String> values){
        var pending=new ArrayDeque<Object>();if(value!=null)pending.add(value);
        while(!pending.isEmpty()){
            var current=pending.removeLast();
            if(current instanceof Map<?,?> map){for(var item:map.values())if(item!=null)pending.add(item);}
            else if(current instanceof List<?> list){for(var item:list)if(item!=null)pending.add(item);}
            else if(current instanceof String text)values.add(text);
            else if(current instanceof Number||current instanceof Boolean)values.add(current.toString());
        }
    }
    private static TapstateException cancelled(ValidationRunIdentity identity){
        return new TapstateException(ValidationError.EXECUTION_CANCELLED,Map.of("node",identity.owner().nodeId()),null);
    }
    @Override public void close(){
        List<Actor> ending;synchronized(runs){if(closed)return;closed=true;ending=List.copyOf(runs.values());}
        for(var run:ending){run.lifecycle.stop();run.dispatch();}
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
    private static TapstateException capacity(){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.runControls"),null);}
}
