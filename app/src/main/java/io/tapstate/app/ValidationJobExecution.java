package io.tapstate.app;

import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationJobGraph;
import io.tapstate.runtime.engine.validation.ValidationProcessorRegistry;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** One actual validation Job and its original local work. All transport runs on admitted control IO. */
final class ValidationJobExecution implements ValidationRunLifecycle.Execution {
    interface Work {
        /** Actual independent reads, checking and known publication completion, never Jet status. */
        boolean businessComplete();
        /** Actual native readers/rows/callbacks released, never Future.cancel or connector.stop alone. */
        boolean readersReleased();
        Throwable failure();
        /** Pure local cancellation signal; no transport, native stop or waiting here. */
        void cancel();
        /** Retry original cleanup only, on admitted control I/O, including after known retirement. */
        default void maintain() {}
        /** Original queue, whose complete envelope and retained leases are charged by this work. */
        default java.util.Optional<io.tapstate.runtime.engine.validation.ValidationCandidateQueue> candidates() { return java.util.Optional.empty(); }
    }
    private final ValidationRunIdentity identity;
    private final ValidationJobGraph graph;
    private final ValidationProcessorRegistry.Run processors;
    private final JetService jet;
    private final Work work;
    private final Runnable requireCurrent, requireOriginalCleanup;
    private final AtomicBoolean attempted = new AtomicBoolean(), cancelled = new AtomicBoolean();
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private volatile Job originalJob;
    private volatile boolean submissionReturned, forgotten;

    ValidationJobExecution(ValidationRunIdentity identity, ValidationJobGraph graph, ValidationProcessorRegistry.Run processors,
            JetService jet, Work work, Runnable requireCurrent, Runnable requireOriginalCleanup) {
        this.identity=Objects.requireNonNull(identity);this.graph=Objects.requireNonNull(graph);this.processors=Objects.requireNonNull(processors);
        this.jet=Objects.requireNonNull(jet);this.work=Objects.requireNonNull(work);this.requireCurrent=Objects.requireNonNull(requireCurrent);
        this.requireOriginalCleanup=Objects.requireNonNull(requireOriginalCleanup);
        if(!identity.equals(processors.identity()))throw invalid("job.executionIdentity");
    }
    @Override public ValidationRunIdentity identity(){return identity;}
    @Override public java.util.Optional<io.tapstate.runtime.engine.validation.ValidationCandidateQueue> candidates(){return work.candidates();}
    @Override public void start() {
        if(!attempted.compareAndSet(false,true))throw invalid("job.executionAlreadyAttempted");
        try {
            originalJob=graph.submit(jet,()->{if(cancelled.get())throw cancelled();requireCurrent.run();});
            originalJob.getFuture().whenComplete((ignored,failed)->{
                processors.seal();
                if(failed!=null){var cause=unwrap(failed);if(!(cause instanceof CancellationException)&&!cancelled.get())failure.record(cause);}
            });
        }catch(RuntimeException|Error failed){failure.record(failed);throw failed;}
        finally{submissionReturned=true;}
    }
    @Override public void cancel() {
        cancelled.set(true);processors.seal();
        try{work.cancel();}catch(RuntimeException|Error failed){failure.record(failed);throw failed;}
    }
    /** Retry only the original lookup/cancel. Invocation must remain separate from deadline callbacks. */
    void maintain() {
        work.maintain();
        if(!cancelled.get())return;requireOriginalCleanup.run();
        Job known=originalJob;
        if(known==null&&graph.handedToJet()) {known=graph.reconcile(jet,requireOriginalCleanup).orElse(null);if(known!=null)originalJob=known;}
        if(known!=null)known.cancel();
    }
    @Override public ValidationRunLifecycle.Progress progress() {
        var reported=work.failure();if(reported!=null)failure.record(reported);
        return new ValidationRunLifecycle.Progress(work.businessComplete(),processors.processorsExited(),work.readersReleased(),failure.get());
    }
    boolean submissionReturned(){return submissionReturned;}
    boolean knownNoSubmission(){return submissionReturned&&!graph.handedToJet();}
    String jobName(){return graph.jobName();}
    /** Forget native factories only after real exit. Unknown control/Job identity stays with its caller. */
    void releaseFactories() {
        if(forgotten)return;
        if(!processors.processorsExited()||!work.readersReleased())throw invalid("job.resourcesNotReleased");
        processors.forget();forgotten=true;
    }
    private static Throwable unwrap(Throwable failure){while(failure instanceof CompletionException&&failure.getCause()!=null)failure=failure.getCause();return failure;}
    private TapstateException cancelled(){return new TapstateException(ValidationError.EXECUTION_CANCELLED,Map.of("node",identity.owner().nodeId()),null);}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
