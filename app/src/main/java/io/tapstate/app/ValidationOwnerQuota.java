package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationProblemQuota;
import io.tapstate.spi.store.ValidationControlStore;
import java.util.*;

/** One original owner/counting generation. Local stop precedes the retryable shared CACHE latch. */
final class ValidationOwnerQuota {
    private final ValidationProblemQuota quota;
    private final ValidationControlStore control;
    private final Map<ValidationRunIdentity,Runnable> stops=new HashMap<>();
    private ValidationControlStore.Run limitOriginal;
    private boolean latchKnown;

    /** Caller reserves this entire metadata allowance before construction, separately from row/IO work. */
    ValidationOwnerQuota(ValidationOwner owner,long generation,long metadataBytes,ValidationControlStore control,Runnable requireOwner) {
        quota=new ValidationProblemQuota(owner,generation,128,metadataBytes,requireOwner);this.control=Objects.requireNonNull(control);
    }
    synchronized void joined(ValidationControlStore.Run run,Runnable requireRun,Runnable stop) {
        if(run.countingGeneration()!=quota.generation())throw invalid("quota.countingGeneration");
        var result=quota.join(run.identity(),requireRun);
        if(result!=ValidationProblemQuota.Join.JOINED&&result!=ValidationProblemQuota.Join.ALREADY_JOINED)throw invalid("quota.closedJoin");
        stops.putIfAbsent(run.identity(),Objects.requireNonNull(stop));
    }
    ValidationProblemQuota.Reservation reserve(ValidationControlStore.Run run,ValidationRoot root) {
        List<Runnable> ending=List.of();ValidationProblemQuota.Reservation result;
        synchronized(this) {
            if(run.countingGeneration()!=quota.generation())throw invalid("quota.countingGeneration");
            result=quota.reserve(run.identity(),root);
            if(result.admission()==ValidationProblemQuota.Admission.LIMIT_REACHED&&limitOriginal==null) {
                limitOriginal=run;ending=List.copyOf(stops.values());
                for(var active:quota.activeRuns())quota.seal(active);
            }
        }
        var failures=new ValidationFailureLatch();
        for(var stop:ending)try{stop.run();}catch(RuntimeException|Error failed){failures.record(failed);ValidationFailureLatch.addContext(failures.get(),failed);}
        if(failures.get() instanceof Error fatal)throw fatal;if(failures.get() instanceof RuntimeException failed)throw failed;
        return result;
    }
    boolean publish(ValidationProblemQuota.PublicationPermit permit,Runnable boundedSubmit){return quota.publish(permit,boundedSubmit);}
    void seal(ValidationRunIdentity run){quota.seal(run);}
    /** Admitted control IO only. A missing/failed reply keeps the local generation irreversibly closed. */
    void reconcileLimit() {
        ValidationControlStore.Run original;synchronized(this){if(latchKnown||limitOriginal==null)return;original=limitOriginal;}
        var outcome=control.latchProblemLimit(original);
        if(outcome==ValidationControlStore.Mutation.STALE)throw invalid("quota.originalLatch");
        synchronized(this){latchKnown=true;}
    }
    synchronized void retired(ValidationControlStore.Run run,ValidationRunState terminal) {
        quota.retireSealedTerminalRun(run.identity(),terminal);stops.remove(run.identity());
    }
    synchronized void expired(ValidationRunIdentity run){quota.discardExpiredSealedRun(run);stops.remove(run);}
    void ownerLost() {
        List<Runnable> ending;synchronized(this){quota.close();ending=List.copyOf(stops.values());}
        var failures=new ValidationFailureLatch();for(var stop:ending)try{stop.run();}catch(RuntimeException|Error failed){failures.record(failed);ValidationFailureLatch.addContext(failures.get(),failed);}
        if(failures.get() instanceof Error fatal)throw fatal;if(failures.get() instanceof RuntimeException failed)throw failed;
    }
    synchronized boolean latchKnown(){return latchKnown;}
    int roots(){return quota.reservedRoots();}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
