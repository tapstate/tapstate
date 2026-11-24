package io.tapstate.app;

import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.function.*;

/** Observes the existing necessary CACHE operations without issuing any extra database request. */
class ObservedWorkloadClaims implements WorkloadClaimStore {
    private final WorkloadClaimStore delegate;
    private final BiConsumer<Boolean,Throwable> observer;
    ObservedWorkloadClaims(WorkloadClaimStore delegate,BiConsumer<Boolean,Throwable> observer){this.delegate=delegate;this.observer=observer;}
    static WorkloadClaimStore observe(WorkloadClaimStore delegate,BiConsumer<Boolean,Throwable> observer) {
        return delegate instanceof DeadlineWorkloadClaimStore bounded
                ? new Bounded(bounded,observer) : new ObservedWorkloadClaims(delegate,observer);
    }
    private static final class Bounded extends ObservedWorkloadClaims implements DeadlineWorkloadClaimStore {
        private final DeadlineWorkloadClaimStore bounded;
        Bounded(DeadlineWorkloadClaimStore bounded,BiConsumer<Boolean,Throwable> observer){super(bounded,observer);this.bounded=bounded;}
        public <T>T withDeadline(long deadline,Runnable active,Supplier<T> operation){return bounded.withDeadline(deadline,active,operation);}
        public <T>T withRenewalDeadline(long deadline,Runnable active,Supplier<T> operation){return bounded.withRenewalDeadline(deadline,active,operation);}
        public void awaitQuiescence(long deadline){bounded.awaitQuiescence(deadline);}
    }
    private <T>T call(boolean write,Supplier<T> action){
        T result;
        try {result=action.get();}
        catch(RuntimeException failure){report(write,failure);throw failure;}
        report(write,null);return result;
    }
    private void report(boolean write,Throwable failure){try{observer.accept(write,failure);}catch(RuntimeException unavailable){org.slf4j.LoggerFactory.getLogger(getClass()).warn("Storage operation collection is degraded");}}
    public WorkloadClaimAttempt acquire(WorkloadClaimKey key,WorkloadOwner owner,long revision,Duration ttl){return call(true,()->delegate.acquire(key,owner,revision,ttl));}
    public WorkloadClaimAttempt acquirePipeline(PipelineIntentScope scope,WorkloadOwner owner,long revision,Duration ttl){return call(true,()->delegate.acquirePipeline(scope,owner,revision,ttl));}
    @Override public WorkloadClaimAttempt acquireNodeBackedPipeline(PipelineIntentScope scope, WorkloadClaimFence node, long revision) {
        return call(true, () -> delegate.acquireNodeBackedPipeline(scope, node, revision));
    }
    public Optional<WorkloadClaimReading> readPipeline(PipelineIntentScope scope){return call(false,()->delegate.readPipeline(scope));}
    public Optional<WorkloadClaim> renew(WorkloadClaim expected,Duration ttl){return call(true,()->delegate.renew(expected,ttl));}
    public boolean release(WorkloadClaim expected){return call(true,()->delegate.release(expected));}
    public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected,long revision,Set<String> members){return call(true,()->delegate.advanceExecution(expected,revision,members));}
    public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected,boolean loss){return call(true,()->delegate.recordExecutionFailure(expected,loss));}
    public Optional<WorkloadClaimReading> read(WorkloadClaimKey key){return call(false,()->delegate.read(key));}
    public Map<WorkloadClaimKey,WorkloadClaimReading> readAll(Collection<WorkloadClaimKey> keys){return keys.isEmpty()?delegate.readAll(keys):call(false,()->delegate.readAll(keys));}
}
