package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.OperatorMapBinding;
import io.tapstate.runtime.engine.nest.RequiredNestDeadLetter;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredDeadLetterStore;
import java.util.Map;
import java.util.Objects;

/** A CONFIG diagnostic writer retains the original member execution and installed operator slot guard. */
final class ScopedRequiredDeadLetterProvider implements RequiredNestDeadLetter.Provider {
    private final RequiredDeadLetterStore store;
    private final ExecutionAuthorization authorization;
    ScopedRequiredDeadLetterProvider(RequiredDeadLetterStore store,ExecutionAuthorization authorization){this.store=Objects.requireNonNull(store,"store");this.authorization=Objects.requireNonNull(authorization,"authorization");}
    @Override public RequiredDeadLetterStore open(OperatorMapBinding original,Runnable requireBindingActive) {
        if(!(original.executionIdentity()instanceof ExecutionFence fence) || fence.scope()==null
                || !fence.scope().equals(original.placement().scope()) || !fence.operatorExecutionKey().equals(original.executionKey()))throw fenced();
        Runnable active=() -> {requireBindingActive.run();authorization.require(fence);};active.run();return store.guardedBy(active);
    }
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
}
