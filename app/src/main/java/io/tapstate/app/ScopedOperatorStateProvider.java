package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.OperatorMapBinding;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.PipelineStoreViews;
import java.util.Map;
import java.util.Objects;

/** Resolves one immutable map descriptor to the existing CACHE owner under its original execution. */
final class ScopedOperatorStateProvider implements OperatorMapBindings.Provider {
    private final PipelineStoreViews stores;
    private final ExecutionAuthorization authorization;
    ScopedOperatorStateProvider(PipelineStoreViews stores, ExecutionAuthorization authorization) {
        this.stores = Objects.requireNonNull(stores, "stores"); this.authorization = Objects.requireNonNull(authorization, "authorization");
    }
    @Override public KeyedStateStore open(OperatorMapBinding original, Runnable requireBindingActive) {
        if (!(original.executionIdentity() instanceof ExecutionFence fence) || fence.scope() == null
                || !original.placement().scope().equals(fence.scope()) || !original.executionKey().equals(fence.operatorExecutionKey())) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        requireBindingActive.run(); var claim = authorization.require(fence);
        return stores.operatorState(original.placement(), claim, () -> { requireBindingActive.run(); authorization.require(fence); });
    }
}
