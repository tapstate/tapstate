package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.*;
import java.util.Map;

/** Resolves a fixed original consumer on its executing member, preserving local and durable fencing. */
final class ScopedSrsConsumerAccess {
    private ScopedSrsConsumerAccess() {}
    static Bound bind(HazelcastInstance member,SrsConsumerRun run) {
        var authorization = ExecutionAuthorization.of(member); var fence = new ExecutionFence(run.scope(),run.originalClaim());
        Runnable requireActive = () -> { if (!run.originalClaim().equals(authorization.require(fence))) throw refused(run); };
        requireActive.run();
        if (!(member.getUserContext().get(SrsStoreViews.USER_CONTEXT_KEY) instanceof SrsStoreViews views)) throw refused(run);
        var stores = views.srs(run.cacheScope(),requireActive);
        return new Bound(run,stores.consumers().guardedBy(requireActive),stores.models(),requireActive);
    }
    record Bound(SrsConsumerRun run,SrsConsumerStore store,SrsModelStore models,Runnable requireActive) {
        void requireClaim(WorkloadClaimFence claimed) { requireActive.run(); if (claimed != null && !run.originalClaim().equals(claimed)) throw refused(run); }
        SrsConsumerProgress progress() {
            requireActive.run(); var held = store.progress(run.context()).orElseThrow(() -> refused(run)); requireActive.run();
            if (held.membershipRevision() != run.membershipRevision()) throw refused(run); return held;
        }
    }
    static TapstateException refused(SrsConsumerRun run) {
        return new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",run.scope().pipelineId()),null);
    }
}
