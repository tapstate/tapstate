package io.tapstate.spi.store;

import java.util.Objects;
import java.util.UUID;

/** Bounded predecessor lineage for one explicitly confirmed replacement of a lost cache domain. */
public record PipelineCacheRecovery(String recoveryId, PipelineIntentScope previousScope, PipelineIntentScope targetScope) {
    public PipelineCacheRecovery {
        Objects.requireNonNull(recoveryId, "recoveryId"); Objects.requireNonNull(targetScope, "targetScope");
        if (!UUID.fromString(recoveryId).toString().equals(recoveryId)) throw new IllegalArgumentException("invalid recovery identity");
        if (previousScope != null && (!previousScope.clusterId().equals(targetScope.clusterId())
                || previousScope.bindingVersion() != targetScope.bindingVersion()
                || !previousScope.pipelineId().equals(targetScope.pipelineId())
                || previousScope.cacheIncarnation().equals(targetScope.cacheIncarnation())
                || previousScope.stateGeneration() >= targetScope.stateGeneration()))
            throw new IllegalArgumentException("invalid recovery scope transition");
    }
}
