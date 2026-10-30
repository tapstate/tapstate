package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Set;

/** A current relation or small detached revision marker; cursor payloads are a separate resource. */
public record SrsConsumerMembership(SrsConsumerContext context, long revision, boolean detached,
                                    Set<String> tables, WorkloadClaimFence commandFence) {
    public SrsConsumerMembership {
        Objects.requireNonNull(context, "context"); Objects.requireNonNull(commandFence, "commandFence");
        tables = Set.copyOf(Objects.requireNonNull(tables, "tables"));
        if (revision < 1 || revision == Long.MAX_VALUE || tables.stream().anyMatch(String::isBlank)
                || detached != tables.isEmpty() || commandFence.executionGeneration() < 1 || commandFence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !context.key().pipelineId().equals(commandFence.key().resourceId())) {
            throw new IllegalArgumentException("incoherent consumer membership");
        }
    }
}
