package io.tapstate.spi.store;

import java.util.Objects;

/** Private qualification shared by one durable lifecycle handoff and its readable continuation. */
public record HandoffIdentity(String pipelineId, String token, StopReservation.CounterPolicy counterPolicy,
        ObservationStore.Scope sourceScope, ObservationStore.Scope targetScope, StopReservation.JobIdentity targetJob) {
    public HandoffIdentity {
        Objects.requireNonNull(pipelineId, "pipelineId"); Objects.requireNonNull(token, "token");
        Objects.requireNonNull(counterPolicy, "counterPolicy"); Objects.requireNonNull(targetScope, "targetScope");
        Objects.requireNonNull(targetJob, "targetJob");
        if (pipelineId.isBlank() || token.isBlank() || sourceScope != null
                && !sourceScope.pipelineIncarnationId().equals(targetScope.pipelineIncarnationId())) {
            throw new IllegalArgumentException("handoff qualification identities disagree");
        }
    }
}
