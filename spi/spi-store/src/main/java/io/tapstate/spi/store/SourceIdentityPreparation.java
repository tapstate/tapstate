package io.tapstate.spi.store;

import java.util.Objects;

/** Bounded current maintenance progress, not a history or proof of cluster/business readiness. */
public record SourceIdentityPreparation(int version, Phase phase, String operationId,
        String lastSourceIncarnation, long preparedCount) {
    public enum Phase { PREPARING, READY }
    public SourceIdentityPreparation {
        Objects.requireNonNull(phase, "phase"); Objects.requireNonNull(operationId, "operationId");
        if (version != 1 || operationId.isBlank() || operationId.length() > 256 || preparedCount < 0
                || (preparedCount == 0) != (lastSourceIncarnation == null)) {
            throw new IllegalArgumentException("Source preparation progress is not a supported bounded state");
        }
    }
}
