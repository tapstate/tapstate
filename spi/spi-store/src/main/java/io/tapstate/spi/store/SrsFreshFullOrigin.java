package io.tapstate.spi.store;

import java.util.Objects;

/** Frozen identity of one completed clear-state full preparation, not an independent creation grant. */
public record SrsFreshFullOrigin(String operationId, PipelineIntentScope targetScope, String preparationId,
                                 String confirmationHash, String sourceId) {
    public SrsFreshFullOrigin {
        Objects.requireNonNull(targetScope, "targetScope");
        if (operationId == null || operationId.isBlank() || preparationId == null || preparationId.isBlank()
                || confirmationHash == null || !confirmationHash.matches("[0-9a-f]{64}")
                || sourceId == null || sourceId.isBlank()) throw new IllegalArgumentException("invalid full preparation origin");
    }
}
