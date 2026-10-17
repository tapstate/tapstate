package io.tapstate.spi.store;

import java.io.Serializable;
import java.util.Objects;

/** The original physical diagnostic scope, captured before a callback rather than resolved from a current mapping. */
public record RequiredDeadLetterContext(String clusterId, long bindingVersion, String pipelineId,
                                        String stepId, String pathId, long stateGeneration, String namespace) implements Serializable {
    public RequiredDeadLetterContext {
        text(clusterId); text(pipelineId); text(stepId); text(namespace); Objects.requireNonNull(pathId, "pathId");
        if (bindingVersion < 1 || bindingVersion == Long.MAX_VALUE || stateGeneration < 1 || stateGeneration == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid required dead-letter context");
        }
    }
    private static void text(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("required dead-letter identity must not be blank");
    }
}
