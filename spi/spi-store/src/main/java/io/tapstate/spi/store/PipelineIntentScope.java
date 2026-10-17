package io.tapstate.spi.store;

/** The original storage coordinates captured before calculating a Pipeline lifecycle intent. */
public record PipelineIntentScope(String clusterId, long bindingVersion, String cacheIncarnation,
        String pipelineId, long stateGeneration) {
    public PipelineIntentScope {
        if (clusterId == null || clusterId.isBlank() || cacheIncarnation == null || cacheIncarnation.isBlank()
                || pipelineId == null || pipelineId.isBlank() || bindingVersion < 1 || bindingVersion == Long.MAX_VALUE
                || stateGeneration < 1 || stateGeneration == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid Pipeline intent scope");
        }
    }
}
