package io.tapstate.spi.store;

/** Explicit source-consumer coordinates; no caller needs to parse an encoded consumer or chain id. */
public record SrsConsumerKey(String miningChainId, String pipelineId, String sourceId) {
    public SrsConsumerKey {
        if (miningChainId == null || miningChainId.isBlank() || pipelineId == null || pipelineId.isBlank()
                || sourceId == null || sourceId.isBlank()) throw new IllegalArgumentException("invalid SRS consumer coordinates");
    }
    public String consumerId() { return SrsConsumerId.of(pipelineId, sourceId).value(); }
}
