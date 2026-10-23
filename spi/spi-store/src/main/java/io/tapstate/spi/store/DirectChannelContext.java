package io.tapstate.spi.store;

import java.util.Objects;

/** A private source channel has explicit original physical and capture-claim coordinates. */
public record DirectChannelContext(PipelineIntentScope scope,String channelId,String sourceId,String captureId) implements java.io.Serializable {
    public DirectChannelContext {
        Objects.requireNonNull(scope,"scope");
        if (channelId == null || channelId.isBlank() || sourceId == null || sourceId.isBlank() || captureId == null || captureId.isBlank())
            throw new IllegalArgumentException("private channel coordinates must be named");
    }
    public String consumerId() { return SrsConsumerId.of(scope.pipelineId(),sourceId).value(); }
}
