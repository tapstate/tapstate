package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Objects;

/** One original producer's ordered batch window; ordinals never reset within this epoch. */
public record DirectCaptureWindow(long epoch,WorkloadClaimFence captureFence,WorkloadClaimFence pipelineFence,String anchor,
        long recordedThrough,long settledThrough,ChainPosition lastBoundary,String lastBatchHash) {
    public DirectCaptureWindow {
        Objects.requireNonNull(captureFence,"captureFence"); Objects.requireNonNull(pipelineFence,"pipelineFence");
        if (epoch < 1 || epoch == Long.MAX_VALUE || captureFence.key().type() != WorkloadClaimType.CAPTURE
                || pipelineFence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION || pipelineFence.executionGeneration() < 1
                || anchor != null && anchor.isBlank() || settledThrough < 0 || recordedThrough < settledThrough || recordedThrough == Long.MAX_VALUE
                || (recordedThrough == 0) != (lastBoundary == null) || (recordedThrough == 0) != (lastBatchHash == null)
                || anchor == null && recordedThrough != 0) throw new IllegalArgumentException("invalid original direct batch window");
        if (lastBoundary != null && (lastBoundary.order().epoch() != epoch || lastBoundary.order().seq() < 0 || lastBoundary.order().seq() == Long.MAX_VALUE
                || lastBoundary.token() == null || lastBoundary.token().isBlank() || !lastBatchHash.matches("[0-9a-f]{64}")))
            throw new IllegalArgumentException("invalid original direct batch receipt");
    }
    public long pendingCount() { return recordedThrough-settledThrough; }
}
