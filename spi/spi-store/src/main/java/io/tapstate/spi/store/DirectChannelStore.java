package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Private direct recovery operations never create a missing channel or borrow shared-SRS authority. */
public interface DirectChannelStore {
    DirectChannelStore guardedBy(Runnable requireActive);
    DirectChannelProgress load(DirectChannelContext context);
    List<DirectBatch> pending(DirectChannelContext context,long epoch);
    OptionalLong beginCapture(DirectChannelContext context,long expectedRevision,long expectedEpoch,
                             WorkloadClaimFence originalCapture,WorkloadClaimFence originalPipeline);
    boolean confirmAnchor(DirectChannelContext context,long epoch,String sourcePosition,
                          WorkloadClaimFence originalCapture,WorkloadClaimFence originalPipeline);
    boolean pinSnapshot(DirectChannelContext context,long expectedRevision,long snapshotEpoch,String sourcePosition,WorkloadClaimFence originalPipeline);
    boolean beginWriterRun(DirectChannelContext context,long epoch,String runId,Map<String,List<String>> expected,WorkloadClaimFence originalPipeline);
    boolean recordBatch(DirectBatch original,long expectedRecordedThrough,WorkloadClaimFence originalCapture,WorkloadClaimFence originalPipeline);
    boolean advanceWriter(DirectChannelContext context,long epoch,String runId,String table,String writer,WriterProgress report,WorkloadClaimFence originalPipeline);
}
