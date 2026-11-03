package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Private direct recovery operations never create a missing channel or borrow shared-SRS authority. */
public interface DirectChannelStore {
    /** Original active-window coordinates only, never cursor, pending batches or recovery proof. */
    record Control(DirectChannelContext context,long epoch,WorkloadClaimFence capture,WorkloadClaimFence pipeline,boolean selectedTable){
        public Control{java.util.Objects.requireNonNull(context);java.util.Objects.requireNonNull(capture);java.util.Objects.requireNonNull(pipeline);
            if(epoch<1||epoch==Long.MAX_VALUE||capture.key().type()!=WorkloadClaimType.CAPTURE||pipeline.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION
                    ||!capture.key().resourceId().equals(context.captureId())||!pipeline.key().resourceId().equals(context.scope().pipelineId())
                    ||!capture.key().clusterId().equals(context.scope().clusterId())||!pipeline.key().clusterId().equals(context.scope().clusterId()))throw new IllegalArgumentException("private control coordinates disagree");}
    }
    default java.util.Optional<Control> control(DirectChannelContext original,String table){throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,java.util.Map.of(),null);}
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
