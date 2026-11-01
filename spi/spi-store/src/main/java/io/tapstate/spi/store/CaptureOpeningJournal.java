package io.tapstate.spi.store;

import java.util.Objects;

/** CONFIG records physical source openings independently from CACHE execution leases. */
public interface CaptureOpeningJournal {
    /** One immutable allocation boundary; a retry must never adopt a newer boundary. */
    interface Registration {
        Opening register();
    }

    /** Cleanup authority for this exact opening; it grants no reader or writer permission. */
    interface Opening {
        void accessing();
        void closed();
    }

    record Request(CapturePreparation preparation, PipelineIntentScope pipeline,
            WorkloadClaimFence pipelineFence, WorkloadClaimFence captureFence, WorkloadClaimFence node) {
        public Request {
            Objects.requireNonNull(preparation, "preparation");
            Objects.requireNonNull(pipeline, "pipeline");
            Objects.requireNonNull(pipelineFence, "pipelineFence");
            Objects.requireNonNull(captureFence, "captureFence");
            Objects.requireNonNull(node, "node");
            if (!preparation.scope().equals(new CacheBusinessScope(pipeline.clusterId(),
                    pipeline.bindingVersion(), pipeline.cacheIncarnation()))
                    || pipelineFence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                    || !pipeline.pipelineId().equals(pipelineFence.key().resourceId())
                    || captureFence.key().type() != WorkloadClaimType.CAPTURE
                    || !preparation.captureId().equals(captureFence.key().resourceId())
                    || node.key().type() != WorkloadClaimType.NODE_SESSION
                    || !node.owner().equals(pipelineFence.owner())) {
                throw new IllegalArgumentException("invalid original capture opening");
            }
        }
    }

    Registration prepare(Request request);
}
