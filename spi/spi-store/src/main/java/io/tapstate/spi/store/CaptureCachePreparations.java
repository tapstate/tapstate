package io.tapstate.spi.store;

/** Controlled first history insertion and load-only continuation using the original CACHE client. */
public interface CaptureCachePreparations {
    /** Control identity only; excludes checkpoint, model, cursor and row payloads. No lease grant. */
    record Control(CapturePreparation preparation,WorkloadClaimFence claim,long epoch,boolean servesTable){
        public Control {java.util.Objects.requireNonNull(preparation);java.util.Objects.requireNonNull(claim);
            if(epoch<0||epoch==Long.MAX_VALUE||claim.key().type()!=WorkloadClaimType.CAPTURE
                    ||!claim.key().clusterId().equals(preparation.scope().clusterId())||!claim.key().resourceId().equals(preparation.captureId()))throw new IllegalArgumentException("capture control coordinates disagree");}
    }
    default Control control(CapturePreparation original,String table){throw new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED,java.util.Map.of(),null);}
    CaptureCacheState createFirst(FirstCaptureCreation original);
    CaptureCacheState load(CapturePreparation original);
}
