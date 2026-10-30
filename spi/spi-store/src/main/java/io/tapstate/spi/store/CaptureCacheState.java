package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** A load-only snapshot of the retained claim and, only for shared SRS, its original chain facts. */
public record CaptureCacheState(CapturePreparation preparation,WorkloadClaimFence claim,Optional<SrsChainState> chain) {
    public CaptureCacheState {
        Objects.requireNonNull(preparation,"preparation");Objects.requireNonNull(claim,"claim");Objects.requireNonNull(chain,"chain");
        if(claim.key().type()!=WorkloadClaimType.CAPTURE || !claim.key().clusterId().equals(preparation.scope().clusterId())
                || !claim.key().resourceId().equals(preparation.captureId()) || (preparation.mode()==CapturePreparation.Mode.SHARED_SRS)!=chain.isPresent()
                || chain.isPresent() && (!chain.get().miningChainId().equals(preparation.chainId()) || !chain.get().captureId().equals(preparation.captureId())))throw new IllegalArgumentException("capture history does not match its original preparation");
    }
}
