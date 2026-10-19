package io.tapstate.spi.store;

import java.util.Objects;
import java.util.UUID;

/** Immutable first-creation facts; loading this value never grants a new CACHE insertion. */
public record CapturePreparation(CacheBusinessScope scope,Mode mode,String chainId,String captureId,String sourceIdentityHash,String preparationId) {
    public enum Mode { SHARED_SRS, DIRECT, SNAPSHOT_ONLY }
    public CapturePreparation {
        Objects.requireNonNull(scope,"scope");Objects.requireNonNull(mode,"mode");
        if(chainId==null || chainId.isBlank() || captureId==null || captureId.isBlank()
                || sourceIdentityHash==null || !sourceIdentityHash.matches("mc-[0-9a-f]{64}")
                || preparationId==null || !UUID.fromString(preparationId).toString().equals(preparationId))
            throw new IllegalArgumentException("invalid original capture preparation");
    }
}
