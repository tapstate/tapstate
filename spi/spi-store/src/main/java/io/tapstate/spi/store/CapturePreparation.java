package io.tapstate.spi.store;

import java.util.Objects;
import java.util.UUID;

/** Immutable first-creation facts; loading this value never grants a new CACHE insertion. */
public record CapturePreparation(CacheBusinessScope scope,Mode mode,String chainId,String captureId,String sourceIdentityHash,String preparationId,
        String canonicalChainId,String birthPreparationId) {
    public enum Mode { SHARED_SRS, DIRECT, SNAPSHOT_ONLY }
    public CapturePreparation(CacheBusinessScope scope,Mode mode,String chainId,String captureId,String sourceIdentityHash,String preparationId) {
        this(scope,mode,chainId,captureId,sourceIdentityHash,preparationId,chainId,preparationId);
    }
    public CapturePreparation {
        Objects.requireNonNull(scope,"scope");Objects.requireNonNull(mode,"mode");
        if(chainId==null || chainId.isBlank() || captureId==null || captureId.isBlank()
                || sourceIdentityHash==null || !sourceIdentityHash.matches("mc-[0-9a-f]{64}")
                || preparationId==null || !UUID.fromString(preparationId).toString().equals(preparationId)
                || canonicalChainId==null || canonicalChainId.isBlank() || birthPreparationId==null
                || !UUID.fromString(birthPreparationId).toString().equals(birthPreparationId)
                || !chainId.equals(canonicalChainId) && mode!=Mode.SHARED_SRS
                || chainId.equals(canonicalChainId)!=preparationId.equals(birthPreparationId))
            throw new IllegalArgumentException("invalid original capture preparation");
    }
    public boolean replacement(){return !chainId.equals(canonicalChainId);}
    public CapturePreparation birth(){return new CapturePreparation(scope,mode,canonicalChainId,captureId,sourceIdentityHash,birthPreparationId);}
}
