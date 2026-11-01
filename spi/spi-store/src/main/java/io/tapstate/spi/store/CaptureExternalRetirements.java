package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/** Original CONFIG resource operations; a lease is never destructive retirement authority. */
public interface CaptureExternalRetirements extends CaptureOpeningJournal {
    List<CapturePreparation> sharedCaptures(CacheBusinessScope scope);
    boolean retainedPeer(CapturePreparation capture,String relinquishingPipeline);
    record Pending(CaptureClearPreparation preparation,String phase,boolean automatic) {}
    List<Pending> pending(CacheBusinessScope scope);
    void relinquishAliases(PipelineRebuildPreparation original,List<CaptureAliasObservation> contexts);
    void relinquishAliases(PipelineRetirementPreparation original,List<CaptureAliasObservation> contexts);
    List<CaptureResourceNotes> resourceNotes(CacheBusinessScope scope);
    RequiredNoteOrigin admitOrigin(RequiredNoteScope scope,RequiredNoteOrigin origin,String resourceNameHash);
    List<CaptureClearPreparation> reserve(PipelineRebuildPreparation original,List<CaptureClearObservation> observations,
            List<CaptureResourceNotes> relatedNotes,String resourceNameHash,RequiredNoteOrigin expectedOrigin,boolean automaticCleanup,List<CaptureRetirementMember> members);
    List<CaptureClearPreparation> reserve(PipelineRetirementReceipt original,List<CaptureClearObservation> observations,
            List<CaptureResourceNotes> relatedNotes,String resourceNameHash,RequiredNoteOrigin expectedOrigin,boolean automaticCleanup,List<CaptureRetirementMember> members);
    List<CaptureClearPreparation> cohort(CaptureClearPreparation original);
    void frozen(CaptureClearPreparation original,CaptureClearCache.Receipt receipt);
    Optional<Attempt> beginExternal(CaptureClearPreparation original,WorkloadClaimFence node);
    void verified(CaptureClearPreparation original,Attempt attempt);
    void complete(CaptureClearPreparation original,List<CaptureClearCache.Receipt> receipts);
    void cancelling(CaptureClearPreparation original);
    void cancelled(CaptureClearPreparation original,CaptureClearCache.Receipt receipt);
    void recoveredUnknown(CaptureClearPreparation original);
    void cleaned(CaptureClearPreparation original);
    interface Attempt {
        String id();
        void consume();
        void ended();
        void unknown(String reason);
    }
}
