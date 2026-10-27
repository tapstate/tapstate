package io.tapstate.spi.store;

import java.util.Optional;

/** CONFIG first-creation facts for canonical capture contracts; no CACHE or running lease IO occurs here. */
public interface CaptureStatePreparations {
    CaptureStatePreparations guardedBy(Runnable requireActive);
    Optional<CapturePreparation> read(CacheBusinessScope scope,String captureId);
    CaptureCreationResult issueFirstCreate(CapturePreparationRequest original);
    /** Reads the retained shared origin for one exact authoring source; no capture or namespace is prepared. */
    default Optional<RequiredNoteOrigin> inspectSharedOrigin(CacheBusinessScope current,String sourceId,String sourceHash) {
        throw new io.tapstate.core.common.TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,java.util.Map.of(),null);
    }
    /** Inspects trusted history for the exact retained preparation without reissuing its creation right. */
    default CaptureSharedHistory inspectSharedHistory(CapturePreparation original, String sourceId, String sourceHash) {
        throw new io.tapstate.core.common.TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, java.util.Map.of(), null);
    }
}
