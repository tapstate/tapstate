package io.tapstate.spi.store;

import java.util.Optional;

/** CONFIG first-creation facts for canonical capture contracts; no CACHE or running lease IO occurs here. */
public interface CaptureStatePreparations {
    CaptureStatePreparations guardedBy(Runnable requireActive);
    Optional<CapturePreparation> read(CacheBusinessScope scope,String captureId);
    CaptureCreationResult issueFirstCreate(CapturePreparationRequest original);
}
