package io.tapstate.spi.store;

import java.util.Objects;

/** Only a committed winner carries insertion authority; an existing record is load-only. */
public sealed interface CaptureCreationResult permits CaptureCreationResult.Issued,CaptureCreationResult.AlreadyIssued {
    record Issued(FirstCaptureCreation creation)implements CaptureCreationResult{public Issued{Objects.requireNonNull(creation,"creation");}}
    record AlreadyIssued(CapturePreparation preparation)implements CaptureCreationResult{public AlreadyIssued{Objects.requireNonNull(preparation,"preparation");}}
}
