package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** Atomic successor admission and the exact advanced claim used by the cluster ownership view. */
public record SuccessorAdmission(StopReservation reservation, Optional<WorkloadClaim> advancedClaim) {
    public SuccessorAdmission {
        Objects.requireNonNull(reservation, "reservation"); Objects.requireNonNull(advancedClaim, "advancedClaim");
        if (reservation.phase() != StopReservation.Phase.SUCCESSOR_ADMITTED) {
            throw new IllegalArgumentException("successor admission requires an occupied admitted slot");
        }
    }
}
