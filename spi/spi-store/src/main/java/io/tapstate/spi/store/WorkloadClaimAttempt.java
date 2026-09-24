package io.tapstate.spi.store;

import java.util.Objects;

/** Result of an atomic acquire: either this owner holds the returned claim or another owner still does. */
public record WorkloadClaimAttempt(boolean acquired, WorkloadClaim claim) {

    public WorkloadClaimAttempt {
        claim = Objects.requireNonNull(claim, "claim");
    }

    public static WorkloadClaimAttempt acquired(WorkloadClaim claim) {
        return new WorkloadClaimAttempt(true, claim);
    }

    public static WorkloadClaimAttempt refused(WorkloadClaim claim) {
        return new WorkloadClaimAttempt(false, claim);
    }
}
