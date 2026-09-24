package io.tapstate.spi.store;

import java.time.Duration;
import java.util.Objects;

/**
 * One read of a claim, together with how much of its lease the store's own clock says is left.
 *
 * <p>The two travel together because neither answers the question on its own. The generations say which
 * run is current; the lease says whether anybody still owns it. A reader that sees only the generations
 * cannot tell a live holder from one that died and left its record behind, and the record of a dead
 * holder keeps matching until some other member happens to take it over.
 *
 * <p>{@code leaseRemaining} is measured entirely on the store's clock -- the difference between its own
 * current time and the lease deadline it holds -- so no caller has to compare its clock with the store's.
 * It is negative once the lease has lapsed, which is a real answer and not an error: the record is still
 * there, and nobody owns it.
 */
public record WorkloadClaimReading(WorkloadClaim claim, Duration leaseRemaining) {

    public WorkloadClaimReading {
        claim = Objects.requireNonNull(claim, "claim");
        leaseRemaining = Objects.requireNonNull(leaseRemaining, "leaseRemaining");
    }

    /** Whether the store considered this claim still owned at the moment it answered. */
    public boolean leased() {
        return !leaseRemaining.isZero() && !leaseRemaining.isNegative();
    }
}
