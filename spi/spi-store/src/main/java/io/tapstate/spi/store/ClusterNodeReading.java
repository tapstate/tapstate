package io.tapstate.spi.store;

import java.time.Duration;
import java.util.Objects;

/** Node registry and the remaining session lease, evaluated together on the store clock. */
public record ClusterNodeReading(ClusterNodeRegistration registration, Duration leaseRemaining) {
    public ClusterNodeReading {
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(leaseRemaining, "leaseRemaining");
    }

    public boolean leased() {
        return !leaseRemaining.isZero() && !leaseRemaining.isNegative();
    }
}
