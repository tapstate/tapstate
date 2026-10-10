package io.tapstate.spi.store;

import java.util.Objects;

/** Explicit pre-join outcome; refused candidates acquire neither a lease nor a registry entry. */
public record ClusterNodeReservation(Outcome outcome, ClusterNodeReading node, ClusterExecutionProfile profile) {
    public enum Outcome { ACQUIRED, NODE_IN_USE, INCOMPATIBLE, LEGACY_LEASES_ACTIVE, AUTHORIZATION_HORIZON_ACTIVE }

    public ClusterNodeReservation {
        Objects.requireNonNull(outcome, "outcome");
        if (outcome == Outcome.ACQUIRED && (node == null || profile == null)) {
            throw new IllegalArgumentException("an acquired reservation requires its session and profile");
        }
    }

    public boolean acquired() {
        return outcome == Outcome.ACQUIRED;
    }
}
