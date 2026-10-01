package io.tapstate.spi.store;

import java.util.Objects;

/** The current durable execution identity that may carry one pending stop. */
public record StopAuthority(String clusterId, long executionGeneration, WorkloadClaimFence claim) {

    public StopAuthority {
        clusterId = required(clusterId, "clusterId");
        if (executionGeneration < 0 || executionGeneration == 0 && claim == null) {
            throw new IllegalArgumentException("standalone authority requires an admitted execution");
        }
        if (claim != null && (!clusterId.equals(claim.key().clusterId())
                || executionGeneration != claim.executionGeneration()
                || claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION)) {
            throw new IllegalArgumentException("stop authority claim does not name the same execution");
        }
    }

    /** A standalone execution is durable but has no owner lease. */
    public static StopAuthority standalone(String clusterId, long executionGeneration) {
        return new StopAuthority(clusterId, executionGeneration, null);
    }

    /** A clustered execution is authorized by its exact live owner and generations. */
    public static StopAuthority claimed(WorkloadClaimFence claim) {
        Objects.requireNonNull(claim, "claim");
        return new StopAuthority(claim.key().clusterId(), claim.executionGeneration(), claim);
    }

    public boolean standalone() {
        return claim == null;
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) { throw new IllegalArgumentException(name + " must not be blank"); }
        return value;
    }
}
