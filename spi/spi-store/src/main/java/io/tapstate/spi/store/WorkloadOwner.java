package io.tapstate.spi.store;

import java.util.Objects;

/** Stable node plus one process boot; both must match before a claim can authorize work. */
public record WorkloadOwner(String nodeId, String bootId) {

    public WorkloadOwner {
        nodeId = required(nodeId, "nodeId");
        bootId = required(bootId, "bootId");
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
