package io.tapstate.runtime.engine;

import io.tapstate.spi.store.OperatorPlacement;
import java.io.Serializable;
import java.util.Objects;

/** A stable cold address and the caller's complete immutable execution identity. */
public record OperatorMapBinding(OperatorPlacement placement, Serializable executionIdentity, String executionKey) implements Serializable {
    public OperatorMapBinding {
        Objects.requireNonNull(placement, "placement"); Objects.requireNonNull(executionIdentity, "executionIdentity");
        if (executionKey == null || !executionKey.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("invalid operator execution key");
    }
    public String mapName() { return placement.mapName() + ".run." + executionKey; }
    /** A generation's memory and database settings are shared by its successive hot maps. */
    public String configurationPattern() { return placement.mapName() + ".run.*"; }
}
