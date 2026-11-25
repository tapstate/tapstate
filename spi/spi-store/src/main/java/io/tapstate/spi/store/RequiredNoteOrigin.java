package io.tapstate.spi.store;

import java.util.Arrays;
import java.util.Objects;

/** The immutable connector and complete settings retained for one physical note scope. */
public record RequiredNoteOrigin(String connectorId, String artifactHash, byte[] settings) {
    public RequiredNoteOrigin {
        Objects.requireNonNull(connectorId, "connectorId");
        Objects.requireNonNull(artifactHash, "artifactHash");
        Objects.requireNonNull(settings, "settings");
        if (connectorId.isBlank() || !artifactHash.matches("[a-f0-9]{64}") || settings.length == 0) {
            throw new IllegalArgumentException("invalid required-note origin");
        }
        settings = settings.clone();
    }

    @Override public byte[] settings() { return settings.clone(); }

    @Override public boolean equals(Object other) {
        return other instanceof RequiredNoteOrigin origin && connectorId.equals(origin.connectorId)
                && artifactHash.equals(origin.artifactHash) && Arrays.equals(settings, origin.settings);
    }

    @Override public int hashCode() {
        return 31 * Objects.hash(connectorId, artifactHash) + Arrays.hashCode(settings);
    }

    @Override public String toString() { return "RequiredNoteOrigin[redacted]"; }
}
