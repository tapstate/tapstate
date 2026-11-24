package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;

/** Possession of verified connector JARs by one node in the admitted CACHE scope. */
public interface ConnectorArtifactHoldings {
    record Held(String sha256, String connectorId, String version) {
        public Held {
            Objects.requireNonNull(connectorId, "connectorId");
            Objects.requireNonNull(version, "version");
            if (!sha256.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Invalid connector artifact hash");
        }
    }

    void publish(String nodeId, Held artifact);

    List<Held> forNode(String nodeId);

    void withdraw(String nodeId, String sha256);
}
