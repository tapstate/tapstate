package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.SourceModelComparison;
import java.util.Objects;

/** Source semantics selected from the resolved implementation, after the host's original-origin checks. */
public final class PdkSourceModelComparison {
    private PdkSourceModelComparison() { }

    public static SourceModelComparison forVerifiedArtifact(String connectorId, ConnectorRef artifact) {
        Objects.requireNonNull(artifact, "artifact");
        boolean sampled = ("mongodb".equals(connectorId) && PdkBsonDecimal.MONGODB.equals(artifact.className()))
                || ("mongodb-atlas".equals(connectorId) && PdkBsonDecimal.ATLAS.equals(artifact.className()));
        if (sampled
                && artifact.contentHash() != null && artifact.contentHash().matches("[a-f0-9]{64}")) {
            return SourceModelComparison.SAMPLED_DOCUMENT;
        }
        return SourceModelComparison.DECLARED_STRUCTURE;
    }
}
