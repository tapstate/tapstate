package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.SourceModelComparison;
import java.util.Objects;

/** Source semantics selected from the resolved implementation, after the host's original-origin checks. */
public final class PdkSourceModelComparison {
    private PdkSourceModelComparison() { }

    public static SourceModelComparison forVerifiedArtifact(String connectorId, ConnectorRef artifact) {
        Objects.requireNonNull(artifact, "artifact");
        if ("mongodb".equals(connectorId)
                && "io.tapdata.mongodb.MongodbConnector".equals(artifact.className())
                && artifact.contentHash() != null && artifact.contentHash().matches("[a-f0-9]{64}")) {
            return SourceModelComparison.SAMPLED_DOCUMENT;
        }
        return SourceModelComparison.DECLARED_STRUCTURE;
    }
}
