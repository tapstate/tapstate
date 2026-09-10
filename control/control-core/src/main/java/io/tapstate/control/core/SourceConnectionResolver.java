package io.tapstate.control.core;

import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;

import java.util.Map;
import java.util.Objects;

/** Resolves a connection request to the complete persisted Source when one exists. */
public final class SourceConnectionResolver {

    private final ArtifactStore artifacts;

    public SourceConnectionResolver(ArtifactStore artifacts) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
    }

    /** Uses the stored Source config for saved ids and preserves ad hoc connection requests otherwise. */
    public ConnectionConfig resolve(
            String connectionId, String connectorId, Map<String, Object> settings) {
        return artifacts.get(connectionId)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .map(source -> new ConnectionConfig(source.id(), source.connector(), source.config()))
                .orElseGet(() -> new ConnectionConfig(connectionId, connectorId, settings));
    }
}
