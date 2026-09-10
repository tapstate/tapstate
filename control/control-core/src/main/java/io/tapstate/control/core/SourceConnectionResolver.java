package io.tapstate.control.core;

import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Resolves a connection request against persisted Source configuration when one exists. */
public final class SourceConnectionResolver {

    private final ArtifactStore artifacts;

    public SourceConnectionResolver(ArtifactStore artifacts) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
    }

    /**
     * Starts from the complete persisted Source config, then overlays values supplied by the request.
     * This restores secrets omitted from a redacted saved-Source view without discarding unsaved draft
     * edits (including replacement secret values). Ad hoc connection requests are preserved unchanged.
     */
    public ConnectionConfig resolve(
            String connectionId, String connectorId, Map<String, Object> settings) {
        return artifacts.get(connectionId)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .map(source -> {
                    Map<String, Object> resolved = new LinkedHashMap<>(source.config());
                    resolved.putAll(settings);
                    return new ConnectionConfig(source.id(), connectorId, resolved);
                })
                .orElseGet(() -> new ConnectionConfig(connectionId, connectorId, settings));
    }
}
