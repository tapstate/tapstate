package io.tapstate.control.core;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Resolves a connection request against persisted Source configuration when one exists. */
public final class SourceConnectionResolver {

    private final ArtifactStore artifacts;
    private final Supplier<TapstateCatalog> catalog;

    public SourceConnectionResolver(ArtifactStore artifacts) {
        this(artifacts, TapstateCatalog::load);
    }

    public SourceConnectionResolver(
            ArtifactStore artifacts, Supplier<TapstateCatalog> catalog) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    /**
     * Preserves request values and restores only secret fields omitted from a redacted saved-Source view.
     * A stored Source is eligible only when its connector agrees with the request; otherwise the request
     * remains an ad hoc connection. A null settings map is treated as an empty request.
     */
    public ConnectionConfig resolve(
            String connectionId, String connectorId, Map<String, Object> settings) {
        Map<String, Object> requested = settings == null ? Map.of() : settings;
        return artifacts.get(connectionId)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .filter(source -> source.connector().equals(connectorId))
                .map(source -> {
                    Map<String, Object> resolved = new LinkedHashMap<>(requested);
                    for (String secret : secretFields(source.connector())) {
                        if (!resolved.containsKey(secret)
                                && source.config().containsKey(secret)
                                && source.config().get(secret) != null) {
                            resolved.put(secret, source.config().get(secret));
                        }
                    }
                    return new ConnectionConfig(source.id(), connectorId, resolved);
                })
                .orElseGet(() -> new ConnectionConfig(connectionId, connectorId, settings));
    }

    private List<String> secretFields(String connectorId) {
        return catalog.get().all().stream()
                .filter(entry -> entry.id().equals(connectorId))
                .findFirst()
                .map(entry -> entry.config().stream()
                        .filter(ConfigField::secret)
                        .map(ConfigField::name)
                        .toList())
                .orElseGet(List::of);
    }
}
