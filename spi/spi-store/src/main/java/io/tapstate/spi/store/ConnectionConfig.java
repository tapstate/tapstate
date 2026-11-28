package io.tapstate.spi.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A stored connection: a named, reusable connector-instance configuration — which connector to run
 * and the settings to run it with. An immutable value.
 *
 * <p>{@code id} is the connection's identity in the catalog. {@code connectorId} is the catalog id
 * of the connector it configures. {@code settings} are the connection values keyed by the
 * connector's config field names, held as an unmodifiable defensive copy; a null map is normalized
 * to empty.
 *
 * <p>This is a connection <em>instance</em> a user has registered, distinct from the
 * connector-type catalog that ships with the product.
 */
public record ConnectionConfig(String id, String connectorId, Map<String, Object> settings,
        Map<String, Object> nodeConfig) {

    public ConnectionConfig(String id, String connectorId, Map<String, Object> settings) {
        this(id, connectorId, settings, Map.of());
    }

    public ConnectionConfig {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(connectorId, "connectorId");
        settings = settings == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(settings));
        nodeConfig = Collections.unmodifiableMap(new LinkedHashMap<>(nodeConfig));
    }
}
