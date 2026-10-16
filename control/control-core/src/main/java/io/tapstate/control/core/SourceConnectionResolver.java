package io.tapstate.control.core;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;

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
     * Preserves request values and restores secret fields omitted from a redacted saved-Source view. Mongo
     * URI userinfo is restored only when the displayed location still matches the stored URI. A stored
     * Source is eligible only when its connector agrees with the request; otherwise the request remains an
     * ad hoc connection. A null settings map is treated as an empty request.
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
                        if (resolved.containsKey(secret)) {
                            if (resolved.get(secret) == null) {
                                resolved.remove(secret);
                            }
                            continue;
                        }
                        if (source.config().containsKey(secret)
                                && source.config().get(secret) != null) {
                            resolved.put(secret, source.config().get(secret));
                        }
                    }
                    restoreNestedSecret(resolved, source.config(), "__TAPDATA", "sshPassword");
                    Object saved = source.config().get("uri");
                    Object supplied = resolved.get("uri");
                    if ("mongodb-atlas".equals(source.connector())) {
                        if (!resolved.containsKey("uri") && saved instanceof String) {
                            resolved.put("uri", saved);
                        }
                    }
                    if (supplied instanceof String displayed
                            && MongoUriUserInfo.isRedactedDisplay(displayed)) {
                        if (!(saved instanceof String savedUri)
                                || !displayed.equals(MongoUriUserInfo.redact(savedUri))) {
                            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                                    Map.of("reason", "a redacted URI cannot address a different connection"), null);
                        }
                        resolved.put("uri", savedUri);
                    }
                    return new ConnectionConfig(source.id(), connectorId, resolved);
                })
                .orElseGet(() -> new ConnectionConfig(connectionId, connectorId, settings));
    }

    /** Returns catalog and shared transport secret values used to redact connector diagnostics. */
    List<String> secretValuesForDiagnostics(String connectorId, Map<String, Object> settings) {
        Map<String, Object> values = settings == null ? Map.of() : settings;
        java.util.LinkedHashSet<String> secrets = new java.util.LinkedHashSet<>(
                ConnectionTestResultRedactor.sharedSecretValues(values));
        for (String field : secretFields(connectorId)) {
            Object value = values.get(field);
            if (value instanceof String text && !text.isBlank()) secrets.add(text);
        }
        return List.copyOf(secrets);
    }

    ConnectionTestResult redactDiagnostics(
            ConnectionTestResult result, String connectorId, Map<String, Object> settings) {
        return ConnectionTestResultRedactor.redact(
                result, secretValuesForDiagnostics(connectorId, settings));
    }

    ConnectionTestResult redactStoredDiagnostics(ConnectionTestResult result) {
        Map<String, Object> settings = artifacts.get(result.connectionId())
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .filter(source -> source.connector().equals(result.connectorId()))
                .map(SourceResource::config)
                .orElseGet(Map::of);
        return redactDiagnostics(result, result.connectorId(), settings);
    }

    private List<String> secretFields(String connectorId) {
        List<String> fields = new java.util.ArrayList<>(catalog.get().all().stream()
                .filter(entry -> entry.id().equals(connectorId))
                .findFirst()
                .map(entry -> entry.config().stream()
                        .filter(ConfigField::secret)
                        .map(ConfigField::name)
                        .toList())
                .orElseGet(List::of));
        if (!fields.contains("sslKey")) fields.add("sslKey");
        if (!fields.contains("sslKeyPassword")) fields.add("sslKeyPassword");
        return fields;
    }

    private static void restoreNestedSecret(
            Map<String, Object> resolved,
            Map<String, Object> saved,
            String group,
            String name) {
        Object requestedValue = resolved.get(group);
        if (resolved.containsKey(group) && !(requestedValue instanceof Map<?, ?>)) return;
        Map<String, Object> requestedGroup = requestedValue instanceof Map<?, ?> map
                ? copyStringMap(map)
                : null;
        Object savedValue = saved.get(group);
        Map<String, Object> savedGroup = savedValue instanceof Map<?, ?> map
                ? copyStringMap(map)
                : Map.of();

        if (requestedGroup != null && requestedGroup.containsKey(name)) {
            if (requestedGroup.get(name) == null) requestedGroup.remove(name);
        } else if (savedGroup.get(name) != null) {
            if (requestedGroup == null) requestedGroup = new LinkedHashMap<>();
            requestedGroup.put(name, savedGroup.get(name));
        }

        if (requestedGroup != null) {
            if (requestedGroup.isEmpty()) resolved.remove(group);
            else resolved.put(group, requestedGroup);
        }
    }

    private static Map<String, Object> copyStringMap(Map<?, ?> values) {
        Map<String, Object> copy = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            if (key instanceof String name) copy.put(name, value);
        });
        return copy;
    }
}
