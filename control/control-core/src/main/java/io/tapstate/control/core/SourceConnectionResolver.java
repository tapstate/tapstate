package io.tapstate.control.core;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.SecretConfigPaths;
import io.tapstate.core.catalog.SecretConfigPaths.Path;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/** Resolves a connection request against persisted Source configuration when one exists. */
public final class SourceConnectionResolver {

    private final Function<String, Optional<Resource>> artifactLookup;
    private final Supplier<TapstateCatalog> catalog;

    public SourceConnectionResolver(ArtifactStore artifacts) {
        this(artifacts, TapstateCatalog::load);
    }

    public SourceConnectionResolver(ArtifactStore artifacts, Supplier<TapstateCatalog> catalog) {
        this(Objects.requireNonNull(artifacts, "artifacts")::get, catalog);
    }

    private SourceConnectionResolver(
            Function<String, Optional<Resource>> artifactLookup, Supplier<TapstateCatalog> catalog) {
        this.artifactLookup = Objects.requireNonNull(artifactLookup, "artifactLookup");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    /** Creates an explicit resolver for an operation that cannot refer to a persisted Source. */
    public static SourceConnectionResolver forAdHocConnections() {
        return new SourceConnectionResolver(id -> Optional.empty(), TapstateCatalog::load);
    }

    /**
     * Resolves the config and the secret values for the same operation using one catalog snapshot and one
     * artifact lookup. Secret fields omitted from a saved-Source view are restored; explicit null values
     * are rejected consistently with the Source update path.
     */
    ResolvedConnection resolveWithSecrets(
            String connectionId, String connectorId, Map<String, Object> settings) {
        Map<String, Object> requested = settings == null ? Map.of() : settings;
        TapstateCatalog catalogSnapshot = catalog.get();
        List<Path> secretPaths = secretPaths(catalogSnapshot, connectorId);
        validateSuppliedSecrets(requested, secretPaths);

        SourceResource source = artifactLookup.apply(connectionId)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .filter(candidate -> candidate.connector().equals(connectorId))
                .orElse(null);

        Map<String, Object> resolved = new LinkedHashMap<>(requested);
        if (source != null) {
            for (Path secret : secretPaths) {
                Object savedValue = secret.value(source.config());
                if (!secret.contains(resolved) && savedValue != null
                        && !secret.put(resolved, savedValue)) {
                    throw malformed("config." + parentPath(secret) + " must be an object");
                }
            }
            Object savedUri = source.config().get("uri");
            Object suppliedUri = resolved.get("uri");
            if ("mongodb-atlas".equals(source.connector())) {
                if (!resolved.containsKey("uri") && savedUri instanceof String) {
                    resolved.put("uri", savedUri);
                }
            }
            if (suppliedUri instanceof String displayed
                    && MongoUriUserInfo.isRedactedDisplay(displayed)) {
                if (!(savedUri instanceof String storedUri)
                        || !displayed.equals(MongoUriUserInfo.redact(storedUri))) {
                    throw malformed("a redacted URI cannot address a different connection");
                }
                resolved.put("uri", savedUri);
            }
        }

        ConnectionConfig config = new ConnectionConfig(
                source == null ? connectionId : source.id(), connectorId, resolved);
        return new ResolvedConnection(config, secretValues(secretPaths, config.settings()), catalogSnapshot);
    }

    /** Resolves the connection config without exposing the credential snapshot to older callers. */
    public ConnectionConfig resolve(String connectionId, String connectorId, Map<String, Object> settings) {
        return resolveWithSecrets(connectionId, connectorId, settings).config();
    }

    private static List<Path> secretPaths(TapstateCatalog catalog, String connectorId) {
        try {
            ConnectorCatalogEntry connector = catalog.byId(connectorId);
            return SecretConfigPaths.forConnector(connector);
        } catch (IllegalArgumentException unavailable) {
            return SecretConfigPaths.shared();
        }
    }

    private static void validateSuppliedSecrets(Map<String, Object> settings, List<Path> secretPaths) {
        for (Path secret : secretPaths) {
            if (secret.contains(settings) && secret.value(settings) == null) {
                throw malformed("a supplied secret cannot be null: " + secret.name());
            }
        }
    }

    private static List<String> secretValues(List<Path> secretPaths, Map<String, Object> settings) {
        Set<String> values = new LinkedHashSet<>();
        for (Path secret : secretPaths) {
            collectStrings(secret.value(settings), values);
        }
        Object uri = settings.get("uri");
        if (uri instanceof String text) {
            values.addAll(MongoUriUserInfo.secretValues(text));
        }
        return List.copyOf(values);
    }

    private static void collectStrings(Object value, Set<String> values) {
        if (value instanceof String text && !text.isBlank()) {
            values.add(text);
        } else if (value instanceof Map<?, ?> map) {
            map.values().forEach(child -> collectStrings(child, values));
        } else if (value instanceof Iterable<?> iterable) {
            iterable.forEach(child -> collectStrings(child, values));
        }
    }

    private static String parentPath(Path path) {
        return String.join(".", path.segments().subList(0, path.segments().size() - 1));
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }

    /** The connection config and the credentials needed to redact its diagnostics. */
    record ResolvedConnection(
            ConnectionConfig config, List<String> secretValues, TapstateCatalog catalogSnapshot) {

        public ResolvedConnection {
            Objects.requireNonNull(config, "config");
            secretValues = secretValues == null ? List.of() : List.copyOf(secretValues);
            Objects.requireNonNull(catalogSnapshot, "catalogSnapshot");
        }
    }
}
