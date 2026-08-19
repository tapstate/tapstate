package io.tapstate.app;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Keeps the process-wide log redactor aligned with the Sources in the authoritative artifact store.
 * Mutation methods update the registry only after the delegated write succeeds.
 */
final class SecretTrackingArtifactStore implements ArtifactStore {

    private final ArtifactStore delegate;
    private final Supplier<TapstateCatalog> catalog;
    private final SecretRedactor redactor;

    SecretTrackingArtifactStore(
            ArtifactStore delegate, Supplier<TapstateCatalog> catalog, SecretRedactor redactor) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.redactor = Objects.requireNonNull(redactor, "redactor");
        delegate.list().forEach(this::track);
    }

    @Override
    public synchronized ArtifactMutation create(Resource artifact) {
        ArtifactMutation result = delegate.create(artifact);
        if (result == ArtifactMutation.CREATED) {
            track(artifact);
        }
        return result;
    }

    @Override
    public synchronized ArtifactMutation replace(
            String id, String expectedContentHash, Resource replacement) {
        ArtifactMutation result = delegate.replace(id, expectedContentHash, replacement);
        if (result == ArtifactMutation.REPLACED) {
            track(replacement);
        }
        return result;
    }

    @Override
    public synchronized ArtifactMutation delete(String id, String expectedContentHash) {
        ArtifactMutation result = delegate.delete(id, expectedContentHash);
        if (result == ArtifactMutation.DELETED) {
            redactor.remove(id);
        }
        return result;
    }

    @Override
    public synchronized ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
        ArtifactBatchWrite result = delegate.writeAll(writes);
        if (result.appliedSuccessfully()) {
            writes.forEach(write -> track(write.resource()));
        }
        return result;
    }

    @Override
    public synchronized void saveAll(List<Resource> artifacts) {
        delegate.saveAll(artifacts);
        artifacts.forEach(this::track);
    }

    @Override
    public synchronized Optional<String> saveAll(
            List<Resource> artifacts, Map<String, String> expectedContentHashes) {
        Optional<String> conflicted = delegate.saveAll(artifacts, expectedContentHashes);
        // A refused batch wrote nothing, so nothing it named is in the store to track — tracking it
        // would teach the redactor secrets that were never stored.
        if (conflicted.isEmpty()) {
            artifacts.forEach(this::track);
        }
        return conflicted;
    }

    @Override
    public Optional<Resource> get(String id) {
        return delegate.get(id);
    }

    @Override
    public List<Resource> list() {
        return delegate.list();
    }

    private void track(Resource resource) {
        if (!(resource instanceof SourceResource source)) {
            redactor.remove(resource.id());
            return;
        }
        redactor.replace(source.id(), secretValues(source));
    }

    private Collection<String> secretValues(SourceResource source) {
        ConnectorCatalogEntry connector;
        try {
            connector = catalog.get().byId(source.connector());
        } catch (IllegalArgumentException unavailable) {
            return namedSecretValues(source.config());
        }

        List<String> secrets = new ArrayList<>();
        connector.config().stream()
                .filter(ConfigField::secret)
                .map(field -> configValue(source.config(), field.name()))
                .filter(Objects::nonNull)
                .forEach(value -> collectScalars(value, secrets));
        return secrets;
    }

    private static Object configValue(Map<String, Object> config, String name) {
        if (config.containsKey(name)) {
            return config.get(name);
        }
        Object current = config;
        for (String part : name.split("\\.")) {
            if (!(current instanceof Map<?, ?> map) || !map.containsKey(part)) {
                return null;
            }
            current = map.get(part);
        }
        return current;
    }

    private static Collection<String> namedSecretValues(Object value) {
        List<String> values = new ArrayList<>();
        collectNamedSecretValues(value, null, values);
        return values;
    }

    private static void collectNamedSecretValues(
            Object value, String fieldName, Collection<String> target) {
        if (value instanceof Map<?, ?> map) {
            map.forEach((name, child) -> collectNamedSecretValues(child, String.valueOf(name), target));
        } else if (value instanceof Iterable<?> iterable) {
            iterable.forEach(child -> collectNamedSecretValues(child, fieldName, target));
        } else if (value != null && fieldName != null && isLikelySecretField(fieldName)) {
            target.add(String.valueOf(value));
        }
    }

    private static boolean isLikelySecretField(String fieldName) {
        String normalized = fieldName.toLowerCase(Locale.ROOT);
        return normalized.contains("password")
                || normalized.contains("secret")
                || normalized.contains("token")
                || normalized.contains("credential")
                || normalized.equals("key")
                || normalized.endsWith("key")
                || normalized.contains("uri")
                || normalized.contains("connectionstring");
    }

    private static void collectScalars(Object value, Collection<String> target) {
        if (value instanceof Map<?, ?> map) {
            map.values().forEach(child -> collectScalars(child, target));
        } else if (value instanceof Iterable<?> iterable) {
            iterable.forEach(child -> collectScalars(child, target));
        } else if (value != null) {
            target.add(String.valueOf(value));
        }
    }
}
