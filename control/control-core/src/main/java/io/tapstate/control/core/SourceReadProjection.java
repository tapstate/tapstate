package io.tapstate.control.core;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Makes generic Source reads safe to display without modifying the authoritative resource. */
final class SourceReadProjection {

    static final String WITHHELD = "<redacted-source>";

    private final Supplier<TapstateCatalog> catalog;
    private final CanonicalWriter writer = new CanonicalWriter();

    SourceReadProjection(Supplier<TapstateCatalog> catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    String canonicalForRead(SourceResource source, String originalCanonical) {
        return canonicalForRead(source, originalCanonical, catalogSnapshot());
    }

    TapstateCatalog catalogSnapshot() {
        try {
            return catalog.get();
        } catch (RuntimeException unavailable) {
            return null;
        }
    }

    String canonicalForRead(SourceResource source, String originalCanonical, TapstateCatalog snapshot) {
        if (snapshot == null) {
            return WITHHELD;
        }
        try {
            ConnectorCatalogEntry connector = snapshot.byId(source.connector());
            Map<String, Object> config = new LinkedHashMap<>(source.config());
            Set<String> knownTopLevel = connector.config().stream()
                    .map(field -> field.name().split("\\.", 2)[0])
                    .collect(Collectors.toSet());
            if (!knownTopLevel.containsAll(config.keySet())) {
                return WITHHELD;
            }
            // A catalog entry names a top-level field but does not certify arbitrary nested values.
            // Exposing such a value would make a misspelled nested secret look safely classified.
            if (config.values().stream().anyMatch(value -> value instanceof Map<?, ?> || value instanceof List<?>)) {
                return WITHHELD;
            }
            boolean changed = false;
            for (ConfigField field : connector.config()) {
                if (field.secret()) {
                    changed |= maskDeclaredSecret(config, field.name());
                }
            }
            changed |= maskCredentialShapedValues(config);
            if (!changed) {
                return originalCanonical;
            }
            SourceResource display = new SourceResource(
                    source.id(), source.metadata(), source.connector(), config,
                    source.mode(), source.tables(), source.srs(), source.experimental());
            return writer.write(display);
        } catch (RuntimeException unsafeProjection) {
            // Connector metadata and malformed stored values are not permission to return raw credentials.
            return WITHHELD;
        }
    }

    static boolean containsDisplayMarker(Object value) {
        if (value instanceof String text) {
            return MongoUriUserInfo.REDACTED.equals(text)
                    || MongoUriUserInfo.isRedactedDisplay(text);
        }
        if (value instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(SourceReadProjection::containsDisplayMarker);
        }
        if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                if (containsDisplayMarker(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean maskDeclaredSecret(Map<String, Object> config, String name) {
        if (config.get(name) == null) {
            return false;
        }
        config.put(name, MongoUriUserInfo.REDACTED);
        return true;
    }

    private static boolean maskCredentialShapedValues(Map<String, Object> config) {
        boolean changed = false;
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String name = entry.getKey().toLowerCase(Locale.ROOT);
            Object value = entry.getValue();
            if (value == null || MongoUriUserInfo.REDACTED.equals(value)) {
                continue;
            }
            if (looksSecret(name)) {
                entry.setValue(MongoUriUserInfo.REDACTED);
                changed = true;
            } else if (value instanceof String text && text.indexOf('@') >= 0
                    && (text.contains("://") || looksLikeUri(name))) {
                String redacted = MongoUriUserInfo.redact(text);
                if (!redacted.equals(text)) {
                    entry.setValue(redacted);
                    changed = true;
                }
            }
        }
        return changed;
    }

    private static boolean looksSecret(String name) {
        return name.contains("password") || name.contains("secret") || name.contains("token")
                || name.contains("credential") || name.endsWith("apikey") || name.endsWith("privatekey");
    }

    private static boolean looksLikeUri(String name) {
        return name.contains("uri") || name.contains("url") || name.contains("dsn")
                || name.contains("connectionstring");
    }

}
