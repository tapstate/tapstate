package io.tapstate.core.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Canonical config paths that carry credentials across Source and connection operations. */
public final class SecretConfigPaths {

    private static final List<Path> SHARED = List.of(
            Path.field("sslKey", false),
            Path.field("sslKeyPassword", false),
            new Path("__TAPDATA.sshPassword", List.of("__TAPDATA", "sshPassword"), false));

    private SecretConfigPaths() {
    }

    /** Secret fields declared by a connector plus the shared TLS and SSH credentials. */
    public static List<Path> forConnector(ConnectorCatalogEntry connector) {
        Objects.requireNonNull(connector, "connector");
        Map<String, Path> paths = new LinkedHashMap<>();
        connector.config().stream()
                .filter(ConfigField::secret)
                .map(field -> Path.field(field.name(), field.required()))
                .forEach(path -> paths.put(path.name(), path));
        SHARED.forEach(path -> paths.putIfAbsent(path.name(), path));
        return List.copyOf(paths.values());
    }

    /** Shared injected transport credentials, independent of connector catalog contents. */
    public static List<Path> shared() {
        return SHARED;
    }

    /** Whether a raw connector spec field carries a credential. */
    public static boolean isSecretField(String name, Map<String, Object> definition) {
        String component = definition.get("x-component") instanceof String value ? value : null;
        return "Password".equals(component)
                || SHARED.stream().anyMatch(path -> path.segments().size() == 1
                        && path.segments().getFirst().equals(name))
                || ("TextFileReader".equals(component) && isCredentialFileField(name));
    }

    private static boolean isCredentialFileField(String name) {
        String normalized = name.replaceAll("([a-z0-9])([A-Z])", "$1 $2").toLowerCase(Locale.ROOT);
        for (String word : normalized.split("[^a-z0-9]+")) {
            if (word.equals("key") || word.equals("keyfile") || word.equals("keytab")
                    || word.equals("keystore") || word.equals("password") || word.equals("secret")
                    || word.equals("token") || word.startsWith("credential")) {
                return true;
            }
        }
        return false;
    }

    /** A credential path in the Source config tree. Catalog field names remain literal top-level keys. */
    public record Path(String name, List<String> segments, boolean required) {

        public Path {
            Objects.requireNonNull(name, "name");
            segments = List.copyOf(segments);
            if (segments.isEmpty()) {
                throw new IllegalArgumentException("a config path must have at least one segment");
            }
        }

        public static Path field(String name, boolean required) {
            return new Path(name, List.of(name), required);
        }

        public boolean contains(Map<String, Object> config) {
            Object current = config;
            for (String segment : segments) {
                if (!(current instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return false;
                }
                current = map.get(segment);
            }
            return true;
        }

        public Object value(Map<String, Object> config) {
            Object current = config;
            for (String segment : segments) {
                if (!(current instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return null;
                }
                current = map.get(segment);
            }
            return current;
        }

        /** Copies nested maps before writing. Returns false when an existing parent is not an object. */
        public boolean put(Map<String, Object> config, Object value) {
            Map<String, Object> current = config;
            for (int index = 0; index < segments.size() - 1; index++) {
                String segment = segments.get(index);
                Object existing = current.get(segment);
                if (current.containsKey(segment) && !(existing instanceof Map<?, ?>)) {
                    return false;
                }
                Map<String, Object> nested = existing instanceof Map<?, ?> map
                        ? copyStringMap(map)
                        : new LinkedHashMap<>();
                current.put(segment, nested);
                current = nested;
            }
            current.put(segments.getLast(), value);
            return true;
        }

        /** Removes the path while preserving sibling fields and existing parent objects. */
        public void remove(Map<String, Object> config) {
            remove(config, 0);
        }

        private boolean remove(Map<String, Object> config, int index) {
            String segment = segments.get(index);
            if (index == segments.size() - 1) {
                if (!config.containsKey(segment)) {
                    return false;
                }
                config.remove(segment);
                return true;
            }
            Object existing = config.get(segment);
            if (!(existing instanceof Map<?, ?> map)) {
                return false;
            }
            Map<String, Object> nested = copyStringMap(map);
            if (!remove(nested, index + 1)) {
                return false;
            }
            config.put(segment, nested);
            return true;
        }

        private static Map<String, Object> copyStringMap(Map<?, ?> source) {
            Map<String, Object> copy = new LinkedHashMap<>();
            source.forEach((key, value) -> {
                if (key instanceof String name) {
                    copy.put(name, value);
                }
            });
            return copy;
        }
    }
}
