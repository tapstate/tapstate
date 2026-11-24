package io.tapstate.app;

import io.tapstate.core.common.JsonReader;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/** The product support policy supplied with the server and refreshed by administrators. */
record ConnectorSupportManifest(long version, Map<String, Entry> entries) {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]*");
    private static final Pattern ENGINE_VERSION = Pattern.compile("[0-9]+(\\.[0-9]+){2}");
    private static final List<String> ROLES = List.of("source", "target");

    record Entry(String id, String name, List<String> roles, String minimumTapstateVersion,
                 String repository, String artifactId, boolean loaderRetirementEligible) {
        boolean supportsRole(String role) {
            return roles.contains(role);
        }
    }

    static ConnectorSupportManifest read(String json) throws IOException {
        try {
            if (!(JsonReader.parse(json) instanceof Map<?, ?> root)
                    || !Long.valueOf(1).equals(root.get("schemaVersion"))
                    || !(root.get("version") instanceof Long version) || version < 1
                    || !(root.get("connectors") instanceof List<?> connectors) || connectors.isEmpty()) {
                throw new IllegalArgumentException("Invalid connector support manifest");
            }
            var entries = new LinkedHashMap<String, Entry>();
            for (Object value : connectors) {
                if (!(value instanceof Map<?, ?> row) || !(row.get("roles") instanceof List<?> roles)
                        || roles.isEmpty() || !ROLES.containsAll(roles)) {
                    throw new IllegalArgumentException("Invalid support manifest connector or roles");
                }
                Entry entry = new Entry(text(row, "id", ID), text(row, "name", null),
                        roles.stream().map(String.class::cast).distinct().toList(),
                        text(row, "minimumTapstateVersion", ENGINE_VERSION), text(row, "repository", ID),
                        text(row, "artifactId", ID), retirementEligibility(row));
                if (entries.putIfAbsent(entry.id(), entry) != null) {
                    throw new IllegalArgumentException("Duplicate connector in support manifest");
                }
            }
            return new ConnectorSupportManifest(version, java.util.Collections.unmodifiableMap(entries));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid connector support manifest", invalid);
        }
    }

    Optional<Entry> admitted(String id, String engineVersion) {
        Entry entry = entries.get(id);
        if (entry == null || MavenConnectorCatalog.compareVersions(
                engineVersion.split("-", 2)[0], entry.minimumTapstateVersion()) < 0) {
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    private static boolean retirementEligibility(Map<?, ?> row) {
        String key = "loaderRetirementEligible";
        if (!row.containsKey(key)) return false;
        if (row.get(key) instanceof Boolean eligible) return eligible;
        throw new IllegalArgumentException("Invalid support manifest field: " + key);
    }

    private static String text(Map<?, ?> row, String key, Pattern pattern) {
        if (row.get(key) instanceof String text && !text.isBlank()
                && (pattern == null || pattern.matcher(text).matches())) {
            return text;
        }
        throw new IllegalArgumentException("Invalid support manifest field: " + key);
    }
}
