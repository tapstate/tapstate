package io.tapstate.control.core;

import io.tapstate.core.catalog.ConnectorForm;
import io.tapstate.core.catalog.SecretConfigPaths;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Admits Pipeline overrides against the same node form PDK uses at runtime. */
final class SourceNodeConfigRules {

    private SourceNodeConfigRules() {
    }

    static void validate(SourceResource source, Map<String, Object> overrides,
            Function<String, ConnectorDetail> connectors) {
        if (source == null) {
            throw malformed("source node settings require an existing Source");
        }
        ConnectorDetail connector = connectors.apply(source.connector());
        String spec = connector == null || connector.spec() == null ? null : connector.spec().text();
        if (spec == null || !(JsonReader.parse(spec) instanceof Map<?, ?> root)) {
            throw malformed("the connector specification is required for Source node settings");
        }
        Map<String, Map<?, ?>> declared = ConnectorForm.NODE.itemsIn(root);
        Map<String, Map<?, ?>> editable = ConnectorForm.NODE.itemsIn(root, (name, item) ->
                !isCredential(connector, name, item) && !hasCredentialChild(connector, item));
        for (String name : overrides.keySet()) {
            Map<?, ?> item = declared.get(name);
            if (item == null) {
                throw malformed("source node settings contain an undeclared field");
            }
            if (editable.get(name) != item) {
                throw malformed("source node settings cannot include connector credentials");
            }
        }
    }

    private static boolean hasCredentialChild(ConnectorDetail connector, Map<?, ?> item) {
        for (String key : List.of("items", "prefixItems", "additionalItems", "additionalProperties",
                "contains", "allOf", "anyOf", "oneOf", "not", "if", "then", "else")) {
            if (containsCredential(connector, "", item.get(key))) return true;
        }
        for (String key : List.of("patternProperties", "dependentSchemas")) {
            if (item.get(key) instanceof Map<?, ?> properties && hasCredentialProperty(connector, properties)) {
                return true;
            }
        }
        // The outer properties of a group are validated individually as flattened fields.
        return "array".equals(item.get("type")) && item.get("properties") instanceof Map<?, ?> properties
                && hasCredentialProperty(connector, properties);
    }

    private static boolean hasCredentialProperty(ConnectorDetail connector, Map<?, ?> properties) {
        return properties.entrySet().stream().anyMatch(
                entry -> containsCredential(connector, String.valueOf(entry.getKey()), entry.getValue()));
    }

    private static boolean containsCredential(ConnectorDetail connector, String name, Object value) {
        if (value instanceof List<?> schemas) {
            return schemas.stream().anyMatch(schema -> containsCredential(connector, "", schema));
        }
        return value instanceof Map<?, ?> item && (isCredential(connector, name, item)
                || (item.get("properties") instanceof Map<?, ?> properties && hasCredentialProperty(connector, properties))
                || hasCredentialChild(connector, item));
    }

    private static boolean isCredential(ConnectorDetail connector, String name, Map<?, ?> item) {
        Map<String, Object> definition = new LinkedHashMap<>();
        item.forEach((key, value) -> definition.put(String.valueOf(key), value));
        return "__TAPDATA".equals(name) || "password".equals(item.get("format"))
                || Boolean.TRUE.equals(item.get("writeOnly")) || Boolean.TRUE.equals(item.get("secret"))
                || SecretConfigPaths.isSecretField(name, definition)
                || connector.config().stream().anyMatch(field -> field.name().equals(name) && field.secret());
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }
}
