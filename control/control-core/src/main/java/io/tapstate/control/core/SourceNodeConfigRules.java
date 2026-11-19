package io.tapstate.control.core;

import io.tapstate.core.catalog.ConnectorForm;
import io.tapstate.core.catalog.SecretConfigPaths;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;

import java.util.LinkedHashMap;
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
        for (String name : overrides.keySet()) {
            Map<?, ?> item = declared.get(name);
            if (item == null) {
                throw malformed("source node settings contain an undeclared field");
            }
            if (isCredential(connector, name, item)) {
                throw malformed("source node settings cannot include connector credentials");
            }
        }
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
